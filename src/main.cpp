/**
 * Converged Binary - Unified EloqKV and EloqSQL Server
 *
 * Initialization order (critical for mutex dependencies):
 * 1. Start MySQL main thread
 * 2. MySQL performs basic initialization (mutexes, thread-specific memory)
 * 3. Wait for MySQL basic init complete signal
 * 4. Initialize data substrate (shared by all engines)
 * 5. Signal data substrate init complete
 * 6. MySQL continues with rest of server initialization
 * 7. Start EloqKV server
 */

#include <atomic>
#include <chrono>
#include <csignal>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <iostream>
#include <memory>
#include <thread>

#include "data_substrate.h"

#ifdef ELOQ_MODULE_ELOQKV
#include "redis_service.h"
#include <brpc/server.h>
#endif
#if BRPC_WITH_GLOG
#include "glog_error_logging.h"
#endif
// Command line flags
DEFINE_string(config, "", "Path to data substrate configuration file");
DEFINE_string(eloqkv_config, "",
              "Path to EloqKV configuration file (optional)");
DEFINE_string(eloqsql_config, "",
              "Path to EloqSQL configuration file (optional)");

constexpr char VERSION[] = "1.0.0";

// Global state for signal handling
std::atomic<bool> g_shutdown_requested{false};

// State tracking for cleanup
struct InitState {
  bool data_substrate_init = false;
  bool eloqkv_init = false;
  bool eloqsql_thread_started = false;
  EloqKV::RedisServiceImpl* eloqkv_service_ptr = nullptr; // Track before release()
} g_init_state;

#ifdef ELOQ_MODULE_ELOQKV
std::unique_ptr<brpc::Server> g_eloqkv_server;
std::unique_ptr<EloqKV::RedisServiceImpl> g_eloqkv_service;
extern std::string EloqKV::redis_ip_port;
extern brpc::Acceptor *EloqKV::server_acceptor;
#endif

#ifdef ELOQ_MODULE_ELOQSQL
std::thread g_eloqsql_thread;
extern int mysqld_main(int argc, char **argv);
extern void shutdown_mysqld();
#endif

// Forward declaration
void CleanupComponents();

void SignalHandler(int signal) {
  bool expected = false;
  if (!g_shutdown_requested.compare_exchange_strong(expected, true)) {
    // Already shutting down, ignore this signal
    return;
  }
  LOG(INFO) << "Received signal " << signal << ", initiating shutdown...";
  
  // Use CleanupComponents() which is idempotent and checks state flags
  CleanupComponents();
  
  LOG(INFO) << "Shutdown complete";
}

void CleanupComponents() {
  // Cleanup order: DataSubstrate → EloqKV → EloqSQL → Google Logging
  // Note: DataSubstrate is cleaned up first as requested, even though engines depend on it.
  // This assumes engines can handle DataSubstrate being shut down (they should stop accepting requests first).
  // DataSubstrate cleanup (only if Init() succeeded)
  if (g_init_state.data_substrate_init) {
    LOG(INFO) << "Shutting down DataSubstrate";
    DataSubstrate::Instance().Shutdown();
    LOG(INFO) << "DataSubstrate shut down";
    g_init_state.data_substrate_init = false;
  }
  
#ifdef ELOQ_MODULE_ELOQKV
  // EloqKV cleanup
  if (g_init_state.eloqkv_init && g_init_state.eloqkv_service_ptr) {
    // Only stop service if server didn't start (we still own it)
    LOG(INFO) << "Stopping EloqKV service";
    g_init_state.eloqkv_service_ptr->Stop();
    LOG(INFO) << "EloqKV service stopped";
    g_init_state.eloqkv_init = false;
  }
  
  // Reset pointers (safe even if already reset)
  g_eloqkv_service.reset();
  g_eloqkv_server.reset();
  g_init_state.eloqkv_service_ptr = nullptr;
  g_init_state.eloqkv_init = false;
#endif

#ifdef ELOQ_MODULE_ELOQSQL
  // EloqSQL cleanup
  if (g_init_state.eloqsql_thread_started) {
    LOG(INFO) << "Shutting down EloqSQL server";
    shutdown_mysqld();
    
    if (g_eloqsql_thread.joinable()) {
      LOG(INFO) << "Joining EloqSQL thread";
      g_eloqsql_thread.join();
      LOG(INFO) << "EloqSQL thread joined";
    }
    g_init_state.eloqsql_thread_started = false;
  }
#endif
}

int main(int argc, char *argv[]) {
  google::SetVersionString(VERSION);
  google::ParseCommandLineFlags(&argc, &argv, true);
#if BRPC_WITH_GLOG
  InitGoogleLogging(argv);
#endif

  std::cout << "======================================" << std::endl;
  std::cout << "EloqDB Database Server v" << VERSION << std::endl;
  std::cout << "======================================" << std::endl;

  // Install signal handlers
  std::signal(SIGINT, SignalHandler);
  std::signal(SIGTERM, SignalHandler);

  int return_code = 0;

  // Step 1: Always initialize DataSubstrate first
  std::cout << "Initializing data substrate..." << std::endl;
  if (!DataSubstrate::Init(FLAGS_config)) {
    LOG(ERROR) << "Failed to initialize DataSubstrate";
    google::ShutdownGoogleLogging();
    return -1;
  }
  g_init_state.data_substrate_init = true;
  LOG(INFO) << "Data substrate initialized (config loaded)";
  std::cout << "Data substrate initialized" << std::endl;

  auto &ds = DataSubstrate::Instance();

  // Step 2: Depending on ELOQ_MODULE_* macros, mark enabled engines and start
  // their init
#ifdef ELOQ_MODULE_ELOQSQL
  // EloqSQL engine:
  // - Enable EloqSQL engine in DataSubstrate so main will wait for its
  // registration.
  // - Start MySQL main thread; EloqSQL will call RegisterEngine(EloqSql, ...)
  //   from within its own initialization code in ha_eloq.cc.
  ds.EnableEngine(txservice::TableEngine::EloqSql);

  std::cout << "Starting EloqSQL initialization..." << std::endl;
  LOG(INFO) << "Launching EloqSQL main thread";

  g_eloqsql_thread = std::thread([argc, argv]() {
    int result = mysqld_main(argc, argv);
    if (result != 0) {
      LOG(ERROR) << "EloqSQL server exited with error: " << result;
    }
  });
  g_init_state.eloqsql_thread_started = true;
#endif

#ifdef ELOQ_MODULE_ELOQKV
  // EloqKV engine:
  // - Enable EloqKV engine in DataSubstrate so main will wait for its
  // registration.
  // - EloqKV brpc server startup (in main or a helper) must:
  //   - Construct RedisServiceImpl with the config path
  //   - Call RedisServiceImpl::Init(), which will call RegisterEngine(EloqKv,
  //   ...)
  //     as defined in the dedicated engine-registration plan.
  ds.EnableEngine(txservice::TableEngine::EloqKv);
  std::cout << "Starting EloqKV server..." << std::endl;

  std::string eloqkv_config =
      FLAGS_eloqkv_config.empty() ? FLAGS_config : FLAGS_eloqkv_config;

  g_eloqkv_server = std::make_unique<brpc::Server>();
  g_eloqkv_service =
      std::make_unique<EloqKV::RedisServiceImpl>(eloqkv_config, VERSION);
  EloqKV::RedisServiceImpl *eloqkv_service_ptr = g_eloqkv_service.get();
  brpc::ServerOptions eloqkv_options;
  std::string n_bthreads;
  GFLAGS_NAMESPACE::GetCommandLineOption("bthread_concurrency", &n_bthreads);

  if (!g_eloqkv_service->Init(*g_eloqkv_server)) {
    LOG(ERROR) << "Failed to initialize EloqKV service";
    return_code = -1;
    goto cleanup;
  }
  g_init_state.eloqkv_init = true;
  g_init_state.eloqkv_service_ptr = eloqkv_service_ptr;
#endif

#ifdef ELOQ_MODULE_ELOQDOC
  // TODO: Enable EloqDoc engine and start its initialization
  // ds.EnableEngine(txservice::TableEngine::EloqDoc);
  // Start EloqDoc init, which will call RegisterEngine(EloqDoc, ...) when
  // ready.
#endif

  // Step 3: Main thread waits for all enabled engines to finish initialization
  // and register
  std::cout << "Waiting for enabled engines to register..." << std::endl;
  if (!ds.WaitForEnabledEnginesRegistered(
          std::chrono::milliseconds(600000))) { // e.g., 10 min timeout
    LOG(ERROR) << "Timed out waiting for engines to register";
    return_code = -1;
    goto cleanup;
  }
  LOG(INFO) << "All enabled engines registered successfully";

  // Step 4: Start DataSubstrate and notify engines
  std::cout << "Starting data substrate services..." << std::endl;
  if (!ds.Start()) {
    LOG(ERROR) << "Failed to start DataSubstrate";
    return_code = -1;
    goto cleanup;
  }
  LOG(INFO) << "Data substrate started successfully";
  std::cout << "Data substrate started" << std::endl;

  // Step 5 (engine side, not shown here):
  // - Engines call WaitForDataSubstrateStarted() before entering serve loop
  // - Ensures engines only start serving after DataSubstrate::Start() completes

#ifdef ELOQ_MODULE_ELOQKV
  // EloqKV: complete second-phase startup now that DataSubstrate has started.
  if (!eloqkv_service_ptr->Start(*g_eloqkv_server)) {
    LOG(ERROR) << "Failed to start EloqKV service (second phase)";
    return_code = -1;
    goto cleanup;
  }

  // Start EloqKV server (after data substrate is initialized)
  // Track pointer before release (release transfers ownership to brpc)
  g_init_state.eloqkv_service_ptr = eloqkv_service_ptr;
  eloqkv_options.num_threads = std::stoi(n_bthreads);
  eloqkv_options.redis_service = g_eloqkv_service.release();
  eloqkv_options.has_builtin_services = false;

  if (g_eloqkv_server->Start(EloqKV::redis_ip_port.c_str(), &eloqkv_options) !=
      0) {
    // TODO(liunyl): notify EloqSQL to shutdown
    LOG(ERROR) << "Failed to start EloqKV server";
    return_code = -1;
    goto cleanup;
  }

  LOG(INFO) << "EloqKV server started on " << EloqKV::redis_ip_port;
  std::cout << "EloqKV server listening on " << EloqKV::redis_ip_port
            << std::endl;
  EloqKV::server_acceptor = g_eloqkv_server->GetAcceptor();
#endif

  std::cout << "======================================" << std::endl;
  std::cout << "All servers started successfully" << std::endl;
  std::cout << "Press Ctrl+C to shutdown" << std::endl;
  std::cout << "======================================" << std::endl;

  // Wait for shutdown signal
#ifdef ELOQ_MODULE_ELOQKV
  if (g_eloqkv_server) {
    g_eloqkv_server->RunUntilAskedToQuit();
  }
#endif

cleanup:
  CleanupComponents();
#if BRPC_WITH_GLOG
  // Google logging cleanup (always safe to call, but only once)
  google::ShutdownGoogleLogging();
#endif
  return return_code;
}
