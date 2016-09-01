#include <string>
#include <iostream>
#include <fstream>
#include <sstream>

#include <grpc++/grpc++.h>
#include <hiredis/hiredis.h>

struct RayConfig {
  bool log_to_file = false;
  bool log_to_redis = false;
  const char* origin;
  std::ofstream logfile;
  redisContext* redis;
};

extern RayConfig global_ray_config;

#define RAY_VERBOSE -1
#define RAY_INFO 0
#define RAY_DEBUG 1
#define RAY_FATAL 2
#define RAY_REFCOUNT RAY_VERBOSE
#define RAY_ALIAS RAY_VERBOSE

#ifdef _MSC_VER
extern "C" __declspec(dllimport) int __stdcall IsDebuggerPresent();
#define RAY_BREAK_IF_DEBUGGING() IsDebuggerPresent() && (__debugbreak(), 1)
#else
#define RAY_BREAK_IF_DEBUGGING()
#endif


static const char* log_levels[3] = {"INFO", "DEBUG", "FATAL"};

static inline void redis_log(RayConfig& ray_config,
                             int log_level,
                             const char* entity_type,
                             const char* entity_id,
                             const char* event_type,
                             const char* message) {
  if (ray_config.log_to_redis) {
    struct timeval tv;
    time_t now;
    struct tm *local_now;
    char timestamp[27];

    gettimeofday(&tv, NULL);
    now = tv.tv_sec;
    local_now = localtime(&now);
    strftime(timestamp, sizeof(timestamp), "%F %T", local_now);
    redisCommand(global_ray_config.redis,
                 "HMSET log:%s.%06d log_level %s entity_type %s entity_id %s event_type %s origin %s message %s",
                 timestamp,
                 (int) tv.tv_usec,
                 log_levels[log_level],
                 entity_type,
                 entity_id,
                 event_type,
                 global_ray_config.origin,
                 message);
    std::cout << log_levels[log_level] << std::endl;
  }
}


#define RAY_LOG(LEVEL, MESSAGE) \
  if (LEVEL == RAY_VERBOSE) { \
    \
  } else if (LEVEL == RAY_FATAL) { \
    std::cerr << "fatal error occured: " << MESSAGE << std::endl; \
    if (global_ray_config.log_to_file) { \
      global_ray_config.logfile << "fatal error occured: " << MESSAGE << std::endl; \
    } \
    RAY_BREAK_IF_DEBUGGING();  \
    std::exit(1); \
  } else if (LEVEL == RAY_DEBUG) { \
    \
  } else { \
    if (global_ray_config.log_to_file) { \
      global_ray_config.logfile << MESSAGE << std::endl; \
      std::stringstream RAY_LOG_ss; \
      RAY_LOG_ss << MESSAGE; \
      redis_log(global_ray_config, LEVEL, "", "", "", RAY_LOG_ss.str().c_str()); \
    } else { \
      std::cout << MESSAGE << std::endl; \
    } \
  }

#define RAY_CHECK(condition, message) \
  if (!(condition)) {\
     RAY_LOG(RAY_FATAL, "Check failed at line " << __LINE__ << " in " << __FILE__ << ": " << #condition << " with message " << message) \
  }
#define RAY_WARN(condition, message) \
  if (!(condition)) {\
     RAY_LOG(RAY_INFO, "Check failed at line " << __LINE__ << " in " << __FILE__ << ": " << #condition << " with message " << message) \
  }

#define RAY_CHECK_EQ(var1, var2, message) RAY_CHECK((var1) == (var2), message)
#define RAY_CHECK_NEQ(var1, var2, message) RAY_CHECK((var1) != (var2), message)
#define RAY_CHECK_LE(var1, var2, message) RAY_CHECK((var1) <= (var2), message)
#define RAY_CHECK_LT(var1, var2, message) RAY_CHECK((var1) < (var2), message)
#define RAY_CHECK_GE(var1, var2, message) RAY_CHECK((var1) >= (var2), message)
#define RAY_CHECK_GT(var1, var2, message) RAY_CHECK((var1) > (var2), message)

#define RAY_CHECK_GRPC(expr) \
  do { \
    grpc::Status _s = (expr); \
    RAY_WARN(_s.ok(), "grpc call failed with message " << _s.error_message()); \
  } while (0);
