#include <string>
#include <iostream>
#include <fstream>
#include <sstream>

#include <grpc++/grpc++.h>
#include <hiredis/hiredis.h>

#define RAY_VERBOSE -1
#define RAY_DEBUG 0
#define RAY_INFO 1
#define RAY_WARNING 2
#define RAY_ERROR 3
#define RAY_FATAL 4
#define RAY_REFCOUNT RAY_VERBOSE
#define RAY_ALIAS RAY_VERBOSE

// Entity types.
#define RAY_FUNCTION "FUNCTION"
#define RAY_OBJECT "OBJECT"
#define RAY_TASK "TASK"

static const char* log_levels[5] = {"DEBUG", "INFO", "WARN", "ERROR", "FATAL"};

struct RayConfig {
  bool log_to_file = false;
  bool log_to_redis = false;
  int logging_level = RAY_DEBUG;
  const char* origin_type;
  const char* address;
  std::ofstream logfile;
  redisContext* redis;
};

extern RayConfig global_ray_config;

#ifdef _MSC_VER
extern "C" __declspec(dllimport) int __stdcall IsDebuggerPresent();
#define RAY_BREAK_IF_DEBUGGING() IsDebuggerPresent() && (__debugbreak(), 1)
#else
#define RAY_BREAK_IF_DEBUGGING()
#endif


static inline void init_redis_log(RayConfig& ray_config,
                                  const char* origin_type,
                                  const char* address) {
  ray_config.log_to_redis = true;
  ray_config.origin_type = origin_type;
  ray_config.address = address;
  ray_config.redis = redisConnect("127.0.0.1", 6379);
}

static inline void redis_log(RayConfig& ray_config,
                             int log_level,
                             const char* entity_type,
                             const char* entity_id,
                             const char* event_type,
                             const char* message,
                             const char* related_entity_ids) {
  if (log_level < RAY_DEBUG || log_level > RAY_FATAL) {
    return;
  }
  if (ray_config.log_to_redis && log_level >= ray_config.logging_level) {
    std::stringstream timestamp_ss;
    struct timeval tv;

    gettimeofday(&tv, NULL);
    timestamp_ss << tv.tv_sec << "." << tv.tv_usec;
    redisCommand(global_ray_config.redis,
                 "HMSET log:%s:%s:%s log_level %s entity_type %s entity_id %s related_entity_ids %s event_type %s message %s timestamp %s",
                 global_ray_config.origin_type,
                 global_ray_config.address,
                 timestamp_ss.str().c_str(),
                 log_levels[log_level],
                 entity_type,
                 entity_id,
                 related_entity_ids,
                 event_type,
                 message,
                 timestamp_ss.str().c_str());
  }
}

template<class T = int>
static inline void ray_log(int log_level,
                           const char* entity_type,
                           const char* entity_id,
                           const char* event_type,
                           const char* message="",
                           const std::vector<T> related_entity_ids=std::vector<T>()) {
  if (log_level < global_ray_config.logging_level) {
    return;
  }
  if (global_ray_config.log_to_redis) {
    std::stringstream ss;
    for (size_t i = 0; i < related_entity_ids.size(); ++i) {
      if (i > 0) {
        ss << ",";
      }
      ss << related_entity_ids[i];
    }
    redis_log(global_ray_config, log_level, entity_type, entity_id, event_type, message, ss.str().c_str());
  }
  if (log_level == RAY_FATAL) {
    std::cerr << "fatal error occured: " << message << std::endl;
    if (global_ray_config.log_to_file) {
      global_ray_config.logfile << "fatal error occured: " << message << std::endl;
    }
    RAY_BREAK_IF_DEBUGGING();
    std::exit(1);
  } else if (log_level == RAY_DEBUG) {
  } else {
    if (global_ray_config.log_to_file) {
      global_ray_config.logfile << message << std::endl;
    } else {
      std::cout << message << std::endl;
    }
  }
}


#define RAY_LOG(LEVEL, MESSAGE) \
  if (LEVEL >= global_ray_config.logging_level) { \
    if (LEVEL == RAY_FATAL) { \
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
      } else { \
        std::cout << MESSAGE << std::endl; \
      } \
    } \
  }

#define RAY_CHECK(condition, message) \
  if (!(condition)) {\
     RAY_LOG(RAY_FATAL, "Check failed at line " << __LINE__ << " in " << __FILE__ << ": " << #condition << " with message " << message) \
  }
#define RAY_WARN(condition, message) \
  if (!(condition)) {\
     RAY_LOG(RAY_WARNING, "Check failed at line " << __LINE__ << " in " << __FILE__ << ": " << #condition << " with message " << message) \
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
