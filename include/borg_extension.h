#ifndef BORG_EXTENSION_H
#define BORG_EXTENSION_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define BORG_EXTENSION_ABI_V2 2u

typedef void (*borg_extension_log_fn)(uint32_t level, const char *message);
typedef int32_t (*borg_extension_emit_event_fn)(const char *event_json);

typedef struct borg_extension_host_v2 {
  uint32_t abi_version;
  size_t struct_size;
  const char *extension_id;
  const char *config_json;
  borg_extension_log_fn log;
  borg_extension_emit_event_fn emit_event;
} borg_extension_host_v2;

int32_t borg_extension_init_v2(const borg_extension_host_v2 *host,
                               void **handle);
void borg_extension_shutdown_v2(void *handle);

#ifdef __cplusplus
}
#endif

#endif
