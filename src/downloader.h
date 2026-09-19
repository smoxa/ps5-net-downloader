#ifndef DOWNLOADER_H
#define DOWNLOADER_H

#include <stdint.h>
#include <stdbool.h>

typedef enum {
    STATUS_IDLE = 0,
    STATUS_CONNECTING,
    STATUS_DOWNLOADING,
    STATUS_COMPLETED,
    STATUS_ERROR
} download_state_t;

typedef struct {
    download_state_t state;
    char url[1024];
    char save_path[512];
    char filename[256];
    uint64_t downloaded_bytes;
    uint64_t total_bytes;
    double speed_bytes_sec;
    uint64_t eta_seconds;
    int num_threads;
    char error_message[256];
} download_status_t;

void downloader_init(void);
int downloader_start(const char *url, const char *save_dir, const char *filename, int threads);
void downloader_abort(void);
void downloader_get_status(download_status_t *out_status);

#endif // DOWNLOADER_H
