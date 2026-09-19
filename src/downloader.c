#include "downloader.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <fcntl.h>
#include <errno.h>

// Sony PS5 native library function declarations
int sceNetInit(void);
int sceNetPoolCreate(const char *name, int size, int flags);
int sceNetPoolDestroy(int memId);

int sceSslInit(size_t poolSize);
int sceSslTerm(int sslCtxId);
int sceSslDisableVerifyOption(int sslCtxId, unsigned int flags);

int sceHttp2Init(int netMemId, int sslCtxId, size_t poolSize, int flags);
int sceHttp2Term(int httpCtxId);
int sceHttp2CreateTemplate(int httpCtxId, const char *userAgent, int httpVer, int autoRedirect);
int sceHttp2DeleteTemplate(int tmplId);
int sceHttp2CreateRequestWithURL(int tmplId, const char *method, const char *url, uint64_t contentLength);
int sceHttp2DeleteRequest(int reqId);
int sceHttp2SendRequest(int reqId, const void *data, size_t size);
int sceHttp2GetStatusCode(int reqId, int *statusCode);
int sceHttp2GetResponseContentLength(int reqId, uint64_t *contentLength);
int sceHttp2GetAllResponseHeaders(int reqId, char **headers, size_t *size);
int sceHttp2ReadData(int reqId, void *buf, size_t size);
int sceHttp2AbortRequest(int reqId);
int sceHttp2AddRequestHeader(int reqId, const char *name, const char *value, int mode);
int sceHttp2SetConnectTimeOut(int id, unsigned int timeout_usec);
int sceHttp2SetRecvTimeOut(int id, unsigned int timeout_usec);
int sceHttp2SetSendTimeOut(int id, unsigned int timeout_usec);
int sceHttp2SslDisableOption(int id, unsigned int flags);

#define CHUNK_BUFFER_SIZE (512 * 1024) // 512 KB read buffer
#define MAX_THREADS 8

typedef struct {
    int thread_id;
    int tmpl_id;
    int file_fd;
    char url[2048];
    uint64_t start_byte;
    uint64_t end_byte;
    uint64_t bytes_downloaded;
    volatile int finished;
    volatile int error;
    int req_id;
} worker_args_t;

static download_status_t g_status;
static pthread_mutex_t g_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_t g_master_thread;
static volatile int g_abort_flag = 0;
static volatile int g_is_running = 0;
static int g_requested_threads = 4;

static int g_net_mem_id  = -1;
static int g_ssl_ctx_id  = -1;
static int g_http_ctx_id = -1;
static int g_tmpl_id     = -1;

static double get_time_seconds(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (double)tv.tv_sec + (double)tv.tv_usec / 1000000.0;
}

static void ensure_directory(const char *dir_path) {
    char tmp[512];
    char *p = NULL;
    size_t len;

    snprintf(tmp, sizeof(tmp), "%s", dir_path);
    len = strlen(tmp);
    if (len > 0 && tmp[len - 1] == '/') {
        tmp[len - 1] = 0;
    }
    for (p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = 0;
            mkdir(tmp, 0777);
            *p = '/';
        }
    }
    mkdir(tmp, 0777);
}

static void sanitize_filename(char *name) {
    for (char *p = name; *p; p++) {
        if (*p == '/' || *p == '\\' || *p == ':' || *p == '*' || *p == '?' || *p == '"' || *p == '<' || *p == '>' || *p == '|') {
            *p = '_';
        }
    }
}

static void url_decode(char *dst, const char *src, size_t max_len) {
    size_t i = 0;
    while (*src && i < max_len - 1) {
        if (*src == '%' && src[1] && src[2]) {
            char hex[3] = { src[1], src[2], '\0' };
            dst[i++] = (char)strtol(hex, NULL, 16);
            src += 3;
        } else if (*src == '+') {
            dst[i++] = ' ';
            src++;
        } else {
            dst[i++] = *src++;
        }
    }
    dst[i] = '\0';
}

static void extract_filename_from_url(const char *url, char *out_filename, size_t max_len) {
    const char *last_slash = strrchr(url, '/');
    if (last_slash && *(last_slash + 1) != '\0') {
        const char *name = last_slash + 1;
        const char *query = strchr(name, '?');
        size_t len = query ? (size_t)(query - name) : strlen(name);
        char raw_name[512];
        if (len >= sizeof(raw_name)) len = sizeof(raw_name) - 1;
        strncpy(raw_name, name, len);
        raw_name[len] = '\0';

        url_decode(out_filename, raw_name, max_len);
        sanitize_filename(out_filename);
    } else {
        snprintf(out_filename, max_len, "download_%ld.pkg", (long)time(NULL));
    }
}

static int extract_filename_from_headers(const char *headers, char *out_filename, size_t max_len) {
    if (!headers) return 0;
    const char *cd = strcasestr(headers, "Content-Disposition:");
    if (!cd) return 0;

    // Check for filename*=UTF-8''filename.ext
    const char *fn_pos = strcasestr(cd, "filename*=");
    if (fn_pos) {
        const char *utf = strstr(fn_pos, "UTF-8''");
        if (utf) {
            const char *val = utf + 7;
            char encoded[512];
            size_t i = 0;
            while (*val && *val != '\r' && *val != '\n' && *val != ';' && i < sizeof(encoded) - 1) {
                encoded[i++] = *val++;
            }
            encoded[i] = '\0';
            url_decode(out_filename, encoded, max_len);
            sanitize_filename(out_filename);
            if (strlen(out_filename) > 0) return 1;
        }
    }

    // Check for standard filename="filename.ext" or filename=filename.ext
    fn_pos = strcasestr(cd, "filename=");
    if (fn_pos) {
        const char *val = fn_pos + 9;
        while (*val == ' ' || *val == '\t') val++;
        if (*val == '\"') {
            val++;
            const char *end_quote = strchr(val, '\"');
            if (end_quote) {
                size_t len = end_quote - val;
                if (len >= max_len) len = max_len - 1;
                strncpy(out_filename, val, len);
                out_filename[len] = '\0';
                sanitize_filename(out_filename);
                return 1;
            }
        } else {
            size_t i = 0;
            while (*val && *val != '\r' && *val != '\n' && *val != ';' && *val != ' ' && i < max_len - 1) {
                out_filename[i++] = *val++;
            }
            out_filename[i] = '\0';
            sanitize_filename(out_filename);
            if (i > 0) return 1;
        }
    }

    return 0;
}

static int init_sony_http(void) {
    if (g_tmpl_id >= 0) return 0;

    sceNetInit();

    if (g_net_mem_id < 0) {
        g_net_mem_id = sceNetPoolCreate("ps5_nd_pool", 1024 * 1024, 0);
    }

    if (g_ssl_ctx_id < 0) {
        g_ssl_ctx_id = sceSslInit(2 * 1024 * 1024);
    }

    if (g_http_ctx_id < 0) {
        g_http_ctx_id = sceHttp2Init(g_net_mem_id, g_ssl_ctx_id, 4 * 1024 * 1024, 1);
        if (g_http_ctx_id < 0) {
            return -1;
        }
    }

    g_tmpl_id = sceHttp2CreateTemplate(g_http_ctx_id, "PS5-Net-Downloader-Turbo/1.3", 3, 1);
    if (g_tmpl_id < 0) {
        return -1;
    }

    // Disable strict SSL server verification for maximum CDN compatibility
    sceHttp2SslDisableOption(g_tmpl_id, 0x01);
    if (g_ssl_ctx_id >= 0) {
        sceSslDisableVerifyOption(g_ssl_ctx_id, 0xffffffff);
    }

    // Configure timeouts (microseconds): 15s connect, 30s recv, 15s send
    sceHttp2SetConnectTimeOut(g_tmpl_id, 15 * 1000 * 1000);
    sceHttp2SetRecvTimeOut(g_tmpl_id, 30 * 1000 * 1000);
    sceHttp2SetSendTimeOut(g_tmpl_id, 15 * 1000 * 1000);

    return 0;
}

static void *parallel_chunk_worker(void *arg) {
    worker_args_t *args = (worker_args_t *)arg;
    args->finished = 0;
    args->error = 0;
    args->bytes_downloaded = 0;

    int req_id = sceHttp2CreateRequestWithURL(args->tmpl_id, "GET", args->url, 0);
    if (req_id < 0) {
        args->error = 1;
        args->finished = 1;
        return NULL;
    }
    args->req_id = req_id;

    char range_val[64];
    snprintf(range_val, sizeof(range_val), "bytes=%llu-%llu",
             (unsigned long long)args->start_byte,
             (unsigned long long)args->end_byte);
    sceHttp2AddRequestHeader(req_id, "Range", range_val, 0);

    if (sceHttp2SendRequest(req_id, NULL, 0) != 0) {
        sceHttp2DeleteRequest(req_id);
        args->error = 1;
        args->finished = 1;
        return NULL;
    }

    int status_code = 0;
    sceHttp2GetStatusCode(req_id, &status_code);
    if (status_code != 200 && status_code != 206) {
        sceHttp2DeleteRequest(req_id);
        args->error = 1;
        args->finished = 1;
        return NULL;
    }

    char *buf = malloc(CHUNK_BUFFER_SIZE);
    if (!buf) {
        sceHttp2DeleteRequest(req_id);
        args->error = 1;
        args->finished = 1;
        return NULL;
    }

    uint64_t current_offset = args->start_byte;
    uint64_t total_expected = (args->end_byte - args->start_byte) + 1;

    while (!g_abort_flag && args->bytes_downloaded < total_expected) {
        size_t to_read = CHUNK_BUFFER_SIZE;
        if (to_read > (total_expected - args->bytes_downloaded)) {
            to_read = total_expected - args->bytes_downloaded;
        }

        int n = sceHttp2ReadData(req_id, buf, to_read);
        if (n <= 0) {
            if (n < 0) args->error = 1;
            break;
        }

        ssize_t written = pwrite(args->file_fd, buf, n, current_offset);
        if (written != n) {
            args->error = 1;
            break;
        }

        current_offset += n;
        args->bytes_downloaded += n;

        pthread_mutex_lock(&g_mutex);
        g_status.downloaded_bytes += n;
        pthread_mutex_unlock(&g_mutex);
    }

    free(buf);
    sceHttp2DeleteRequest(req_id);
    args->finished = 1;
    return NULL;
}

static void *master_download_thread(void *arg) {
    (void)arg;
    char target_url[2048];

    pthread_mutex_lock(&g_mutex);
    g_status.state = STATUS_CONNECTING;
    g_status.downloaded_bytes = 0;
    g_status.total_bytes = 0;
    g_status.speed_bytes_sec = 0.0;
    g_status.eta_seconds = 0;
    g_status.error_message[0] = '\0';
    g_status.num_threads = 1;
    strncpy(target_url, g_status.url, sizeof(target_url) - 1);
    target_url[sizeof(target_url) - 1] = '\0';
    pthread_mutex_unlock(&g_mutex);

    if (init_sony_http() != 0) {
        pthread_mutex_lock(&g_mutex);
        g_status.state = STATUS_ERROR;
        snprintf(g_status.error_message, sizeof(g_status.error_message), "Failed to initialize native PS5 HTTP subsystem");
        g_is_running = 0;
        pthread_mutex_unlock(&g_mutex);
        return NULL;
    }

    // Step 1: Probe URL with Range: bytes=0-0 to check range support, size, and filename
    int probe_req = sceHttp2CreateRequestWithURL(g_tmpl_id, "GET", target_url, 0);
    if (probe_req < 0) {
        pthread_mutex_lock(&g_mutex);
        g_status.state = STATUS_ERROR;
        snprintf(g_status.error_message, sizeof(g_status.error_message), "Invalid URL or request creation error");
        g_is_running = 0;
        pthread_mutex_unlock(&g_mutex);
        return NULL;
    }

    sceHttp2AddRequestHeader(probe_req, "Range", "bytes=0-0", 0);

    if (sceHttp2SendRequest(probe_req, NULL, 0) != 0) {
        sceHttp2DeleteRequest(probe_req);
        pthread_mutex_lock(&g_mutex);
        g_status.state = STATUS_ERROR;
        snprintf(g_status.error_message, sizeof(g_status.error_message), "Connection failed or SSL handshake error");
        g_is_running = 0;
        pthread_mutex_unlock(&g_mutex);
        return NULL;
    }

    int status_code = 0;
    sceHttp2GetStatusCode(probe_req, &status_code);

    char *raw_headers = NULL;
    size_t headers_sz = 0;
    sceHttp2GetAllResponseHeaders(probe_req, &raw_headers, &headers_sz);

    uint64_t total_size = 0;
    int supports_range = (status_code == 206);

    if (raw_headers) {
        // Try parsing Content-Range: bytes 0-0/123456
        char *cr_pos = strcasestr(raw_headers, "Content-Range:");
        if (cr_pos) {
            char *slash = strchr(cr_pos, '/');
            if (slash) {
                total_size = strtoull(slash + 1, NULL, 10);
                supports_range = 1;
            }
        }

        // Try parsing Content-Length if Content-Range wasn't found
        if (total_size == 0) {
            char *cl_pos = strcasestr(raw_headers, "Content-Length:");
            if (cl_pos) {
                total_size = strtoull(cl_pos + 15, NULL, 10);
            }
        }

        // Check if filename can be resolved from Content-Disposition
        char header_fn[256];
        if (extract_filename_from_headers(raw_headers, header_fn, sizeof(header_fn))) {
            pthread_mutex_lock(&g_mutex);
            int is_auto_name = (strstr(g_status.filename, "download_") != NULL || (strstr(g_status.filename, ".pkg") != NULL && strlen(g_status.filename) < 12));
            if (is_auto_name || g_status.filename[0] == '\0') {
                strncpy(g_status.filename, header_fn, sizeof(g_status.filename) - 1);
                char *last_sl = strrchr(g_status.save_path, '/');
                if (last_sl) {
                    *(last_sl + 1) = '\0';
                    strncat(g_status.save_path, g_status.filename, sizeof(g_status.save_path) - strlen(g_status.save_path) - 1);
                }
            }
            pthread_mutex_unlock(&g_mutex);
        }
    }

    // Also query ContentLength via native API if still 0
    if (total_size == 0) {
        sceHttp2GetResponseContentLength(probe_req, &total_size);
    }

    sceHttp2DeleteRequest(probe_req);

    if (status_code != 200 && status_code != 206) {
        pthread_mutex_lock(&g_mutex);
        g_status.state = STATUS_ERROR;
        snprintf(g_status.error_message, sizeof(g_status.error_message), "Server returned HTTP status %d", status_code);
        g_is_running = 0;
        pthread_mutex_unlock(&g_mutex);
        return NULL;
    }

    // Step 2: Open target file
    int file_fd = open(g_status.save_path, O_RDWR | O_CREAT | O_TRUNC, 0666);
    if (file_fd < 0) {
        pthread_mutex_lock(&g_mutex);
        g_status.state = STATUS_ERROR;
        snprintf(g_status.error_message, sizeof(g_status.error_message), "Cannot create file: %s (errno %d)", g_status.save_path, errno);
        g_is_running = 0;
        pthread_mutex_unlock(&g_mutex);
        return NULL;
    }

    if (total_size > 0) {
        ftruncate(file_fd, total_size);
    }

    // Step 3: Determine thread count (fallback to 1 if Range not supported)
    int thread_count = g_requested_threads;
    if (!supports_range || total_size < (uint64_t)(thread_count * 1024 * 1024)) {
        thread_count = 1;
    }
    if (thread_count > MAX_THREADS) thread_count = MAX_THREADS;

    pthread_mutex_lock(&g_mutex);
    g_status.state = STATUS_DOWNLOADING;
    g_status.total_bytes = total_size;
    g_status.num_threads = thread_count;
    pthread_mutex_unlock(&g_mutex);

    double last_speed_time = get_time_seconds();
    uint64_t last_downloaded_bytes = 0;

    if (thread_count > 1 && total_size > 0) {
        // Multi-threaded parallel download
        pthread_t threads[MAX_THREADS];
        worker_args_t args[MAX_THREADS];
        uint64_t segment_size = total_size / thread_count;

        for (int i = 0; i < thread_count; i++) {
            args[i].thread_id = i;
            args[i].tmpl_id = g_tmpl_id;
            args[i].file_fd = file_fd;
            strncpy(args[i].url, target_url, sizeof(args[i].url) - 1);
            args[i].url[sizeof(args[i].url) - 1] = '\0';
            args[i].start_byte = i * segment_size;
            args[i].end_byte = (i == thread_count - 1) ? (total_size - 1) : ((i + 1) * segment_size - 1);
            args[i].bytes_downloaded = 0;
            args[i].finished = 0;
            args[i].error = 0;
            args[i].req_id = -1;

            pthread_create(&threads[i], NULL, parallel_chunk_worker, &args[i]);
        }

        while (!g_abort_flag) {
            usleep(250000); // 250 ms update

            pthread_mutex_lock(&g_mutex);
            uint64_t current_dl = g_status.downloaded_bytes;
            double now = get_time_seconds();
            double elapsed = now - last_speed_time;

            if (elapsed >= 0.5) {
                uint64_t diff = (current_dl >= last_downloaded_bytes) ? (current_dl - last_downloaded_bytes) : 0;
                double speed = (double)diff / elapsed;
                g_status.speed_bytes_sec = speed;
                last_speed_time = now;
                last_downloaded_bytes = current_dl;

                if (speed > 1024.0 && total_size > current_dl) {
                    g_status.eta_seconds = (uint64_t)((total_size - current_dl) / speed);
                } else {
                    g_status.eta_seconds = 0;
                }
            }

            int all_done = (total_size > 0 && current_dl >= total_size);
            pthread_mutex_unlock(&g_mutex);

            if (all_done) break;

            int any_alive = 0;
            int any_error = 0;
            for (int i = 0; i < thread_count; i++) {
                if (!args[i].finished) any_alive = 1;
                if (args[i].error) any_error = 1;
            }

            if (any_error) {
                pthread_mutex_lock(&g_mutex);
                g_status.state = STATUS_ERROR;
                snprintf(g_status.error_message, sizeof(g_status.error_message), "Worker connection dropped or chunk error");
                pthread_mutex_unlock(&g_mutex);
                break;
            }

            if (!any_alive) break;
        }

        for (int i = 0; i < thread_count; i++) {
            if (g_abort_flag && args[i].req_id >= 0) {
                sceHttp2AbortRequest(args[i].req_id);
            }
            pthread_join(threads[i], NULL);
        }
    } else {
        // Single-stream download (handles servers without Range support like VikingFile)
        int stream_req = sceHttp2CreateRequestWithURL(g_tmpl_id, "GET", target_url, 0);
        if (stream_req >= 0) {
            if (sceHttp2SendRequest(stream_req, NULL, 0) == 0) {
                char *buf = malloc(CHUNK_BUFFER_SIZE);
                if (buf) {
                    while (!g_abort_flag) {
                        int n = sceHttp2ReadData(stream_req, buf, CHUNK_BUFFER_SIZE);
                        if (n <= 0) break;

                        write(file_fd, buf, n);

                        pthread_mutex_lock(&g_mutex);
                        g_status.downloaded_bytes += n;
                        double now = get_time_seconds();
                        double elapsed = now - last_speed_time;
                        if (elapsed >= 0.5) {
                            uint64_t diff = g_status.downloaded_bytes - last_downloaded_bytes;
                            double speed = (double)diff / elapsed;
                            g_status.speed_bytes_sec = speed;
                            last_speed_time = now;
                            last_downloaded_bytes = g_status.downloaded_bytes;
                            if (speed > 1024.0 && total_size > g_status.downloaded_bytes) {
                                g_status.eta_seconds = (uint64_t)((total_size - g_status.downloaded_bytes) / speed);
                            }
                        }
                        pthread_mutex_unlock(&g_mutex);
                    }
                    free(buf);
                }
            } else {
                pthread_mutex_lock(&g_mutex);
                g_status.state = STATUS_ERROR;
                snprintf(g_status.error_message, sizeof(g_status.error_message), "Single stream connection failed");
                pthread_mutex_unlock(&g_mutex);
            }
            sceHttp2DeleteRequest(stream_req);
        }
    }

    close(file_fd);

    pthread_mutex_lock(&g_mutex);
    if (g_abort_flag) {
        g_status.state = STATUS_IDLE;
        unlink(g_status.save_path);
    } else if (g_status.state != STATUS_ERROR) {
        if (total_size > 0 && g_status.downloaded_bytes < total_size) {
            g_status.state = STATUS_ERROR;
            snprintf(g_status.error_message, sizeof(g_status.error_message), "Premature EOF: %llu / %llu bytes",
                     (unsigned long long)g_status.downloaded_bytes, (unsigned long long)total_size);
        } else {
            g_status.state = STATUS_COMPLETED;
            g_status.speed_bytes_sec = 0.0;
            g_status.eta_seconds = 0;
        }
    }
    g_is_running = 0;
    pthread_mutex_unlock(&g_mutex);

    return NULL;
}

void downloader_init(void) {
    pthread_mutex_lock(&g_mutex);
    memset(&g_status, 0, sizeof(g_status));
    g_status.state = STATUS_IDLE;
    g_abort_flag = 0;
    g_is_running = 0;
    g_requested_threads = 4;
    pthread_mutex_unlock(&g_mutex);

    init_sony_http();
}

int downloader_start(const char *url, const char *save_dir, const char *filename, int threads) {
    pthread_mutex_lock(&g_mutex);
    if (g_is_running) {
        pthread_mutex_unlock(&g_mutex);
        return -1;
    }

    g_abort_flag = 0;
    g_is_running = 1;
    g_requested_threads = (threads >= 1 && threads <= MAX_THREADS) ? threads : 4;

    strncpy(g_status.url, url, sizeof(g_status.url) - 1);
    g_status.url[sizeof(g_status.url) - 1] = '\0';

    if (filename && filename[0] != '\0') {
        strncpy(g_status.filename, filename, sizeof(g_status.filename) - 1);
        g_status.filename[sizeof(g_status.filename) - 1] = '\0';
    } else {
        extract_filename_from_url(url, g_status.filename, sizeof(g_status.filename));
    }

    ensure_directory(save_dir);

    snprintf(g_status.save_path, sizeof(g_status.save_path), "%s/%s", 
             save_dir, g_status.filename);

    pthread_create(&g_master_thread, NULL, master_download_thread, NULL);
    pthread_mutex_unlock(&g_mutex);
    return 0;
}

void downloader_abort(void) {
    pthread_mutex_lock(&g_mutex);
    if (g_is_running) {
        g_abort_flag = 1;
    }
    pthread_mutex_unlock(&g_mutex);
}

void downloader_reset(void) {
    pthread_mutex_lock(&g_mutex);
    g_abort_flag = 1;
    g_is_running = 0;
    g_status.state = STATUS_IDLE;
    g_status.error_message[0] = '\0';
    g_status.downloaded_bytes = 0;
    g_status.total_bytes = 0;
    g_status.speed_bytes_sec = 0.0;
    g_status.eta_seconds = 0;
    pthread_mutex_unlock(&g_mutex);
}

void downloader_get_status(download_status_t *out_status) {
    pthread_mutex_lock(&g_mutex);
    memcpy(out_status, &g_status, sizeof(download_status_t));
    pthread_mutex_unlock(&g_mutex);
}
