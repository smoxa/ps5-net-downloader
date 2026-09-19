#define CURL_DISABLE_TYPECHECK 1
#include "downloader.h"

#include <curl/curl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

#define MAX_THREADS 32
#define CHUNK_BUFFER_SIZE (512 * 1024L) // 512 KB curl socket buffer
#define WORKER_WRITE_BUF_SIZE (2 * 1024 * 1024) // 2 MB RAM buffer for write coalescing
#define SINGLE_WRITE_BUF_SIZE (2 * 1024 * 1024) // 2 MB RAM buffer for single-stream

static int curl_sockopt_cb(void *clientp, curl_socket_t curlfd, curlsocktype purpose) {
    (void)clientp;
    (void)purpose;
    // Enable TCP_NODELAY and preserve OS TCP window autotuning
    // (Manual SO_RCVBUF is omitted to allow dynamic TCP window scaling)
    int nodelay = 1;
    setsockopt(curlfd, IPPROTO_TCP, TCP_NODELAY, (const char *)&nodelay, sizeof(nodelay));
    return CURL_SOCKOPT_OK;
}

typedef struct {
    int thread_id;
    char save_path[512];
    int file_fd;
    char url[2048];
    uint64_t start_byte;
    uint64_t end_byte;
    uint64_t current_offset;
    uint64_t bytes_downloaded;
    uint64_t current_chunk_bytes;
    volatile int finished;
    volatile int error;
    char *write_buf;
    size_t write_buf_len;
} worker_args_t;

typedef struct {
    int file_fd;
    char *write_buf;
    size_t write_buf_len;
    uint64_t bytes_downloaded;
    double last_speed_time;
    uint64_t last_bytes;
} single_ctx_t;

typedef struct {
    uint64_t total_size;
    int supports_range;
    int status_code;
    char filename[256];
} probe_info_t;

static download_status_t g_status;
static pthread_mutex_t g_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_t g_master_thread;
static volatile int g_abort_flag = 0;
static volatile int g_is_running = 0;
static int g_requested_threads = 8;

// Dynamic Work-Stealing Job Queue
static pthread_mutex_t g_job_mutex = PTHREAD_MUTEX_INITIALIZER;
static uint64_t g_job_next_offset = 0;
static uint64_t g_job_total_size = 0;
static uint64_t g_job_chunk_size = 32 * 1024 * 1024; // 32 MB dynamic chunks

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
    if (!cd) cd = headers;

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

static size_t probe_header_callback(char *buffer, size_t size, size_t nitems, void *userdata) {
    size_t total = size * nitems;
    probe_info_t *info = (probe_info_t *)userdata;

    char header[1024];
    size_t copy_len = (total < sizeof(header) - 1) ? total : sizeof(header) - 1;
    memcpy(header, buffer, copy_len);
    header[copy_len] = '\0';

    if (strncasecmp(header, "HTTP/", 5) == 0) {
        int code = 0;
        if (sscanf(header, "HTTP/%*f %d", &code) == 1) {
            info->status_code = code;
            if (code == 206) {
                info->supports_range = 1;
            }
        }
    } else if (strncasecmp(header, "Content-Range:", 14) == 0) {
        info->supports_range = 1;
        char *slash = strchr(header, '/');
        if (slash) {
            info->total_size = strtoull(slash + 1, NULL, 10);
        }
    } else if (strncasecmp(header, "Content-Length:", 15) == 0) {
        if (info->total_size == 0) {
            info->total_size = strtoull(header + 15, NULL, 10);
        }
    } else if (strncasecmp(header, "Accept-Ranges: bytes", 20) == 0) {
        info->supports_range = 1;
    } else if (strncasecmp(header, "Content-Disposition:", 20) == 0) {
        extract_filename_from_headers(header, info->filename, sizeof(info->filename));
    }

    return total;
}

static size_t probe_write_dummy_callback(char *ptr, size_t size, size_t nmemb, void *userdata) {
    (void)ptr;
    (void)userdata;
    return size * nmemb;
}

static int download_xferinfo_callback(void *clientp, curl_off_t dltotal, curl_off_t dlnow, curl_off_t ultotal, curl_off_t ulnow) {
    (void)clientp;
    (void)dltotal;
    (void)dlnow;
    (void)ultotal;
    (void)ulnow;
    if (g_abort_flag) {
        return 1; // Abort transfer immediately
    }
    return 0;
}

static size_t worker_write_callback(char *ptr, size_t size, size_t nmemb, void *userdata) {
    worker_args_t *args = (worker_args_t *)userdata;
    size_t total_bytes = size * nmemb;

    if (g_abort_flag) {
        return 0; // Abort curl immediately
    }

    if (!args->write_buf) {
        ssize_t written = pwrite(args->file_fd, ptr, total_bytes, args->current_offset);
        if (written != (ssize_t)total_bytes) {
            args->error = 1;
            return 0;
        }
        args->current_offset += total_bytes;
        args->bytes_downloaded += total_bytes;
        args->current_chunk_bytes += total_bytes;
        return total_bytes;
    }

    size_t remaining = total_bytes;
    const char *src = ptr;

    while (remaining > 0) {
        size_t space = WORKER_WRITE_BUF_SIZE - args->write_buf_len;
        size_t to_copy = (remaining < space) ? remaining : space;

        memcpy(args->write_buf + args->write_buf_len, src, to_copy);
        args->write_buf_len += to_copy;
        src += to_copy;
        remaining -= to_copy;

        if (args->write_buf_len >= WORKER_WRITE_BUF_SIZE) {
            ssize_t written = pwrite(args->file_fd, args->write_buf, args->write_buf_len, args->current_offset);
            if (written != (ssize_t)args->write_buf_len) {
                args->error = 1;
                return 0;
            }
            args->current_offset += written;
            args->write_buf_len = 0;
        }
    }

    args->bytes_downloaded += total_bytes;
    args->current_chunk_bytes += total_bytes;
    return total_bytes;
}

static size_t single_stream_write_callback(char *ptr, size_t size, size_t nmemb, void *userdata) {
    single_ctx_t *ctx = (single_ctx_t *)userdata;
    size_t total_bytes = size * nmemb;

    if (g_abort_flag) {
        return 0;
    }

    if (!ctx->write_buf) {
        ssize_t written = write(ctx->file_fd, ptr, total_bytes);
        if (written != (ssize_t)total_bytes) {
            return 0;
        }
        ctx->bytes_downloaded += total_bytes;
        return total_bytes;
    }

    size_t remaining = total_bytes;
    const char *src = ptr;

    while (remaining > 0) {
        size_t space = SINGLE_WRITE_BUF_SIZE - ctx->write_buf_len;
        size_t to_copy = (remaining < space) ? remaining : space;

        memcpy(ctx->write_buf + ctx->write_buf_len, src, to_copy);
        ctx->write_buf_len += to_copy;
        src += to_copy;
        remaining -= to_copy;

        if (ctx->write_buf_len >= SINGLE_WRITE_BUF_SIZE) {
            ssize_t written = write(ctx->file_fd, ctx->write_buf, ctx->write_buf_len);
            if (written != (ssize_t)ctx->write_buf_len) {
                return 0;
            }
            ctx->write_buf_len = 0;
        }
    }

    ctx->bytes_downloaded += total_bytes;
    return total_bytes;
}

static int single_stream_xferinfo_callback(void *clientp, curl_off_t dltotal, curl_off_t dlnow, curl_off_t ultotal, curl_off_t ulnow) {
    (void)ultotal;
    (void)ulnow;
    if (g_abort_flag) return 1;

    single_ctx_t *ctx = (single_ctx_t *)clientp;
    double now = get_time_seconds();

    pthread_mutex_lock(&g_mutex);
    g_status.downloaded_bytes = (uint64_t)dlnow;
    if (dltotal > 0) {
        g_status.total_bytes = (uint64_t)dltotal;
    }

    double elapsed = now - ctx->last_speed_time;
    if (elapsed >= 0.5) {
        uint64_t diff = (dlnow >= (curl_off_t)ctx->last_bytes) ? (uint64_t)(dlnow - ctx->last_bytes) : 0;
        double speed = (double)diff / elapsed;
        g_status.speed_bytes_sec = speed;
        ctx->last_speed_time = now;
        ctx->last_bytes = (uint64_t)dlnow;

        if (speed > 1024.0 && dltotal > dlnow) {
            g_status.eta_seconds = (uint64_t)((dltotal - dlnow) / speed);
        } else {
            g_status.eta_seconds = 0;
        }
    }
    pthread_mutex_unlock(&g_mutex);

    return 0;
}

static void *parallel_chunk_worker(void *arg) {
    worker_args_t *args = (worker_args_t *)arg;
    args->finished = 0;
    args->error = 0;
    args->bytes_downloaded = 0;
    args->current_chunk_bytes = 0;
    args->write_buf_len = 0;

    args->write_buf = malloc(WORKER_WRITE_BUF_SIZE);
    if (!args->write_buf) {
        args->error = 1;
        args->finished = 1;
        return NULL;
    }

    // Open independent file descriptor for this worker thread to eliminate
    // kernel file-table descriptor lock contention among concurrent threads
    int thread_fd = open(args->save_path, O_WRONLY);
    int orig_fd = args->file_fd;
    if (thread_fd >= 0) {
        args->file_fd = thread_fd;
    }

    CURL *curl = curl_easy_init();
    if (!curl) {
        if (thread_fd >= 0) {
            close(thread_fd);
        }
        args->file_fd = orig_fd;
        free(args->write_buf);
        args->write_buf = NULL;
        args->error = 1;
        args->finished = 1;
        return NULL;
    }

    curl_easy_setopt(curl, CURLOPT_URL, args->url);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, worker_write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, args);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 10L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "Mozilla/5.0 (PlayStation 5; PS5-Net-Downloader-Turbo/1.7)");
    curl_easy_setopt(curl, CURLOPT_BUFFERSIZE, CHUNK_BUFFER_SIZE);
    curl_easy_setopt(curl, CURLOPT_TCP_NODELAY, 1L);
    curl_easy_setopt(curl, CURLOPT_SOCKOPTFUNCTION, curl_sockopt_cb);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 15L);
    curl_easy_setopt(curl, CURLOPT_LOW_SPEED_LIMIT, 1024L);
    curl_easy_setopt(curl, CURLOPT_LOW_SPEED_TIME, 30L);
    curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 0L);
    curl_easy_setopt(curl, CURLOPT_XFERINFOFUNCTION, download_xferinfo_callback);

    // Keep-Alive connection reuse across dynamic chunks
    curl_easy_setopt(curl, CURLOPT_FORBID_REUSE, 0L);

    // TLS Ciphers optimization: Prefer ChaCha20-Poly1305 and AES-128 for lowest CPU overhead
    curl_easy_setopt(curl, CURLOPT_SSL_CIPHER_LIST, "ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305:ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:HIGH:!aNULL:!MD5:!RC4");
#ifdef CURLOPT_TLS13_CIPHERS
    curl_easy_setopt(curl, CURLOPT_TLS13_CIPHERS, "TLS_CHACHA20_POLY1305_SHA256:TLS_AES_128_GCM_SHA256:TLS_AES_256_GCM_SHA384");
#endif

    // Dynamic Work-Stealing loop: continuously claim next chunk from queue
    while (!g_abort_flag) {
        uint64_t chunk_start = 0;
        uint64_t chunk_end = 0;

        pthread_mutex_lock(&g_job_mutex);
        if (g_job_next_offset >= g_job_total_size) {
            pthread_mutex_unlock(&g_job_mutex);
            break; // All dynamic chunks have been assigned!
        }
        chunk_start = g_job_next_offset;
        chunk_end = chunk_start + g_job_chunk_size - 1;
        if (chunk_end >= g_job_total_size) {
            chunk_end = g_job_total_size - 1;
        }
        g_job_next_offset = chunk_end + 1;
        pthread_mutex_unlock(&g_job_mutex);

        args->start_byte = chunk_start;
        args->end_byte = chunk_end;

        char range_str[64];
        snprintf(range_str, sizeof(range_str), "%llu-%llu",
                 (unsigned long long)chunk_start,
                 (unsigned long long)chunk_end);
        curl_easy_setopt(curl, CURLOPT_RANGE, range_str);

        int chunk_success = 0;
        for (int attempt = 0; attempt < 3 && !g_abort_flag; attempt++) {
            if (attempt > 0) {
                // Roll back progress from failed attempt
                if (args->current_chunk_bytes > 0) {
                    if (args->bytes_downloaded >= args->current_chunk_bytes) {
                        args->bytes_downloaded -= args->current_chunk_bytes;
                    }
                }
                usleep(500000); // 500ms backoff
            }

            args->current_offset = chunk_start;
            args->current_chunk_bytes = 0;
            args->write_buf_len = 0;

            CURLcode res = curl_easy_perform(curl);
            if (res == CURLE_OK) {
                chunk_success = 1;
                break;
            }
            if (g_abort_flag || res == CURLE_ABORTED_BY_CALLBACK) {
                break;
            }
        }

        if (!chunk_success && !g_abort_flag) {
            args->error = 1;
            break;
        }

        // Flush any remaining buffered data in RAM for this completed chunk
        if (!args->error && !g_abort_flag && args->write_buf && args->write_buf_len > 0) {
            ssize_t written = pwrite(args->file_fd, args->write_buf, args->write_buf_len, args->current_offset);
            if (written != (ssize_t)args->write_buf_len) {
                args->error = 1;
                break;
            }
            args->current_offset += written;
            args->write_buf_len = 0;
        }
    }

    curl_easy_cleanup(curl);

    if (thread_fd >= 0) {
        close(thread_fd);
    }
    args->file_fd = orig_fd;

    if (args->write_buf) {
        free(args->write_buf);
        args->write_buf = NULL;
    }

    args->finished = 1;
    return NULL;
}

static void *master_download_thread(void *arg) {
    (void)arg;
    pthread_detach(pthread_self()); // Auto free thread resources upon exit

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

    // Step 1: Probe URL with curl to determine Range support, size, and redirects
    CURL *probe_curl = curl_easy_init();
    if (!probe_curl) {
        pthread_mutex_lock(&g_mutex);
        g_status.state = STATUS_ERROR;
        snprintf(g_status.error_message, sizeof(g_status.error_message), "Failed to initialize libcurl");
        g_is_running = 0;
        pthread_mutex_unlock(&g_mutex);
        return NULL;
    }

    probe_info_t probe_info;
    memset(&probe_info, 0, sizeof(probe_info));

    curl_easy_setopt(probe_curl, CURLOPT_URL, target_url);
    curl_easy_setopt(probe_curl, CURLOPT_RANGE, "0-0");
    curl_easy_setopt(probe_curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(probe_curl, CURLOPT_MAXREDIRS, 10L);
    curl_easy_setopt(probe_curl, CURLOPT_SSL_VERIFYPEER, 0L);
    curl_easy_setopt(probe_curl, CURLOPT_SSL_VERIFYHOST, 0L);
    curl_easy_setopt(probe_curl, CURLOPT_USERAGENT, "Mozilla/5.0 (PlayStation 5; PS5-Net-Downloader-Turbo/1.5)");
    curl_easy_setopt(probe_curl, CURLOPT_SOCKOPTFUNCTION, curl_sockopt_cb);
    curl_easy_setopt(probe_curl, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(probe_curl, CURLOPT_HEADERFUNCTION, probe_header_callback);
    curl_easy_setopt(probe_curl, CURLOPT_HEADERDATA, &probe_info);
    curl_easy_setopt(probe_curl, CURLOPT_WRITEFUNCTION, probe_write_dummy_callback);
    curl_easy_setopt(probe_curl, CURLOPT_CONNECTTIMEOUT, 15L);
    curl_easy_setopt(probe_curl, CURLOPT_TIMEOUT, 20L);

    CURLcode probe_res = curl_easy_perform(probe_curl);

    // If Range probe failed or returned an error, retry with simple HEAD request
    if (probe_res != CURLE_OK || (probe_info.status_code != 200 && probe_info.status_code != 206)) {
        curl_easy_setopt(probe_curl, CURLOPT_RANGE, NULL);
        curl_easy_setopt(probe_curl, CURLOPT_NOBODY, 1L);
        probe_res = curl_easy_perform(probe_curl);
    }

    // Get final resolved URL after redirects
    char *effective_url = NULL;
    curl_easy_getinfo(probe_curl, CURLINFO_EFFECTIVE_URL, &effective_url);
    if (effective_url && strlen(effective_url) > 0) {
        strncpy(target_url, effective_url, sizeof(target_url) - 1);
        target_url[sizeof(target_url) - 1] = '\0';
    }

    // Get final HTTP status code
    long http_code = 0;
    curl_easy_getinfo(probe_curl, CURLINFO_RESPONSE_CODE, &http_code);
    if (http_code > 0) {
        probe_info.status_code = (int)http_code;
    }

    // Get content length via curl if not already parsed
    if (probe_info.total_size == 0) {
        curl_off_t cl = 0;
        curl_easy_getinfo(probe_curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD_T, &cl);
        if (cl > 0) {
            probe_info.total_size = (uint64_t)cl;
        }
    }

    curl_easy_cleanup(probe_curl);

    if (probe_res != CURLE_OK && probe_info.total_size == 0 && probe_info.status_code == 0) {
        pthread_mutex_lock(&g_mutex);
        g_status.state = STATUS_ERROR;
        snprintf(g_status.error_message, sizeof(g_status.error_message), "Network error: %s", curl_easy_strerror(probe_res));
        g_is_running = 0;
        pthread_mutex_unlock(&g_mutex);
        return NULL;
    }

    if (probe_info.status_code >= 400) {
        pthread_mutex_lock(&g_mutex);
        g_status.state = STATUS_ERROR;
        snprintf(g_status.error_message, sizeof(g_status.error_message), "Server returned HTTP status %d", probe_info.status_code);
        g_is_running = 0;
        pthread_mutex_unlock(&g_mutex);
        return NULL;
    }

    // Check if filename was extracted from Content-Disposition
    if (probe_info.filename[0] != '\0') {
        pthread_mutex_lock(&g_mutex);
        int is_auto_name = (strstr(g_status.filename, "download_") != NULL || (strstr(g_status.filename, ".pkg") != NULL && strlen(g_status.filename) < 12));
        if (is_auto_name || g_status.filename[0] == '\0') {
            strncpy(g_status.filename, probe_info.filename, sizeof(g_status.filename) - 1);
            char *last_sl = strrchr(g_status.save_path, '/');
            if (last_sl) {
                *(last_sl + 1) = '\0';
                strncat(g_status.save_path, g_status.filename, sizeof(g_status.save_path) - strlen(g_status.save_path) - 1);
            }
        }
        pthread_mutex_unlock(&g_mutex);
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

    uint64_t total_size = probe_info.total_size;
    if (total_size > 0) {
        ftruncate(file_fd, total_size);
    }

    // Step 3: Determine thread count (fallback to 1 if Range not supported)
    int thread_count = g_requested_threads;
    if (!probe_info.supports_range || total_size < (uint64_t)(thread_count * 1024 * 1024)) {
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
        // Multi-threaded parallel download via libcurl
        pthread_t *threads = calloc(thread_count, sizeof(pthread_t));
        worker_args_t *args = calloc(thread_count, sizeof(worker_args_t));
        if (!threads || !args) {
            free(threads);
            free(args);
            close(file_fd);
            pthread_mutex_lock(&g_mutex);
            g_status.state = STATUS_ERROR;
            snprintf(g_status.error_message, sizeof(g_status.error_message), "Memory allocation failed for %d workers", thread_count);
            g_is_running = 0;
            g_abort_flag = 0;
            pthread_mutex_unlock(&g_mutex);
            return NULL;
        }

        pthread_mutex_lock(&g_job_mutex);
        g_job_total_size = total_size;
        g_job_next_offset = 0;
        // Dynamic chunk sizing: default 32 MB, adapt for smaller files so all threads are utilized
        uint64_t dynamic_chunk = 32 * 1024 * 1024;
        if (total_size < dynamic_chunk * thread_count * 2) {
            dynamic_chunk = total_size / (thread_count * 4);
            if (dynamic_chunk < 2 * 1024 * 1024) {
                dynamic_chunk = 2 * 1024 * 1024;
            }
        }
        g_job_chunk_size = dynamic_chunk;
        pthread_mutex_unlock(&g_job_mutex);

        for (int i = 0; i < thread_count; i++) {
            args[i].thread_id = i;
            args[i].file_fd = file_fd;
            strncpy(args[i].save_path, g_status.save_path, sizeof(args[i].save_path) - 1);
            args[i].save_path[sizeof(args[i].save_path) - 1] = '\0';
            strncpy(args[i].url, target_url, sizeof(args[i].url) - 1);
            args[i].url[sizeof(args[i].url) - 1] = '\0';
            args[i].start_byte = 0;
            args[i].end_byte = 0;
            args[i].current_offset = 0;
            args[i].bytes_downloaded = 0;
            args[i].current_chunk_bytes = 0;
            args[i].finished = 0;
            args[i].error = 0;

            pthread_create(&threads[i], NULL, parallel_chunk_worker, &args[i]);
        }

        while (!g_abort_flag) {
            usleep(100000); // 100 ms polling

            uint64_t current_dl = 0;
            int all_done = 1;
            int any_alive = 0;
            int any_error = 0;

            for (int i = 0; i < thread_count; i++) {
                current_dl += args[i].bytes_downloaded;
                if (!args[i].finished) {
                    all_done = 0;
                    any_alive = 1;
                }
                if (args[i].error) {
                    any_error = 1;
                }
            }

            pthread_mutex_lock(&g_mutex);
            g_status.downloaded_bytes = current_dl;
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

            if (total_size > 0 && current_dl >= total_size) {
                all_done = 1;
            }
            pthread_mutex_unlock(&g_mutex);

            if (all_done) break;

            if (any_error) {
                pthread_mutex_lock(&g_mutex);
                g_status.state = STATUS_ERROR;
                snprintf(g_status.error_message, sizeof(g_status.error_message), "Worker connection error or chunk write failure");
                pthread_mutex_unlock(&g_mutex);
                break;
            }

            if (!any_alive) break;
        }

        for (int i = 0; i < thread_count; i++) {
            pthread_join(threads[i], NULL);
        }

        free(threads);
        free(args);
    } else {
        // Single-stream download via libcurl with 2 MB write buffer (handles non-Range servers like VikingFile)
        single_ctx_t ctx;
        memset(&ctx, 0, sizeof(ctx));
        ctx.file_fd = file_fd;
        ctx.write_buf = malloc(SINGLE_WRITE_BUF_SIZE);
        ctx.write_buf_len = 0;
        ctx.bytes_downloaded = 0;
        ctx.last_speed_time = get_time_seconds();
        ctx.last_bytes = 0;

        CURL *single_curl = curl_easy_init();
        if (single_curl) {
            curl_easy_setopt(single_curl, CURLOPT_URL, target_url);
            curl_easy_setopt(single_curl, CURLOPT_WRITEFUNCTION, single_stream_write_callback);
            curl_easy_setopt(single_curl, CURLOPT_WRITEDATA, &ctx);
            curl_easy_setopt(single_curl, CURLOPT_FOLLOWLOCATION, 1L);
            curl_easy_setopt(single_curl, CURLOPT_MAXREDIRS, 10L);
            curl_easy_setopt(single_curl, CURLOPT_SSL_VERIFYPEER, 0L);
            curl_easy_setopt(single_curl, CURLOPT_SSL_VERIFYHOST, 0L);
            curl_easy_setopt(single_curl, CURLOPT_USERAGENT, "Mozilla/5.0 (PlayStation 5; PS5-Net-Downloader-Turbo/1.5)");
            curl_easy_setopt(single_curl, CURLOPT_BUFFERSIZE, CHUNK_BUFFER_SIZE);
            curl_easy_setopt(single_curl, CURLOPT_TCP_NODELAY, 1L);
            curl_easy_setopt(single_curl, CURLOPT_SOCKOPTFUNCTION, curl_sockopt_cb);
            curl_easy_setopt(single_curl, CURLOPT_NOSIGNAL, 1L);
            curl_easy_setopt(single_curl, CURLOPT_CONNECTTIMEOUT, 15L);
            curl_easy_setopt(single_curl, CURLOPT_LOW_SPEED_LIMIT, 1024L);
            curl_easy_setopt(single_curl, CURLOPT_LOW_SPEED_TIME, 30L);
            curl_easy_setopt(single_curl, CURLOPT_NOPROGRESS, 0L);
            curl_easy_setopt(single_curl, CURLOPT_XFERINFOFUNCTION, single_stream_xferinfo_callback);
            curl_easy_setopt(single_curl, CURLOPT_XFERINFODATA, &ctx);

            curl_easy_setopt(single_curl, CURLOPT_SSL_CIPHER_LIST, "ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305:ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:HIGH:!aNULL:!MD5:!RC4");
#ifdef CURLOPT_TLS13_CIPHERS
            curl_easy_setopt(single_curl, CURLOPT_TLS13_CIPHERS, "TLS_CHACHA20_POLY1305_SHA256:TLS_AES_128_GCM_SHA256:TLS_AES_256_GCM_SHA384");
#endif

            CURLcode res = curl_easy_perform(single_curl);
            if (res != CURLE_OK && res != CURLE_ABORTED_BY_CALLBACK && res != CURLE_WRITE_ERROR) {
                pthread_mutex_lock(&g_mutex);
                g_status.state = STATUS_ERROR;
                snprintf(g_status.error_message, sizeof(g_status.error_message), "Download failed: %s", curl_easy_strerror(res));
                pthread_mutex_unlock(&g_mutex);
            }

            // Flush remaining single stream buffer
            if (ctx.write_buf && ctx.write_buf_len > 0 && !g_abort_flag) {
                write(file_fd, ctx.write_buf, ctx.write_buf_len);
                ctx.write_buf_len = 0;
            }

            pthread_mutex_lock(&g_mutex);
            g_status.downloaded_bytes = ctx.bytes_downloaded;
            pthread_mutex_unlock(&g_mutex);

            curl_easy_cleanup(single_curl);
        }

        if (ctx.write_buf) {
            free(ctx.write_buf);
        }
    }

    close(file_fd);

    pthread_mutex_lock(&g_mutex);
    if (g_abort_flag) {
        g_status.state = STATUS_IDLE;
        g_status.speed_bytes_sec = 0.0;
        g_status.eta_seconds = 0;
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
    g_abort_flag = 0;
    pthread_mutex_unlock(&g_mutex);

    return NULL;
}

void downloader_init(void) {
    pthread_mutex_lock(&g_mutex);
    memset(&g_status, 0, sizeof(g_status));
    g_status.state = STATUS_IDLE;
    g_abort_flag = 0;
    g_is_running = 0;
    g_requested_threads = 8;
    pthread_mutex_unlock(&g_mutex);

    curl_global_init(CURL_GLOBAL_ALL);
}

int downloader_start(const char *url, const char *save_dir, const char *filename, int threads) {
    pthread_mutex_lock(&g_mutex);
    // If previous download was aborting, wait up to 3 seconds for workers to exit cleanly
    int wait_count = 0;
    while (g_is_running && g_abort_flag && wait_count < 30) {
        pthread_mutex_unlock(&g_mutex);
        usleep(100000);
        pthread_mutex_lock(&g_mutex);
        wait_count++;
    }

    if (g_is_running) {
        pthread_mutex_unlock(&g_mutex);
        return -1;
    }

    g_abort_flag = 0;
    g_is_running = 1;
    g_requested_threads = (threads >= 1 && threads <= MAX_THREADS) ? threads : 8;

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
        g_status.state = STATUS_ABORTING;
    }
    pthread_mutex_unlock(&g_mutex);
}

void downloader_reset(void) {
    pthread_mutex_lock(&g_mutex);
    if (g_is_running) {
        g_abort_flag = 1;
        g_status.state = STATUS_ABORTING;
    }
    pthread_mutex_unlock(&g_mutex);

    int wait_count = 0;
    while (g_is_running && wait_count < 20) {
        usleep(100000);
        wait_count++;
    }

    pthread_mutex_lock(&g_job_mutex);
    g_job_next_offset = 0;
    g_job_total_size = 0;
    pthread_mutex_unlock(&g_job_mutex);

    pthread_mutex_lock(&g_mutex);
    g_abort_flag = 0;
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
