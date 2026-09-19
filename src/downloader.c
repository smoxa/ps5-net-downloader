#include "downloader.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <fcntl.h>
#include <errno.h>

#define CHUNK_BUFFER_SIZE (1024 * 1024) // 1 MB chunk buffer
#define MAX_THREADS 8

typedef struct {
    int thread_id;
    int file_fd;
    char host[256];
    int port;
    char path[1024];
    uint64_t start_byte;
    uint64_t end_byte;
    uint64_t bytes_downloaded;
    int error;
} worker_args_t;

static download_status_t g_status;
static pthread_mutex_t g_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_t g_master_thread;
static volatile int g_abort_flag = 0;
static volatile int g_is_running = 0;
static int g_requested_threads = 4;

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
    if (tmp[len - 1] == '/') {
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

static int parse_url(const char *url, char *host, int *port, char *path) {
    const char *p = url;
    if (strncmp(p, "http://", 7) == 0) {
        p += 7;
    } else if (strncmp(p, "https://", 8) == 0) {
        p += 8;
    }

    const char *slash = strchr(p, '/');
    const char *colon = strchr(p, ':');

    *port = 80;

    if (slash == NULL) {
        if (colon != NULL) {
            size_t host_len = colon - p;
            strncpy(host, p, host_len);
            host[host_len] = '\0';
            *port = atoi(colon + 1);
        } else {
            strcpy(host, p);
        }
        strcpy(path, "/");
        return 0;
    }

    if (colon != NULL && colon < slash) {
        size_t host_len = colon - p;
        strncpy(host, p, host_len);
        host[host_len] = '\0';
        *port = atoi(colon + 1);
    } else {
        size_t host_len = slash - p;
        strncpy(host, p, host_len);
        host[host_len] = '\0';
    }

    strcpy(path, slash);
    return 0;
}

static void extract_filename_from_url(const char *url, char *out_filename, size_t max_len) {
    const char *last_slash = strrchr(url, '/');
    if (last_slash && *(last_slash + 1) != '\0') {
        const char *name = last_slash + 1;
        const char *query = strchr(name, '?');
        if (query) {
            size_t len = query - name;
            if (len >= max_len) len = max_len - 1;
            strncpy(out_filename, name, len);
            out_filename[len] = '\0';
        } else {
            strncpy(out_filename, name, max_len - 1);
            out_filename[max_len - 1] = '\0';
        }
    } else {
        snprintf(out_filename, max_len, "download_%ld.pkg", (long)time(NULL));
    }
}

static void optimize_socket(int sockfd) {
    int rcvbuf = 4 * 1024 * 1024; // 4 MB TCP receive buffer
    setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
    int nodelay = 1;
    setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));
}

static int connect_to_host(const char *host, int port) {
    struct hostent *server = gethostbyname(host);
    if (!server) {
        return -1;
    }

    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        return -1;
    }

    optimize_socket(sockfd);

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    memcpy(&serv_addr.sin_addr.s_addr, server->h_addr, server->h_length);
    serv_addr.sin_port = htons(port);

    if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        close(sockfd);
        return -1;
    }

    return sockfd;
}

static int read_http_headers(int sockfd, char *header_buf, size_t max_len, int *status_code, uint64_t *content_length, int *supports_range) {
    size_t header_bytes = 0;
    *status_code = 0;
    *content_length = 0;
    *supports_range = 0;

    while (header_bytes < max_len - 1) {
        int r = recv(sockfd, header_buf + header_bytes, 1, 0);
        if (r <= 0) break;
        header_bytes += r;
        header_buf[header_bytes] = '\0';
        if (strstr(header_buf, "\r\n\r\n")) {
            break;
        }
    }

    sscanf(header_buf, "HTTP/%*f %d", status_code);

    char *cl_pos = strcasestr(header_buf, "Content-Length:");
    if (cl_pos) {
        *content_length = strtoull(cl_pos + 15, NULL, 10);
    }

    char *cr_pos = strcasestr(header_buf, "Content-Range:");
    if (cr_pos) {
        char *slash = strchr(cr_pos, '/');
        if (slash) {
            *content_length = strtoull(slash + 1, NULL, 10);
            *supports_range = 1;
        }
    }

    if (strcasestr(header_buf, "Accept-Ranges: bytes") || *status_code == 206) {
        *supports_range = 1;
    }

    return 0;
}

static void *parallel_chunk_worker(void *arg) {
    worker_args_t *args = (worker_args_t *)arg;

    int sockfd = connect_to_host(args->host, args->port);
    if (sockfd < 0) {
        args->error = 1;
        return NULL;
    }

    char req[2048];
    int req_len = snprintf(req, sizeof(req),
        "GET %s HTTP/1.1\r\n"
        "Host: %s\r\n"
        "User-Agent: PS5-Net-Downloader-Turbo/1.1\r\n"
        "Range: bytes=%llu-%llu\r\n"
        "Connection: close\r\n\r\n",
        args->path, args->host,
        (unsigned long long)args->start_byte,
        (unsigned long long)args->end_byte);

    send(sockfd, req, req_len, 0);

    char header_buf[4096];
    int status_code = 0;
    uint64_t dummy_cl = 0;
    int dummy_range = 0;
    read_http_headers(sockfd, header_buf, sizeof(header_buf), &status_code, &dummy_cl, &dummy_range);

    if (status_code != 200 && status_code != 206) {
        close(sockfd);
        args->error = 1;
        return NULL;
    }

    char *chunk = malloc(CHUNK_BUFFER_SIZE);
    if (!chunk) {
        close(sockfd);
        args->error = 1;
        return NULL;
    }

    uint64_t current_offset = args->start_byte;
    uint64_t total_expected = (args->end_byte - args->start_byte) + 1;

    while (!g_abort_flag && args->bytes_downloaded < total_expected) {
        size_t to_read = CHUNK_BUFFER_SIZE;
        if (to_read > (total_expected - args->bytes_downloaded)) {
            to_read = total_expected - args->bytes_downloaded;
        }

        ssize_t n = recv(sockfd, chunk, to_read, 0);
        if (n <= 0) {
            break;
        }

        ssize_t written = pwrite(args->file_fd, chunk, n, current_offset);
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

    free(chunk);
    close(sockfd);
    return NULL;
}

static void *master_download_thread(void *arg) {
    (void)arg;
    char host[256];
    int port = 80;
    char path[1024];

    pthread_mutex_lock(&g_mutex);
    g_status.state = STATUS_CONNECTING;
    g_status.downloaded_bytes = 0;
    g_status.total_bytes = 0;
    g_status.speed_bytes_sec = 0.0;
    g_status.eta_seconds = 0;
    g_status.error_message[0] = '\0';
    g_status.num_threads = 1;
    pthread_mutex_unlock(&g_mutex);

    parse_url(g_status.url, host, &port, path);

    // Initial probe connection
    int probe_fd = connect_to_host(host, port);
    if (probe_fd < 0) {
        pthread_mutex_lock(&g_mutex);
        g_status.state = STATUS_ERROR;
        snprintf(g_status.error_message, sizeof(g_status.error_message), "Failed to connect to %s:%d", host, port);
        g_is_running = 0;
        pthread_mutex_unlock(&g_mutex);
        return NULL;
    }

    // Send probe request with Range
    char probe_req[2048];
    int probe_len = snprintf(probe_req, sizeof(probe_req),
        "GET %s HTTP/1.1\r\n"
        "Host: %s\r\n"
        "User-Agent: PS5-Net-Downloader-Turbo/1.1\r\n"
        "Range: bytes=0-0\r\n"
        "Connection: close\r\n\r\n",
        path, host);
    send(probe_fd, probe_req, probe_len, 0);

    char probe_headers[4096];
    int status_code = 0;
    uint64_t total_size = 0;
    int supports_range = 0;
    read_http_headers(probe_fd, probe_headers, sizeof(probe_headers), &status_code, &total_size, &supports_range);
    close(probe_fd);

    if (total_size == 0 || (status_code != 200 && status_code != 206)) {
        // Fallback: standard GET request to determine size
        int fallback_fd = connect_to_host(host, port);
        if (fallback_fd >= 0) {
            char f_req[2048];
            int f_len = snprintf(f_req, sizeof(f_req),
                "GET %s HTTP/1.1\r\n"
                "Host: %s\r\n"
                "User-Agent: PS5-Net-Downloader-Turbo/1.1\r\n"
                "Connection: close\r\n\r\n",
                path, host);
            send(fallback_fd, f_req, f_len, 0);
            read_http_headers(fallback_fd, probe_headers, sizeof(probe_headers), &status_code, &total_size, &supports_range);
            close(fallback_fd);
        }
    }

    if (status_code != 200 && status_code != 206) {
        pthread_mutex_lock(&g_mutex);
        g_status.state = STATUS_ERROR;
        snprintf(g_status.error_message, sizeof(g_status.error_message), "Server returned HTTP status %d", status_code);
        g_is_running = 0;
        pthread_mutex_unlock(&g_mutex);
        return NULL;
    }

    // Open file for writing
    int file_fd = open(g_status.save_path, O_RDWR | O_CREAT | O_TRUNC, 0666);
    if (file_fd < 0) {
        pthread_mutex_lock(&g_mutex);
        g_status.state = STATUS_ERROR;
        snprintf(g_status.error_message, sizeof(g_status.error_message), "Cannot create file: %s", g_status.save_path);
        g_is_running = 0;
        pthread_mutex_unlock(&g_mutex);
        return NULL;
    }

    // Pre-allocate file size if known
    if (total_size > 0) {
        ftruncate(file_fd, total_size);
    }

    int thread_count = g_requested_threads;
    if (!supports_range || total_size < (uint64_t)(thread_count * 1024 * 1024)) {
        thread_count = 1; // Fallback to single stream
    }
    if (thread_count > MAX_THREADS) thread_count = MAX_THREADS;

    pthread_mutex_lock(&g_mutex);
    g_status.state = STATUS_DOWNLOADING;
    g_status.total_bytes = total_size;
    g_status.num_threads = thread_count;
    pthread_mutex_unlock(&g_mutex);

    pthread_t threads[MAX_THREADS];
    worker_args_t args[MAX_THREADS];
    uint64_t segment_size = (thread_count > 0 && total_size > 0) ? (total_size / thread_count) : 0;

    double start_time = get_time_seconds();
    double last_speed_time = start_time;
    uint64_t last_downloaded_bytes = 0;

    if (thread_count > 1 && total_size > 0) {
        // Multi-threaded parallel download
        for (int i = 0; i < thread_count; i++) {
            args[i].thread_id = i;
            args[i].file_fd = file_fd;
            strncpy(args[i].host, host, sizeof(args[i].host) - 1);
            args[i].port = port;
            strncpy(args[i].path, path, sizeof(args[i].path) - 1);
            args[i].start_byte = i * segment_size;
            args[i].end_byte = (i == thread_count - 1) ? (total_size - 1) : ((i + 1) * segment_size - 1);
            args[i].bytes_downloaded = 0;
            args[i].error = 0;

            pthread_create(&threads[i], NULL, parallel_chunk_worker, &args[i]);
        }

        // Monitoring loop
        while (!g_abort_flag) {
            usleep(500000); // 500 ms

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

            int all_done = (current_dl >= total_size);
            pthread_mutex_unlock(&g_mutex);

            if (all_done) break;
        }

        for (int i = 0; i < thread_count; i++) {
            pthread_join(threads[i], NULL);
        }
    } else {
        // Single thread fallback
        int stream_fd = connect_to_host(host, port);
        if (stream_fd >= 0) {
            char req[2048];
            int r_len = snprintf(req, sizeof(req),
                "GET %s HTTP/1.1\r\n"
                "Host: %s\r\n"
                "User-Agent: PS5-Net-Downloader-Turbo/1.1\r\n"
                "Connection: close\r\n\r\n",
                path, host);
            send(stream_fd, req, r_len, 0);

            char h_buf[4096];
            int s_code = 0;
            uint64_t c_len = 0;
            int s_range = 0;
            read_http_headers(stream_fd, h_buf, sizeof(h_buf), &s_code, &c_len, &s_range);

            char *chunk = malloc(CHUNK_BUFFER_SIZE);
            if (chunk) {
                while (!g_abort_flag) {
                    ssize_t n = recv(stream_fd, chunk, CHUNK_BUFFER_SIZE, 0);
                    if (n <= 0) break;

                    write(file_fd, chunk, n);

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
                free(chunk);
            }
            close(stream_fd);
        }
    }

    close(file_fd);

    pthread_mutex_lock(&g_mutex);
    if (g_abort_flag) {
        g_status.state = STATUS_IDLE;
        unlink(g_status.save_path);
    } else if (g_status.state != STATUS_ERROR) {
        g_status.state = STATUS_COMPLETED;
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

    if (filename && filename[0] != '\0') {
        strncpy(g_status.filename, filename, sizeof(g_status.filename) - 1);
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

void downloader_get_status(download_status_t *out_status) {
    pthread_mutex_lock(&g_mutex);
    memcpy(out_status, &g_status, sizeof(download_status_t));
    pthread_mutex_unlock(&g_mutex);
}
