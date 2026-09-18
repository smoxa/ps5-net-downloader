#include "downloader.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <errno.h>

#define CHUNK_BUFFER_SIZE (128 * 1024) // 128 KB buffer

static download_status_t g_status;
static pthread_mutex_t g_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_t g_download_thread;
static volatile int g_abort_flag = 0;
static volatile int g_is_running = 0;

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
        // HTTPS would require TLS wrapper like mbedtls/openssl
        p += 8;
    }

    const char *slash = strchr(p, '/');
    const char *colon = strchr(p, ':');

    *port = 80;

    if (slash == NULL) {
        // No path, e.g. "http://example.com"
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
        // Strip query string if present
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

static void *download_worker(void *arg) {
    (void)arg;
    char host[256];
    int port = 80;
    char path[1024];

    pthread_mutex_lock(&g_mutex);
    g_status.state = STATUS_CONNECTING;
    g_status.downloaded_bytes = 0;
    g_status.total_bytes = 0;
    g_status.speed_bytes_sec = 0.0;
    g_status.error_message[0] = '\0';
    pthread_mutex_unlock(&g_mutex);

    parse_url(g_status.url, host, &port, path);

    // Resolve host
    struct hostent *server = gethostbyname(host);
    if (!server) {
        pthread_mutex_lock(&g_mutex);
        g_status.state = STATUS_ERROR;
        snprintf(g_status.error_message, sizeof(g_status.error_message), "Failed to resolve host: %s", host);
        g_is_running = 0;
        pthread_mutex_unlock(&g_mutex);
        return NULL;
    }

    // Create socket
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        pthread_mutex_lock(&g_mutex);
        g_status.state = STATUS_ERROR;
        snprintf(g_status.error_message, sizeof(g_status.error_message), "Failed to create socket");
        g_is_running = 0;
        pthread_mutex_unlock(&g_mutex);
        return NULL;
    }

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    memcpy(&serv_addr.sin_addr.s_addr, server->h_addr, server->h_length);
    serv_addr.sin_port = htons(port);

    // Connect
    if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        close(sockfd);
        pthread_mutex_lock(&g_mutex);
        g_status.state = STATUS_ERROR;
        snprintf(g_status.error_message, sizeof(g_status.error_message), "Connection failed to %s:%d", host, port);
        g_is_running = 0;
        pthread_mutex_unlock(&g_mutex);
        return NULL;
    }

    // Send HTTP GET Request
    char req[2048];
    int req_len = snprintf(req, sizeof(req),
        "GET %s HTTP/1.1\r\n"
        "Host: %s\r\n"
        "User-Agent: PS5-Net-Downloader/1.0\r\n"
        "Connection: close\r\n\r\n",
        path, host);

    send(sockfd, req, req_len, 0);

    // Read header line by line
    char header_buf[4096];
    int header_bytes = 0;
    int body_start_offset = 0;
    while (header_bytes < (int)sizeof(header_buf) - 1) {
        int r = recv(sockfd, header_buf + header_bytes, 1, 0);
        if (r <= 0) break;
        header_bytes += r;
        header_buf[header_bytes] = '\0';
        char *end_of_headers = strstr(header_buf, "\r\n\r\n");
        if (end_of_headers) {
            body_start_offset = (end_of_headers - header_buf) + 4;
            break;
        }
    }

    // Check HTTP Status Code
    int http_code = 0;
    sscanf(header_buf, "HTTP/%*f %d", &http_code);
    if (http_code != 200 && http_code != 206) {
        close(sockfd);
        pthread_mutex_lock(&g_mutex);
        g_status.state = STATUS_ERROR;
        snprintf(g_status.error_message, sizeof(g_status.error_message), "HTTP error code: %d", http_code);
        g_is_running = 0;
        pthread_mutex_unlock(&g_mutex);
        return NULL;
    }

    // Parse Content-Length
    uint64_t total_size = 0;
    char *cl_pos = strstr(header_buf, "Content-Length:");
    if (!cl_pos) cl_pos = strstr(header_buf, "content-length:");
    if (cl_pos) {
        total_size = strtoull(cl_pos + 15, NULL, 10);
    }

    // Open output file
    FILE *fp = fopen(g_status.save_path, "wb");
    if (!fp) {
        close(sockfd);
        pthread_mutex_lock(&g_mutex);
        g_status.state = STATUS_ERROR;
        snprintf(g_status.error_message, sizeof(g_status.error_message), "Cannot open file: %s", g_status.save_path);
        g_is_running = 0;
        pthread_mutex_unlock(&g_mutex);
        return NULL;
    }

    pthread_mutex_lock(&g_mutex);
    g_status.state = STATUS_DOWNLOADING;
    g_status.total_bytes = total_size;
    pthread_mutex_unlock(&g_mutex);

    // Stream download
    char *chunk = malloc(CHUNK_BUFFER_SIZE);
    if (!chunk) {
        fclose(fp);
        close(sockfd);
        pthread_mutex_lock(&g_mutex);
        g_status.state = STATUS_ERROR;
        snprintf(g_status.error_message, sizeof(g_status.error_message), "Out of memory allocating buffer");
        g_is_running = 0;
        pthread_mutex_unlock(&g_mutex);
        return NULL;
    }

    double start_time = get_time_seconds();
    double last_speed_time = start_time;
    uint64_t bytes_since_last_speed = 0;

    while (!g_abort_flag) {
        ssize_t n = recv(sockfd, chunk, CHUNK_BUFFER_SIZE, 0);
        if (n <= 0) {
            break; // Finished or closed
        }

        size_t written = fwrite(chunk, 1, n, fp);
        if (written != (size_t)n) {
            pthread_mutex_lock(&g_mutex);
            g_status.state = STATUS_ERROR;
            snprintf(g_status.error_message, sizeof(g_status.error_message), "Write error (disk full?)");
            pthread_mutex_unlock(&g_mutex);
            break;
        }

        pthread_mutex_lock(&g_mutex);
        g_status.downloaded_bytes += n;
        bytes_since_last_speed += n;

        double now = get_time_seconds();
        double elapsed = now - last_speed_time;
        if (elapsed >= 0.5) {
            g_status.speed_bytes_sec = (double)bytes_since_last_speed / elapsed;
            last_speed_time = now;
            bytes_since_last_speed = 0;
        }
        pthread_mutex_unlock(&g_mutex);
    }

    free(chunk);
    fclose(fp);
    close(sockfd);

    pthread_mutex_lock(&g_mutex);
    if (g_abort_flag) {
        g_status.state = STATUS_IDLE;
        remove(g_status.save_path); // Remove partial download on abort
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
    pthread_mutex_unlock(&g_mutex);
}

int downloader_start(const char *url, const char *save_dir, const char *filename) {
    pthread_mutex_lock(&g_mutex);
    if (g_is_running) {
        pthread_mutex_unlock(&g_mutex);
        return -1; // Already downloading
    }

    g_abort_flag = 0;
    g_is_running = 1;

    strncpy(g_status.url, url, sizeof(g_status.url) - 1);

    if (filename && filename[0] != '\0') {
        strncpy(g_status.filename, filename, sizeof(g_status.filename) - 1);
    } else {
        extract_filename_from_url(url, g_status.filename, sizeof(g_status.filename));
    }

    ensure_directory(save_dir);

    // Build full path
    snprintf(g_status.save_path, sizeof(g_status.save_path), "%s/%s", 
             (save_dir[strlen(save_dir) - 1] == '/') ? save_dir : save_dir, 
             g_status.filename);

    pthread_create(&g_download_thread, NULL, download_worker, NULL);
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
