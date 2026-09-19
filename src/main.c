#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>

#include "downloader.h"
#include "server.h"

#define DEFAULT_PORT 8080

static void handle_signal(int sig) {
    (void)sig;
    printf("[PS5-ND] Terminating server...\n");
    downloader_abort();
    server_stop();
    exit(0);
}

int main(int argc, char *argv[]) {
    int port = DEFAULT_PORT;
    if (argc > 1) {
        port = atoi(argv[1]);
        if (port <= 0 || port > 65535) {
            port = DEFAULT_PORT;
        }
    }

    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);

    printf("========================================\n");
    printf("  PS5 Net Downloader Turbo Edition v1.4 \n");
    printf("========================================\n");

    downloader_init();

    printf("[PS5-ND] Initializing HTTP server on port %d...\n", port);
    printf("[PS5-ND] Open http://<PS5_IP>:%d in your browser.\n", port);

    server_start(port);

    return 0;
}
