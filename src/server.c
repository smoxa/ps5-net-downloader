#include "server.h"
#include "downloader.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <errno.h>
#include <sys/time.h>

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

static int g_server_fd = -1;
static volatile int g_server_running = 1;

// Embedded HTML UI
static const char INDEX_HTML[] = 
"<!DOCTYPE html><html lang=\"ru\"><head><meta charset=\"UTF-8\">"
"<meta name=\"viewport\" content=\"width=device-width,initial-scale=1.0\">"
"<title>PS5 Net Downloader Turbo v1.7</title><style>"
":root{--bg-base:#0a0e17;--bg-card:#131b2e;--bg-input:#1b2640;--accent:#0070d1;--accent-hover:#0084f7;--accent-glow:rgba(0,112,209,0.4);--text-main:#f0f4fc;--text-muted:#8c9bb5;--border:#233354;--success:#00d26a;--error:#f8312f;}"
"*{box-sizing:border-box;margin:0;padding:0;font-family:-apple-system,BlinkMacSystemFont,\"Segoe UI\",Roboto,sans-serif;}"
"body{background:var(--bg-base);color:var(--text-main);min-height:100vh;display:flex;flex-direction:column;align-items:center;padding:24px 16px;}"
".container{width:100%;max-width:680px;margin-top:10px;}"
"header{display:flex;align-items:center;justify-content:space-between;margin-bottom:24px;padding-bottom:16px;border-bottom:1px solid var(--border);}"
".brand{display:flex;align-items:center;gap:12px;}"
".ps-logo{width:40px;height:40px;background:linear-gradient(135deg,var(--accent),#00b4d8);border-radius:10px;display:flex;align-items:center;justify-content:center;font-weight:900;font-size:20px;box-shadow:0 0 20px var(--accent-glow);}"
"h1{font-size:22px;font-weight:700;}.subtitle{font-size:11px;color:#00b4d8;font-weight:700;text-transform:uppercase;letter-spacing:1.5px;}"
".status-badge{font-size:12px;padding:5px 14px;border-radius:20px;font-weight:600;text-transform:uppercase;letter-spacing:0.8px;background:var(--border);color:var(--text-muted);}"
".status-badge.idle{background:rgba(140,155,181,0.15);color:var(--text-muted);}"
".status-badge.downloading{background:rgba(0,112,209,0.2);color:#4da3ff;border:1px solid var(--accent);}"
".status-badge.completed{background:rgba(0,210,106,0.15);color:var(--success);}"
".status-badge.error{background:rgba(248,49,47,0.15);color:var(--error);}"
".card{background:var(--bg-card);border:1px solid var(--border);border-radius:14px;padding:24px;margin-bottom:20px;box-shadow:0 10px 30px rgba(0,0,0,0.3);}"
".card-title{font-size:16px;font-weight:600;margin-bottom:18px;color:var(--text-main);display:flex;align-items:center;justify-content:space-between;}"
".thread-badge{font-size:11px;background:rgba(0,180,216,0.2);color:#00d2ff;padding:3px 8px;border-radius:6px;border:1px solid rgba(0,180,216,0.4);font-weight:600;}"
".form-group{margin-bottom:16px;}label{display:block;font-size:13px;font-weight:500;color:var(--text-muted);margin-bottom:6px;}"
"input[type=\"text\"],input[type=\"url\"],select{width:100%;background:var(--bg-input);border:1px solid var(--border);border-radius:8px;padding:12px 14px;color:var(--text-main);font-size:14px;outline:none;}"
"input[type=\"text\"]:focus,input[type=\"url\"]:focus,select:focus{border-color:var(--accent);box-shadow:0 0 0 3px var(--accent-glow);}"
".btn{display:inline-flex;align-items:center;justify-content:center;padding:14px 20px;border-radius:8px;font-size:14px;font-weight:700;cursor:pointer;border:none;transition:all 0.2s;}"
".btn-primary{background:linear-gradient(135deg,var(--accent),#0084f7);color:#fff;width:100%;margin-top:8px;box-shadow:0 4px 18px var(--accent-glow);}"
".btn-primary:hover:not(:disabled){filter:brightness(1.1);transform:translateY(-1px);}"
".btn-primary:disabled{opacity:0.5;cursor:not-allowed;}"
".btn-danger{background:rgba(248,49,47,0.15);color:var(--error);border:1px solid rgba(248,49,47,0.4);width:100%;}"
".btn-secondary{background:rgba(140,155,181,0.15);color:var(--text-muted);border:1px solid var(--border);width:100%;}"
".btn-secondary:hover{background:rgba(140,155,181,0.25);color:var(--text-main);}"
".progress-box{display:none;margin-top:10px;}.progress-box.active{display:block;}"
".file-info{display:flex;justify-content:space-between;font-size:13px;margin-bottom:8px;word-break:break-all;}"
".progress-bar-container{width:100%;height:14px;background:var(--bg-input);border-radius:7px;overflow:hidden;position:relative;}"
".progress-bar{height:100%;width:0%;background:linear-gradient(90deg,var(--accent),#00d2ff);border-radius:7px;transition:width 0.3s ease;box-shadow:0 0 10px rgba(0,180,216,0.5);}"
".stats-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-top:16px;text-align:center;}"
".stat-item{background:var(--bg-input);padding:10px 6px;border-radius:8px;border:1px solid var(--border);}"
".stat-label{font-size:10px;color:var(--text-muted);text-transform:uppercase;margin-bottom:4px;}"
".stat-value{font-size:13px;font-weight:700;color:var(--text-main);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}"
".help-box{font-size:12px;color:var(--text-muted);line-height:1.6;}"
"</style></head><body><div class=\"container\">"
"<header><div class=\"brand\"><div class=\"ps-logo\">PS</div><div><h1>Net Downloader</h1><div class=\"subtitle\">Turbo Dynamic Multi-Threaded v1.7</div></div></div><div id=\"statusBadge\" class=\"status-badge idle\">Ожидание</div></header>"
"<div class=\"card\"><div class=\"card-title\">Новая загрузка</div>"
"<div class=\"form-group\"><label>Прямая ссылка на файл (HTTP / HTTPS):</label><input type=\"url\" id=\"fileUrl\" placeholder=\"https://...\" required></div>"
"<div style=\"display:grid;grid-template-columns:2fr 1fr;gap:12px;\"><div class=\"form-group\"><label>Директория на PS5:</label><input type=\"text\" id=\"savePath\" value=\"/data/pkg/\" placeholder=\"/data/pkg/\"></div>"
"<div class=\"form-group\"><label>Потоки (Turbo):</label><select id=\"threadsSelect\"><option value=\"8\" selected>8 потоков (Рекомендуется Turbo)</option><option value=\"16\">16 потоков (Ultra Turbo)</option><option value=\"4\">4 потока (Быстро)</option><option value=\"32\">32 потока (Max Power)</option><option value=\"1\">1 поток (Обычный)</option></select></div></div>"
"<div class=\"form-group\"><label>Имя файла (опционально):</label><input type=\"text\" id=\"customFilename\" placeholder=\"Автоопределение из ссылки\"></div>"
"<button id=\"startBtn\" class=\"btn btn-primary\" onclick=\"startDownload()\">⚡ Начать Turbo-загрузку на PS5</button></div>"
"<div id=\"progressCard\" class=\"card progress-box\"><div class=\"card-title\"><span>Текущая загрузка</span><span id=\"activeThreadsBadge\" class=\"thread-badge\">8 потоков (Turbo)</span></div>"
"<div class=\"file-info\"><span id=\"currentFileName\" style=\"font-weight:600;\">game.pkg</span><span id=\"percentText\" style=\"font-weight:700;color:#00d2ff;\">0%</span></div>"
"<div class=\"progress-bar-container\"><div id=\"progressBar\" class=\"progress-bar\"></div></div>"
"<div class=\"stats-grid\"><div class=\"stat-item\"><div class=\"stat-label\">Размер</div><div id=\"sizeStat\" class=\"stat-value\">0 / 0 MB</div></div>"
"<div class=\"stat-item\"><div class=\"stat-label\">Скорость</div><div id=\"speedStat\" class=\"stat-value\" style=\"color:#00d2ff;\">0 MB/s</div></div>"
"<div class=\"stat-item\"><div class=\"stat-label\">Осталось</div><div id=\"etaStat\" class=\"stat-value\">--</div></div>"
"<div class=\"stat-item\"><div class=\"stat-label\">Статус</div><div id=\"stateStat\" class=\"stat-value\">Подключение...</div></div></div>"
"<div style=\"display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:14px;\"><button id=\"abortBtn\" class=\"btn btn-danger\" onclick=\"abortDownload()\">Прервать</button><button id=\"resetBtn\" class=\"btn btn-secondary\" onclick=\"resetDownload()\">Сбросить</button></div></div>"
"<div class=\"card help-box\"><p>• <b>Dynamic Work-Stealing Queue</b>: файл динамически нарезается на блоки по 32 MB. Все потоки работают на полную мощность вплоть до 99.9% без просадок скорости.</p><p>• <b>Turbo RAM-Coalesced I/O</b>: Буферизация 2 MB в RAM устраняет дисковые блокировки UFS/PFS.</p><p>• <b>Тест максимальной скорости сети</b>: <a href=\"javascript:void(0)\" onclick=\"setTestUrl('http://speedtest.selectel.ru/10GB')\" style=\"color:#00d2ff;text-decoration:underline;\">Вставить HTTP 10GB</a> | <a href=\"javascript:void(0)\" onclick=\"setTestUrl('https://speedtest.selectel.ru/10GB')\" style=\"color:#00d2ff;text-decoration:underline;\">HTTPS 10GB</a></p></div></div>"
"<script>"
"let lastState='';"
"function setTestUrl(u){document.getElementById('fileUrl').value=u;document.getElementById('customFilename').value='speedtest_10gb.bin';}"
"function formatBytes(b){if(!b||b<=0)return'0 B';const k=1024,s=['B','KB','MB','GB','TB'],i=Math.floor(Math.log(b)/Math.log(k));return parseFloat((b/Math.pow(k,i)).toFixed(2))+' '+s[i];}"
"function formatEta(s){if(!s||s<=0)return'--';if(s<60)return s+' сек';const m=Math.floor(s/60);if(m<60)return m+' мин '+(s%60)+' с';return Math.floor(m/60)+' ч '+(m%60)+' мин';}"
"async function fetchStatus(){try{const r=await fetch('/api/status');if(!r.ok)return;const d=await r.json();updateUI(d);}catch(e){}}"
"function updateUI(d){const badge=document.getElementById('statusBadge'),card=document.getElementById('progressCard'),btn=document.getElementById('startBtn'),bar=document.getElementById('progressBar'),pText=document.getElementById('percentText'),fName=document.getElementById('currentFileName'),sStat=document.getElementById('sizeStat'),spStat=document.getElementById('speedStat'),etaStat=document.getElementById('etaStat'),stStat=document.getElementById('stateStat'),tBadge=document.getElementById('activeThreadsBadge'),abBtn=document.getElementById('abortBtn');"
"if(d.status==='downloading'||d.status==='connecting'){badge.className='status-badge downloading';badge.textContent=(d.status==='connecting')?'Подключение':'Скачивание';card.classList.add('active');btn.disabled=true;if(abBtn){abBtn.disabled=false;abBtn.textContent='Прервать';}"
"const p=d.total_bytes>0?Math.min(100,(d.downloaded_bytes/d.total_bytes*100)).toFixed(1):0;bar.style.width=p+'%';pText.textContent=p+'%';fName.textContent=d.filename||'Загрузка...';"
"sStat.textContent=formatBytes(d.downloaded_bytes)+(d.total_bytes>0?' / '+formatBytes(d.total_bytes):'');spStat.textContent=(d.speed_bytes_sec/(1024*1024)).toFixed(2)+' MB/s';etaStat.textContent=formatEta(d.eta_seconds);stStat.textContent=(d.status==='connecting')?'Подключение...':'В процессе';"
"const t=d.num_threads||1;tBadge.textContent=t>1?t+' потоков (Turbo)':'1 поток (Обычный)';}"
"else if(d.status==='aborting'){badge.className='status-badge error';badge.textContent='Остановка...';card.classList.add('active');btn.disabled=true;stStat.textContent='Прерывание загрузки...';if(abBtn){abBtn.disabled=true;abBtn.textContent='Остановка...';}}"
"else if(d.status==='completed'){badge.className='status-badge completed';badge.textContent='Завершено';bar.style.width='100%';pText.textContent='100%';stStat.textContent='Готово!';etaStat.textContent='0 сек';btn.disabled=false;if(abBtn){abBtn.disabled=false;abBtn.textContent='Прервать';}}"
"else if(d.status==='error'){badge.className='status-badge error';badge.textContent='Ошибка';card.classList.add('active');stStat.textContent=d.error_message||'Сбой';btn.disabled=false;if(abBtn){abBtn.disabled=false;abBtn.textContent='Прервать';}}"
"else{badge.className='status-badge idle';badge.textContent='Ожидание';card.classList.remove('active');btn.disabled=false;if(abBtn){abBtn.disabled=false;abBtn.textContent='Прервать';}}"
"lastState=d.status;}"
"async function startDownload(){const u=document.getElementById('fileUrl').value.trim();if(!u){alert('Введите ссылку');return;}"
"const t=parseInt(document.getElementById('threadsSelect').value,10)||8;"
"const payload={url:u,save_dir:document.getElementById('savePath').value.trim()||'/data/pkg/',filename:document.getElementById('customFilename').value.trim(),threads:t};"
"document.getElementById('startBtn').disabled=true;"
"try{const r=await fetch('/api/download',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});"
"if(!r.ok){const e=await r.json();alert('Ошибка: '+(e.error||r.statusText));document.getElementById('startBtn').disabled=false;return;}"
"document.getElementById('progressCard').classList.add('active');fetchStatus();}catch(err){alert('Сбой сети: '+err.message);document.getElementById('startBtn').disabled=false;}}"
"async function abortDownload(){if(!confirm('Прервать загрузку?'))return;const b=document.getElementById('abortBtn');if(b){b.disabled=true;b.textContent='Остановка...';}try{await fetch('/api/abort',{method:'POST'});}catch(e){}fetchStatus();}"
"async function resetDownload(){const b=document.getElementById('resetBtn');if(b){b.disabled=true;b.textContent='Сброс...';}try{await fetch('/api/reset',{method:'POST'});}catch(e){}if(b){b.disabled=false;b.textContent='Сбросить';}fetchStatus();}"
"setInterval(fetchStatus,1000);fetchStatus();"
"</script></body></html>";

static void extract_json_string(const char *json, const char *key, char *out, size_t max_len) {
    char search_pattern[128];
    snprintf(search_pattern, sizeof(search_pattern), "\"%s\"", key);
    char *pos = strstr(json, search_pattern);
    if (!pos) {
        out[0] = '\0';
        return;
    }
    pos = strchr(pos + strlen(search_pattern), ':');
    if (!pos) {
        out[0] = '\0';
        return;
    }
    pos = strchr(pos, '\"');
    if (!pos) {
        out[0] = '\0';
        return;
    }
    pos++;
    char *end = strchr(pos, '\"');
    if (!end) {
        out[0] = '\0';
        return;
    }
    size_t len = end - pos;
    if (len >= max_len) len = max_len - 1;
    strncpy(out, pos, len);
    out[len] = '\0';
}

static int extract_json_int(const char *json, const char *key, int default_val) {
    char search_pattern[128];
    snprintf(search_pattern, sizeof(search_pattern), "\"%s\"", key);
    char *pos = strstr(json, search_pattern);
    if (!pos) return default_val;
    pos = strchr(pos + strlen(search_pattern), ':');
    if (!pos) return default_val;
    pos++;
    while (*pos == ' ' || *pos == '\t') pos++;
    return atoi(pos);
}

static void send_response(int client_fd, int status_code, const char *content_type, const char *body) {
    char header[512];
    size_t body_len = body ? strlen(body) : 0;
    const char *status_msg = (status_code == 200) ? "OK" : 
                            (status_code == 400) ? "Bad Request" : 
                            (status_code == 404) ? "Not Found" : "Internal Server Error";

    int header_len = snprintf(header, sizeof(header),
        "HTTP/1.1 %d %s\r\n"
        "Content-Type: %s\r\n"
        "Content-Length: %zu\r\n"
        "Connection: close\r\n"
        "Access-Control-Allow-Origin: *\r\n"
        "Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n"
        "Access-Control-Allow-Headers: Content-Type\r\n\r\n",
        status_code, status_msg, content_type, body_len);

    send(client_fd, header, header_len, MSG_NOSIGNAL);
    if (body_len > 0) {
        send(client_fd, body, body_len, MSG_NOSIGNAL);
    }
}

static void handle_client(int client_fd) {
    struct timeval tv;
    tv.tv_sec = 5;
    tv.tv_usec = 0;
    setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, (const char *)&tv, sizeof(tv));
    setsockopt(client_fd, SOL_SOCKET, SO_SNDTIMEO, (const char *)&tv, sizeof(tv));

    char buffer[4096];
    ssize_t bytes_read = recv(client_fd, buffer, sizeof(buffer) - 1, 0);
    if (bytes_read <= 0) {
        close(client_fd);
        return;
    }
    buffer[bytes_read] = '\0';

    char method[16];
    char path[256];
    sscanf(buffer, "%15s %255s", method, path);

    // CORS preflight
    if (strcmp(method, "OPTIONS") == 0) {
        send_response(client_fd, 200, "text/plain", "");
        close(client_fd);
        return;
    }

    if (strcmp(method, "GET") == 0) {
        if (strcmp(path, "/") == 0 || strcmp(path, "/index.html") == 0) {
            send_response(client_fd, 200, "text/html; charset=utf-8", INDEX_HTML);
        } else if (strcmp(path, "/api/status") == 0) {
            download_status_t status;
            downloader_get_status(&status);

            const char *state_str = "idle";
            switch (status.state) {
                case STATUS_CONNECTING: state_str = "connecting"; break;
                case STATUS_DOWNLOADING: state_str = "downloading"; break;
                case STATUS_ABORTING: state_str = "aborting"; break;
                case STATUS_COMPLETED: state_str = "completed"; break;
                case STATUS_ERROR: state_str = "error"; break;
                default: state_str = "idle"; break;
            }

            char json_resp[1024];
            snprintf(json_resp, sizeof(json_resp),
                "{\"status\":\"%s\",\"filename\":\"%s\",\"downloaded_bytes\":%llu,\"total_bytes\":%llu,\"speed_bytes_sec\":%.2f,\"eta_seconds\":%llu,\"num_threads\":%d,\"error_message\":\"%s\"}",
                state_str,
                status.filename,
                (unsigned long long)status.downloaded_bytes,
                (unsigned long long)status.total_bytes,
                status.speed_bytes_sec,
                (unsigned long long)status.eta_seconds,
                status.num_threads,
                status.error_message);

            send_response(client_fd, 200, "application/json", json_resp);
        } else {
            send_response(client_fd, 404, "text/plain", "Not Found");
        }
    } else if (strcmp(method, "POST") == 0) {
        if (strcmp(path, "/api/download") == 0) {
            char *body = strstr(buffer, "\r\n\r\n");
            if (body) body += 4;
            else body = "";

            char url[2048] = {0};
            char save_dir[256] = "/data/pkg/";
            char filename[256] = {0};

            extract_json_string(body, "url", url, sizeof(url));
            extract_json_string(body, "save_dir", save_dir, sizeof(save_dir));
            extract_json_string(body, "filename", filename, sizeof(filename));
            int threads = extract_json_int(body, "threads", 4);

            if (strlen(url) == 0) {
                send_response(client_fd, 400, "application/json", "{\"error\":\"URL is required\"}");
            } else {
                int res = downloader_start(url, save_dir, filename, threads);
                if (res == 0) {
                    send_response(client_fd, 200, "application/json", "{\"ok\":true}");
                } else {
                    send_response(client_fd, 400, "application/json", "{\"error\":\"Download already in progress\"}");
                }
            }
        } else if (strcmp(path, "/api/abort") == 0) {
            downloader_abort();
            send_response(client_fd, 200, "application/json", "{\"ok\":true}");
        } else if (strcmp(path, "/api/reset") == 0) {
            downloader_reset();
            send_response(client_fd, 200, "application/json", "{\"ok\":true}");
        } else {
            send_response(client_fd, 404, "text/plain", "Not Found");
        }
    } else {
        send_response(client_fd, 405, "text/plain", "Method Not Allowed");
    }

    close(client_fd);
}

int server_start(int port) {
    g_server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (g_server_fd < 0) {
        perror("Socket creation failed");
        return -1;
    }

    int opt = 1;
    setsockopt(g_server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in address;
    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);

    if (bind(g_server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("Bind failed");
        close(g_server_fd);
        return -1;
    }

    if (listen(g_server_fd, 10) < 0) {
        perror("Listen failed");
        close(g_server_fd);
        return -1;
    }

    printf("[PS5-ND] Turbo Web server listening on http://0.0.0.0:%d\n", port);

    while (g_server_running) {
        struct sockaddr_in client_addr;
        socklen_t addrlen = sizeof(client_addr);
        int client_fd = accept(g_server_fd, (struct sockaddr *)&client_addr, &addrlen);
        if (client_fd >= 0) {
            handle_client(client_fd);
        } else if (errno == EINTR || errno == ECONNABORTED) {
            continue;
        } else {
            usleep(10000); // 10ms backoff
        }
    }

    close(g_server_fd);
    return 0;
}

void server_stop(void) {
    g_server_running = 0;
    if (g_server_fd >= 0) {
        close(g_server_fd);
        g_server_fd = -1;
    }
}
