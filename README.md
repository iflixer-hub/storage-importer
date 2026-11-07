# player-stat-collector

Сервис берет еще не импортированные урлы из таблицы files скачивает их в s3 storage

Хранит информацию про загруженные файлы в таблице files-storage


# Ограничение по качествам

`ALLOWED_QUALITIES` — список качеств через запятую, например `720,480`: импортируются только эти варианты мастер-плейлиста,
остальные пропускаются (не скачиваются и не попадают в итоговый master). Пусто или не задано — импортируются все.
Качество сопоставляется с папкой `hls/<N>/` в URL варианта, а если её нет, то с высотой из `RESOLUTION=WxH`.
Если ни один вариант не подошёл, задача падает с ошибкой `no variant matches ALLOWED_QUALITIES` и перечислением качеств источника.
Источник без вариантов (одиночный media-плейлист, качество `default`) фильтром не ограничивается.

# Прокси для источника

Запросы к источнику (плейлисты и сегменты ashdi) идут через прокси из строки таблицы `settings` (JSON-список,
как у flixhub-mono: `["http://user:pass@isp.decodo.com:10004", ...]`). Список кэшируется на 10 минут, при недоступности БД
остаётся предыдущий. На каждую задачу выбирается следующий прокси по кругу, и вся задача (master, плейлисты, сегменты) идёт через него.
Если настройка пуста или отсутствует, используются `HTTP_PROXY`/`HTTPS_PROXY` из окружения (warp). Загрузка в S3 всегда идёт напрямую.
В лог пишется только `host:port` прокси: `[job N] source proxy=isp.decodo.com:10004`.

Какую строку `settings` использовать, задаёт `SOURCE_PROXIES_SETTING` (по умолчанию `ashdi_proxies`; например `luxhost_proxies` для другого провайдера). Формат списка тот же. Имя выбранной строки пишется в лог при старте: `source proxies: settings row "luxhost_proxies"`; список перечитывается раз в 10 минут, смена переменной требует перезапуска сервиса.

# Заголовки запросов к источнику

Источник отвечает `403` на запросы с `User-Agent: Go-http-client` (проверено через прокси: с браузерным `User-Agent` `200`, с Go `403`), поэтому каждый запрос к нему идёт с заголовками браузера (тот же профиль Chrome 140, что в flixhub-mono): `User-Agent`, `Accept`, `Accept-Language`, `Referer`, `Origin`, `Sec-Fetch-*`, `Sec-CH-UA*`. `Accept-Encoding` не задаётся: Go сам просит gzip и сам его распаковывает.
- `SOURCE_USER_AGENT` - `User-Agent` (по умолчанию Chrome 140 на Windows; `Sec-CH-UA*` добавляются только для него)
- `SOURCE_REFERER` - страница-источник запроса (по умолчанию `https://uakino.best/`), из неё же берётся `Origin`
- `SOURCE_ACCEPT_LANGUAGE` - по умолчанию `uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7`

Значения при старте пишутся в лог: `source request headers: user-agent=… referer=…`. Загрузка в S3 этих заголовков не получает.

# Паузы и 429

Источник (ashdi, DDoS-Guard) режет по числу запросов с одного IP. Поэтому запросы к источнику идут через общий ограничитель на каждый прокси:
- `SOURCE_REQUEST_DELAY_MS` — пауза между стартами запросов через один прокси, ±20% случайно (чтобы воркеры не бились синхронно). `0` — без паузы.
- `SOURCE_429_RETRIES` (по умолчанию 3) — сколько раз повторить запрос после `429`. Ждём `Retry-After` (до 60 с) или 5, 10, 20 с; на это время
  приостанавливаются все запросы через этот прокси. Если повторы закончились, `429` обрабатывается как обычная ошибка (качество станет `partial`).

Ограничитель работает внутри одного процесса: при нескольких нодах (`mode: global`) каждая нода держит свою паузу, то есть общий темп на один прокси выше.

# Метрики

`:9090/metrics` (`METRICS_ADDR`), в swarm их собирает job `importer` в `swarm/monitor/prometheus/prometheus.yml`, дашборд — `swarm/monitor/grafana/provisioning/dashboards/importer.json`.

- `import_jobs_total{result}` — `claimed`, `done`, `partial` (завершено с упавшими чанками), `failed`; `import_job_duration_seconds{result}`; `import_inflight_jobs`; `import_last_success_timestamp_seconds`.
- `import_qualities_total{result}` — `imported`, `partial`, `failed`, `kept` (уже было импортировано), `skipped` (не в `ALLOWED_QUALITIES`).
- `import_db_jobs_{total,new,running,done,failed,partial,dead}` — состояние очереди в БД (одинаковое на всех нодах, брать `max`). `dead` — `failed` с исчерпанными 10 попытками.
- `import_http_requests_total{kind,code}`, `import_http_duration_seconds{kind}`, `import_bytes_total{dir}`, `import_objects_total{type}` — обмен с источником и хранилищем.
- `import_proxy_jobs_total{proxy}`, `import_source_rate_limited_total{proxy}` (429), `import_source_throttle_wait_seconds_total{proxy}` — прокси источника (`proxy` — `host:port`, `env` для HTTP(S)_PROXY).
- `import_s3_errors_total{op}` — ошибки записи в хранилище (`playlist`, `object`).

# qualities_json и повторный импорт

В `files_storage.qualities_json` хранится список полностью импортированных качеств, например `["480/index.m3u8","720/index.m3u8"]`.
Качество попадает туда только если все его сегменты и плейлист загружены без ошибок (качество с упавшими чанками остаётся в master, но не в списке).
Запись обновляется после каждого завершённого качества.

При следующем запуске задачи (`new` или `failed`) качества из `qualities_json` не скачиваются заново, а только возвращаются в master.
Импортируются остальные разрешённые качества, например при `ALLOWED_QUALITIES=720,480` и `["480/index.m3u8"]` будет импортирован только 720.
Чтобы переимпортировать всё (например, после смены бакета), сначала очистите `qualities_json`: `UPDATE files_storage SET qualities_json=NULL WHERE ...`.

# 404 источника

Если источник отвечает `404` на любой ресурс файла (сегмент, ключ/init, плейлист качества или сам master), файл считается пропавшим: обработка останавливается на первом таком ответе (остальные сегменты не качаются, в лог попадает одна строка вместо строки на каждый сегмент), а задача получает `failed` с текстом вида `permanent: source file is gone: quality=720: GET …/segment26.ts status=404 …` и **больше не берётся в работу** (`attempts` выставляется в 10, она видна как `dead` в метриках и на дашборде).
Остальные ошибки (403, 429, 5xx, таймауты) по-прежнему дают `partial` или обычный `failed` с повторами. Тело страницы ошибки в логе обрезается до 200 символов.
Master на S3 при этом не загружается (он пишется последним), уже полностью импортированные качества остаются в `qualities_json`. Чтобы повторить такую задачу вручную, верните её в работу: `UPDATE files_storage SET status='new', attempts=0, last_error=NULL WHERE file_id=…;`.

# Порядок выдачи задач

Сначала берутся задачи `new`, потом `failed` (с меньшим числом попыток). Среди `new` первыми идут те, у кого больше `files_storage.m3u8_errors`: так player сообщает, что для файла он не нашёл master-манифест в хранилище (ему всё равно отдают на него ссылку), и такие файлы нужно импортировать в первую очередь. Дальше порядок прежний: меньше попыток, меньший `file_id`. На `failed` счётчик не влияет.
В логе claim видно значение: `[claim] file_id=… attempt=… m3u8_errors=…`. Если колонки ещё нет (не накатили миграцию flixhub-mono `add_m3u8_errors_to_files_storage_table`), importer работает по старому порядку и раз в 5 минут проверяет её снова, ничего перезапускать не нужно.

# Ключевая договорённость по объектам в R2 

Для каждой строки:
	•	files/<id>/index.m3u8 — master
	•	files/<id>/<quality>/index.m3u8 — плейлист качества
	•	files/<id>/<quality>/1.ts, 2.ts, … — сегменты

И внутри всех m3u8 только относительные пути, например:
	•	в master: 480/index.m3u8
	•	в quality: 1.ts, 2.ts, …

Master обязан лежать файлом — да, делаем его последним upload’ом.

# Алгоритм
	•	берём files_storage (а files не трогаем, кроме опциональной очистки files.path после успеха),
	•	в R2 создаём структуру:
	•	files/<id>/index.m3u8 (master)
	•	files/<id>/<quality>/index.m3u8
	•	files/<id>/<quality>/1.ts, 2.ts, …
	•	внутри m3u8 — только относительные пути,
	•	master загружается последним (как “флаг готовности”),
	•	многопроцессно/многоподовно безопасно (через FOR UPDATE SKIP LOCKED).

Постановка задач (кроном? ендпоинт?)
INSERT INTO files_storage (file_id, status, attempts, source_master_url, r2_prefix, r2_master_key)
SELECT f.id, 'new', 0, f.path,
       CONCAT('files/', f.id) AS r2_prefix,
       CONCAT('files/', f.id, '/index.m3u8') AS r2_master_key
FROM files f
LEFT JOIN files_storage s ON s.file_id = f.id
WHERE s.file_id IS NULL
  AND f.path IS NOT NULL
  AND f.path <> '';

# атомарно взять в работу

START TRANSACTION;

SELECT file_id, source_master_url, r2_prefix, r2_master_key, attempts
FROM files_storage
WHERE status IN ('new','failed')
  AND attempts < 10
ORDER BY status='failed', attempts, file_id
LIMIT 1
FOR UPDATE SKIP LOCKED;

-- если строка нашлась, сразу помечаем
UPDATE files_storage
SET status='in_progress',
    attempts = attempts + 1,
    started_at = NOW(),
    last_error = NULL
WHERE file_id = ?;

COMMIT;

# тесты
Посмотреть статус очереди
SELECT status, COUNT(*) FROM files_storage GROUP BY status;
SELECT * FROM files_storage WHERE status='failed' ORDER BY finished_at DESC LIMIT 20;

Перезапустить фейлы
UPDATE files_storage
SET status='new'
WHERE status='failed' AND attempts < 10;

# Автоматический recovery

Во время обработки сервис обновляет `started_at` задания. Если экземпляр сервиса
погиб и heartbeat прекратился, другой экземпляр автоматически переводит
просроченное `in_progress` в `failed`; после этого задание снова доступно через
`FOR UPDATE SKIP LOCKED`. Номер `attempts` используется как fencing token, поэтому
старый worker не сможет завершить уже перевыданное задание.

Параметры:

```yaml
JOB_HEARTBEAT_SECONDS: "60"
JOB_RECOVERY_TIMEOUT_SECONDS: "600"
JOB_RECOVERY_INTERVAL_SECONDS: "60"
```

`JOB_RECOVERY_TIMEOUT_SECONDS` должен быть больше удвоенного heartbeat-интервала.
После 10 попыток задание остается `failed` и автоматически больше не запускается.
