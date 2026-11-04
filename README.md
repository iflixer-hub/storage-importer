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

Запросы к источнику (плейлисты и сегменты ashdi) идут через прокси из строки `ashdi_proxies` таблицы `settings` (JSON-список,
как у flixhub-mono: `["http://user:pass@isp.decodo.com:10004", ...]`). Список кэшируется на 10 минут, при недоступности БД
остаётся предыдущий. На каждую задачу выбирается следующий прокси по кругу, и вся задача (master, плейлисты, сегменты) идёт через него.
Если настройка пуста или отсутствует, используются `HTTP_PROXY`/`HTTPS_PROXY` из окружения (warp). Загрузка в S3 всегда идёт напрямую.
В лог пишется только `host:port` прокси: `[job N] source proxy=isp.decodo.com:10004`.

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
