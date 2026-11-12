# player-stat-collector

Сервис берет еще не импортированные урлы из таблицы files скачивает их в s3 storage

Хранит информацию про загруженные файлы в таблице files-storage


# Ограничение по качествам

`ALLOWED_QUALITIES` — список качеств через запятую, например `720,480`: импортируются только эти варианты мастер-плейлиста,
остальные пропускаются (не скачиваются и не попадают в итоговый master). Пусто или не задано — импортируются все.
Качество сопоставляется с папкой `hls/<N>/` в URL варианта, а если её нет, то с высотой из `RESOLUTION=WxH`.
Если ни один вариант не подошёл, задача падает с ошибкой `no variant matches ALLOWED_QUALITIES` и перечислением качеств источника.

**Остановка (SIGTERM, деплой).** Importer перехватывает SIGTERM/SIGINT: новые задачи не берутся, текущие отменяются и возвращаются в очередь (`status='new'`, счётчик `attempts` откатывается на эту попытку, `last_error` очищается). Раньше задача оставалась `in_progress` и через `JOB_RECOVERY_TIMEOUT_SECONDS` помечалась `failed` с `worker lease expired`. Уже готовый результат сохраняется и при остановке. Задача, взятая из `failed`, возвращается как `new`. На остановку даётся до 20 с (`stop_grace_period: 30s` в стеке). При SIGKILL, OOM или потере хоста по-прежнему работает recovery по таймауту.

`SOURCE_403_RETRIES` (по умолчанию `2`, `0` отключает) — ответ 403 источника (ashdi) на любой запрос (мастер, плейлист, чанк) повторяется до N раз через 2 с, затем 4 с, через тот же прокси; бюджет повторов 429 при этом не тратится. Источник иногда отвечает 403 на файл, который через минуту отдаёт с того же IP, поэтому раньше такая задача падала с `fetch master: … status=403`. Постоянный 403 возвращается вызывающему после повторов. Метрика `import_source_forbidden_retries_total{proxy}`.

`TAIL_SEGMENTS_TOLERANCE` (по умолчанию `30`, `0` отключает) — любая ошибка скачивания (в том числе 404) у одного из последних N чанков варианта не считается сбоем: такой чанк пропускается и вместе со своим `#EXTINF` убирается из плейлиста, а файл считается полностью импортированным и задача закрывается успешно. Окно не больше половины файла (в файле из 5 чанков допустимы последние 2), поэтому сбой в середине короткого файла по-прежнему роняет задачу. Сбой чанка вне окна работает как раньше (404 — сразу `permanent`, остальное — `partial`). В логе `tail chunk i/N skipped`, метрика `import_tail_segments_skipped_total`.
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

# Проверка хранилища перед импортом

Перед импортом задача сверяется с тем, что реально лежит в хранилище (`qualities_json` только подсказка: бакет могли заменить). Для каждого разрешённого качества (и для уже записанных в `qualities_json`):
1. листингом `<r2_prefix>/<качество>/` берутся имена всех объектов (один запрос на 1000 объектов, а не по запросу на каждый чанк);
2. читается сохранённый `index.m3u8` качества, и **каждый чанк из него** (сегменты, `#EXT-X-KEY`/`#EXT-X-MAP`) должен быть в листинге.

- Всё на месте: качество считается готовым. Его плейлисты и сегменты у источника не запрашиваются, оно только возвращается в master. Если готовы все качества, а master в хранилище уже совпадает, ничего не скачивается и не загружается: `everything is already in the storage, nothing to import`.
- Чего-то не хватает: качество импортируется заново, но объекты, которые уже лежат в хранилище (объект там появляется только целиком), не скачиваются повторно: докачиваются только недостающие чанки. Метрика `import_objects_reused_total`.
- `qualities_json` не совпадает с хранилищем (качество записано, а файлов нет): запись исправляется и качество импортируется заново.
- Если хранилище не ответило на проверку, используется `qualities_json`, а ошибка пишется в лог (`cannot verify quality=… in the storage`).

Нужны права на листинг бакета (`s3:ListBucket`) и чтение (`s3:GetObject`) по префиксу `files/`; на каждое проверяемое качество приходится один листинг и один `GET` плейлиста.

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
