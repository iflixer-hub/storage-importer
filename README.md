# player-stat-collector

Сервис берет еще не импортированные урлы из таблицы files скачивает их в s3 storage

Хранит информацию про загруженные файлы в таблице files-storage


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
