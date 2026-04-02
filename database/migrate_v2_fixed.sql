-- ============================================================
--  MT5_Level_Bot — database/migrate_v2_fixed.sql
--
--  ИСПРАВЛЕННАЯ миграция — на основе реальной структуры БД
--  (скрин phpMyAdmin от пользователя).
--
--  Исправлено относительно первой версии:
--  - Таблица level_history (не level_events)
--  - Учтена колонка matches_ichimoku в raw_levels
--  - Учтены opened_at / closed_at в signal_queue
--  - close_reason в trades — ENUM
--  - Убраны ссылки на несуществующие колонки
--
--  КАК ПРИМЕНИТЬ:
--    phpMyAdmin → mt5_level_engine → вкладка SQL →
--    вставить этот файл → нажать "Вперёд"
-- ============================================================

SET NAMES utf8mb4;
SET time_zone = '+00:00';

-- ============================================================
-- ШАГ 1: raw_levels — UNIQUE индекс (ДБ-1)
-- ============================================================

-- Удаляем дубли (оставляем запись с наибольшим id)
DELETE r1 FROM raw_levels r1
INNER JOIN raw_levels r2
  ON  r1.symbol     = r2.symbol
  AND r1.timeframe  = r2.timeframe
  AND r1.price_zone = r2.price_zone
  AND r1.direction  = r2.direction
  AND r1.id < r2.id;

-- UNIQUE ключ
SET @i1 = (SELECT COUNT(*) FROM information_schema.statistics
  WHERE table_schema = DATABASE()
    AND table_name   = 'raw_levels'
    AND index_name   = 'uq_level');
SET @s1 = IF(@i1 = 0,
  'ALTER TABLE raw_levels ADD UNIQUE KEY uq_level (symbol, timeframe, price_zone, direction)',
  'SELECT "raw_levels: uq_level уже есть" AS статус');
PREPARE p1 FROM @s1; EXECUTE p1; DEALLOCATE PREPARE p1;

-- Индекс на last_touch_time
SET @i2 = (SELECT COUNT(*) FROM information_schema.statistics
  WHERE table_schema = DATABASE()
    AND table_name   = 'raw_levels'
    AND index_name   = 'idx_rl_touch');
SET @s2 = IF(@i2 = 0,
  'ALTER TABLE raw_levels ADD INDEX idx_rl_touch (last_touch_time)',
  'SELECT "idx_rl_touch уже есть" AS статус');
PREPARE p2 FROM @s2; EXECUTE p2; DEALLOCATE PREPARE p2;

-- Индекс на bounce_count
SET @i3 = (SELECT COUNT(*) FROM information_schema.statistics
  WHERE table_schema = DATABASE()
    AND table_name   = 'raw_levels'
    AND index_name   = 'idx_rl_bounce');
SET @s3 = IF(@i3 = 0,
  'ALTER TABLE raw_levels ADD INDEX idx_rl_bounce (bounce_count)',
  'SELECT "idx_rl_bounce уже есть" AS статус');
PREPARE p3 FROM @s3; EXECUTE p3; DEALLOCATE PREPARE p3;

-- ============================================================
-- ШАГ 2: analyzed_levels — UNIQUE индекс + индекс S-score (ДБ-2)
-- ============================================================

-- Удаляем дубли
DELETE a1 FROM analyzed_levels a1
INNER JOIN analyzed_levels a2
  ON  a1.symbol     = a2.symbol
  AND a1.timeframe  = a2.timeframe
  AND a1.price_zone = a2.price_zone
  AND a1.direction  = a2.direction
  AND a1.id < a2.id;

-- UNIQUE ключ
SET @i4 = (SELECT COUNT(*) FROM information_schema.statistics
  WHERE table_schema = DATABASE()
    AND table_name   = 'analyzed_levels'
    AND index_name   = 'uq_analyzed');
SET @s4 = IF(@i4 = 0,
  'ALTER TABLE analyzed_levels ADD UNIQUE KEY uq_analyzed (symbol, timeframe, price_zone, direction)',
  'SELECT "uq_analyzed уже есть" AS статус');
PREPARE p4 FROM @s4; EXECUTE p4; DEALLOCATE PREPARE p4;

-- Индекс на strength_score — ускоряет ORDER BY strength_score DESC в Signal Engine
SET @i5 = (SELECT COUNT(*) FROM information_schema.statistics
  WHERE table_schema = DATABASE()
    AND table_name   = 'analyzed_levels'
    AND index_name   = 'idx_al_strength');
SET @s5 = IF(@i5 = 0,
  'ALTER TABLE analyzed_levels ADD INDEX idx_al_strength (strength_score)',
  'SELECT "idx_al_strength уже есть" AS статус');
PREPARE p5 FROM @s5; EXECUTE p5; DEALLOCATE PREPARE p5;

-- ============================================================
-- ШАГ 3: signal_queue — индекс на expires_at (ДБ-3 / БАГ-5)
-- ============================================================

-- Индекс на expires_at (после исправления БАГ-5 этот запрос стал частым)
SET @i6 = (SELECT COUNT(*) FROM information_schema.statistics
  WHERE table_schema = DATABASE()
    AND table_name   = 'signal_queue'
    AND index_name   = 'idx_sq_expires');
SET @s6 = IF(@i6 = 0,
  'ALTER TABLE signal_queue ADD INDEX idx_sq_expires (expires_at)',
  'SELECT "idx_sq_expires уже есть" AS статус');
PREPARE p6 FROM @s6; EXECUTE p6; DEALLOCATE PREPARE p6;

-- Индекс на status
SET @i7 = (SELECT COUNT(*) FROM information_schema.statistics
  WHERE table_schema = DATABASE()
    AND table_name   = 'signal_queue'
    AND index_name   = 'idx_sq_status');
SET @s7 = IF(@i7 = 0,
  'ALTER TABLE signal_queue ADD INDEX idx_sq_status (status)',
  'SELECT "idx_sq_status уже есть" AS статус');
PREPARE p7 FROM @s7; EXECUTE p7; DEALLOCATE PREPARE p7;

-- ============================================================
-- ШАГ 4: trades — индексы на status и mt5_ticket (ДБ-4)
-- ============================================================

SET @i8 = (SELECT COUNT(*) FROM information_schema.statistics
  WHERE table_schema = DATABASE()
    AND table_name   = 'trades'
    AND index_name   = 'idx_tr_status');
SET @s8 = IF(@i8 = 0,
  'ALTER TABLE trades ADD INDEX idx_tr_status (status)',
  'SELECT "idx_tr_status уже есть" AS статус');
PREPARE p8 FROM @s8; EXECUTE p8; DEALLOCATE PREPARE p8;

SET @i9 = (SELECT COUNT(*) FROM information_schema.statistics
  WHERE table_schema = DATABASE()
    AND table_name   = 'trades'
    AND index_name   = 'idx_tr_ticket');
SET @s9 = IF(@i9 = 0,
  'ALTER TABLE trades ADD INDEX idx_tr_ticket (mt5_ticket)',
  'SELECT "idx_tr_ticket уже есть" AS статус');
PREPARE p9 FROM @s9; EXECUTE p9; DEALLOCATE PREPARE p9;

-- ============================================================
-- ШАГ 5: bot_logs — индекс на created_at (ДБ-5)
-- ============================================================

SET @iA = (SELECT COUNT(*) FROM information_schema.statistics
  WHERE table_schema = DATABASE()
    AND table_name   = 'bot_logs'
    AND index_name   = 'idx_bl_created');
SET @sA = IF(@iA = 0,
  'ALTER TABLE bot_logs ADD INDEX idx_bl_created (created_at)',
  'SELECT "idx_bl_created уже есть" AS статус');
PREPARE pA FROM @sA; EXECUTE pA; DEALLOCATE PREPARE pA;

-- ============================================================
-- ШАГ 6: module_registry — UNIQUE на module_name (ДБ-6)
-- ============================================================

SET @iB = (SELECT COUNT(*) FROM information_schema.statistics
  WHERE table_schema = DATABASE()
    AND table_name   = 'module_registry'
    AND index_name   = 'uq_module_name');
SET @sB = IF(@iB = 0,
  'ALTER TABLE module_registry ADD UNIQUE KEY uq_module_name (module_name)',
  'SELECT "uq_module_name уже есть" AS статус');
PREPARE pB FROM @sB; EXECUTE pB; DEALLOCATE PREPARE pB;

-- ============================================================
-- ШАГ 7: level_history — индекс на raw_level_id (ДБ-8)
-- (таблица существует — просто добавляем индекс)
-- ============================================================

SET @iC = (SELECT COUNT(*) FROM information_schema.statistics
  WHERE table_schema = DATABASE()
    AND table_name   = 'level_history'
    AND index_name   = 'idx_lh_raw');
SET @sC = IF(@iC = 0,
  'ALTER TABLE level_history ADD INDEX idx_lh_raw (raw_level_id)',
  'SELECT "idx_lh_raw уже есть" AS статус');
PREPARE pC FROM @sC; EXECUTE pC; DEALLOCATE PREPARE pC;

-- ============================================================
-- ШАГ 8: RSI — синхронизируем rsi_value из raw_levels (ДБ-7)
--
-- Все записи analyzed_levels имели rsi_value = 50.0 (дефолт).
-- Обновляем реальными значениями из raw_levels + пересчитываем f_rsi.
-- ============================================================

UPDATE analyzed_levels a
INNER JOIN raw_levels r
  ON  r.symbol     = a.symbol
  AND r.timeframe  = a.timeframe
  AND r.price_zone = a.price_zone
  AND r.direction  = a.direction
SET
  a.rsi_value = r.rsi_value,
  a.f_rsi = CASE
    WHEN a.direction = 'Support'    AND r.rsi_value < 30 THEN 1.0
    WHEN a.direction = 'Support'    AND r.rsi_value < 40 THEN 0.8
    WHEN a.direction = 'Support'    AND r.rsi_value < 60 THEN 0.6
    WHEN a.direction = 'Support'    AND r.rsi_value < 70 THEN 0.3
    WHEN a.direction = 'Support'                         THEN 0.1
    WHEN a.direction = 'Resistance' AND r.rsi_value > 70 THEN 1.0
    WHEN a.direction = 'Resistance' AND r.rsi_value > 60 THEN 0.8
    WHEN a.direction = 'Resistance' AND r.rsi_value > 40 THEN 0.6
    WHEN a.direction = 'Resistance' AND r.rsi_value > 30 THEN 0.3
    ELSE 0.1
  END
WHERE r.rsi_value IS NOT NULL
  AND r.rsi_value != 50.0;

SELECT CONCAT('RSI обновлён у ', ROW_COUNT(), ' записей в analyzed_levels') AS результат;

-- ============================================================
-- ИТОГОВАЯ ПРОВЕРКА
-- Все значения в колонке "индекс_добавлен" должны быть = 1
-- ============================================================

SELECT
  'raw_levels'       AS таблица,
  COUNT(*)           AS записей,
  (SELECT COUNT(*) FROM information_schema.statistics
   WHERE table_schema = DATABASE()
     AND table_name = 'raw_levels'
     AND index_name = 'uq_level') AS uq_индекс_добавлен
FROM raw_levels

UNION ALL SELECT
  'analyzed_levels',
  COUNT(*),
  (SELECT COUNT(*) FROM information_schema.statistics
   WHERE table_schema = DATABASE()
     AND table_name = 'analyzed_levels'
     AND index_name = 'uq_analyzed')
FROM analyzed_levels

UNION ALL SELECT
  'signal_queue',
  COUNT(*),
  (SELECT COUNT(*) FROM information_schema.statistics
   WHERE table_schema = DATABASE()
     AND table_name = 'signal_queue'
     AND index_name = 'idx_sq_expires')
FROM signal_queue

UNION ALL SELECT
  'trades',
  COUNT(*),
  (SELECT COUNT(*) FROM information_schema.statistics
   WHERE table_schema = DATABASE()
     AND table_name = 'trades'
     AND index_name = 'idx_tr_status')
FROM trades

UNION ALL SELECT
  'level_history',
  COUNT(*),
  (SELECT COUNT(*) FROM information_schema.statistics
   WHERE table_schema = DATABASE()
     AND table_name = 'level_history'
     AND index_name = 'idx_lh_raw')
FROM level_history

UNION ALL SELECT
  'module_registry',
  COUNT(*),
  (SELECT COUNT(*) FROM information_schema.statistics
   WHERE table_schema = DATABASE()
     AND table_name = 'module_registry'
     AND index_name = 'uq_module_name')
FROM module_registry;
