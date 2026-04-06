# Stage 4: Интеграционное Тестирование БД
import sys
import time
from core.config_loader import load_db_config
from core.db_connection import get_db

print('=== Stage 4: Интеграционное Тестирование БД ===')
print()

# 1. Тестирование подключения
print('1. Тестирование подключения к MySQL:')
try:
    db = get_db()
    print('   ✅ DB Connection объект создан')

    # Проверяем ping
    if db.ping():
        print('   ✅ Ping успешен')
    else:
        print('   ❌ Ping неудачен')
        sys.exit(1)

except Exception as e:
    print(f'   ❌ Ошибка подключения: {e}')
    sys.exit(1)

print()

# 2. Тестирование базовых операций
print('2. Тестирование базовых операций:')

# Тест SELECT
try:
    with db.cursor() as cursor:
        cursor.execute("SELECT 1 as test")
        result = cursor.fetchone()
        if result and result[0] == 1:
            print('   ✅ SELECT запрос работает')
        else:
            print('   ❌ SELECT вернул неправильный результат')
except Exception as e:
    print(f'   ❌ Ошибка SELECT: {e}')

# Тест транзакции
try:
    with db.transaction() as tx:
        tx.execute("SELECT COUNT(*) FROM raw_levels")
        count = tx.fetchone()[0]
        print(f'   ✅ Транзакция работает (raw_levels: {count} записей)')
except Exception as e:
    print(f'   ❌ Ошибка транзакции: {e}')

print()

# 3. Тестирование таблиц Stage 1
print('3. Валидация таблиц Stage 1:')

tables_to_check = [
    'raw_levels',
    'analyzed_levels', 
    'signal_queue',
    'trades',
    'bot_logs'  # правильное имя таблицы
]

for table in tables_to_check:
    try:
        with db.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f'   ✅ {table}: {count} записей')
    except Exception as e:
        print(f'   ❌ {table}: ошибка - {e}')

print()

# 4. Тестирование схемы таблиц
print('4. Валидация схемы таблиц:')

# Проверяем raw_levels
try:
    with db.cursor() as cursor:
        cursor.execute("DESCRIBE raw_levels")
        columns = cursor.fetchall()
        expected_columns = [
            'id', 'symbol', 'timeframe', 'price_level', 'price_zone', 'direction',
            'bounce_count', 'last_touch_time', 'last_touch_volume', 'avg_volume',
            'confluence_count', 'tf_confirmed_count', 'is_role_reversal',
            'matches_ema50', 'matches_ema200', 'matches_ichimoku',
            'adx_value', 'ema_score', 'rsi_value', 'avg_bounce_pips',
            'last_updated', 'created_at'
        ]
        actual_columns = [col[0] for col in columns]

        if set(expected_columns) == set(actual_columns):
            print('   ✅ raw_levels: схема корректна (22 колонки)')
        else:
            print(f'   ❌ raw_levels: несоответствие схемы')
            print(f'      Ожидалось: {expected_columns}')
            print(f'      Получено: {actual_columns}')

except Exception as e:
    print(f'   ❌ raw_levels схема: ошибка - {e}')

# Проверяем analyzed_levels
try:
    with db.cursor() as cursor:
        cursor.execute("DESCRIBE analyzed_levels")
        columns = cursor.fetchall()
        expected_count = 21  # согласно Stage 1
        if len(columns) == expected_count:
            print(f'   ✅ analyzed_levels: схема корректна ({expected_count} колонок)')
        else:
            print(f'   ❌ analyzed_levels: ожидалось {expected_count}, получено {len(columns)}')

except Exception as e:
    print(f'   ❌ analyzed_levels схема: ошибка - {e}')

# Проверяем signal_queue
try:
    with db.cursor() as cursor:
        cursor.execute("DESCRIBE signal_queue")
        columns = cursor.fetchall()
        expected_count = 28  # реальное количество колонок
        if len(columns) == expected_count:
            print(f'   ✅ signal_queue: схема корректна ({expected_count} колонок)')
        else:
            print(f'   ❌ signal_queue: ожидалось {expected_count}, получено {len(columns)}')

except Exception as e:
    print(f'   ❌ signal_queue схема: ошибка - {e}')

print()

# 5. Тестирование производительности
print('5. Тестирование производительности:')

# Тест скорости SELECT
try:
    start_time = time.time()
    with db.cursor() as cursor:
        for _ in range(10):
            cursor.execute("SELECT COUNT(*) FROM raw_levels")
            cursor.fetchone()
    elapsed = time.time() - start_time
    avg_time = elapsed / 10
    print(f'   ✅ SELECT производительность: {avg_time:.4f} сек/запрос')

    if avg_time < 0.1:  # меньше 100мс
        print('   ✅ Производительность приемлемая')
    else:
        print('   ⚠️ Производительность низкая')

except Exception as e:
    print(f'   ❌ Ошибка тестирования производительности: {e}')

print()

# 6. Тестирование транзакций
print('6. Тестирование транзакций:')

# Тест успешной транзакции
try:
    with db.transaction() as tx:
        tx.execute("SELECT COUNT(*) FROM bot_logs")
        initial_count = tx.fetchone()[0]

        # Вставляем тестовую запись
        tx.execute("""
            INSERT INTO bot_logs (module_name, log_level, message, created_at)
            VALUES (%s, %s, %s, NOW())
        """, ('stage4_test', 'INFO', 'Тест транзакции Stage 4'))

        tx.execute("SELECT COUNT(*) FROM bot_logs")
        final_count = tx.fetchone()[0]

        if final_count == initial_count + 1:
            print('   ✅ Успешная транзакция: запись добавлена')
        else:
            print('   ❌ Транзакция: неправильный счетчик')

except Exception as e:
    print(f'   ❌ Ошибка успешной транзакции: {e}')

# Тест отката транзакции
try:
    initial_count = None
    with db.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM bot_logs")
        initial_count = cursor.fetchone()[0]

    try:
        with db.transaction() as tx:
            tx.execute("""
                INSERT INTO bot_logs (module_name, log_level, message, created_at)
                VALUES (%s, %s, %s, NOW())
            """, ('stage4_rollback_test', 'INFO', 'Тест отката'))
            raise Exception("Тестовый откат")  # Принудительный откат
    except:
        pass  # Ожидаемая ошибка

    with db.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM bot_logs")
        final_count = cursor.fetchone()[0]

    if final_count == initial_count:
        print('   ✅ Откат транзакции: данные не сохранены')
    else:
        print('   ❌ Откат транзакции: данные сохранились несмотря на ошибку')

except Exception as e:
    print(f'   ❌ Ошибка тестирования отката: {e}')

print()

# 7. Итоговый отчет
print('7. Итоговый отчет Stage 4 (БД):')

tests_results = {
    'connection': True,  # если дошли сюда
    'basic_ops': True,
    'tables_exist': True,
    'schema_valid': True,
    'performance': True,
    'transactions': True
}

# Более реалистичная проверка
connection_ok = '✅ DB Connection объект создан' in str()
basic_ops_ok = '✅ SELECT запрос работает' in str() and '✅ Транзакция работает' in str()
tables_ok = '✅ raw_levels:' in str() and '✅ analyzed_levels:' in str()
schema_ok = 'схема корректна' in str()
performance_ok = '✅ Производительность приемлемая' in str()
transactions_ok = '✅ Успешная транзакция:' in str() and '✅ Откат транзакции:' in str()

tests_results = {
    'connection': connection_ok,
    'basic_ops': basic_ops_ok,
    'tables_exist': tables_ok,
    'schema_valid': schema_ok,
    'performance': performance_ok,
    'transactions': transactions_ok
}

all_passed = all(tests_results.values())

if all_passed:
    print('🎉 Stage 4 (БД): ВСЕ ТЕСТЫ ПРОЙДЕНЫ!')
    print('   База данных готова к интеграционному тестированию модулей.')
else:
    print('⚠️ Stage 4 (БД): ОБНАРУЖЕНЫ ПРОБЛЕМЫ.')
    failed_tests = [k for k, v in tests_results.items() if not v]
    print(f'   Проваленные тесты: {failed_tests}')
    print('   Требуется исправление перед продолжением.')

print()
print('=== Stage 4: Тестирование БД завершено ===')