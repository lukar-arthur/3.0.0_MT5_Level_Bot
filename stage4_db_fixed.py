# Stage 4: Исправленное тестирование БД
import mysql.connector
import time

print('=== Stage 4: Исправленное Тестирование БД ===')

def get_connection():
    return mysql.connector.connect(
        host='localhost',
        port=3306,
        user='root',
        password='',
        database='mt5_level_engine'
    )

# 1. Тестирование подключения
print('1. Тестирование подключения:')
try:
    conn = get_connection()
    print('   ✅ Подключение успешно')
    conn.close()
except Exception as e:
    print(f'   ❌ Ошибка подключения: {e}')
    exit(1)

# 2. Тестирование таблиц
print('2. Проверка таблиц:')
tables_status = {}
required_tables = ['raw_levels', 'analyzed_levels', 'signal_queue', 'trades', 'bot_logs']

try:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES")
    existing_tables = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()

    for table in required_tables:
        if table in existing_tables:
            tables_status[table] = '✅'
        else:
            tables_status[table] = '❌'

    for table, status in tables_status.items():
        print(f'   {table}: {status}')

except Exception as e:
    print(f'   ❌ Ошибка проверки таблиц: {e}')

# 3. Тестирование схем
print('3. Проверка схем:')
schema_status = {}

try:
    conn = get_connection()
    cursor = conn.cursor()

    # raw_levels
    cursor.execute("DESCRIBE raw_levels")
    raw_cols = cursor.fetchall()
    schema_status['raw_levels'] = '✅' if len(raw_cols) == 22 else f'❌ ({len(raw_cols)}/22)'

    # analyzed_levels
    cursor.execute("DESCRIBE analyzed_levels")
    analyzed_cols = cursor.fetchall()
    schema_status['analyzed_levels'] = '✅' if len(analyzed_cols) == 21 else f'❌ ({len(analyzed_cols)}/21)'

    # signal_queue
    cursor.execute("DESCRIBE signal_queue")
    signal_cols = cursor.fetchall()
    schema_status['signal_queue'] = '✅' if len(signal_cols) == 28 else f'❌ ({len(signal_cols)}/28)'

    cursor.close()
    conn.close()

    for table, status in schema_status.items():
        print(f'   {table}: {status}')

except Exception as e:
    print(f'   ❌ Ошибка проверки схем: {e}')

# 4. Тестирование операций
print('4. Тестирование операций:')

ops_status = {}

# SELECT
try:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM raw_levels")
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    ops_status['select'] = f'✅ ({count} записей)'
except Exception as e:
    ops_status['select'] = f'❌ ({e})'

# INSERT
try:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO bot_logs (module_name, log_level, message, created_at)
        VALUES (%s, %s, %s, NOW())
    """, ('stage4_test', 'INFO', 'Тест Stage 4'))
    conn.commit()
    cursor.close()
    conn.close()
    ops_status['insert'] = '✅'
except Exception as e:
    ops_status['insert'] = f'❌ ({e})'

for op, status in ops_status.items():
    print(f'   {op}: {status}')

# 5. Тестирование производительности
print('5. Производительность:')
try:
    conn = get_connection()
    cursor = conn.cursor()

    start_time = time.time()
    for _ in range(10):
        cursor.execute("SELECT 1")
        cursor.fetchone()

    elapsed = time.time() - start_time
    avg_time = elapsed / 10
    perf_status = '✅' if avg_time < 0.01 else '⚠️'
    print(f'   SELECT: {avg_time:.4f} сек/запрос {perf_status}')

    cursor.close()
    conn.close()

except Exception as e:
    print(f'   ❌ Ошибка тестирования производительности: {e}')

# 6. Итоговый отчет
print('6. Итоговый отчет:')

all_tables_ok = all(status == '✅' for status in tables_status.values())
all_schemas_ok = all(status == '✅' for status in schema_status.values())
all_ops_ok = all('✅' in status for status in ops_status.values())

if all_tables_ok and all_schemas_ok and all_ops_ok:
    print('🎉 Stage 4 (БД): ВСЕ ТЕСТЫ ПРОЙДЕНЫ!')
    print('   База данных полностью готова к работе.')
else:
    print('⚠️ Stage 4 (БД): ОБНАРУЖЕНЫ ПРОБЛЕМЫ:')
    if not all_tables_ok:
        missing = [t for t, s in tables_status.items() if s == '❌']
        print(f'   - Отсутствующие таблицы: {missing}')
    if not all_schemas_ok:
        bad_schemas = [t for t, s in schema_status.items() if s != '✅']
        print(f'   - Проблемные схемы: {bad_schemas}')
    if not all_ops_ok:
        bad_ops = [op for op, s in ops_status.items() if '✅' not in s]
        print(f'   - Проблемные операции: {bad_ops}')

print()
print('=== Stage 4: Тестирование завершено ===')