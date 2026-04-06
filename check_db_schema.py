# Проверка схемы БД
from core.db_connection import get_db
import sys

print('=== Проверка схемы БД ===')

try:
    db = get_db()

    # Проверяем какие таблицы существуют
    with db.cursor() as cursor:
        cursor.execute('SHOW TABLES')
        tables = [row[0] for row in cursor.fetchall()]
        print(f'Существующие таблицы: {tables}')

        expected_tables = ['raw_levels', 'analyzed_levels', 'signal_queue', 'trades', 'logs']
        missing_tables = [t for t in expected_tables if t not in tables]

        if missing_tables:
            print(f'❌ Отсутствующие таблицы: {missing_tables}')
        else:
            print('✅ Все таблицы существуют')

        # Проверяем схему каждой таблицы
        for table in tables:
            if table in expected_tables:
                cursor.execute(f'DESCRIBE {table}')
                columns = cursor.fetchall()
                print(f'{table}: {len(columns)} колонок')

                if table == 'signal_queue':
                    expected_cols = 32
                    if len(columns) != expected_cols:
                        print(f'  ❌ {table}: ожидалось {expected_cols}, получено {len(columns)}')
                        print('  Колонки:')
                        for col in columns:
                            print(f'    {col[0]}: {col[1]}')
                    else:
                        print(f'  ✅ {table}: схема корректна')

                elif table == 'raw_levels':
                    expected_cols = 22
                    if len(columns) != expected_cols:
                        print(f'  ❌ {table}: ожидалось {expected_cols}, получено {len(columns)}')
                    else:
                        print(f'  ✅ {table}: схема корректна')

                elif table == 'analyzed_levels':
                    expected_cols = 21
                    if len(columns) != expected_cols:
                        print(f'  ❌ {table}: ожидалось {expected_cols}, получено {len(columns)}')
                    else:
                        print(f'  ✅ {table}: схема корректна')

except Exception as e:
    print(f'Ошибка: {e}')
    sys.exit(1)

print()
print('=== Проверка завершена ===')