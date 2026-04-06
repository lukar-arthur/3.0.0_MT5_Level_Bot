import mysql.connector

print('Проверка таблицы logs...')
try:
    conn = mysql.connector.connect(
        host='localhost', 
        port=3306, 
        user='root', 
        password='', 
        database='mt5_level_engine'
    )
    cursor = conn.cursor()
    
    # Ищем таблицы с logs
    cursor.execute("SHOW TABLES LIKE '%logs%'")
    log_tables = cursor.fetchall()
    print(f'Таблицы с logs: {[t[0] for t in log_tables]}')
    
    if log_tables:
        table_name = log_tables[0][0]
        cursor.execute(f"DESCRIBE {table_name}")
        columns = cursor.fetchall()
        print(f'{table_name}: {len(columns)} колонок')
        for col in columns[:5]:  # первые 5 колонок
            print(f'  {col[0]}: {col[1]}')
    
    # Также проверим raw_levels
    print('\nПроверка raw_levels:')
    cursor.execute("DESCRIBE raw_levels")
    columns = cursor.fetchall()
    print(f'raw_levels: {len(columns)} колонок')
    if len(columns) != 22:
        print(f'  ❌ Ожидалось 22, получено {len(columns)}')
    else:
        print('  ✅ Схема корректна')
        
    conn.close()
except Exception as e:
    print(f'Ошибка: {e}')