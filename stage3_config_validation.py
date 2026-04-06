# Stage 3: Конфигурационная Валидация
from core.config_loader import load_module_config, load_db_config, load_mt5_config
import json

print('=== Stage 3: Конфигурационная Валидация ===')
print()

# Проверяем загрузку глобальной конфигурации
print('1. Глобальная конфигурация (config.ini):')
try:
    global_cfg = load_module_config()
    print('   ✅ Загружена успешно')
    mt5_path = global_cfg.get('MT5', 'terminal_path', 'не указан')
    db_name = global_cfg.get('DATABASE', 'db_name', 'не указана')
    print(f'   MT5: {mt5_path}')
    print(f'   DB: {db_name}')
except Exception as e:
    print(f'   ❌ Ошибка: {e}')

print()

# Проверяем загрузку модульных конфигураций
modules = ['collector', 'analyzer', 'scalping']
for module in modules:
    print(f'2. Конфигурация модуля {module}:')
    try:
        cfg = load_module_config(module)
        cfg.reload()  # Горячая перезагрузка
        print('   ✅ Загружена успешно')

        if module == 'collector':
            symbols = cfg.get('COLLECTOR', 'symbols', 'не указаны')
            timeframes = cfg.get('COLLECTOR', 'timeframes', 'не указаны')
            print(f'   Символы: {symbols}')
            print(f'   Таймфреймы: {timeframes}')

        elif module == 'analyzer':
            min_score = cfg.getfloat('ANALYZER', 'min_bounce_count', -1)
            print(f'   Минимальный bounce: {min_score}')

        elif module == 'scalping':
            min_s = cfg.getfloat('SIGNAL', 'min_s_score', -1)
            min_t = cfg.getfloat('SIGNAL', 'min_t_score', -1)
            print(f'   Min S-score: {min_s}, Min T-score: {min_t}')

    except Exception as e:
        print(f'   ❌ Ошибка: {e}')
    print()

# Проверяем специализированные загрузчики
print('3. Специализированные конфигурации:')
try:
    db_cfg = load_db_config()
    print('   ✅ DB config загружена')
    print(f'   Host: {db_cfg["host"]}, DB: {db_cfg["database"]}')
except Exception as e:
    print(f'   ❌ DB config ошибка: {e}')

try:
    mt5_cfg = load_mt5_config()
    print('   ✅ MT5 config загружена')
    terminal_short = mt5_cfg['terminal_path'][:50] + '...' if len(mt5_cfg['terminal_path']) > 50 else mt5_cfg['terminal_path']
    print(f'   Terminal: {terminal_short}')
    print(f'   Login: {mt5_cfg["login"]}')
except Exception as e:
    print(f'   ❌ MT5 config ошибка: {e}')

print()
print('=== Валидация диапазонов ===')

# Проверяем диапазоны значений
def validate_range(value, min_val, max_val, name):
    try:
        val = float(value)
        if min_val <= val <= max_val:
            return f'✅ {name}: {val} (в диапазоне {min_val}-{max_val})'
        else:
            return f'⚠️ {name}: {val} (ВНЕ диапазона {min_val}-{max_val})'
    except:
        return f'❌ {name}: {value} (не число)'

collector_cfg = load_module_config('collector')
analyzer_cfg = load_module_config('analyzer')
scalping_cfg = load_module_config('scalping')

print('Collector параметры:')
bars_fetch = collector_cfg.get('COLLECTOR', 'bars_to_fetch', '700')
interval = collector_cfg.get('COLLECTOR', 'interval_sec', '1800')
print(f'   {validate_range(bars_fetch, 100, 2000, "bars_to_fetch")}')
print(f'   {validate_range(interval, 300, 3600, "interval_sec")}')

print('Analyzer веса (должны суммироваться к 1.0):')
weights = ['w_bounce', 'w_freshness', 'w_confluence', 'w_volume', 'w_multitf', 'w_reversal', 'w_dynamics', 'w_stat', 'w_rsi']
total_weight = 0
for w in weights:
    val = analyzer_cfg.getfloat('WEIGHTS', w, 0)
    total_weight += val
    print(f'   {w}: {val}')
weight_status = '✅' if abs(total_weight - 1.0) < 0.01 else '❌'
print(f'   Сумма весов: {total_weight} {weight_status} (должна быть 1.0)')

print('Scalping пороги:')
min_s_score = scalping_cfg.get('SIGNAL', 'min_s_score', '7.5')
min_t_score = scalping_cfg.get('SIGNAL', 'min_t_score', '0.7')
print(f'   {validate_range(min_s_score, 0, 10, "min_s_score")}')
print(f'   {validate_range(min_t_score, 0, 1, "min_t_score")}')

print()
print('=== Проверка типов данных ===')

# Проверяем типы данных
def check_type(value, expected_type, name):
    try:
        if expected_type == 'int':
            int(value)
            return f'✅ {name}: {value} (int)'
        elif expected_type == 'float':
            float(value)
            return f'✅ {name}: {value} (float)'
        elif expected_type == 'bool':
            if value.lower() in ('true', 'false', '1', '0', 'yes', 'no'):
                return f'✅ {name}: {value} (bool)'
            else:
                return f'❌ {name}: {value} (не bool)'
        else:
            return f'✅ {name}: {value} (string)'
    except:
        return f'❌ {name}: {value} (не {expected_type})'

print('Типы данных в конфигурациях:')
print(f'   {check_type(collector_cfg.get("COLLECTOR", "min_bounce_to_record", "1"), "int", "min_bounce_to_record")}')
print(f'   {check_type(analyzer_cfg.get("ANALYZER", "max_touch_age_days", "3"), "int", "max_touch_age_days")}')
print(f'   {check_type(scalping_cfg.get("TRADER", "allow_real", "false"), "bool", "allow_real")}')
print(f'   {check_type(scalping_cfg.get("TRADER", "lot_size", "0.01"), "float", "lot_size")}')

print()
print('=== Stage 3: Валидация завершена ===')