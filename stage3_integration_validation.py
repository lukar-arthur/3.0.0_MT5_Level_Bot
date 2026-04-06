# Stage 3: Интеграционная Валидация
import sys
import os
sys.path.append(os.path.dirname(__file__))

from core.config_loader import load_module_config, load_db_config
from core.db_connection import get_db
from core.mt5_bridge import get_mt5_bridge
from modules.collector.collector import CollectorModule
from modules.analyzer.analyzer import AnalyzerModule
from modules.strategies.scalping.signal_engine import ScalpingModule
from modules.strategies.scalping.trader import TraderModule
from modules.strategies.scalping.evaluator import SignalEvaluator

print('=== Stage 3: Интеграционная Валидация ===')
print()

# 1. Проверяем подключение к базе данных
print('1. Тестирование подключения к БД:')
try:
    db = get_db()
    with db.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM raw_levels")
        raw_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM analyzed_levels")
        analyzed_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM signal_queue")
        signal_count = cursor.fetchone()[0]
        print('   ✅ Подключение к БД успешно')
        print(f'   raw_levels: {raw_count} записей')
        print(f'   analyzed_levels: {analyzed_count} записей')
        print(f'   signal_queue: {signal_count} записей')
except Exception as e:
    print(f'   ❌ Ошибка подключения к БД: {e}')
    sys.exit(1)

print()

# 2. Проверяем инициализацию модулей
print('2. Тестирование инициализации модулей:')
modules_status = {}

try:
    collector = CollectorModule()
    collector_cfg = collector.get_config()
    modules_status['collector'] = '✅'
    print(f'   ✅ Collector инициализирован')
    print(f'      Символы: {len(collector_cfg["symbols"])}')
    print(f'      Таймфреймы: {collector_cfg["timeframes"]}')
except Exception as e:
    modules_status['collector'] = '❌'
    print(f'   ❌ Collector ошибка: {e}')

try:
    analyzer = AnalyzerModule()
    analyzer_cfg = analyzer.get_config()
    modules_status['analyzer'] = '✅'
    print(f'   ✅ Analyzer инициализирован')
    print(f'      Минимальный bounce: {analyzer_cfg["min_bounce_count"]}')
    print(f'      Весов факторов: {len([k for k in analyzer_cfg.keys() if k.startswith("w_")])}')
except Exception as e:
    modules_status['analyzer'] = '❌'
    print(f'   ❌ Analyzer ошибка: {e}')

try:
    scalping_module = ScalpingModule()
    scalping_cfg = scalping_module.get_config()
    modules_status['scalping'] = '✅'
    print(f'   ✅ Scalping Module инициализирован')
    print(f'      Min S-score: {scalping_cfg["signal"]["min_s_score"]}')
    print(f'      Min T-score: {scalping_cfg["signal"]["min_t_score"]}')
except Exception as e:
    modules_status['scalping'] = '❌'
    print(f'   ❌ Scalping Module ошибка: {e}')

try:
    trader = TraderModule()
    trader_cfg = trader.get_config()
    modules_status['trader'] = '✅'
    print(f'   ✅ Trader инициализирован')
    print(f'      Lot size: {trader_cfg["lot_size"]}')
    print(f'      Allow real: {trader_cfg["allow_real"]}')
except Exception as e:
    modules_status['trader'] = '❌'
    print(f'   ❌ Trader ошибка: {e}')

try:
    evaluator = SignalEvaluator()
    evaluator_cfg = evaluator.get_config()
    modules_status['evaluator'] = '✅'
    print(f'   ✅ Evaluator инициализирован')
    print(f'      Интервал: {evaluator_cfg["interval_sec"]} сек')
except Exception as e:
    modules_status['evaluator'] = '❌'
    print(f'   ❌ Evaluator ошибка: {e}')

print()

# 3. Проверяем конфигурационную изоляцию
print('3. Тестирование конфигурационной изоляции:')
isolation_test = {}

# Проверяем, что модули не видят чужие параметры
collector_cfg = load_module_config('collector')
analyzer_cfg = load_module_config('analyzer')
scalping_cfg = load_module_config('scalping')

# Collector не должен видеть analyzer параметры
analyzer_param = analyzer_cfg.get('WEIGHTS', 'w_bounce', 'NOT_FOUND')
collector_sees_analyzer = collector_cfg.get('WEIGHTS', 'w_bounce', 'NOT_FOUND')

if analyzer_param != 'NOT_FOUND' and collector_sees_analyzer == 'NOT_FOUND':
    isolation_test['collector_isolation'] = '✅'
    print('   ✅ Collector изолирован от analyzer параметров')
else:
    isolation_test['collector_isolation'] = '❌'
    print('   ❌ Collector видит analyzer параметры')

# Проверяем fallback к глобальной конфигурации
global_cfg = load_module_config()  # Без имени модуля
global_mt5 = global_cfg.get('MT5', 'terminal_path', 'NOT_FOUND')
collector_mt5 = collector_cfg.get('MT5', 'terminal_path', 'NOT_FOUND')

if global_mt5 != 'NOT_FOUND' and collector_mt5 == global_mt5:
    isolation_test['fallback_works'] = '✅'
    print('   ✅ Fallback к глобальной конфигурации работает')
else:
    isolation_test['fallback_works'] = '❌'
    print('   ❌ Fallback к глобальной конфигурации не работает')

print()

# 4. Проверяем параметры на соответствие логике
print('4. Валидация параметров на соответствие логике:')

logic_checks = {}

# Collector: ATR мультипликаторы должны быть в разумных пределах
atr_mult_checks = []
for tf in ['D', 'H4', 'H1']:
    mult = collector_cfg.getfloat('COLLECTOR', f'atr_zone_mult_{tf}', 0)
    if 0.05 <= mult <= 1.0:
        atr_mult_checks.append('✅')
    else:
        atr_mult_checks.append('❌')

if all(c == '✅' for c in atr_mult_checks):
    logic_checks['atr_multipliers'] = '✅'
    print('   ✅ ATR мультипликаторы в допустимых пределах')
else:
    logic_checks['atr_multipliers'] = '❌'
    print('   ❌ ATR мультипликаторы вне допустимых пределов')

# Analyzer: веса должны быть положительными
weight_checks = []
for w in ['w_bounce', 'w_freshness', 'w_confluence', 'w_volume', 'w_multitf']:
    weight = analyzer_cfg.getfloat('WEIGHTS', w, -1)
    if weight >= 0:
        weight_checks.append('✅')
    else:
        weight_checks.append('❌')

if all(c == '✅' for c in weight_checks):
    logic_checks['positive_weights'] = '✅'
    print('   ✅ Все веса факторов положительные')
else:
    logic_checks['positive_weights'] = '❌'
    print('   ❌ Некоторые веса факторов отрицательные')

# Scalping: SL должен быть меньше TP
sl_mult = scalping_cfg['signal'].get('sl_atr_mult', 0)
tp_mult = scalping_cfg['signal'].get('tp_atr_mult', 0)
if sl_mult < tp_mult:
    logic_checks['sl_tp_logic'] = '✅'
    print('   ✅ SL множитель меньше TP множителя')
else:
    logic_checks['sl_tp_logic'] = '❌'
    print('   ❌ SL множитель не меньше TP множителя')

print()

# 5. Итоговый отчет
print('5. Итоговый отчет Stage 3:')
print()

all_passed = True

print('Модули:')
for module, status in modules_status.items():
    print(f'   {module}: {status}')
    if status == '❌':
        all_passed = False

print()
print('Конфигурационная изоляция:')
for test, status in isolation_test.items():
    print(f'   {test}: {status}')
    if status == '❌':
        all_passed = False

print()
print('Логическая валидация:')
for check, status in logic_checks.items():
    print(f'   {check}: {status}')
    if status == '❌':
        all_passed = False

print()
if all_passed:
    print('🎉 Stage 3: ВСЕ ТЕСТЫ ПРОЙДЕНЫ! Конфигурационная система готова к продакшену.')
else:
    print('⚠️ Stage 3: ОБНАРУЖЕНЫ ПРОБЛЕМЫ. Требуется исправление перед продакшеном.')

print()
print('=== Stage 3: Интеграционная валидация завершена ===')