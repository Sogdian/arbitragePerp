# ArbitragePerp Bot

Бот для анализа арбитража криптовалюты на фьючерсах между биржами.

## Описание

Бот анализирует арбитражные возможности на фьючерсных рынках между двумя биржами. Он получает данные о ценах фьючерсов и ставках фандинга, вычисляет спреды и выводит информацию в лог.

## Установка

1. Установите зависимости:
```bash
pip install -r requirements.txt
```

2. Создайте файл `.env` (опционально):
```
LOG_FILE=arbitrage_perp_bot.log
LOG_LEVEL=INFO
FUN_FAST_PREP_LEAD_SEC=2.0
FUN_FAST_CLOSE_DELAY_SEC=1.0
FUN_FAST_CLOSE_MAX_ATTEMPTS=15
FUN_FAST_SILENT_TRADING=1
FUN_FAST_OPEN_LEAD_MS=150
FUN_FIX_PRICE_MODE=last
FUN_SHORT_OPEN_FALLBACK_MARKET=0
# Для фиксации close_price в HH:MM:59 по умолчанию достаточно 1m (close последней 1m-свечи).
# Важно: большие интервалы (например 60) могут вернуть "предыдущую закрытую свечу", что не равно цене в HH:MM:59.
FUN_FIX_KLINE_INTERVAL=1
```

## ⚠️ Важно: Работа с терминалом

**НЕ используйте heredoc-команды** (например, `python - <<'PY'`) в PowerShell — это вызывает `SyntaxError`. Используйте только простые команды вида `python -c "..."` или временные файлы. Подробнее см. раздел "Важные правила работы с терминалом" в `CONTEXT.md`.

## Использование (контекст в CONTEXT.md)

### Способ 1: Аргумент командной строки (пример)

**Windows/Linux:**
```bash
python scan_spreads.py (тут SCAN_COIN_INVEST)
python one_coin_bot.py CLO (тут SCAN_COIN_INVEST)
python bot.py "STO Long (mexc), Short (bybit) 30" --no-monitor (30 = количество монет CVC)
python scan_fundings.py
python fun.py "RIVER Bybit 30 -0.3%"
```

**macOS:**
```bash
python3 scan_spreads.py 
python3 bot.py "CVC Long (bybit), Short (gate) 30" --no-monitor
python3 bot.py "CVC Long (bybit), Shor
python3 scan_fundings.py
```

### Способ 2: Интерактивный ввод
```bash
python bot.py
```
Затем введите данные в формате: `монета Long (биржа), Short (биржа) количество_монет`

## Формат ввода

```
монета Long (биржа), Short (биржа) количество_монет
```
## Поддерживаемые биржи

- **Bybit** (bybit) ✅
- **Gate.io** (gate) ✅
- **MEXC** (mexc) ✅
- **LBank** (lbank) ✅
- **XT.com** (xt) ✅
- **Binance** (binance) ✅
- **Bitget** (bitget) ✅
- **OKX** (okx) ✅
- **BingX** (bingx) ✅

## Вывод

Бот выводит следующую информацию:
- Цена монеты на фьючерс для Long биржи
- Фандинг для Long биржи
- Цена монеты на фьючерс для Short биржи
- Фандинг для Short биржи
- Спред на цену на фьючерс между биржами
- Спред на фандинги между биржами

## Логи

Логи сохраняются в файл, указанный в `LOG_FILE` (по умолчанию `arbitrage_perp_bot.log`).

