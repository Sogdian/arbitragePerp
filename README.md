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

## ⚠️ Важно: Работа с терминалом

**НЕ используйте heredoc-команды** (например, `python - <<'PY'`) в PowerShell — это вызывает `SyntaxError`. Используйте только простые команды вида `python -c "..."` или временные файлы. Подробнее см. раздел "Важные правила работы с терминалом" в `CONTEXT.md`.

## Использование (контекст в CONTEXT.md)

### Способ 1: Аргумент командной строки (пример)

**Windows/Linux:**
```bash
python scan_spreads.py (тут SCAN_COIN_INVEST) - спред цен
python one_coin_bot.py CLO (тут SCAN_COIN_INVEST) - спред цен по одной монете
python bot.py "STO Long (mexc), Short (bybit) 30" --no-monitor (30 = количество монет CVC) - открытие и закрытие ордеров
python scan_fundings.py - поиск фанлингов
python fun.py "LPT Bybit 10 -0.1%" - скальпинг фандингов
python scan_fundings_spreads.py - скан спреда фандингов
```

**macOS:**
```bash
python3 scan_spreads.py 
python3 bot.py "CVC Long (bybit), Short (gate) 30" --no-monitor
python3 bot.py "CVC Long (bybit), Short (gate)"
python3 scan_fundings.py
python3 scan_fundings_spreads.py
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

