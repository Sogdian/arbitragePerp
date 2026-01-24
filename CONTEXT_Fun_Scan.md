# CONTEXT: Скальпинг фандинга (fun.py + scan_fundings.py)

## Обзор

Проект содержит два основных модуля для работы с фандингом на бирже Bybit:

1. **`fun.py`** — боевой скальпинг отрицательного фандинга (открытие short после payout)
2. **`scan_fundings.py`** — сканер фандингов с уведомлениями в Telegram

---

## 1. fun.py — Боевой скальпинг фандинга

### Цель
Получить funding, не неся направленного риска цены, через мгновенный short после payout.

### Архитектура (WebSocket-первый подход)

#### Критическое окно (payout-30ms до payout+1.2s)
```
13:59:59.970   WS FIX (snapshot)      ← за 30ms до payout
14:00:00.000   PAYOUT (funding выплата)
14:00:00.010   OPEN SEND (Sell IOC)   ← ордер отправлен через Trade WS
14:00:00.183   OPEN ACK              ← получен order_id
14:00:00.240   OPEN FILLED           ← подтверждение через Private WS
14:00:01.200   CLOSE START           ← начало закрытия позиции
14:00:01.640   CLOSE DONE            ← закрытие через reduceOnly Buy IOC
```

**Важно**: В критическом окне НЕТ REST запросов — только WS snapshots и create_order.

#### WebSocket модули

1. **Public WS (`exchanges/bybit_ws.py`)**
   - Темы: `orderbook.1`, `publicTrade`, `tickers`
   - Метод: `snapshot()` → `{best_bid, best_ask, last_trade, staleness_ms}`
   - Использование: ценовые данные в реальном времени для открытия

2. **Trade WS (`exchanges/bybit_ws_trade.py`)**
   - Эндпоинт: `wss://stream.bybit.com/v5/trade`
   - Метод: `create_order(order, server_ts_ms, timeout=0.5s)`
   - Возврат: `{"ok": True/False, "order_id": str, "error": str, "raw": dict}`
   - Латентность: ~150-250ms (от send до ACK)
   - **Автоматический retry при `retCode=10001` (positionIdx mismatch)**

3. **Private WS (`exchanges/bybit_ws_private.py`)**
   - Темы: `order`, `execution`, `position`
   - Методы:
     - `wait_final(order_id, timeout=1.5s)` → `OrderFinal` (status, filled_qty, avg_price)
     - `get_position_size(symbol, position_idx, side)` → float (0-REST position snapshot)
     - `get_executions(symbol, start_ms, end_ms)` → List[dict] (0-REST PnL calc)
   - Кэши:
     - `_positions: Dict[(symbol, positionIdx, side), float]` — позиции
     - `_exec_cache: deque(maxlen=5000)` — executions для расчета PnL

#### Timing и серверная синхронизация

```python
# Оценка смещения серверного времени (median из 5 проб)
offset_ms = await _bybit_estimate_time_offset_ms(exchange_obj, samples=5)
# offset_ms = server_ms - local_ms

# План времени (server-time)
payout_server_ms = next_funding_time_ms  # из /v5/market/tickers
open_server_ms = payout_server_ms - OPEN_EARLY_MS  # отправка за 30ms до payout
close_server_ms = payout_server_ms + int(FAST_CLOSE_DELAY_SEC * 1000)  # +1.2s

# Преобразование в local time для asyncio.sleep
await _sleep_until_server_ms(open_server_ms, offset_ms)
```

**ENV параметры для timing:**
- `FUN_OPEN_EARLY_MS=30` — отправлять ордер за N мс ДО payout (server-time)
- `FUN_OPEN_AFTER_MS=0` — устарел (теперь используется OPEN_EARLY_MS)
- `FUN_WS_FIX_LEAD_MS=30` — фиксация ref_px за N мс до payout
- `FUN_FAST_CLOSE_DELAY_SEC=1.2` — старт закрытия ПОСЛЕ payout (в секундах)

#### Entry Admission Model (BPS)

Модель допуска входа основана на величине фандинга и проверяет, не упала ли цена слишком сильно после payout.

```python
# Расчет допустимого дропа (bps)
entry_bps_plan = ENTRY_BASE_BPS + ENTRY_FUNDING_MULT * abs(funding_pct * 10000)
entry_bps_plan = max(ENTRY_MIN_BPS, min(ENTRY_MAX_BPS, entry_bps_plan))

# Проверка при OPEN
down_bps = (ref_px_fix - best_bid_open) / ref_px_fix * 10000
if down_bps > entry_bps_plan:
    SKIP OPEN  # не открываем, т.к. рынок упал слишком сильно
```

**ENV параметры:**
- `FUN_ENTRY_BASE_BPS=40` — базовый допуск (в bps)
- `FUN_ENTRY_FUNDING_MULT=0.9` — множитель от величины фандинга
- `FUN_ENTRY_MIN_BPS=30` — минимальный допуск
- `FUN_ENTRY_MAX_BPS=2500` — максимальный допуск (25%)

**Пример**: При funding=-0.56%, `entry_bps_plan = 40 + 0.9 * 56.4 = 90.8 bps` (~0.9%)

#### Цена ордера (limit_px)

```python
# 1. FIX reference (за 30ms до payout)
ref_px_fix = min(close_price, best_bid_fix)  # для admission check

# 2. OPEN: динамическая цена в момент отправки
best_bid_open = ws_public.snapshot()["best_bid"]  # свежий snapshot
entry_ticks = max(1, OPEN_LIMIT_TICKS, OPEN_SAFETY_TICKS, OPEN_SAFETY_MIN_TICKS)
limit_px = best_bid_open - entry_ticks * tick

order["price"] = format_price(limit_px)
```

**ENV параметры:**
- `FUN_OPEN_LIMIT_TICKS=1` — отступ от `best_bid_open` для качества входа
- `FUN_OPEN_SAFETY_TICKS=1` — дополнительная агрессия (устарело, но используется в max())
- `FUN_OPEN_SAFETY_MIN_TICKS=3` — минимальный отступ от bid

**Итоговый limit_px:** `entry_ticks = max(1, 1, 1, 3) = 3 тика ниже best_bid_open`

#### Логика OPEN (упрощенная)

```python
# 1. Подготовить шаблон ордера (без цены)
order_tmpl = {
    "category": "linear",
    "symbol": symbol,
    "side": "Sell",
    "orderType": "Limit",
    "qty": qty_str,
    "timeInForce": "IOC",
    "positionIdx": position_idx,  # 0 (one-way) или 2 (hedge-short)
}

# 2. В момент OPEN (payout - 30ms)
snap_open = ws_public.snapshot()
best_bid_open = snap_open["best_bid"]

# Admission: пропускаем, если рынок уже сильно упал
down_bps = (ref_px_fix - best_bid_open) / ref_px_fix * 10000
if down_bps > entry_bps_plan:
    logger.info(f"⛔ SKIP OPEN: down_bps={down_bps:.1f} > {entry_bps_plan:.1f}")
    return 0

# 3. Ценообразование: качество vs fill rate
limit_px = best_bid_open - entry_ticks * tick
order = dict(order_tmpl)
order["price"] = format_price(limit_px)

# 4. Отправка через Trade WS (с retry на retCode=10001)
result = await ws_trade.create_order(order, server_ts_ms, timeout=0.5)
if result["ok"]:
    order_id = result["order_id"]
    logger.info(f"✅ OPEN ACK: {order_id}")
else:
    # Retry на positionIdx mismatch (автоматически в create_order)
    logger.error(f"❌ OPEN FAILED: {result['error']}")
    # ВАЖНО: НЕ return 0 — продолжаем до close window для проверки позиции
    order_id = None

# 5. Ждем финальный статус через Private WS
final = await ws_private.wait_final(order_id, timeout=1.5)
open_filled_qty = final.filled_qty
```

#### Логика CLOSE

```python
# 1. Определяем opened_qty (приоритет: Private WS → REST fallback)
if open_filled_qty is not None and open_filled_qty > 0:
    opened_qty = open_filled_qty
else:
    # REST fallback (или WS cache через get_position_size)
    short_after = await _bybit_get_short_qty_snapshot(
        exchange_obj, api_key, api_secret, coin,
        ws_private=ws_private,
        symbol=symbol,
        position_idx=position_idx,
    )
    opened_qty = short_after - short_before

if opened_qty <= 0:
    logger.info("✅ Ничего не открылось, завершаем")
    return 0

# 2. Закрытие через position_opener (WS-first, REST fallback)
ok_close, avg_exit = await po._bybit_close_leg_partial_ioc(
    exchange_obj=exchange_obj,
    coin=coin,
    position_direction="short",
    coin_amount=opened_qty,
    position_idx=position_idx,
    ws_public=ws_public,
    ws_trade=ws_trade,
    ws_private=ws_private,
    tick_raw=tick_raw,
    qty_step_raw=qty_step_raw,
    offset_ms=offset_ms,
)

if ok_close:
    logger.info(f"✅ Short закрыт | qty={opened_qty} | avg_exit={avg_exit}")
else:
    logger.error(f"❌ Short НЕ закрыт полностью")
```

#### Расчет PnL

```python
# После CLOSE: собираем executions (WS cache → REST fallback)
t_start_ms = open_server_ms - 5_000
t_end_ms = close_server_ms + 10_000

# WS cache (0-REST)
execs = ws_private.get_executions(symbol=symbol, start_ms=t_start_ms, end_ms=t_end_ms)
logger.info(f"📦 WS executions cache: got {len(execs)} items")

# REST fallback если WS cache пуст
if not execs:
    execs = await _bybit_fetch_executions(
        exchange_obj, api_key, api_secret, coin,
        start_ms=t_start_ms, end_ms=t_end_ms
    )
    logger.info(f"🌐 REST executions: got {len(execs)} items")

# Расчет PnL
pnl, buys, sells, avg_buy, avg_sell = _bybit_calc_pnl_usdt_from_execs(execs)
logger.info(
    f"📊 Итог (БОЕВОЙ): монета={coin} | "
    f"ср_цена_покупки={avg_buy} | ср_цена_продажи={avg_sell} | "
    f"покупок={buys} продаж={sells} | PnL_USDT_итого={pnl:.3f}"
)
```

#### Обработка ошибок и критические фиксы

**✅ Фикс №1: Дефолтный `positionIdx=0` (one-way mode)**
```python
def _bybit_detect_position_idx(...) -> int:
    """Всегда возвращаем 0 (one-way mode) для скальпинга фандинга."""
    return 0
```

**✅ Фикс №2: Автоматический retry на retCode=10001**
```python
# В bybit_ws_trade.create_order и _bybit_place_limit (REST)
if ret_code == 10001:  # position idx not match position mode
    alt_idx = 0 if position_idx == 2 else 2
    logger.warning(f"🔁 Retry: positionIdx {position_idx} -> {alt_idx}")
    # Повтор с альтернативным positionIdx
```

**✅ Фикс №3: Нет раннего return 0 при ошибке OPEN**
```python
# Старый код (ПЛОХО):
if not order_id:
    await ws_trade.stop()
    await ws_private.stop()
    return 0  # ← ОПАСНО: позиция могла открыться

# Новый код (ХОРОШО):
if not order_id:
    logger.error("❌ OPEN FAILED | ⚠️ Продолжаем до close window")
    order_id = None  # ← продолжаем, чтобы проверить позицию и закрыть
```

**✅ Фикс №4: Funding summary в логах (не print())**
```python
# Старый: print("\n".join(summary_lines))
# Новый:
for line in summary_lines:
    logger.info(line)
```

**✅ Фикс №5: Асинхронное логирование (QueueHandler + QueueListener)**
```python
# Неблокирующий logging через SimpleQueue
_log_queue = _queue.SimpleQueue()
_queue_handler = QueueHandler(_log_queue)
_listener = QueueListener(_log_queue, _file_h, _stream_h)
_listener.start()
atexit.register(lambda: _listener.stop())

# Явный flush перед input() (блокирующая операция)
def _flush_logs():
    for _ in range(50):
        if _log_queue.empty():
            break
        time.sleep(0.01)
    for handler in (_file_h, _stream_h):
        handler.flush()

_flush_logs()
ans = input("Открывать БОЕВОЙ short? (Да/Нет): ")
```

#### ENV параметры (полный список)

**Логирование:**
- `FUN_LOG_LEVEL=INFO`
- `FUN_LOG_FILE=fun.log`

**Тестирование:**
- `FUN_TEST_OB_LEVELS=15` — уровни стакана для тестовых ордеров
- `FUN_MAIN_OB_LEVELS=15` — уровни стакана для preflight проверки

**Timing:**
- `FUN_FAST_PREP_LEAD_SEC=2.0` — подготовка биржи за N секунд до payout
- `FUN_FAST_CLOSE_DELAY_SEC=1.2` — начало закрытия после payout (секунды)
- `FUN_FAST_CLOSE_MAX_ATTEMPTS=15` — макс попыток закрытия
- `FUN_OPEN_EARLY_MS=30` — отправка ордера ДО payout (в мс)
- `FUN_WS_FIX_LEAD_MS=30` — фиксация ref_px до payout (в мс)
- `FUN_LATE_TOL_MS=400` — допуск на позднее начало (в мс)

**Entry admission:**
- `FUN_ENTRY_BASE_BPS=40` — базовый допуск (bps)
- `FUN_ENTRY_FUNDING_MULT=0.9` — множитель от фандинга
- `FUN_ENTRY_MIN_BPS=30` — минимум
- `FUN_ENTRY_MAX_BPS=2500` — максимум

**Open pricing:**
- `FUN_OPEN_LIMIT_TICKS=1` — отступ от best_bid_open
- `FUN_OPEN_SAFETY_TICKS=1` — доп агрессия (устарело)
- `FUN_OPEN_SAFETY_MIN_TICKS=3` — минимальный отступ

**Безопасность:**
- `FUN_OPEN_MAX_STALENESS_MS=200` — макс staleness WS для OPEN
- `FUN_NEWS_DAYS_BACK=60` — глубина проверки новостей (дней)
- `FUN_BALANCE_BUFFER_USDT=0` — буфер баланса USDT
- `FUN_BALANCE_FEE_SAFETY_BPS=20` — запас на комиссии (bps)

**WebSocket:**
- `FUN_USE_TRADE_WS=1` — использовать Trade WS (1) или REST (0)

---

## 2. scan_fundings.py — Сканер фандингов

### Цель
Непрерывное сканирование монет на Bybit для поиска фандингов >= `MIN_FUNDING_SPREAD` с отправкой уведомлений в Telegram.

### Архитектура

```
┌─────────────────────────────────────────────────────┐
│  scan_fundings.py (main loop)                       │
│  ├── collect_coins_by_exchange()                    │
│  │   └── get_all_futures_coins() для каждой биржи  │
│  └── scan_once()                                    │
│      ├── process_coin() × N (батчами)              │
│      │   ├── fetch_funding_info() (semaphore)      │
│      │   ├── fetch_ticker_info() (semaphore)       │
│      │   ├── calculate_min_qty_for_exchange()      │
│      │   └── send_message() → Telegram            │
│      └── sleep(SCAN_INTERVAL_SEC)                   │
└─────────────────────────────────────────────────────┘
```

### Логика сканирования

```python
# 1. Получить монету
coins = await exchange.get_all_futures_coins()
coins_filtered = {c for c in coins if not is_ignored_coin(c) and c not in EXCLUDE_COINS}

# 2. Запросить фандинг (параллельно с ограничением через Semaphore)
sem = asyncio.Semaphore(MAX_CONCURRENCY)
async with sem:
    funding_info = await exchange.get_funding_info(coin)
    funding_rate_pct = funding_info["funding_rate"] * 100

# 3. Проверить условие
if MIN_FUNDING_SPREAD < 0:
    # Для отрицательных порогов: ищем более отрицательные
    if funding_rate_pct > MIN_FUNDING_SPREAD:
        return None  # не подходит
else:
    # Для положительных порогов: ищем >= MIN_FUNDING_SPREAD
    if funding_rate_pct < MIN_FUNDING_SPREAD:
        return None

# 4. Вычислить время до выплаты
next_funding_time = funding_info["next_funding_time"]  # ms
minutes_until = (next_funding_time / 1000 - time.time()) / 60

# 5. Вычислить минимальное кол-во монет (переиспользуется fun._bybit_preflight_and_min_qty)
ticker = await exchange.get_futures_ticker(coin)
min_qty = await calculate_min_qty_for_exchange(bot, exchange, coin, ticker["bid"], SCAN_COIN_INVEST)

# 6. Отправить в Telegram (если minutes_until < SCAN_FUNDING_MIN_TIME_TO_PAY)
if minutes_until < SCAN_FUNDING_MIN_TIME_TO_PAY:
    message = format_telegram_message(opportunity)
    await telegram.send_message(message, channel_id)
```

### Формат сообщения Telegram

```
🔔💲 bybit LPT

funding: -0.800%

time to pay: 19 min

https://www.bybit.com/trade/usdt/LPTUSDT
```

**Поддерживаемые ссылки:**
- Bybit: `https://www.bybit.com/trade/usdt/{coin}USDT`
- Binance: `https://www.binance.com/en/futures/{coin}USDT`
- OKX: `https://www.okx.com/trade-swap/{coin.lower()}-usdt-swap`
- Gate: `https://www.gate.io/futures/usdt/{coin}_USDT`
- Bitget: `https://www.bitget.com/futures/usdt/{coin}USDT`

### ENV параметры

- `MIN_FUNDING_SPREAD=-1` — порог фандинга (в %, например -1 = ищем <= -1%)
- `SCAN_FUNDING_INTERVAL_SEC=60` — интервал сканирования
- `SCAN_FUNDING_MAX_CONCURRENCY=20` — макс параллельных запросов
- `SCAN_FUNDING_COIN_BATCH_SIZE=50` — размер батча монет
- `SCAN_FUNDING_REQ_TIMEOUT_SEC=12` — таймаут запроса к бирже
- `SCAN_FUNDING_MIN_TIME_TO_PAY=0` — минимальное время до выплаты для отправки в TG (мин)
- `SCAN_COIN_INVEST=50` — размер позиции в USDT для расчета min_qty
- `EXCLUDE_COINS=` — монеты для исключения (через запятую, например `FLOW,BTC`)

---

## 3. Интеграция модулей

### position_opener.py
Переиспользуется для открытия/закрытия позиций:
- `_bybit_close_leg_partial_ioc()` — закрытие через Buy IOC reduceOnly (WS-first, REST fallback)
- `_prepare_exchange_for_trading()` — настройка isolated/leverage=1 (best-effort)
- `_format_by_step()`, `_floor_to_step()`, `_ceil_to_step()` — нормализация цены/количества

### WebSocket модули (exchanges/)
- `bybit_ws.py` — Public WS (orderbook, trades, tickers)
- `bybit_ws_trade.py` — Trade WS (order.create/amend/cancel)
- `bybit_ws_private.py` — Private WS (order/execution/position updates)

### Утилиты
- `bot.py` — `PerpArbitrageBot` (wrapper для бирж)
- `config.py` — конфигурация Telegram и ENV_MODE
- `telegram_sender.py` — отправка уведомлений в Telegram

---

## 4. Критические моменты и лучшие практики

### ✅ DO:
1. **Используй WS для критического окна** (payout ±2s) — никаких REST запросов
2. **Делай REST preflight заранее** — фильтры, баланс, news check ДО payout
3. **Синхронизируй с server time** — `_bybit_estimate_time_offset_ms()` с median из 5 проб
4. **Кэшируй позиции через Private WS** — 0-REST для `short_before` и `opened_qty`
5. **Логируй асинхронно** — `QueueHandler` + `QueueListener` для неблокирующего I/O
6. **Не останавливай WS на ошибке OPEN** — продолжай до close window для проверки позиции
7. **Retry на retCode=10001** — автоматически переключай `positionIdx` (0 ↔ 2)
8. **Используй admission model** — пропускай OPEN если `down_bps > entry_bps_plan`

### ❌ DON'T:
1. **НЕ делай REST в критическом окне** — только WS snapshots
2. **НЕ возвращайся рано на ошибку OPEN** — позиция могла открыться несмотря на timeout/error
3. **НЕ используй `print()` для важных логов** — только `logger.info()`
4. **НЕ забывай `_flush_logs()` перед `input()`** — иначе логи зависнут
5. **НЕ используй `best_bid_fix` для pricing** — это только для admission check; pricing делается от `best_bid_open`

---

## 5. Примеры использования

### fun.py (боевой)
```bash
python fun.py "MMT Bybit 50 -0.3%"
```

**Лог (боевой запуск):**
```
💸 FUNDING CONTEXT: rate=-0.564404% | qty=50 | px_pre=0.24897 | notional~12.448 USDT | est_funding~-0.0703 USDT
🧮 ENTRY PLAN (bps): entry_bps_plan=90.8 | base=40.0 mult=0.9 funding_bps=56.4 | min=30.0 max=2500.0
📍 Baseline short BEFORE window: short_before=0 MMT
✅ Public WS готов
✅ Trade WS готов
✅ Private WS готов
📌 WS FIX: close_price=0.25018 best_bid=0.25019 staleness_ms=1.0
🧷 OPEN PREPARED: ref_px=0.25018 entry_bps_plan=90.8 limit_px=0.24789
🚀 OPEN SEND: best_bid_open=0.25018 limit_px=0.24789 qty=50 | funding=-0.564404%
✅ OPEN ACK: order_id=4d501774-a48d-47ac-b89a-0e7be426464b
✅ OPEN FILLED: filled_qty=50 avg_price=0.24944
📍 CLOSE PLAN: using open_filled_qty from private ws = 50 MMT
✅ Short закрыт | qty=50 | avg_exit_buy=0.25015
📦 WS executions cache: got 2 items
📊 Итог (БОЕВОЙ): ср_цена_покупки=0.25015 | ср_цена_продажи=0.24944 | PnL_USDT_итого=-0.06
```

### scan_fundings.py (сканер)
```bash
python scan_fundings.py
```

**Лог:**
```
scan_fundings started | MIN_FUNDING_SPREAD=-1.00% | interval=60s | telegram=enabled
🔄 Новый цикл поиска фандингов | exchanges=['bybit']
💲 LPT bybit | Фандинг: -1.158% | Время выплаты: 19 мин ✅ арбитражить
📱 Отправлено сообщение в Telegram для LPT bybit (время до выплаты: 19 мин)
scan_once finished in 12.3s; sleeping 60.0s
```

---

## 6. Тестирование и мониторинг

### Тестовые ордера (fun.py)
```
Совершить тестовые открытия шорт и лонг? (Да/Нет): да
🧪 Тестовые ордера: запуск
✅ Тест(A): Short открыт | filled=0.2 MMT | avg_entry=0.24950
✅ Тест(A): Short закрыт | avg_exit_buy=0.24955
📊 Итог (ТЕСТ A): PnL_USDT_итого=-0.001
✅ Тестовые ордера прошли успешно
```

### Проверка логов
```bash
tail -f fun.log | grep "📊"
tail -f scan_fundings.log | grep "💲"
```

### Мониторинг WebSocket
```python
# Проверка staleness
print(f"Public WS staleness: {ws_public.snapshot()['staleness_ms']:.1f}ms")
print(f"Private WS staleness: {ws_private.staleness_ms():.1f}ms")

# Проверка ready
assert ws_public.ready
assert ws_trade.is_ready
assert ws_private.ready
```

---

## 7. Известные ограничения и TODO

### Ограничения:
- ⚠️ Только Bybit (другие биржи не поддерживаются в `fun.py`)
- ⚠️ Только short (long не реализован в боевом режиме)
- ⚠️ WS cache executions может иметь gaps при reconnect (REST fallback обязателен для отчетности)
- ⚠️ `positionIdx` определяется статически как 0 (one-way), runtime fallback через retry

### TODO:
- [ ] Поддержка других бирж (Binance, OKX) в `fun.py`
- [ ] Поддержка long позиций для положительного фандинга
- [ ] Websocket reconnect logic с восстановлением subscriptions
- [ ] Advanced order types (PostOnly, GTX) для maker rebates
- [ ] Multi-coin funding scalping (параллельное открытие нескольких монет)
- [ ] Backtesting framework на исторических фандингах + orderbook snapshots

---

## 8. Контракт WebSocket модулей (Protocol)

### BybitTradeWS (bybit_ws_trade.py)
```python
class BybitTradeWS:
    """Trade WS Protocol."""
    
    @property
    def is_ready(self) -> bool:
        """Returns True if authenticated and ready to send orders."""
        ...
    
    async def start(self) -> None:
        """Connects, authenticates, starts reader/ping loops."""
        ...
    
    async def stop(self) -> None:
        """Stops WS, cancels tasks, closes connection (idempotent)."""
        ...
    
    async def create_order(
        self,
        *,
        order: Dict[str, Any],
        server_ts_ms: Optional[int] = None,
        recv_window_ms: Optional[int] = None,
        req_id: Optional[str] = None,
        timeout_sec: float = 0.5,
    ) -> Dict[str, Any]:
        """
        Sends order.create via WS.
        
        Returns:
            Success: {"ok": True, "order_id": str, "raw": dict}
            Failure: {"ok": False, "error": str, "raw": dict}
        
        Note: ACK != Filled. Use Private WS order-stream for fills.
        """
        ...
```

### BybitPrivateWS (bybit_ws_private.py)
```python
class BybitPrivateWS:
    """Private WS Protocol."""
    
    @property
    def ready(self) -> bool:
        """Returns True if authenticated and subscribed."""
        ...
    
    async def wait_final(self, order_id: str, timeout: float = 2.0) -> OrderFinal:
        """Waits for final order status (Filled/Cancelled/Rejected)."""
        ...
    
    def get_position_size(self, *, symbol: str, position_idx: int, side: str) -> Optional[float]:
        """Returns cached position size (0-REST). None if not yet available."""
        ...
    
    def get_executions(self, *, symbol: str, start_ms: int, end_ms: int) -> List[Dict[str, Any]]:
        """Returns cached executions within time window (0-REST PnL calc)."""
        ...
```

---

## Итоговая структура проекта (только скальпинг фандинга)

```
arbitragePerp/
├── fun.py                          ← Боевой скальпинг (MAIN)
├── scan_fundings.py                ← Сканер фандингов + Telegram
├── position_opener.py              ← Общие функции открытия/закрытия
├── exchanges/
│   ├── bybit_ws.py                 ← Public WS (orderbook, trades, tickers)
│   ├── bybit_ws_trade.py           ← Trade WS (order.create)
│   ├── bybit_ws_private.py         ← Private WS (order/execution/position)
│   └── async_bybit.py              ← REST wrapper
├── telegram_sender.py              ← Telegram notifications
├── bot.py                          ← PerpArbitrageBot wrapper
├── config.py                       ← ENV_MODE, TEST_CHANNEL_ID
├── .env                            ← API keys, ENV settings
├── fun.log                         ← Логи fun.py
├── scan_fundings.log               ← Логи scan_fundings.py
├── SCHEMA_OPEN_SHORT.md            ← Временная диаграмма OPEN
└── CONTEXT_Fun_Scan.md             ← ЭТА ДОКУМЕНТАЦИЯ
```

---

## Контакты и поддержка

При возникновении вопросов или проблем:
1. Проверьте логи (`fun.log`, `scan_fundings.log`)
2. Убедитесь, что WebSocket ready (`ws.ready`, `ws.is_ready`)
3. Проверьте staleness WS данных (`snapshot()['staleness_ms']`)
4. Проверьте серверное время (`_bybit_estimate_time_offset_ms()`)
5. Проверьте preflight фильтры (tickSize, qtyStep, minOrderQty)

---

**Версия документа:** 2.0 (2026-01-24)
**Автор:** AI Assistant
**Статус:** Production Ready ✅
