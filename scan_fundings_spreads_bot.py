"""
Интерактивный бот для одной пары Long/Short по фандинг-арбитражу:
ввод строки → анализ → вопрос → при ответе «Да, X» открытие позиций и мониторинг
до срабатывания порога |Спред закр| ≤ X% → Telegram и закрытие этой пары.
"""
import asyncio
import logging
import os
import re
import sys
import threading
import time
from datetime import datetime
from typing import Optional

# Флаг «закрыть позиции по CTRL+Z» (на Windows — поток читает stdin, EOF = Ctrl+Z+Enter)
_close_positions_requested: list[bool] = [False]

_windows_close_listener_started: list[bool] = [False]


def _windows_ctrl_z_listener() -> None:
    """Только Windows: в консоли Ctrl+Z и Enter даёт EOF; при EOF выставляем запрос на закрытие."""
    try:
        while True:
            line = sys.stdin.readline()
            if line == "":
                _close_positions_requested[0] = True
                break
    except (EOFError, OSError):
        _close_positions_requested[0] = True

import config
from bot import PerpArbitrageBot, format_number
from input_parser import parse_input
from position_opener import open_long_short_positions, close_long_short_positions, get_binance_fees_from_trades, get_binance_funding_from_income
from telegram_sender import TelegramSender
from fun import _bybit_fetch_executions, _bybit_fetch_funding_from_transaction_log

# ----------------------------
# Logging - используем настройки из bot.py
# ----------------------------
logger = logging.getLogger(__name__)
# fun.py при импорте ставит bot в CRITICAL — для этого скрипта нужен вывод анализа из bot
logging.getLogger("bot").setLevel(logging.INFO)


def _minutes_until_funding(next_funding_time: Optional[int]) -> Optional[int]:
    """Минуты до следующей выплаты фандинга. next_funding_time — секунды или миллисекунды."""
    if next_funding_time is None:
        return None
    try:
        is_seconds = next_funding_time < 10**12
        ts = float(next_funding_time) if is_seconds else next_funding_time / 1000
        sec = ts - time.time()
        if sec < 0:
            return None
        return int(sec / 60)
    except Exception:
        return None


async def _get_real_fees_from_executions(
    bot: PerpArbitrageBot,
    exchange_name: str,
    coin: str,
    direction: str,  # "long" or "short"
    time_window_sec: int = 10,
) -> Optional[float]:
    """
    Получает реальные комиссии из executions за последние time_window_sec секунд.
    
    Args:
        bot: Экземпляр PerpArbitrageBot
        exchange_name: Название биржи
        coin: Название монеты
        direction: Направление позиции ("long" или "short")
        time_window_sec: Окно времени в секундах для поиска executions
    
    Returns:
        Сумма комиссий в USDT или None если не удалось получить
    """
    try:
        exchange_obj = bot.exchanges.get(exchange_name)
        if not exchange_obj:
            return None
        
        # Получаем API ключи из окружения
        api_key_env_map = {
            "bybit": ("BYBIT_API_KEY", "BYBIT_API_SECRET"),
            "gate": ("GATEIO_API_KEY", "GATEIO_API_SECRET"),
            "binance": ("BINANCE_API_KEY", "BINANCE_API_SECRET"),
            "mexc": ("MEXC_API_KEY", "MEXC_API_SECRET"),
            "bitget": ("BITGET_API_KEY", "BITGET_API_SECRET", "BITGET_API_PASSPHRASE"),
            "bingx": ("BINGX_API_KEY", "BINGX_API_SECRET"),
        }
        
        env_keys = api_key_env_map.get(exchange_name.lower())
        if not env_keys:
            return None
        
        api_key = os.getenv(env_keys[0])
        api_secret = os.getenv(env_keys[1])
        if not api_key or not api_secret:
            return None
        
        # Для Bybit используем executions API
        if exchange_name.lower() == "bybit":
            end_ms = int(time.time() * 1000)
            start_ms = end_ms - (time_window_sec * 1000)
            
            execs = await _bybit_fetch_executions(
                exchange_obj=exchange_obj,
                api_key=api_key,
                api_secret=api_secret,
                coin=coin,
                start_ms=start_ms,
                end_ms=end_ms,
                limit=200,
            )
            
            if not execs:
                return None
            
            # Фильтруем executions по направлению
            # Для long: side == "Buy"
            # Для short: side == "Sell"
            filtered_execs = []
            want_side = "Buy" if direction.lower() == "long" else "Sell"
            for exec_item in execs:
                if isinstance(exec_item, dict):
                    side = str(exec_item.get("side") or "")
                    if side == want_side:
                        filtered_execs.append(exec_item)
            
            if not filtered_execs:
                return None
            
            # Рассчитываем сумму комиссий из executions
            fee_total = 0.0
            for exec_item in filtered_execs:
                raw_fee = exec_item.get("execFee")
                if raw_fee is not None:
                    try:
                        fee_total += abs(float(raw_fee))
                    except Exception:
                        pass
                elif exec_item.get("execFeeRate") is not None:
                    try:
                        fee_rate = float(exec_item.get("execFeeRate", 0))
                        if abs(fee_rate) > 0.01:
                            fee_rate = fee_rate / 100.0
                        px = float(exec_item.get("execPrice") or 0.0)
                        q = float(exec_item.get("execQty") or 0.0)
                        if px > 0 and q > 0:
                            fee_total += abs(px * q * fee_rate)
                    except Exception:
                        pass
            
            return fee_total if fee_total > 0 else None
        
        if exchange_name.lower() == "binance":
            end_ms = int(time.time() * 1000)
            start_ms = end_ms - (time_window_sec * 1000)
            fee_total = await get_binance_fees_from_trades(
                exchange_obj=exchange_obj,
                api_key=api_key,
                api_secret=api_secret,
                coin=coin,
                direction=direction,
                start_ms=start_ms,
                end_ms=end_ms,
            )
            return fee_total
        
        # TODO: Реализовать получение комиссий для Gate, MEXC, Bitget, BingX
        return None
        
    except Exception as e:
        logger.debug(f"Ошибка при получении комиссий с {exchange_name}: {e}")
        return None


async def _get_real_funding_usdt(
    bot: PerpArbitrageBot,
    exchange_name: str,
    coin: str,
    open_time: float,
) -> Optional[float]:
    """
    Запрос к бирже: сумма полученного/уплаченного фандинга (USDT) с момента open_time.
    Положительное = получено, отрицательное = уплачено. None при ошибке или неподдерживаемой бирже.
    """
    try:
        exchange_obj = bot.exchanges.get(exchange_name)
        if not exchange_obj:
            return None
        api_key_env_map = {
            "bybit": ("BYBIT_API_KEY", "BYBIT_API_SECRET"),
            "gate": ("GATEIO_API_KEY", "GATEIO_API_SECRET"),
            "binance": ("BINANCE_API_KEY", "BINANCE_API_SECRET"),
            "mexc": ("MEXC_API_KEY", "MEXC_API_SECRET"),
            "bitget": ("BITGET_API_KEY", "BITGET_API_SECRET", "BITGET_API_PASSPHRASE"),
            "bingx": ("BINGX_API_KEY", "BINGX_API_SECRET"),
            "okx": ("OKX_API_KEY", "OKX_API_SECRET"),
        }
        env_keys = api_key_env_map.get(exchange_name.lower())
        if not env_keys:
            return None
        api_key = os.getenv(env_keys[0])
        api_secret = os.getenv(env_keys[1])
        if not api_key or not api_secret:
            return None
        start_ms = int(open_time * 1000)
        end_ms = int(time.time() * 1000)
        if exchange_name.lower() == "bybit":
            return await _bybit_fetch_funding_from_transaction_log(
                exchange_obj=exchange_obj,
                api_key=api_key,
                api_secret=api_secret,
                coin=coin,
                start_ms=start_ms,
                end_ms=end_ms,
            )
        if exchange_name.lower() == "binance":
            return await get_binance_funding_from_income(
                exchange_obj=exchange_obj,
                api_key=api_key,
                api_secret=api_secret,
                coin=coin,
                start_ms=start_ms,
                end_ms=end_ms,
            )
        # TODO: Gate, OKX, Bingx и др. — добавить запрос истории фандинга по API
        return None
    except Exception as e:
        logger.debug(f"Ошибка при получении фандинга с {exchange_name}: {e}")
        return None


def _calculate_pnl_usdt(
    coin_amount: float,
    ask_long_open: Optional[float],
    bid_long_current: Optional[float],
    bid_short_open: Optional[float],
    ask_short_current: Optional[float],
    fee_long: Optional[float] = 0.05,
    fee_short: Optional[float] = 0.05,
    funding_impact_usdt: Optional[float] = None,
) -> Optional[float]:
    """
    Рассчитывает PNL в USDT для арбитража Long/Short.

    Учитывает: разницу цен открытия/закрытия, комиссии (при None комиссия не вычитается), опционально — начисленный фандинг.
    Фандинг: Long платит при rate > 0, Short получает; funding_impact_usdt — суммарный эффект в USDT
    (положительный = мы получили, отрицательный = мы заплатили).
    """
    if (ask_long_open is None or bid_long_current is None or
        bid_short_open is None or ask_short_current is None):
        return None

    if coin_amount <= 0 or ask_long_open <= 0 or ask_short_current <= 0:
        return None

    # Long: покупаем по ask_long_open, продаем по bid_long_current; при fee_long=None комиссию не считаем
    fee_l = fee_long if fee_long is not None else 0.0
    fee_s = fee_short if fee_short is not None else 0.0
    pnl_long = (bid_long_current - ask_long_open) * coin_amount - fee_l

    # Short: продаем по bid_short_open, покупаем по ask_short_current
    pnl_short = (bid_short_open - ask_short_current) * coin_amount - fee_s

    total = pnl_long + pnl_short
    if funding_impact_usdt is not None:
        total += funding_impact_usdt
    return total


async def _monitor_until_close(
    bot: PerpArbitrageBot,
    coin: str,
    long_exchange: str,
    short_exchange: str,
    coin_amount: float,
    close_threshold_pct: Optional[float] = None,
    close_positions_on_trigger: bool = True,
    ask_long_open: Optional[float] = None,
    bid_short_open: Optional[float] = None,
    fee_long: Optional[float] = 0.05,
    fee_short: Optional[float] = 0.05,
):
    """
    Мониторинг каждую секунду. При |Спред закр| ≤ close_threshold_pct:
    отправка в Telegram (FREE_CHANNEL_ID); если close_positions_on_trigger — также закрытие позиций; выход.
    Если close_threshold_pct=None, мониторинг работает без порога закрытия.
    """
    logger.info("=" * 60)
    if close_threshold_pct is not None:
        logger.info(f"Начало мониторинга для {coin} | порог закрытия |спред закр| ≤ {close_threshold_pct}%")
    else:
        logger.info(f"Начало мониторинга для {coin} | без порога закрытия")
    if not close_positions_on_trigger:
        logger.info("Позиции не открыты — при срабатывании порога только отправка в Telegram")
    if close_positions_on_trigger and sys.platform == "win32" and not _windows_close_listener_started[0]:
        _windows_close_listener_started[0] = True
        threading.Thread(target=_windows_ctrl_z_listener, daemon=True).start()
    logger.info("=" * 60)

    # Замороженные цены открытия — не меняются за весь цикл мониторинга.
    # Нужны, чтобы в логе "⛳ Откр" и PNL не пересчитывались от тика к тику по текущему рынку.
    # frozen_ask_long_open — цена ask на бирже Long в момент «открытия» (старт мониторинга или реальный ордер).
    # frozen_bid_short_open — цена bid на бирже Short в момент «открытия».
    # frozen_opening_spread — спред открытия (bid_short - ask_long) / ask_long * 100, считается один раз по замороженным ценам.
    frozen_ask_long_open: Optional[float] = ask_long_open
    frozen_bid_short_open: Optional[float] = bid_short_open
    frozen_opening_spread: Optional[float] = None
    # open_time — время «открытия» (момент заморозки цен), для расчёта начисленного фандинга по числу периодов.
    open_time: Optional[float] = None
    # Интервал выплаты фандинга: 1 час (проверка каждый час).
    FUNDING_INTERVAL_SEC = 3600
    # Для теоретических сделок: ставки фандинга, зафиксированные в последнюю минуту каждого часа (час_индекс -> (rate_long, rate_short)).
    # В PNL фандинг попадает только после закрытия часа, по ставке, запрошенной перед закрытием.
    funding_rates_by_hour: dict[int, tuple[float, float]] = {}
    last_logged_completed_hours: int = -1
    prev_cumulative_funding_long: Optional[float] = None
    prev_cumulative_funding_short: Optional[float] = None

    try:
        while True:
            if close_positions_on_trigger and _close_positions_requested[0]:
                _close_positions_requested[0] = False
                logger.info("Запрос на закрытие позиций (CTRL+Z)")
                break  # выходим в блок закрытия ниже (дублируем логику после цикла)

            try:
                # Данные тикера (bid, ask, funding_rate)
                long_data_task = bot.get_futures_data(long_exchange, coin)
                short_data_task = bot.get_futures_data(short_exchange, coin)
                long_data, short_data = await asyncio.gather(long_data_task, short_data_task, return_exceptions=True)
            except (KeyboardInterrupt, asyncio.CancelledError):
                # CTRL+C — только остановка, без закрытия позиций
                raise KeyboardInterrupt("Остановка пользователем")

            if isinstance(long_data, Exception):
                # Пропускаем CancelledError, чтобы не логировать его как обычную ошибку
                if isinstance(long_data, asyncio.CancelledError):
                    raise KeyboardInterrupt("Прервано пользователем")
                logger.debug(f"Ошибка Long {long_exchange}: {long_data}")
                long_data = None
            if isinstance(short_data, Exception):
                if isinstance(short_data, asyncio.CancelledError):
                    raise KeyboardInterrupt("Прервано пользователем")
                logger.debug(f"Ошибка Short {short_exchange}: {short_data}")
                short_data = None

            # Минуты до выплаты (get_funding_info если есть)
            m_long: Optional[int] = None
            m_short: Optional[int] = None
            long_ex = bot.exchanges.get(long_exchange)
            short_ex = bot.exchanges.get(short_exchange)
            if long_ex and hasattr(long_ex, "get_funding_info"):
                try:
                    fi = await long_ex.get_funding_info(coin)
                    if fi and fi.get("next_funding_time") is not None:
                        m_long = _minutes_until_funding(fi["next_funding_time"])
                except Exception:
                    pass
            if short_ex and hasattr(short_ex, "get_funding_info"):
                try:
                    fi = await short_ex.get_funding_info(coin)
                    if fi and fi.get("next_funding_time") is not None:
                        m_short = _minutes_until_funding(fi["next_funding_time"])
                except Exception:
                    pass

            if long_data and short_data:
                ask_long = long_data.get("ask")
                bid_long = long_data.get("bid")
                bid_short = short_data.get("bid")
                ask_short = short_data.get("ask")
                funding_long = long_data.get("funding_rate")
                funding_short = short_data.get("funding_rate")

                # При первой успешной итерации: если цены открытия не переданы — берём текущий тик; спред считаем один раз; фиксируем время открытия.
                if frozen_opening_spread is None:
                    if frozen_ask_long_open is None:
                        frozen_ask_long_open = ask_long
                    if frozen_bid_short_open is None:
                        frozen_bid_short_open = bid_short
                    frozen_opening_spread = bot.calculate_opening_spread(frozen_ask_long_open, frozen_bid_short_open)
                    open_time = time.time()

                closing_spread = bot.calculate_closing_spread(bid_long, ask_short)
                # В логе и PNL всегда используем замороженные цены открытия и спред
                pnl_ask_long_open = frozen_ask_long_open
                pnl_bid_short_open = frozen_bid_short_open
                opening_spread = frozen_opening_spread
                fr_spread = bot.calculate_funding_spread(funding_long, funding_short)
                total_spread = None
                if opening_spread is not None and fr_spread is not None:
                    total_spread = opening_spread + fr_spread

                # Для лога: спред закр как в scan_fundings_spreads (знак как «убыток при закрытии»)
                closing_display = -closing_spread if closing_spread is not None else None
                
                # Форматируем фандинги и минуты как в scan_fundings_spreads.py
                def _format_funding_time(funding_pct: Optional[float], m: Optional[int]) -> str:
                    """Формат для L/S: '-2% 8 м' или '8 м' или 'N/A'."""
                    if m is None:
                        return "N/A"
                    if funding_pct is not None:
                        return f"{funding_pct:.2f}% {m} м"
                    return f"{m} м"
                
                funding_long_pct = (funding_long * 100) if funding_long is not None else None
                funding_short_pct = (funding_short * 100) if funding_short is not None else None
                l_str = _format_funding_time(funding_long_pct, m_long)
                s_str = _format_funding_time(funding_short_pct, m_short)
                time_str = f" (L: {l_str} | S: {s_str})"
                
                # Фандинг в PNL: только после закрытия часа. Для теоретических сделок — ставку фиксируем в последнюю минуту часа.
                # Накопленный фандинг по биржам в USDT: L и S — отрицательный = уплата, положительный = получение (на обеих биржах ставка может быть и отрицательной). None = нет данных.
                funding_impact_usdt: Optional[float] = None
                funding_long_usdt: Optional[float] = None
                funding_short_usdt: Optional[float] = None
                if open_time is not None and frozen_ask_long_open is not None and frozen_bid_short_open is not None:
                    elapsed_sec = time.time() - open_time
                    notional_long = frozen_ask_long_open * coin_amount
                    notional_short = frozen_bid_short_open * coin_amount
                    if close_positions_on_trigger:
                        # Реальные позиции: только фактические данные с бирж. L и S показываем по отдельности (N/A если нет данных).
                        real_funding_long = await _get_real_funding_usdt(bot, long_exchange, coin, open_time)
                        real_funding_short = await _get_real_funding_usdt(bot, short_exchange, coin, open_time)
                        funding_long_usdt = real_funding_long
                        funding_short_usdt = real_funding_short
                        funding_impact_usdt = (funding_long_usdt + funding_short_usdt) if (funding_long_usdt is not None and funding_short_usdt is not None) else None
                        num_completed_hours = int(elapsed_sec / FUNDING_INTERVAL_SEC)
                        if num_completed_hours > last_logged_completed_hours:
                            delta_long = (real_funding_long or 0.0) - (prev_cumulative_funding_long or 0.0)
                            delta_short = (real_funding_short or 0.0) - (prev_cumulative_funding_short or 0.0)
                            received = max(0.0, delta_long) + max(0.0, delta_short)
                            paid = abs(min(0.0, delta_long)) + abs(min(0.0, delta_short))
                            if received != 0.0 or paid != 0.0:
                                hours_label = "час" if num_completed_hours - last_logged_completed_hours == 1 else "часов"
                                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
                                print(f"{timestamp}    Фандинг за {hours_label}: получено: {format_number(received)} USDT | уплачено: {format_number(paid)} USDT")
                            prev_cumulative_funding_long = real_funding_long
                            prev_cumulative_funding_short = real_funding_short
                            last_logged_completed_hours = num_completed_hours
                    else:
                        # Теоретические: ставки нет в середине часа; в последнюю минуту часа запрашиваем и сохраняем ставку.
                        # В PNL учитываем фандинг только по уже закрытым часам, по сохранённым ставкам.
                        sec_in_hour = elapsed_sec % FUNDING_INTERVAL_SEC
                        # Сохраняем ставку только если обе биржи вернули данные; при None — не подставляем 0, в логе будет N/A.
                        if sec_in_hour >= 3540:  # последняя минута часа (59 мин 0 сек — 59 мин 59 сек)
                            hour_ix = int(elapsed_sec // FUNDING_INTERVAL_SEC)
                            if funding_long is not None and funding_short is not None:
                                funding_rates_by_hour[hour_ix] = (funding_long, funding_short)
                        num_completed_hours = int(elapsed_sec / FUNDING_INTERVAL_SEC)
                        total_funding = 0.0
                        funding_long_total = 0.0
                        funding_short_total = 0.0
                        has_funding_data = False
                        for j in range(num_completed_hours):
                            if j in funding_rates_by_hour:
                                fl, fs = funding_rates_by_hour[j]
                                funding_long_total += -fl * notional_long
                                funding_short_total += fs * notional_short
                                total_funding += -fl * notional_long + fs * notional_short
                                has_funding_data = True
                        if has_funding_data:
                            funding_long_usdt = funding_long_total
                            funding_short_usdt = funding_short_total
                            funding_impact_usdt = total_funding if total_funding != 0.0 else None
                        else:
                            funding_long_usdt = None
                            funding_short_usdt = None
                            funding_impact_usdt = None
                        if num_completed_hours > last_logged_completed_hours:
                            delta_received = 0.0
                            delta_paid = 0.0
                            for hour_ix in range(last_logged_completed_hours + 1, num_completed_hours):
                                if hour_ix in funding_rates_by_hour:
                                    fl, fs = funding_rates_by_hour[hour_ix]
                                    dl = -fl * notional_long
                                    ds = fs * notional_short
                                    delta_received += max(0.0, dl) + max(0.0, ds)
                                    delta_paid += abs(min(0.0, dl)) + abs(min(0.0, ds))
                            if delta_received != 0.0 or delta_paid != 0.0:
                                hours_label = "час" if num_completed_hours - last_logged_completed_hours == 1 else "часов"
                                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
                                print(f"{timestamp}    Фандинг за {hours_label}: получено: {format_number(delta_received)} USDT | уплачено: {format_number(delta_paid)} USDT")
                            last_logged_completed_hours = num_completed_hours

                # Расчет PNL: + комиссии + фандинг за прошедшие периоды
                pnl_usdt = _calculate_pnl_usdt(
                    coin_amount=coin_amount,
                    ask_long_open=pnl_ask_long_open,
                    bid_long_current=bid_long,
                    bid_short_open=pnl_bid_short_open,
                    ask_short_current=ask_short,
                    fee_long=fee_long,
                    fee_short=fee_short,
                    funding_impact_usdt=funding_impact_usdt,
                )
                pnl_str = f"💲 PNL: {format_number(pnl_usdt)} USDT" if pnl_usdt is not None else "💲 PNL: N/A"

                # Форматируем цены открытия для лога (5 знаков после запятой)
                opening_price_long = f"{pnl_ask_long_open:.5f}" if pnl_ask_long_open is not None else "N/A"
                opening_price_short = f"{pnl_bid_short_open:.5f}" if pnl_bid_short_open is not None else "N/A"
                opening_str = f"⛳ Отк: {format_number(opening_spread)}% (L: {opening_price_long}, S: {opening_price_short})"
                
                # Для отладки: показываем текущие цены закрытия (можно убрать позже)
                # closing_price_long = format_number(bid_long) if bid_long is not None else "N/A"
                # closing_price_short = format_number(ask_short) if ask_short is not None else "N/A"
                # debug_pnl = f" [L: {opening_price_long}→{closing_price_long}, S: {opening_price_short}→{closing_price_short}]"
                
                fund_l_str = format_number(funding_long_usdt) if funding_long_usdt is not None else "N/A"
                fund_s_str = format_number(funding_short_usdt) if funding_short_usdt is not None else "N/A"
                fund_str = f"Фанд L: {fund_l_str} | S: {fund_s_str}"
                log_line = (
                    f"🚩 Спред зак: {format_number(closing_display)}% "
                    f"{opening_str} "
                    f"💰 Фанд: {format_number(fr_spread)}%{time_str} "
                    f"🎯 Общ: {format_number(total_spread)} "
                    f"{pnl_str} | {fund_str} "
                    f"⚙️ L {long_exchange} S {short_exchange} | {coin}"
                )
                # Выводим без префикса "__main__ - INFO", добавляем временную метку
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
                print(f"{timestamp} {log_line}")

                # Условие: |Спред закр| ≤ X% (только если порог задан)
                if close_threshold_pct is not None and closing_spread is not None and abs(closing_spread) <= close_threshold_pct:
                    logger.info(f"Порог достигнут: |{closing_spread:.3f}%| ≤ {close_threshold_pct}%")
                    try:
                        telegram = TelegramSender()
                        if telegram.enabled and config.FREE_CHANNEL_ID:
                            long_cap = long_exchange.capitalize()
                            short_cap = short_exchange.capitalize()
                            exit_threshold = bot.get_exit_threshold_pct()
                            message_lines = [
                                f"⏰ <b>Time to close {coin}:</b> Long ({long_cap}) / Short ({short_cap})",
                                f"🚩 <b>Close price:</b> {format_number(closing_display)}% (min: {format_number(exit_threshold)}% цель: {format_number(close_threshold_pct)}%)",
                                f"💰 fr_spread: {format_number(fr_spread)} | 🎯 total_spread: {format_number(total_spread)}",
                            ]
                            await telegram.send_message("\n".join(message_lines), channel_id=config.FREE_CHANNEL_ID)
                            logger.info("Сообщение отправлено в Telegram")
                    except Exception as e:
                        logger.warning(f"Ошибка отправки в Telegram: {e}")

                    if close_positions_on_trigger:
                        ok_closed = await close_long_short_positions(
                            bot=bot,
                            coin=coin,
                            long_exchange=long_exchange,
                            short_exchange=short_exchange,
                            coin_amount=coin_amount,
                        )
                        if ok_closed:
                            logger.info("Позиции закрыты, мониторинг остановлен")
                        else:
                            logger.error("Не удалось закрыть позиции")
                    else:
                        logger.info("Порог достигнут, мониторинг остановлен (позиции не закрывались)")
                    return
            else:
                logger.debug("Нет данных с одной из бирж, пропуск итерации")

            await asyncio.sleep(1)

        # Выход из цикла по CTRL+Z — закрываем позиции и выводим статистику
        if close_positions_on_trigger:
            try:
                bid_long_close = None
                ask_short_close = None
                try:
                    long_data_final = await asyncio.wait_for(
                        bot.get_futures_data(long_exchange, coin, need_funding=False),
                        timeout=3.0
                    )
                    bid_long_close = long_data_final.get("bid") if long_data_final else None
                except Exception as e:
                    logger.debug(f"Не удалось получить цену закрытия Long: {e}")
                try:
                    short_data_final = await asyncio.wait_for(
                        bot.get_futures_data(short_exchange, coin, need_funding=False),
                        timeout=3.0
                    )
                    ask_short_close = short_data_final.get("ask") if short_data_final else None
                except Exception as e:
                    logger.debug(f"Не удалось получить цену закрытия Short: {e}")
                final_funding_long_usdt = None
                final_funding_short_usdt = None
                if open_time is not None:
                    try:
                        final_funding_long_usdt = await asyncio.wait_for(
                            _get_real_funding_usdt(bot, long_exchange, coin, open_time),
                            timeout=3.0
                        )
                    except Exception:
                        pass
                    try:
                        final_funding_short_usdt = await asyncio.wait_for(
                            _get_real_funding_usdt(bot, short_exchange, coin, open_time),
                            timeout=3.0
                        )
                    except Exception:
                        pass
                logger.info("Закрываем открытые позиции...")
                try:
                    ok_closed = await asyncio.wait_for(
                        close_long_short_positions(
                            bot=bot,
                            coin=coin,
                            long_exchange=long_exchange,
                            short_exchange=short_exchange,
                            coin_amount=coin_amount,
                        ),
                        timeout=30.0
                    )
                except Exception as e:
                    logger.error(f"Ошибка при закрытии позиций: {e}")
                    ok_closed = False
                if ok_closed:
                    if frozen_ask_long_open is not None and frozen_bid_short_open is not None:
                        await asyncio.sleep(1.5)
                        fee_long_close = await _get_real_fees_from_executions(
                            bot, long_exchange, coin, "short", time_window_sec=15
                        )
                        fee_short_close = await _get_real_fees_from_executions(
                            bot, short_exchange, coin, "long", time_window_sec=15
                        )
                        fee_l_open = fee_long if fee_long is not None else 0.0
                        fee_s_open = fee_short if fee_short is not None else 0.0
                        fee_l_close = fee_long_close if fee_long_close is not None else 0.0
                        fee_s_close = fee_short_close if fee_short_close is not None else 0.0
                        fee_l_close_str = format_number(fee_long_close) if fee_long_close is not None else "N/A"
                        fee_s_close_str = format_number(fee_short_close) if fee_short_close is not None else "N/A"
                        fee_long_total = fee_l_open + fee_l_close
                        fee_short_total = fee_s_open + fee_s_close
                        fee_total_str = format_number(fee_long_total + fee_short_total)
                        fund_l_usdt = final_funding_long_usdt if final_funding_long_usdt is not None else 0.0
                        fund_s_usdt = final_funding_short_usdt if final_funding_short_usdt is not None else 0.0
                        # 1) Комиссии
                        logger.info(
                            f"L комиссия закр: {fee_l_close_str} | S комиссия закр: {fee_s_close_str} | "
                            f"L комиссия общая: {format_number(fee_long_total)} | S комиссия общая: {format_number(fee_short_total)} | Итоговая комиссия: {fee_total_str}"
                        )
                        # 2) Фандинг L / S
                        if final_funding_long_usdt is not None and final_funding_short_usdt is not None:
                            fund_l_str = format_number(final_funding_long_usdt)
                            fund_s_str = format_number(final_funding_short_usdt)
                            logger.info(f"Фанд L: {fund_l_str} | S: {fund_s_str}")
                            received = max(0.0, final_funding_long_usdt) + max(0.0, final_funding_short_usdt)
                            paid = abs(min(0.0, final_funding_long_usdt)) + abs(min(0.0, final_funding_short_usdt))
                            logger.info(f"Фандинг получено: {format_number(received)} USDT | уплачено: {format_number(paid)} USDT")
                        else:
                            logger.info("Фанд L: N/A | S: N/A")
                        # 3) L/S доход (включая фандинг; при N/A фандинг = 0)
                        income_l = None
                        if bid_long_close is not None and frozen_ask_long_open is not None:
                            income_l = (bid_long_close - frozen_ask_long_open) * coin_amount - fee_l_open - fee_l_close + fund_l_usdt
                        income_s = None
                        if ask_short_close is not None and frozen_bid_short_open is not None:
                            income_s = (frozen_bid_short_open - ask_short_close) * coin_amount - fee_s_open - fee_s_close + fund_s_usdt
                        income_l_str = f"{income_l:.8f}" if income_l is not None else "N/A"
                        income_s_str = f"{income_s:.8f}" if income_s is not None else "N/A"
                        logger.info(f"L доход: {income_l_str} | S доход: {income_s_str}")
                        # 4) Финальный PNL
                        final_pnl = _calculate_pnl_usdt(
                            coin_amount=coin_amount,
                            ask_long_open=frozen_ask_long_open,
                            bid_long_current=bid_long_close,
                            bid_short_open=frozen_bid_short_open,
                            ask_short_current=ask_short_close,
                            fee_long=fee_long_total,
                            fee_short=fee_short_total,
                            funding_impact_usdt=(final_funding_long_usdt + final_funding_short_usdt) if (final_funding_long_usdt is not None and final_funding_short_usdt is not None) else None,
                        )
                        if final_pnl is not None:
                            logger.info(f"💲 Финальный PNL: {format_number(final_pnl)} USDT")
                else:
                    logger.error("Не удалось закрыть позиции")
            except Exception as e:
                logger.error(f"Ошибка при закрытии позиций: {e}", exc_info=True)

    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Мониторинг остановлен (CTRL+C). Позиции не закрываются. Для закрытия позиций используйте CTRL+Z.")
    except Exception as e:
        logger.error(f"Ошибка в мониторинге: {e}", exc_info=True)


async def main():
    bot = PerpArbitrageBot()
    positions_opened = False
    positions_info: dict[str, any] = {}  # coin, long_exchange, short_exchange, coin_amount

    try:
        raw_args = [a.strip() for a in sys.argv[1:]]
        filtered = [a for a in raw_args if a and not a.startswith("--")]
        if filtered:
            input_text = " ".join(filtered)
        else:
            print("Введите данные в формате: монета Long (биржа), Short (биржа) количество_монет")
            print('Пример: STO Long (mexc), Short (bybit) 30')
            input_text = (input().strip() or "").strip()

        if not input_text:
            logger.error("Не указаны вводные данные")
            return

        parsed = parse_input(input_text)
        if not parsed:
            logger.error("Не удалось распарсить ввод")
            return

        coin = parsed["coin"]
        long_exchange = parsed["long_exchange"]
        short_exchange = parsed["short_exchange"]
        coin_amount = parsed.get("coin_amount")
        if coin_amount is None:
            logger.error("Не указано количество монет")
            return

        # Анализ как в bot.py
        monitoring_data = await bot.process_input(input_text)
        if not monitoring_data:
            return

        # Убеждаемся, что все логи выведены перед вопросом
        # Небольшая задержка для завершения всех асинхронных операций логирования
        await asyncio.sleep(0.1)
        sys.stdout.flush()

        # Первый вопрос: открыть позиции?
        print("\nОткрыть позиции в лонг и шорт? Введите 'Да' или 'Нет': если 'Да', то позиции будут открыты. Опционально можно указать min цену (через .) закр для отправки сообщения в тг и закрытия всех позиций (например: 'Да, 1' или 'Да, 0.5').")
        sys.stdout.flush()
        if not sys.stdin.isatty() or os.getenv("BOT_NO_PROMPT") == "1":
            logger.info("Интерактивный ввод недоступен, выход без открытия позиций")
            return

        answer = input().strip().lower()
        open_positions = answer.startswith("да") or answer.startswith("yes") or answer.startswith("y")

        close_threshold_pct: Optional[float] = None
        if open_positions:
            # Порог закрытия опционален: если указан — используем, если нет — мониторинг без порога
            match = re.search(r"([-]?\d+(?:\.\d+)?)", answer)
            if match:
                try:
                    close_threshold_pct = float(match.group(1))
                except ValueError:
                    logger.warning("Некорректное число для порога закрытия, мониторинг без порога")
                    close_threshold_pct = None
            else:
                logger.info("Порог закрытия не указан, мониторинг без порога закрытия")

            opened_ok, long_px_actual, short_px_actual = await open_long_short_positions(
                bot=bot,
                coin=coin,
                long_exchange=long_exchange,
                short_exchange=short_exchange,
                coin_amount=coin_amount,
            )
            if not opened_ok:
                logger.error("Не удалось открыть позиции, мониторинг не запущен")
                return
            
            # Получаем реальные комиссии из исполненных сделок
            # Ждем немного, чтобы executions успели появиться в API
            await asyncio.sleep(1.0)
            
            fee_long = await _get_real_fees_from_executions(
                bot=bot,
                exchange_name=long_exchange,
                coin=coin,
                direction="long",
                time_window_sec=10,
            )
            fee_short = await _get_real_fees_from_executions(
                bot=bot,
                exchange_name=short_exchange,
                coin=coin,
                direction="short",
                time_window_sec=10,
            )
            # При None комиссия в PNL не учитывается, в логе — N/A
            fee_long_str = format_number(fee_long) if fee_long is not None else "N/A"
            fee_short_str = format_number(fee_short) if fee_short is not None else "N/A"
            logger.info(f"Комиссии: Long {long_exchange}={fee_long_str} USDT, Short {short_exchange}={fee_short_str} USDT")

            # Сохраняем информацию об открытых позициях для возможного закрытия при CTRL+C
            positions_opened = True
            positions_info = {
                "coin": coin,
                "long_exchange": long_exchange,
                "short_exchange": short_exchange,
                "coin_amount": coin_amount,
            }
            # Фактические цены исполнения (из результата открытия) — для единых цен в логе мониторинга
            ask_long_open = long_px_actual if long_px_actual is not None else None
            bid_short_open = short_px_actual if short_px_actual is not None else None
            try:
                await _monitor_until_close(
                    bot=bot,
                    coin=coin,
                    long_exchange=long_exchange,
                    short_exchange=short_exchange,
                    coin_amount=coin_amount,
                    close_threshold_pct=close_threshold_pct,
                    close_positions_on_trigger=True,
                    ask_long_open=ask_long_open,
                    bid_short_open=bid_short_open,
                    fee_long=fee_long,
                    fee_short=fee_short,
                )
            except (KeyboardInterrupt, asyncio.CancelledError):
                # Если мониторинг был прерван, но позиции не были закрыты в _monitor_until_close, закрываем здесь
                logger.info("Попытка закрыть позиции после прерывания мониторинга...")
                try:
                    ok_closed = await close_long_short_positions(
                        bot=bot,
                        coin=positions_info["coin"],
                        long_exchange=positions_info["long_exchange"],
                        short_exchange=positions_info["short_exchange"],
                        coin_amount=positions_info["coin_amount"],
                    )
                    if ok_closed:
                        logger.info("Позиции закрыты")
                    else:
                        logger.error("Не удалось закрыть позиции")
                except Exception as e:
                    logger.error(f"Ошибка при закрытии позиций: {e}", exc_info=True)
                raise
            finally:
                positions_opened = False
            return
        # Ответ "Нет" на открытие — второй вопрос: включить мониторинг?
        print("\nВключить мониторинг?")
        print("Введите 'Да' или 'Нет': если 'Да', то введите min цену (через .) закр, для отправки сообщения в тг")
        answer2 = input().strip().lower()
        monitor_yes = answer2.startswith("да") or answer2.startswith("yes") or answer2.startswith("y")
        if not monitor_yes:
            logger.info("Мониторинг не запущен")
            return

        match2 = re.search(r"([-]?\d+(?:\.\d+)?)", answer2)
        if not match2:
            logger.warning("Не найден порог закрытия в ответе, мониторинг без порога закрытия")
            close_threshold_pct = None
        else:
            try:
                close_threshold_pct = float(match2.group(1))
            except ValueError:
                logger.warning("Не удалось распарсить порог закрытия, мониторинг без порога закрытия")
                close_threshold_pct = None

        # Получаем текущие цены для теоретического PNL (как будто открыли позиции сейчас)
        long_data_before = await bot.get_futures_data(long_exchange, coin, need_funding=False)
        short_data_before = await bot.get_futures_data(short_exchange, coin, need_funding=False)
        ask_long_open = long_data_before.get("ask") if long_data_before else None
        bid_short_open = short_data_before.get("bid") if short_data_before else None
        
        # Для теоретического PNL используем фиксированные комиссии 0.05 USDT на биржу
        await _monitor_until_close(
            bot=bot,
            coin=coin,
            long_exchange=long_exchange,
            short_exchange=short_exchange,
            coin_amount=coin_amount,
            close_threshold_pct=close_threshold_pct,
            close_positions_on_trigger=False,
            ask_long_open=ask_long_open,
            bid_short_open=bid_short_open,
            fee_long=0.05,
            fee_short=0.05,
        )

    except KeyboardInterrupt:
        logger.info("Прервано пользователем")
        # Если позиции были открыты, пытаемся их закрыть
        if positions_opened and positions_info:
            try:
                logger.info("Закрываем открытые позиции...")
                ok_closed = await close_long_short_positions(
                    bot=bot,
                    coin=positions_info["coin"],
                    long_exchange=positions_info["long_exchange"],
                    short_exchange=positions_info["short_exchange"],
                    coin_amount=positions_info["coin_amount"],
                )
                if ok_closed:
                    logger.info("Позиции закрыты")
                else:
                    logger.error("Не удалось закрыть позиции")
            except Exception as e:
                logger.error(f"Ошибка при закрытии позиций: {e}", exc_info=True)
    except asyncio.CancelledError:
        logger.info("Операция отменена")
        # Если позиции были открыты, пытаемся их закрыть
        if positions_opened and positions_info:
            try:
                logger.info("Закрываем открытые позиции...")
                ok_closed = await close_long_short_positions(
                    bot=bot,
                    coin=positions_info["coin"],
                    long_exchange=positions_info["long_exchange"],
                    short_exchange=positions_info["short_exchange"],
                    coin_amount=positions_info["coin_amount"],
                )
                if ok_closed:
                    logger.info("Позиции закрыты")
                else:
                    logger.error("Не удалось закрыть позиции")
            except Exception as e:
                logger.error(f"Ошибка при закрытии позиций: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Ошибка: {e}", exc_info=True)
    finally:
        await bot.close()


if __name__ == "__main__":
    asyncio.run(main())
