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
import time
from typing import Optional

import config
from bot import PerpArbitrageBot, format_number
from input_parser import parse_input
from position_opener import open_long_short_positions, close_long_short_positions
from telegram_sender import TelegramSender
from fun import _bybit_fetch_executions

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
        
        # Для других бирж пока возвращаем None (можно расширить позже)
        # TODO: Реализовать получение комиссий для Gate, Binance, MEXC, Bitget, BingX
        return None
        
    except Exception as e:
        logger.debug(f"Ошибка при получении комиссий с {exchange_name}: {e}")
        return None


def _calculate_pnl_usdt(
    coin_amount: float,
    ask_long_open: Optional[float],
    bid_long_current: Optional[float],
    bid_short_open: Optional[float],
    ask_short_current: Optional[float],
    fee_long: float = 0.05,
    fee_short: float = 0.05,
) -> Optional[float]:
    """
    Рассчитывает PNL в USDT для арбитража Long/Short.
    
    Args:
        coin_amount: Количество монет
        ask_long_open: Цена покупки Long при открытии (ask)
        bid_long_current: Текущая цена продажи Long (bid)
        bid_short_open: Цена продажи Short при открытии (bid)
        ask_short_current: Текущая цена покупки Short (ask)
        fee_long: Комиссия Long в USDT (по умолчанию 0.05)
        fee_short: Комиссия Short в USDT (по умолчанию 0.05)
    
    Returns:
        PNL в USDT или None если недостаточно данных
    """
    if (ask_long_open is None or bid_long_current is None or 
        bid_short_open is None or ask_short_current is None):
        return None
    
    if coin_amount <= 0 or ask_long_open <= 0 or ask_short_current <= 0:
        return None
    
    # Long: покупаем по ask_long_open, продаем по bid_long_current
    pnl_long = (bid_long_current - ask_long_open) * coin_amount - fee_long
    
    # Short: продаем по bid_short_open, покупаем по ask_short_current
    pnl_short = (bid_short_open - ask_short_current) * coin_amount - fee_short
    
    return pnl_long + pnl_short


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
    fee_long: float = 0.05,
    fee_short: float = 0.05,
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
    logger.info("=" * 60)

    try:
        while True:
            # Данные тикера (bid, ask, funding_rate)
            long_data_task = bot.get_futures_data(long_exchange, coin)
            short_data_task = bot.get_futures_data(short_exchange, coin)
            long_data, short_data = await asyncio.gather(long_data_task, short_data_task, return_exceptions=True)

            if isinstance(long_data, Exception):
                logger.debug(f"Ошибка Long {long_exchange}: {long_data}")
                long_data = None
            if isinstance(short_data, Exception):
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

                closing_spread = bot.calculate_closing_spread(bid_long, ask_short)
                # Цены открытия: при запуске мониторинга без позиций — цены на момент старта; с позициями — цены при открытии
                pnl_ask_long_open = ask_long_open if ask_long_open is not None else ask_long
                pnl_bid_short_open = bid_short_open if bid_short_open is not None else bid_short
                # Спред открытия и L, S в логе — всегда одни и те же (по ценам открытия), не текущие
                opening_spread = bot.calculate_opening_spread(pnl_ask_long_open, pnl_bid_short_open)
                fr_spread = bot.calculate_funding_spread(funding_long, funding_short)
                total_spread = None
                if opening_spread is not None and fr_spread is not None:
                    total_spread = opening_spread + fr_spread

                # Для лога: спред закр как в scan_fundings_spreads (знак как «убыток при закрытии»)
                closing_display = -closing_spread if closing_spread is not None else None
                
                # Форматируем фандинги и минуты как в scan_fundings_spreads.py
                def _format_funding_time(funding_pct: Optional[float], m: Optional[int]) -> str:
                    """Формат для L/S: '-2% 8 мин' или '8 мин' или 'N/A'."""
                    if m is None:
                        return "N/A"
                    if funding_pct is not None:
                        return f"{funding_pct:.2f}% {m} мин"
                    return f"{m} мин"
                
                funding_long_pct = (funding_long * 100) if funding_long is not None else None
                funding_short_pct = (funding_short * 100) if funding_short is not None else None
                l_str = _format_funding_time(funding_long_pct, m_long)
                s_str = _format_funding_time(funding_short_pct, m_short)
                time_str = f" (L: {l_str} | S: {s_str})"
                
                # Расчет PNL: цены открытия — фиксированные (на момент старта мониторинга или открытия ордеров)
                pnl_usdt = _calculate_pnl_usdt(
                    coin_amount=coin_amount,
                    ask_long_open=pnl_ask_long_open,
                    bid_long_current=bid_long,
                    bid_short_open=pnl_bid_short_open,
                    ask_short_current=ask_short,
                    fee_long=fee_long,
                    fee_short=fee_short,
                )
                pnl_str = f"PNL: {format_number(pnl_usdt)} USDT" if pnl_usdt is not None else "PNL: N/A"
                
                # Форматируем цены открытия для лога (5 знаков после запятой)
                opening_price_long = f"{pnl_ask_long_open:.5f}" if pnl_ask_long_open is not None else "N/A"
                opening_price_short = f"{pnl_bid_short_open:.5f}" if pnl_bid_short_open is not None else "N/A"
                opening_str = f"⛳ Откр: {format_number(opening_spread)}% (L: {opening_price_long}, S: {opening_price_short})"
                
                # Для отладки: показываем текущие цены закрытия (можно убрать позже)
                # closing_price_long = format_number(bid_long) if bid_long is not None else "N/A"
                # closing_price_short = format_number(ask_short) if ask_short is not None else "N/A"
                # debug_pnl = f" [L: {opening_price_long}→{closing_price_long}, S: {opening_price_short}→{closing_price_short}]"
                
                log_line = (
                    f"🚩 Спред закр: {format_number(closing_display)}% | "
                    f"{opening_str} | "
                    f"💰 Фанд: {format_number(fr_spread)}%{time_str} | "
                    f"🎯 Общ: {format_number(total_spread)} | "
                    f"{pnl_str}"
                )
                # Выводим без префикса "__main__ - INFO"
                print(log_line)

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

    except KeyboardInterrupt:
        logger.info("Мониторинг прерван пользователем")
    except Exception as e:
        logger.error(f"Ошибка в мониторинге: {e}", exc_info=True)


async def main():
    bot = PerpArbitrageBot()

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
        print("\nОткрыть позиции в лонг и шорт? Введите 'Да' или 'Нет': если 'Да', то позиции будут открыты и введите min цену (через .) закр, для отправки сообщения в тг и закрытия всех позиций.")
        sys.stdout.flush()
        if not sys.stdin.isatty() or os.getenv("BOT_NO_PROMPT") == "1":
            logger.info("Интерактивный ввод недоступен, выход без открытия позиций")
            return

        answer = input().strip().lower()
        open_positions = answer.startswith("да") or answer.startswith("yes") or answer.startswith("y")

        close_threshold_pct: Optional[float] = None
        if open_positions:
            match = re.search(r"([-]?\d+(?:\.\d+)?)", answer)
            if not match:
                logger.error("Не указана min цена закрытия (проценты). Ожидается формат: 'Да, 1' или 'Да, 0.5'")
                return
            try:
                close_threshold_pct = float(match.group(1))
            except ValueError:
                logger.error("Некорректное число для порога закрытия")
                return

            # Получаем текущие цены перед открытием позиций для расчета PNL
            long_data_before = await bot.get_futures_data(long_exchange, coin, need_funding=False)
            short_data_before = await bot.get_futures_data(short_exchange, coin, need_funding=False)
            ask_long_open = long_data_before.get("ask") if long_data_before else None
            bid_short_open = short_data_before.get("bid") if short_data_before else None
            
            opened_ok = await open_long_short_positions(
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
            if fee_long is None:
                logger.warning(f"Не удалось получить реальные комиссии для {long_exchange} Long, используем фиксированные 0.05 USDT")
                fee_long = 0.05
            
            fee_short = await _get_real_fees_from_executions(
                bot=bot,
                exchange_name=short_exchange,
                coin=coin,
                direction="short",
                time_window_sec=10,
            )
            if fee_short is None:
                logger.warning(f"Не удалось получить реальные комиссии для {short_exchange} Short, используем фиксированные 0.05 USDT")
                fee_short = 0.05
            
            logger.info(f"Комиссии: Long {long_exchange}={format_number(fee_long)} USDT, Short {short_exchange}={format_number(fee_short)} USDT")

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
    except Exception as e:
        logger.error(f"Ошибка: {e}", exc_info=True)
    finally:
        await bot.close()


if __name__ == "__main__":
    asyncio.run(main())
