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

# ----------------------------
# Logging
# ----------------------------
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)


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


async def _monitor_until_close(
    bot: PerpArbitrageBot,
    coin: str,
    long_exchange: str,
    short_exchange: str,
    coin_amount: float,
    close_threshold_pct: Optional[float] = None,
    close_positions_on_trigger: bool = True,
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
                opening_spread = bot.calculate_opening_spread(ask_long, bid_short)
                fr_spread = bot.calculate_funding_spread(funding_long, funding_short)
                total_spread = None
                if opening_spread is not None and fr_spread is not None:
                    total_spread = opening_spread + fr_spread

                # Для лога: спред закр как в scan_fundings_spreads (знак как «убыток при закрытии»)
                closing_display = -closing_spread if closing_spread is not None else None
                l_str = f"{m_long} мин" if m_long is not None else "N/A"
                s_str = f"{m_short} мин" if m_short is not None else "N/A"
                time_str = f" (L: {l_str} | S: {s_str})"
                log_line = (
                    f"🚩 Спред закр: {format_number(closing_display)}% | "
                    f"⛳ Откр: {format_number(opening_spread)}% | "
                    f"💰 Фанд: {format_number(fr_spread)}%{time_str} | "
                    f"🎯 Общ: {format_number(total_spread)} ⚙️ Long {long_exchange} | Short {short_exchange} | {coin}"
                )
                logger.info(log_line)

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
        logger.info("Анализ арбитража для %s", coin)
        monitoring_data = await bot.process_input(input_text)
        if not monitoring_data:
            return

        # Первый вопрос: открыть позиции?
        print("\nОткрыть позиции в лонг и шорт? Введите 'Да' или 'Нет': если 'Да', то позиции будут открыты и введите min цену (через .) закр, для отправки сообщения в тг и закрытия всех позиций.")
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

            await _monitor_until_close(
                bot=bot,
                coin=coin,
                long_exchange=long_exchange,
                short_exchange=short_exchange,
                coin_amount=coin_amount,
                close_threshold_pct=close_threshold_pct,
                close_positions_on_trigger=True,
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

        await _monitor_until_close(
            bot=bot,
            coin=coin,
            long_exchange=long_exchange,
            short_exchange=short_exchange,
            coin_amount=coin_amount,
            close_threshold_pct=close_threshold_pct,
            close_positions_on_trigger=False,
        )

    except KeyboardInterrupt:
        logger.info("Прервано пользователем")
    except Exception as e:
        logger.error(f"Ошибка: {e}", exc_info=True)
    finally:
        await bot.close()


if __name__ == "__main__":
    asyncio.run(main())
