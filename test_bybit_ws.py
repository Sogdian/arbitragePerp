"""
Тестовый скрипт для проверки WebSocket Bybit.
Печатает данные каждую секунду в течение 20 секунд.
"""

import asyncio
import logging
import sys
import time
from exchanges.bybit_ws import BybitPublicWS

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger(__name__)


async def test_ws():
    """Тестирует WebSocket для одного символа."""
    symbol = "RIVERUSDT"
    
    logger.info(f"🔌 Запуск WebSocket теста для {symbol}")
    
    ws_client = BybitPublicWS(symbol=symbol)
    ws_task = asyncio.create_task(ws_client.run())
    
    try:
        # Ждем готовности
        logger.info("⏳ Ожидание готовности WebSocket...")
        ready = await ws_client.wait_ready(timeout=15.0)
        if not ready:
            logger.warning("⚠️ WebSocket не готов в течение 15 секунд")
        else:
            logger.info("✅ WebSocket готов")
        
        # Печатаем данные каждую секунду в течение 20 секунд
        logger.info("📊 Начинаем печать данных (каждую секунду, 20 секунд)...")
        print("\n" + "=" * 80)
        print(f"{'Время':<10} | {'best_bid':<15} | {'best_ask':<15} | {'last_trade':<15} | {'staleness_ms':<15}")
        print("=" * 80)
        
        start_time = time.monotonic()
        iteration = 0
        max_iterations = 20
        
        while iteration < max_iterations:
            await asyncio.sleep(1.0)
            iteration += 1
            
            snapshot = await ws_client.get_snapshot()
            now = time.monotonic()
            
            # Вычисляем staleness для каждого типа данных
            bidask_staleness_ms = (now - snapshot.ts_bidask_monotonic) * 1000 if snapshot.ts_bidask_monotonic else None
            trade_staleness_ms = (now - snapshot.ts_trade_monotonic) * 1000 if snapshot.ts_trade_monotonic else None
            ticker_staleness_ms = (now - snapshot.ts_ticker_monotonic) * 1000 if snapshot.ts_ticker_monotonic else None
            
            # Берем максимальный staleness (самые старые данные)
            max_staleness_ms = None
            if bidask_staleness_ms is not None:
                max_staleness_ms = bidask_staleness_ms
            if trade_staleness_ms is not None:
                if max_staleness_ms is None or trade_staleness_ms > max_staleness_ms:
                    max_staleness_ms = trade_staleness_ms
            if ticker_staleness_ms is not None:
                if max_staleness_ms is None or ticker_staleness_ms > max_staleness_ms:
                    max_staleness_ms = ticker_staleness_ms
            
            # Форматируем значения
            best_bid_str = f"{snapshot.best_bid:.8f}" if snapshot.best_bid is not None else "N/A"
            best_ask_str = f"{snapshot.best_ask:.8f}" if snapshot.best_ask is not None else "N/A"
            last_trade_str = f"{snapshot.last_trade:.8f}" if snapshot.last_trade is not None else "N/A"
            staleness_str = f"{max_staleness_ms:.1f}" if max_staleness_ms is not None else "N/A"
            
            elapsed = time.monotonic() - start_time
            print(f"{elapsed:>6.1f}s | {best_bid_str:>15} | {best_ask_str:>15} | {last_trade_str:>15} | {staleness_str:>15}ms")
        
        print("=" * 80)
        logger.info("✅ Тест завершен")
        
    except KeyboardInterrupt:
        logger.info("⏹️ Тест прерван пользователем")
    except Exception as e:
        logger.error(f"❌ Ошибка в тесте: {e}", exc_info=True)
    finally:
        logger.info("🔌 Остановка WebSocket...")
        await ws_client.stop()
        logger.info("✅ WebSocket остановлен")


if __name__ == "__main__":
    try:
        asyncio.run(test_ws())
    except KeyboardInterrupt:
        print("\n⏹️ Прервано пользователем")
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}", file=sys.stderr)
        sys.exit(1)

