import asyncio
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from shared.database import init_db
from shared.message_queue import queue
from worker import TriageWorker


async def main():
    """Основная функция воркера"""

    init_db()

    queue.create_consumer_group()

    worker = TriageWorker()
    await worker.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Worker stopped")
    except Exception as e:
        print(f"Worker error: {e}")



