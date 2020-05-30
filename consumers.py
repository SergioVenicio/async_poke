import asyncio
import argparse

from rabbitmq import Queues


__WORKERS__ = {
    'url': Queues.URLQueue,
    'download': Queues.DownloadQueue,
    'sprite': Queues.SpritesQueue,
    'error': Queues.ErrorQueue,
}

parser = argparse.ArgumentParser()
parser.add_argument(
    'worker',
    type=str,
)
args = parser.parse_args()


async def main(loop):
    worker_maker = __WORKERS__.get(args.worker, 'download')
    worker = worker_maker(loop)
    await worker.consume()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
