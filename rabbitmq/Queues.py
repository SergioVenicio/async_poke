import base64
import json
from datetime import datetime

import aiofiles
import aio_pika
from aio_pika.pool import Pool
from grequests import get


class AsyncBaseQueue:
    def __init__(self, exchange, queue, key, io_loop):
        self.exchange = exchange
        self.queue = queue
        self.key = key
        self.io_loop = io_loop
        self.connection_pool = Pool(self.connect, loop=io_loop)
        self.channel_pool = Pool(self.get_channel, loop=io_loop)

    async def setUpQueue(self):

        channel = await self.get_channel()
        await channel.set_qos(1)

        self._exchange = await self.declare_exchange(channel)
        self._queue = await channel.declare_queue(
            self.queue,
            auto_delete=False,
            durable=True,
            exclusive=False
        )

    async def bind(self):
        await self._queue.bind(self._exchange, self.key)

    async def get_channel(self):
        async with self.connection_pool.acquire() as connection:
            return await connection.channel()

    async def connect(self):
        return await aio_pika.connect_robust(
            "amqp://rabbitmq_user:rabbitmq_password@localhost:5672/",
            loop=self.io_loop
        )

    async def declare_exchange(self, channel):
        return await channel.declare_exchange(
            self.exchange, auto_delete=False
        )

    async def publish(self, body):
        msg = await self.message(body)
        await self._exchange.publish(msg, self.key)

    async def consume(self):
        async with self._queue.iterator() as queue_messages:
            async for message in queue_messages:
                await self.callback(message)

    async def message(self, body, content_type='application/json'):
        return aio_pika.Message(
            body.encode('utf-8'), content_type=content_type
        )

    async def request(self, url):
        return get(url).send().response


class DownloadQueue(AsyncBaseQueue):
    def __init__(self, io_loop):
        super().__init__('pokemons', 'download', 'download', io_loop)

    async def write_content(self, content, name):
        _path = f'downloads/{name}.png'
        print(f'[DownloadQueue] Writing {_path}...')
        async with aiofiles.open(_path, 'wb') as _f:
            return await _f.write(content)

    async def callback(self, message):
        img = json.loads(message.body)
        _path = f"{img['name']}_{img['sprite']}"

        print(f"[DownloadQueue] Downloading {_path} image ...")
        b64_text = base64.b64decode(
            img['src'].encode().decode().replace("b'", '').replace("'", '')
        )
        await self.write_content(b64_text, _path)
        await message.ack()


class SpritesQueue(AsyncBaseQueue):
    def __init__(self, io_loop):
        super().__init__('pokemons', 'urls_sprites', 'urls_sprites', io_loop)
        self.dwl_queue = DownloadQueue(io_loop)
        self.error_queue = ErrorQueue(io_loop)

    async def setUpQueue(self):
        await super().setUpQueue()
        await self.dwl_queue.setUpQueue()
        await self.error_queue.setUpQueue()

    async def callback(self, message):
        pokemon = json.loads(message.body.decode('utf8'))

        if not pokemon['url']:
            await self.error_queue.publish(
                origin_queue='SpritesQueue',
                msg_body=pokemon,
                error=f"[ERROR] {pokemon['url']} not found..."
            )
            await message.ack()
            return

        img = await self.request(pokemon['url'])

        if not img:
            await self.error_queue.publish(
                origin_queue='SpritesQueue',
                msg_body=pokemon,
                error=f"[ERROR] {pokemon['name']} image not found!"
            )
            return await message.ack()

        info = f"{pokemon['name']}/{pokemon['sprite']}"
        print(f"[SpriteQueue] Getting sprite {info}...")

        msg = json.dumps({
            'src': str(base64.b64encode(img.content)),
            'name': pokemon['name'],
            'sprite': pokemon['sprite']
        })
        await message.ack()
        await self.dwl_queue.publish(msg)


class URLQueue(AsyncBaseQueue):

    def __init__(self, io_loop):
        super().__init__('pokemons', 'urls', 'urls', io_loop)
        self.sprite_queue = SpritesQueue(io_loop)
        self.error_queue = ErrorQueue(io_loop)

    async def setUpQueue(self):
        await super().setUpQueue()
        await self.sprite_queue.setUpQueue()
        await self.error_queue.setUpQueue()

    async def callback(self, message):
        body = json.loads(message.body.decode('utf8'))
        print(f"[URLQueue] Getting {body['url']} ...")

        try:
            resp = get(body['url']).send().response.json()
        except AttributeError as e:
            await self.error_queue.publish(
                origin_queue='URLQueue',
                msg_body=body,
                error=str(e)
            )
            await message.ack()
            return

        for sprite in self.iter_resp_sprites(resp):
            await self.publish_sprite(sprite)

        await message.ack()

    def iter_resp_sprites(self, resp):
        for sprite in resp['sprites']:
            if (isinstance(resp['sprites'][sprite], dict) or
                    not resp['sprites'][sprite]):
                continue

            yield {
                'name': resp['name'],
                'url': resp['sprites'][sprite],
                'sprite': sprite
            }

    async def publish_sprite(self, sprite):
        return await self.sprite_queue.publish(json.dumps(sprite))


class ErrorQueue(AsyncBaseQueue):
    def __init__(self, io_loop):
        super().__init__('pokemons', 'errors', 'errors', io_loop)
        self._file = 'errors.txt'

    async def publish(self, origin_queue, msg_body, error=''):
        msg = {
            'queue': origin_queue,
            'msg_body': json.dumps(msg_body),
            'error': error,
            'timestamp': datetime.now().timestamp(),
        }
        return await super().publish(json.dumps(msg))

    async def callback(self, message):
        body = json.loads(message.body.decode('utf8'))
        with open(self._file, 'a') as _f:
            _f.write(str(body) + '\n')
        await message.ack()
