import asyncio
import json
import os
from shutil import rmtree

from grequests import get

from rabbitmq.Queues import URLQueue


def make_urls():
    offset = 0
    while offset <= 2000:
        yield f'https://pokeapi.co/api/v2/pokemon/?limit=100&offset={offset}'
        offset += 100


async def img_url(url, sprite='front_default'):
    return get(url).send().response.json()['sprites'][sprite]


def iter_poke(poke_map):
    print('Generating pokes...')
    for pokes in poke_map:
        for p in pokes:
            yield p


async def main(loop):
    url_queue = URLQueue(loop)
    urls = make_urls()

    if os.path.exists('downloads'):
        rmtree('downloads')
    os.makedirs('downloads')


    for url in urls:
        pokemons = get(url).send().response.json()['results']

        for pokemon in pokemons:
            print(pokemon)
            await url_queue.publish(
                json.dumps(pokemon)
            )

    return


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
