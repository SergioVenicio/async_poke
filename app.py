from threading import Thread
import concurrent.futures
import asyncio
import aiofiles
import os
import grequests as requests
from datetime import datetime


base_url = 'https://pokeapi.co/api/v2'
download_path = 'downloads/'


def create_dir(dir_path):
    if not os.path.exists(dir_path):
        os.mkdir(dir_path)


async def make_request(url, content=False):
    response = requests.get(url, stream=True).send().response

    if response is None:
        return

    if not content:
        try:
            return response.json()
        except Exception as e:
            print(e)
            return

    return response.content


async def save_file(type_, response):
    if response is None:
        return

    poke_name = response['name']
    print(f'Downloding {poke_name} sprites...')
    poke_path = f'{download_path}/{poke_name}'

    sprites = response['sprites']

    create_dir(poke_path)

    for sprite_name, sprite in sprites.items():
        if sprite is None:
            continue

        file_path = f'{poke_path}/{sprite_name}.{type_}'
        async with aiofiles.open(file_path, 'wb') as _file:
            image_request = await make_request(sprite, content=True)
            await _file.write(image_request)

    print(f'Done download for {poke_name}!')
    return


async def download_file(url, type_='png'):
    try:
        response = await make_request(url)
    except Exception as e:
        print(e)
        print(url)
        return

    return await save_file(type_, response)


async def get_pokes(url):
    pokemons = await make_request(url)

    if pokemons is None:
        return

    download_tasks = [download_file(r['url']) for r in pokemons['results']]

    await asyncio.gather(*download_tasks)


def main_async(url, loop):
    loop.run_until_complete(get_pokes(url))


def spawn_threads(offset=0):
    create_dir(download_path)
    futures = dict()
    executor_pool = concurrent.futures.ProcessPoolExecutor(max_workers=10)
    with executor_pool:
        while offset < 1000:
            loop = asyncio.new_event_loop()
            url = f'{base_url}/pokemon?offset={offset}&limit=100'
            futures[executor_pool.submit(main_async, url, loop)] = url
            offset += 100

    for future in concurrent.futures.as_completed(futures):
        future.result()
        print(f'{futures[future]}: OK')

if __name__ == '__main__':
    start_time = datetime.now()

    spawn_threads()

    print('Time elapsead: ', datetime.now() - start_time)
