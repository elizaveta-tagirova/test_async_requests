import asyncio
import aiohttp
from collections import defaultdict
import functools
import time
from math import ceil
import requests
import json
from time import sleep
from random import randint
from enum import Enum


class PostsItemsConfig(Enum):
    HOST = "https://dummyjson.com"
    ITEMS_ROUTE = "posts/"
    ITEM_WEIGHT_ROUTE = "posts/{item_id}/comments/"
    PAGING_PARAMS = ("skip", "total", "limit")
    ITEMS_KEY = "posts"
    ITEM_WEIGHT_KEY = "tags"


class MoviesItemsConfig(Enum):
    HOST = "http://185.128.106.196:8000"
    ITEMS_ROUTE = "movies/"
    ITEM_WEIGHT_ROUTE = "user_scores/{item_id}/"
    PAGING_PARAMS = ("page", "num_pages", )
    ITEMS_KEY = "data"
    ITEM_WEIGHT_KEY = "genres"


config = MoviesItemsConfig


def get_initial_params_for_pagination(param_keys):
    return {param_keys.value[0]: 0 if isinstance(param_keys, PostsItemsConfig) else 1}


def get_params_for_pagination(data, param_keys):
    if isinstance(param_keys, MoviesItemsConfig):
        pages_num = data[param_keys.value[1]]
        # param_values = [i for i in range(1, pages_num + 1)]
        param_values = [i for i in range(1, 3)]
    else:
        items_total = data[param_keys.value[1]]
        limit = data[param_keys.value[2]]
        pages_num = ceil(items_total / limit)
        # param_values = [i * limit for i in range(0, pages_num + 1)]
        param_values = [i * limit for i in range(0, 2)]
    return [{param_keys.value[0]: value} for value in param_values]


def count_weights(ids, weights):
    if config is MoviesItemsConfig:
        return {_id: sum(weight) / len(weight) for _id, weight in zip(ids, weights)}
    else:
        return {_id: weight.get("total", 0) for _id, weight in zip(ids, weights)}


def save_items(data, all_items, all_items_ids):
    for item in data[config.ITEMS_KEY.value]:
        _id = item["id"]
        all_items_ids.add(_id)
        keys = item[config.ITEM_WEIGHT_KEY.value]
        for key in keys:
            all_items[key].add(_id)


async def fetch_request(session, url, params={}):
    async with session.get(url, params=params) as response:
        if response.status == 200:
            sleep(1)
            return await response.json(content_type=None)
        else:
            # print(response)
            sleep(1)
            return {}


async def get_items(url, all_items, all_items_ids, headers):
    async with aiohttp.ClientSession(headers=headers) as session:
        param_keys = config.PAGING_PARAMS
        params = get_initial_params_for_pagination(param_keys)
        data = await fetch_request(session, url, params)

        params_set = get_params_for_pagination(data, param_keys)
        _requests = [fetch_request(session, url, params=params) for params in params_set]

        for response in asyncio.as_completed(_requests):
            save_items(await response, all_items, all_items_ids)


async def get_items_weights(url, ids, headers):
    async with aiohttp.ClientSession(headers=headers) as session:
        _requests = [fetch_request(session, url.format(item_id=_id)) for _id in ids]
        weights = await asyncio.gather(*_requests)
        return count_weights(ids, weights)


async def main(headers):
    all_items = defaultdict(set)
    all_items_ids = set()

    items_url = f"{config.HOST.value}/{config.ITEMS_ROUTE.value}"
    await get_items(items_url, all_items, all_items_ids, headers)

    items_weights_url = f"{config.HOST.value}/{config.ITEM_WEIGHT_ROUTE.value}"
    items_weights = await get_items_weights(items_weights_url, all_items_ids, headers)
    # items_weights = dict()
    # for _id in all_items_ids:
    #     items_weights[_id] = randint(1, 20)

    for item_category, items in all_items.items():
        items = sorted(items, key=lambda item: items_weights[item], reverse=True)
        print(f"top 5 for {item_category}: {items[:5]}")

    print()


if __name__ == "__main__":
    HEADERS = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
        "Cache-Control": "max-age=0",
        "If-None-Match": 'W/"150-/zXqMrtCidW6hrnDQ2f8cv0k4rc"',
        "Sec-Ch-Ua": '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36"
    }

    # HOST = "https://dummyjson.com"
    # ROUTES = {"items": "posts/", "item_weight": "posts/{item_id}/comments/"}
    # PARAMS = {"skip": 0}

    # HOST = "http://185.128.106.196:8000"
    # ROUTES = {"items": "movies/", "item_data": "user_scores/{item_id}/'"}
    # PARAMS = {"page": 1}

    asyncio.run(main(HEADERS))

    # items_url = f"{HOST}/{ROUTES['items']}"
    # responses = asyncio.run(get_items(items_url, PARAMS))
