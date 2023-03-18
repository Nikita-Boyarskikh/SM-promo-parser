#!/usr/bin/env python3
import sys
import re
import argparse
import asyncio
import aiohttp
import itertools
import json
import warnings
from contextlib import asynccontextmanager
from http.cookies import BaseCookie
from urllib.parse import urlparse, urlunparse, urlencode, parse_qs
from typing import List, Iterator, Optional, Mapping, AsyncIterator, Callable, Awaitable

from bs4 import BeautifulSoup
from aiohttp_retry import RetryClient, ExponentialRetry, EvaluateResponseCallbackType


import logging
logging.getLogger('aiohttp_retry').setLevel(logging.DEBUG)
logging.getLogger('aiohttp_retry').addHandler(logging.StreamHandler())

VK_CLIENT_ID = 51584555
VK_AUTH_SCOPE = 'wall'
VK_API_VERSION = '5.131'
VK_API_BASE_URL = 'https://api.vk.com'
VK_REDIRECT_BLANK_URL = 'https://oauth.vk.com/blank.html'
VK_RETRY_ERROR_CODES = {1, 6, 10}
VK_NEVER_RETRY_ERROR_CODES = {5, 9, 29, 223}


LAST_PAGE_SELECTOR = 'nav.navigation ul.pagination li:nth-last-child(2) a'
CATALOG_SELECTOR = '#main-catalog .product-list__name'
ARTICLE_SELECTOR = '.catalog-detail__article > span'

IWAF_JS_COOKIE_RE = re.compile("(iwaf_js_cookie_[^']+)")
IWAF_CHALLENGE_PATH = '/iwaf-challenge'


async def parse_item(client: RetryClient, item_url: str) -> Optional[str]:
    page = await fetch_page(client, item_url)
    if not page:
        return None

    article = page.select_one(ARTICLE_SELECTOR)
    if not article:
        warnings.warn(f'Article for item "{item_url}" is not found... skip')
        return None
    return str(article.string).strip()


async def parse_page(client: RetryClient, url: str) -> List[str]:
    page = await fetch_page(client, url)
    if not page:
        return []

    return await asyncio.gather(*(
        parse_item(client, item_url)
        for item_url in get_item_links(page)
    ))


def get_item_links(page: BeautifulSoup) -> Iterator[str]:
    item_links = page.select(CATALOG_SELECTOR)
    if not item_links:
        warnings.warn(f'There are no items at page "{page.title}"... skip')
        return

    for link in item_links:
        href = link.get_attribute_list('href')
        if href:
            yield href[0]
        else:
            warnings.warn(f'Item "{link.string}" has not href attribute... skip')


def paginate(page: BeautifulSoup) -> Iterator[str]:
    last_page_link_tag = page.select_one(LAST_PAGE_SELECTOR)
    if not last_page_link_tag:
        return

    last_page_url = last_page_link_tag.get_attribute_list('href')
    if not last_page_url:
        warnings.warn('Last page link has not href attribute... skip pagination')
        return

    try:
        last_page_url_parsed = urlparse(last_page_url[0])
        last_page_url_query = parse_qs(last_page_url_parsed.query)
    except ValueError:
        warnings.warn(f'Failed to parse last page url "{last_page_url[0]}"')
        return

    last_page = last_page_url_query.get('page')
    if not last_page:
        warnings.warn(f'There is no ?page in the last page url "{last_page_url[0]}"')
        return
    try:
        parsed_last_page = int(last_page[0])
    except ValueError:
        warnings.warn(f'Last page "{last_page[0]}" is not a number')
        return

    for page_idx in range(2, parsed_last_page + 1):
        last_page_url_query['page'] = [str(page_idx)]
        yield urlunparse(last_page_url_parsed._replace(query=urlencode(last_page_url_query, doseq=True)))


async def iwaf_challenge(resp: aiohttp.ClientResponse, session: aiohttp.ClientSession) -> bool:
    if resp.url.path != IWAF_CHALLENGE_PATH:
        return True

    html = await resp.text()
    result = IWAF_JS_COOKIE_RE.search(html)
    if not result:
        warnings.warn('Failed to proceed iwaf challenge: js cookie not found... retry')
        return False

    iwaf_cookie = result.group(0)
    session.cookie_jar.update_cookies(BaseCookie(iwaf_cookie), resp.url)
    return False


async def fetch_page(client: RetryClient, url: str) -> Optional[BeautifulSoup]:
    try:
        resp = await client.get(url)
    except aiohttp.ClientError as e:
        warnings.warn(f'Failed to load page {url}... skip ({e})')
        return None

    html = await resp.text()
    page = BeautifulSoup(html, 'html.parser')
    return page


def parse_args(args: List[str] = sys.argv[1:]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Парсит артикулы с сайта Спорт-Марафона и отсылает отдельными комментариями к посту ВКонтакте')
    parser.add_argument('-t', '--token', help='Токен доступа к API VK из URL браузера')
    parser.add_argument('-p', '--post', default='-45599639_85065', help='ID поста, в который отправить комментарии (vk.com/wall<POST>)')
    parser.add_argument('-u', '--url', default='https://sport-marafon.ru/rasprodazha/', help='Адрес раздела товаров Спорт-Марафона, с которого парсить')
    parser.add_argument('-c', '--connections', type=int, default=10, help='Количество одновременных соединений')
    parser.add_argument('-r', '--retries', type=int, default=3, help='Количество повторных запросов при ошибке')
    return parser.parse_args(args)


@asynccontextmanager
async def make_client(
    base_url: str,
    headers: Optional[Mapping] = None,
    connections_limit: int = 10,
    retries: int = 3,
    eval_resp_cb: Optional[Callable[[aiohttp.ClientResponse, aiohttp.ClientSession], Awaitable[bool]]] = None,
) -> AsyncIterator[RetryClient]:
    connector = aiohttp.TCPConnector(limit=connections_limit)
    async with aiohttp.ClientSession(
        base_url=base_url,
        connector=connector,
        headers=headers,
    ) as session:
        async def binded_eval_resp_cb(resp: aiohttp.ClientResponse):
            if not eval_resp_cb:
                return True
            return await eval_resp_cb(resp, session)

        retry_options = ExponentialRetry(evaluate_response_callback=binded_eval_resp_cb, attempts=retries)
        client = RetryClient(
            retry_options=retry_options,
            client_session=session,
            raise_for_status=True,
        )
        yield client


async def parse(client: RetryClient, path: str, connections_limit: int = 10, retries: int = 3) -> Iterator[str]:
    page = await fetch_page(client, path)
    if not page:
        return iter([])

    first_page_items_task = asyncio.gather(*(
        parse_item(client, item_url)
        for item_url in get_item_links(page)
    ))
    other_pages_items_task = asyncio.gather(*(
        parse_page(client, page_url) for page_url in paginate(page)
    ))
    first_page_items, other_pages_items = await asyncio.gather(first_page_items_task, other_pages_items_task)
    return (item for item in itertools.chain(first_page_items, *other_pages_items) if item)


async def comment_for_post(client: RetryClient, token: str, post: str, message: str) -> None:
    owner_id, post_id = post.split('_')
    body = {
        'access_token': token,
        'owner_id': owner_id,
        'post_id': post_id,
        'message': message,
        'v': VK_API_VERSION,
    }

    try:
        resp = await client.post('/method/wall.createComment', data=urlencode(body, doseq=True))
        data = await resp.json()
    except aiohttp.ClientError as e:
        warnings.warn(f'Failed to post comment {message}... skip ({e})')
        return
    except json.JSONDecodeError:
        warnings.warn(f'Invalid response from VK... skip ({await resp.text()})')
        return

    error = data.get('error')
    if error:
        error_msg = error.get('error_msg')
        warnings.warn(f'Failed to post comment {message}... skip ({error_msg})')
        return


async def eval_resp_vk(resp: aiohttp.ClientResponse, session: aiohttp.ClientSession) -> bool:
    try:
        data = await resp.json()
    except json.JSONDecodeError:
        return False

    error = data.get('error')
    if not error:
        return True

    code = error.get('error_code')
    if code in VK_RETRY_ERROR_CODES:
        return False
    return code in VK_NEVER_RETRY_ERROR_CODES


async def parse_then_send(url: str, token: str, post: str, connections_limit: int = 10, retries: int = 3) -> None:
    async with make_client(
        base_url=urlunparse(urlparse(url)._replace(path='')),
        headers={'user-agent': 'Python aiohttp'},
        connections_limit=connections_limit,
        eval_resp_cb=iwaf_challenge,
        retries=retries,
    ) as client:
        # articles = await parse(client, url, connections_limit, retries)
        articles = ['TEST']

    async with make_client(
        base_url=VK_API_BASE_URL,
        headers={'Content-Type': 'application/x-www-form-urlencoded'},
        connections_limit=connections_limit,
        eval_resp_cb=eval_resp_vk,
        retries=retries,
    ) as client:
        await asyncio.gather(*(
            comment_for_post(client, token, post, article)
            for article in articles
        ))


def main():
    args = parse_args()
    if not args.token:
        try:
            import webbrowser
            open_url = webbrowser.open
        except:
            open_url = print
        params = urlencode({
            'client_id': VK_CLIENT_ID,
            'scope': VK_AUTH_SCOPE,
            'redirect_uri': VK_REDIRECT_BLANK_URL,
            'display': 'page',
            'response_type': 'token',
            'v': VK_API_VERSION,
        }, doseq=True)
        open_url(f'https://oauth.vk.com/authorize?{params}')
        print('Copy access_token from browser URL')
        return

    loop = asyncio.get_event_loop()
    loop.run_until_complete(parse_then_send(args.url, args.token, args.post, args.connections, args.retries))
    loop.close()


if __name__ == '__main__':
    main()
