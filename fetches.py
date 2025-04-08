import json

import aiofiles

from converters import *
from postgres import database_egrul, database_bo_nalog

from postgres import database_kad_arbitr

from s3 import add_to_s3
from test_parse_html import parse


class RequestItem(BaseModel):
    endpoint: str
    method: str
    headers: Optional[Dict[str, Union[str, int]]] = None
    body: Optional[Union[str, Dict[str, Union[str, int, None, List, bool]]]] = None
    proxy: Optional[str] = None  # Прокси для конкретного запроса
    repeat_count: Optional[int] = 10  # Количество повторов запроса


class PipelineItem(BaseModel):
    url: str
    requests: List[RequestItem]
    delay: float
    proxy: List | None = None
    pipeline_name: str


async def fetch(pipeline_name: str, len_req: int, session: aiohttp.ClientSession, request: RequestItem,
                proxy: Optional[str] = None):
    # print(f"Fetching URL: {request.endpoint} with method: {request.method}")

    headers = request.headers or {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:127.0) Gecko/20100101 Firefox/127.0",
        "Content-Type": "application/json",
        "Cookie": "__ddg1_=HjV4YnpM5AnzKpXSXNeO; CUID=d7289f8e-7196-4795-aea8-9f46f000b544:7ns4zR1u9oinKbm1kWV4LA==; _ga=GA1.2.725042750.1720434111; _ga_EYS41HMRV3=GS1.2.1721382345.7.0.1721382345.60.0.0; _ga_Q2V7P901XE=GS1.2.1721382345.7.0.1721382345.0.0.0; _ga_9582CL89Y6=GS1.2.1721382345.7.0.1721382345.60.0.0; _ym_uid=1720083697342683285; _ym_d=1720434112; tmr_lvid=259964b518c97e5cac4c377391dceccf; tmr_lvidTS=1720083697213; domain_sid=LzEfxy3YyPptZoWaQbZMU%3A1721382345343; ASP.NET_SessionId=fu3hpvdl0wulrmzss03at5dw; _gid=GA1.2.1903476658.1721382345; _dc_gtm_UA-157906562-1=1; _gat=1; _gat_FrontEndTracker=1; _ym_isad=2; pr_fp=308a1ea21793f273a66bf13650b0637fcb2b95d6b4b74e2c0b951f82975ea874; tmr_detect=0%7C1721382347222; wasm=13e31a211838b78da3a99cc94d6eb71a; rcid=8ac907cf-3525-4738-b9a1-0a4f73c98b8c"}
    # print(f"Request headers: {headers}")
   # print(f"request_body: {request.body}")
    json_body = request.body if request.body else None

    for attempt in range(11):
        try:
            request_proxy = proxy
            connector = None
            if request_proxy:
                connector = ProxyConnector.from_url(request_proxy)
                print(f"Using proxy: {request_proxy}")

            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.request(
                        method=request.method.upper(),
                        url=request.endpoint,
                        headers=headers,
                        json=json_body
                ) as response:
                    response_text = await response.text()
                    #print(
                    #    f"Response status: {response.status}, Response text: {response_text}, Cookies: {response.cookies}")

                    return response_text
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {str(e)}")
            await asyncio.sleep(10)
            if attempt + 1 == request.repeat_count:
                raise HTTPException(status_code=500,
                                    detail=f"Request failed after {request.repeat_count} attempts: {str(e)}")


async def fetch_balancer(batch_responses, pipeline_name: str, len_req: int, session: aiohttp.ClientSession,
                         request: RequestItem, proxy_generator):
    list_urls = ["https://kad.arbitr.ru/Kad/SearchInstances", "https://egrul.nalog.ru/", "https://zakupki.gov.ru", "https://bo.nalog.ru", "https://www.nalog.gov.ru/opendata/7707329152-masaddress/", "https://www.fips.ru","https://2gis.ru/spb/search/%D0%9F%D0%BE%D0%B5%D1%81%D1%82%D1%8C/page/2?m=30.34173%2C59.915548%2F13.2"]
    if request.endpoint == list_urls[0]:
        response = await parse_html_to_json(batch_responses)
        return response

    if request.endpoint == list_urls[1]:
        tasks_balancer = []
        for response_text in batch_responses:
            if response_text:
                request.method = "GET"
                proxy = await anext(proxy_generator)
                dict = json.loads(response_text)
                request.endpoint = f"{list_urls[1]}search-result/{dict.get('t')}"
                response = fetch(f"{pipeline_name}_data1", len_req, session, request, proxy)
                tasks_balancer.append(response)
                print(f"ENDPOINT:  {request.endpoint}")
        batch_balancer_responses = await asyncio.gather(*tasks_balancer)
        return batch_balancer_responses

    if list_urls[2].replace("/", " ") in request.endpoint.replace("/", " "):
        response = await parse_html_to_json_zak(batch_responses)
        print(response)
        return response

    if list_urls[3].replace("/", " ") in request.endpoint.replace("/", " "):
        return batch_responses

    if request.endpoint == list_urls[4]:
        return await parse_html_to_json_nalog_gov(batch_responses)

    if list_urls[5].replace("/", " ") in request.endpoint.replace("/", " "):
        return await parse_to_json_rospatent(batch_responses)

    if request.endpoint == list_urls[6]:
        return parse(batch_responses)








# async def key_gen(filename):
#     async with aiofiles.open(filename, 'r') as file:
#         async for line in file:
#             yield line.strip()


async def html_to_json(response_text, func, pipeline_name):
    response_json = await func(response_text)
    print(f"JSOOOOOOOOON: {response_json}")

    await add_to_s3(response_json, pipeline_name)
    print(f"JSON_MASS: {response_json}")
    return response_json
