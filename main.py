import asyncio
import datetime
import json
from typing import List, Dict, Optional, Union
import aitertools
import aiohttp
from aiohttp_socks import ProxyConnector
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from multiprocessing import Process, Queue
from fetches import fetch, PipelineItem, fetch_balancer
from postgres import database_balancer
from s3 import add_to_s3

app = FastAPI()


  # Прокси на уровне пайплайна


# Хранение занятости прокси-серверов
proxy_locks = {}



async def req_gener(pipeline: PipelineItem):
    for i in pipeline.requests:
        yield i


async def execute_pipeline(pipeline: PipelineItem):
    mass_responses = []
    request_gen = req_gener(pipeline)
    connector = None
    proxy_generator = aitertools.cycle(pipeline.proxy)
    async with aiohttp.ClientSession(connector=connector) as session:
        for i in range(0, len(pipeline.requests), len(pipeline.proxy)):
            tasks = []
            # Создание задач для текущей партии запросов
            for j in range(len(pipeline.proxy)):
                request_index = i + j
                if request_index < len(pipeline.requests):
                    request = pipeline.requests[request_index]
                    proxy = await anext(proxy_generator)
                    task = fetch(len_req=len(pipeline.requests), session=session, request=request,
                                 proxy=proxy, pipeline_name=pipeline.pipeline_name)

                    tasks.append(task)
                    print(f"REQUEST NUMBER: {request_index}")

            batch_responses = await asyncio.gather(*tasks)

            total_response = await fetch_balancer(len_req=len(pipeline.requests), session=session, request=request,
                                                  proxy_generator=proxy_generator, pipeline_name=pipeline.pipeline_name,
                                                  batch_responses=batch_responses)
            mass_responses.extend(total_response)
            # print(f"RESPOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOONSWE{batch_responses}")

            # print(f"bBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB{batch_balancer_responses}")
            # for i in batch_balancer_responses:
            #    await add_to_is3(i, pipeline.pipeline_name)
            #    print(f"ASAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA{i}")
            #   print(f"ASAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA{type(i)}")
            # Задержка перед следующей партией запросов

            await asyncio.sleep(pipeline.delay)
        await database_balancer(mass_responses, pipeline.url)


@app.post("/pipeline")
async def run_pipeline(pipeline: PipelineItem, request: Request):
    #request_body = await request.json()
    #print(f"Received request body: {request_body}")

    if not pipeline.requests:
        raise HTTPException(status_code=400, detail="Pipeline must have at least one request")
    if pipeline.delay <= 0:
        raise HTTPException(status_code=400, detail="Delay must be a positive number")

    if not pipeline.proxy and any(req.proxy for req in pipeline.requests):
        raise HTTPException(status_code=400, detail="Proxy must be specified in PipelineItem or RequestItem")

    start_time = datetime.datetime.now()
    print(f"Start Pipeline: {start_time}")
    await execute_pipeline(pipeline)
    end_time = datetime.datetime.now() - start_time
    print(f"End Pipeline: {end_time}")
    return {"status": "Pipeline executed successfully"}
