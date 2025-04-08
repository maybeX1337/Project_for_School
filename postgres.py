import asyncio
import json
import asyncpg
from typing import List
from datetime import datetime


# Параметры подключения к базе данных

# Глобальная переменная для пула соединений
async def database_balancer(mass_responses: List, url: str):
    list_urls = ["https://kad.arbitr.ru/Kad/SearchInstances", "https://egrul.nalog.ru/", "https://zakupki.gov.ru", "https://bo.nalog.ru", "https://www.nalog.gov.ru/opendata/7707329152-masaddress/", "https://www.fips.ru"]

    if url == list_urls[0]:
        await database_kad_arbitr(mass_responses)

    if url == list_urls[1]:
        await database_egrul(mass_responses)

    if list_urls[3].replace("/", " ") in url.replace("/", " "):
        await database_bo_nalog(mass_responses)
    if url == list_urls[4]:
        await database_nalog_gov(mass_responses)

    if list_urls[5].replace("/", " ") in url.replace("/", " "):
        await database_rospatent(mass_responses)


async def init_pool():
    return await asyncpg.create_pool(
        host="82.97.255.25",
        database="default_db",
        user="gen_user",
        password=")gmq+}_H7mJygd"
    )


async def fetch_in_db(pool, query, *args):
    async with pool.acquire() as connection:
        await connection.fetch(query, *args) 


async def database_kad_arbitr(data_json):
    pool = await init_pool()
    try:
        await pool.execute("""
                CREATE TABLE IF NOT EXISTS kad_arbitr (
                    id SERIAL PRIMARY KEY,
                    date VARCHAR,
                    case_number VARCHAR,
                    case_link VARCHAR,
                    court VARCHAR,
                    plaintiff_name VARCHAR,
                    plaintiff_address VARCHAR,
                    plaintiff_inn VARCHAR,
                    respondent_name VARCHAR,
                    respondent_inn VARCHAR
                );
            """)

        # Сбор данных для вставки
        queries = []
        for row in data_json:
            queries.append(pool.execute("""
                    INSERT INTO kad_arbitr (
                        date, case_number, case_link, court, plaintiff_name, plaintiff_address, plaintiff_inn, respondent_name, respondent_inn
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9
                    );
                """,
                                        row.get("date"),
                                        row.get("case_number"),
                                        row.get("case_link"),
                                        row.get("court"),
                                        row.get("plaintiff").get("name"),
                                        row.get("plaintiff").get("address"),
                                        row.get("plaintiff").get("inn"),
                                        row.get("respondent").get("name"),
                                        row.get("respondent").get("inn")))

            # Запуск всех запросов одновременно
        if queries:
            print(queries)
            await asyncio.gather(*[fetch_in_db(pool, query) for query in queries])
    except Exception as e:
        print(f"Error in database_kad_arbitr: {e}")


async def database_egrul(data_json):
    pool = await init_pool()
    try:
        await pool.execute("""
        CREATE TABLE IF NOT EXISTS egrul1 (
                    id SERIAL PRIMARY KEY,
                    identifier VARCHAR,
                    date DATE,
                    director TEXT,
                    quantity VARCHAR,
                    inn VARCHAR,
                    type VARCHAR,
                    number VARCHAR,
                    ogrn VARCHAR,
                    kpp VARCHAR,
                    registration_date DATE,
                    token TEXT,
                    page INTEGER,
                    total VARCHAR,
                    region TEXT
                );
        """)

        # Сбор данных для вставки
        queries = []
        for i in data_json:
            d = json.loads(i)
            rows = d.get("rows")
            if rows is None:
                print("No rows found in data")
                continue

            for record in rows:
                queries.append((
                    'INSERT INTO egrul (identifier, date, director, quantity, inn, type, number, ogrn, kpp, registration_date, token, page, total, region) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)',
                    [record.get("c"),
                     datetime.strptime(record.get("e", ""), "%d.%m.%Y").date() if record.get("e") else None,
                     record.get("g"), record.get("cnt"), record.get("i"), record.get("k"), record.get("n"),
                     record.get("o"), record.get("p"),
                     datetime.strptime(record.get("r", ""), "%d.%m.%Y").date() if record.get("r") else None,
                     record.get("t"), int(record.get("pg", 0)), record.get("tot"), record.get("rn")]))

            # Запуск всех запросов одновременно
        if queries:
            print(f"queries: GATHER ON DB")
            await asyncio.gather(*[fetch_in_db(pool, query, *args) for query, args in queries])
    finally:
        await pool.close()


async def database_bo_nalog(data_json):
    pool = await init_pool()
    try:
        await pool.execute("""
        CREATE TABLE IF NOT EXISTS bo_nalog (
                    id SERIAL PRIMARY KEY,
                    identifier INTEGER,
                    inn VARCHAR,
                    shortname VARCHAR,
                    ogrn VARCHAR,
                    region TEXT,
                    district VARCHAR,
                    city VARCHAR,
                    settlement VARCHAR,
                    street VARCHAR,
                    house VARCHAR,
                    building VARCHAR,
                    office VARCHAR,
                    okved2 VARCHAR,
                    okopf INTEGER,
                    okato VARCHAR,
                    okfs VARCHAR,
                    statusCode VARCHAR,
                    statusDate DATE
                );
        """)
        await pool.execute("""
        CREATE TABLE IF NOT EXISTS bo_nalog_bfo1 (
                    id SERIAL PRIMARY KEY,
                    period VARCHAR,
                    actualBfoDate DATE,
                    gainSum INTEGER,
                    knd VARCHAR,
                    hasAz BOOLEAN,
                    hasKs BOOLEAN,
                    actualCorrectionNumber INTEGER,
                    actualCorrectionDate DATE,
                    isCb BOOLEAN,
                    id_bo_nalog INTEGER
                );
        """)


        queries = []
        for i in data_json:
            d = json.loads(i)
            rows = d.get("content")
            if rows is None:
                print("No rows found in data")
                continue

            for record in rows:
                

                queries.append((
                    'INSERT INTO bo_nalog (identifier, inn, shortname, ogrn, region, district, city, settlement, street, house, building, office, okved2, okopf, okato, okfs, statusCode, statusDate) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18) RETURNING id;',
                    [record.get("id"),
                     record.get("inn"),
                     record.get("shortName"),
                     record.get("ogrn"),
                     record.get("region"),
                     record.get("district"),
                     record.get("city"),
                     record.get("settlement"),
                     record.get("street"),
                     record.get("house"),
                     record.get("building"),
                     record.get("office"),
                     record.get("okved2"),
                     record.get("okopf"),
                     record.get("okato"),
                     record.get("okfs"),
                     record.get("statusCode"),
                     datetime.strptime(record.get("statusDate"), '%Y-%m-%d').date()]))

                for i in record.get("bfo"):
                    queries.append((
                    'INSERT INTO bo_nalog_bfo1 (period, actualBfoDate, gainSum, knd, hasAz, hasKs, actualCorrectionNumber, actualCorrectionDate, isCb, id_bo_nalog) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) RETURNING id;',
                    [i.get("period"),
                    datetime.strptime(i.get("actualBfoDate"), '%Y-%m-%d').date() if i.get("actualBfoDate") else None,
                    i.get("gainSum"),
                    i.get("knd"),
                    i.get("hasAz"),
                    i.get("hasKs"),
                    i.get("actualCorrectionNumber"),
                    datetime.strptime(i.get("actualCorrectionDate"), '%Y-%m-%d').date(),
                    i.get("isCb"),
                    record.get("id")]))


        if queries:
            print(f"queries: GATHER ON DB bo_nalog")
            await asyncio.gather(*[fetch_in_db(pool, query, *args) for query, args in queries])
    finally:
        await pool.close()



async def database_bo_nalog_bfo(data_json):
    pool = await init_pool()
    try:
        
        queries = []

        for record in data_json:
            queries.append((
                'INSERT INTO bo_nalog_bfo (period, actualBfoDate, gainSum, knd, hasAz, hasKs, actualCorrectionNumber, actualCorrectionDate, isCb, id_bo_nalog) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) RETURNING id;',
                [record.get("period"),
                 datetime.strptime(record.get("actualBfoDate"), '%Y-%m-%d').date() if record.get("actualBfoDate") else None,
                 record.get("gainSum"),
                 record.get("knd"),
                 record.get("hasAz"),
                 record.get("hasKs"),
                 record.get("actualCorrectionNumber"),
                 datetime.strptime(record.get("actualCorrectionDate"), '%Y-%m-%d').date(),
                 record.get("isCb")
                 ]))

        if queries:
            print(f"queries: GATHER ON DB BFO")
            await asyncio.gather(*[fetch_in_db(pool, query, *args) for query, args in queries])
           
    finally:
        await pool.close()


async def database_nalog_gov(data_json):
    pool = await init_pool()
    try:
        await pool.execute("""
        CREATE TABLE IF NOT EXISTS nalog_gov (
                    id SERIAL PRIMARY KEY,
                    url VARCHAR
                );
        """)
        await pool.execute("""
                CREATE TABLE IF NOT EXISTS nalog_gov_archive (
                            id SERIAL PRIMARY KEY,
                            url VARCHAR
                        );
                """)

        # Сбор данных для вставки
        queries = []
        for i in data_json:
            for record in i.get("8"):
                if record is None:
                    print("No record found in data")
                    continue
                queries.append((
                    'INSERT INTO nalog_gov (url) VALUES ($1)',
                    [record]))
            for record in i.get("16"):
                if record is None:
                    print("No record found in data")
                    continue
                queries.append((
                    'INSERT INTO nalog_gov_archive (url) VALUES ($1)',
                    [record]))

            # Запуск всех запросов одновременно
        if queries:
            print(f"queries: GATHER ON DB nalog_gov")
            await asyncio.gather(*[fetch_in_db(pool, query, *args) for query, args in queries])
    finally:
        await pool.close()



async def database_rospatent(data_json):
    pool = await init_pool()
    try:
        await pool.execute("""
        CREATE TABLE IF NOT EXISTS rospatent1 (
                    id SERIAL PRIMARY KEY,
                    patent_number VARCHAR,
                    start_date VARCHAR,
                    registration_date VARCHAR,
                    bulletin_number VARCHAR,
                    correspondence_address VARCHAR,
                    title VARCHAR
                );
        """)

        queries = []
           
        for record1 in data_json:
            record = json.loads(record1)
            queries.append((
                'INSERT INTO rospatent1 (patent_number, start_date, registration_date, bulletin_number, correspondence_address, title) VALUES ($1, $2, $3, $4, $5, $6) RETURNING id;',
                [
                record.get("PatentNumber"),
                record.get("StartDate"),
                record.get("RegistrationDate"),
                record.get("BulletinNumber"),
                record.get("CorrespondenceAddress"),
                record.get("title")]))

        if queries:
            print(f"queries: GATHER ON DB ROSPATENT")
            await asyncio.gather(*[fetch_in_db(pool, query, *args) for query, args in queries])
            
    finally:
        await pool.close()


# psql -h 82.97.255.25 -d default_db -U gen_user
