import asyncio
from typing import List, Dict, Optional, Union
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
import aiohttp
from aiohttp_socks import ProxyConnector
from aiobotocore.session import get_session
from botocore.exceptions import ClientError
from bs4 import BeautifulSoup
import json
import re
from pyquery import PyQuery
from s3 import add_to_s3
import logging



async def parse_html_to_json(html_content: str) -> List[Dict[str, Union[str, Dict[str, str]]]]:
    doc = PyQuery(html_content)
    data = []
    rows = doc('tr.odd, tr:empty')
    for row in rows.items():
        date_div = row.find('div.civil')
        date = date_div.find('span').text().strip() if date_div else "N/A"

        case_a = row.find('a.num_case')
        case_number = case_a.text().strip() if case_a else "N/A"
        case_link = case_a.attr('href').strip() if case_a else "N/A"

        court_td = row.find('td.court')
        court_div = court_td.find('div')
        court = court_div.text().strip() if court_div else "N/A"

        plaintiff_td = row.find('td.plaintiff')
        if plaintiff_td:
            plaintiff_info = plaintiff_td.find('span.js-rolloverHtml')
            if plaintiff_info:
                info_divs = plaintiff_info.find('div')
                plaintiff_name = plaintiff_td.find('strong').text().strip() if plaintiff_td.find('strong') else "N/A"
                plaintiff_address = info_divs.eq(0).text().strip() if len(info_divs) > 0 else "N/A"
                plaintiff_inn = info_divs.eq(1).text().strip().split(': ')[1] if len(info_divs) > 1 else "N/A"
            else:
                plaintiff_name = "N/A"
                plaintiff_address = "N/A"
                plaintiff_inn = "N/A"
        else:
            plaintiff_name = "N/A"
            plaintiff_address = "N/A"
            plaintiff_inn = "N/A"

        respondent_td = row.find('td.respondent')
        if respondent_td:
            respondent_info = respondent_td.find('span.js-rolloverHtml')
            if respondent_info:
                info_divs = respondent_info.find('div')
                respondent_name = respondent_td.find('strong').text().strip() if respondent_td.find('strong') else "N/A"
                respondent_inn = info_divs.eq(0).text().strip().split(': ')[1] if len(info_divs) > 0 else "N/A"
            else:
                respondent_name = "N/A"
                respondent_inn = "N/A"
        else:
            respondent_name = "N/A"
            respondent_inn = "N/A"

        data.append({
            'date': date,
            'case_number': case_number,
            'case_link': case_link,
            'court': court,
            'plaintiff': {
                'name': plaintiff_name,
                'address': plaintiff_address,
                'inn': plaintiff_inn
            },
            'respondent': {
                'name': respondent_name,
                'inn': respondent_inn
            }
        })
    print(data)
    return data


from bs4 import BeautifulSoup
from typing import List, Dict, Union

async def parse_html_to_json_zak(html_content: str) -> List[Dict[str, Union[str, Dict[str, str]]]]:
    soup = BeautifulSoup(html_content, 'html.parser')
    data = []

    entries = soup.find_all('div', class_='search-registry-entry-block')

    for entry in entries:
        title = entry.find('div', class_='registry-entry__header-top__title')
        title_text = title.get_text(strip=True) if title else "N/A"

        organization = entry.find('div', class_='registry-entry__header-mid__number a')
        organization_name = organization.get_text(strip=True) if organization else "N/A"

        ogrn = entry.find('div', class_='registry-entry__body-title', text='ОГРН')
        ogrn_value = ogrn.find_next_sibling('div', class_='registry-entry__body-value').get_text(strip=True) if ogrn else "N/A"

        inn = entry.find('div', class_='registry-entry__body-title', text='ИНН')
        inn_value = inn.find_next_sibling('div', class_='registry-entry__body-value').get_text(strip=True) if inn else "N/A"

        kpp = entry.find('div', class_='registry-entry__body-title', text='КПП')
        kpp_value = kpp.find_next_sibling('div', class_='registry-entry__body-value').get_text(strip=True) if kpp else "N/A"

        level = entry.find('div', class_='text-block__title')
        level_text = level.get_text(strip=True) if level else "N/A"

        description = entry.find('div', class_='data-block__value')
        description_text = description.get_text(strip=True) if description else "N/A"

        links = entry.find_all('div', class_='href')
        links_data = []
        for link in links:
            a_tag = link.find('a')
            if a_tag:
                link_text = a_tag.get_text(strip=True)
                link_url = a_tag['href']
                links_data.append({'text': link_text, 'url': link_url})

        data.append(json.dumps({
            'title':title_text,
            'organization_name': organization_name,
            'ogrn': ogrn_value,
            'inn': inn_value,
            'kpp': kpp_value,
            'level': level_text,
            'description': description_text,
            'links': links_data
        }))

    return data


async def parse_html_to_json_nalog_gov(html_list):
    links_dict = {'8': [], '16': []}
    target_indices = [8, 16]
    mass = []
    for html in html_list:
        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table', class_='border_table')
        if not table:
            continue

        rows = table.find_all('tr')
        for row in rows:
            cols = row.find_all('td')
            if cols and int(cols[0].text.strip()) in target_indices:
                index = int(cols[0].text.strip())
                link_tags = cols[2].find_all('a')
                for link in link_tags:
                    links_dict[str(index)].append(link.get('href'))

    mass.append(links_dict)
    return mass


from bs4 import BeautifulSoup


async def parse_to_json_rospatent(html_contents):
    results = []

    for html_content in html_contents:
        soup = BeautifulSoup(html_content, 'html.parser')

        # Основные данные
        update_date_elem = soup.find('td', id='StatusRAP')
        update_date = update_date_elem.text.strip() if update_date_elem else "Не указано"

        # Словарь для хранения данных
        data = {
            "21": "Не указано",
        }

        # Извлечение данных из таблицы bib
        bib_table = soup.find('table', id='bib')
        if bib_table:
            rows = bib_table.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                for cell in cells:
                    text = cell.get_text(strip=True)
                    if text.startswith("(21)"):
                        data["21"] = text.split(":", 1)[1].strip()

        # Название изобретения
        title_tag = soup.find('p', id='B542')
        title = "Не указано"
        if title_tag:
            title_text = title_tag.get_text(strip=True)
            title_parts = title_text.split(" ", 1)
            if len(title_parts) > 1:
                title = title_parts[1].strip()


        results.append(await parse_patent_info(data["21"], title))

    return results


async def parse_patent_info(full_info, title):
    data = {
        "title": title
    }

    # Регулярные выражения для извлечения данных
    patterns = {
        "PatentNumber": r'(\d{10})',
        "StartDate": r'Дата начала отсчета срока действия патента:(\d{2}\.\d{2}\.\d{4})',
        "RegistrationDate": r'Дата регистрации:(\d{2}\.\d{2}\.\d{4})',
        "PublicationDate": r'Дата публикации заявки:(\d{2}\.\d{2}\.\d{4})',
        "PCTStartDate": r'Дата начала рассмотрения заявки PCT на национальной фазе:(\d{2}\.\d{2}\.\d{4})',
        "Priority": r'Конвенционный приоритет:;(.+)',
        "BulletinNumber": r'Бюл\. №(\d+)',
        "Citations": r'Список документов, цитированных в отчете о поиске:(.+?)\(.*?\)',
        "PCTApplication": r'Заявка PCT:(.+?)\(\d{2}\.\d{2}\.\d{4}\)',
        "PCTPublication": r'Публикация заявки PCT:(.+)',
        "CorrespondenceAddress": r'Адрес для переписки:(.+)'
    }

    # Извлечение данных с использованием регулярных выражений
    for key, pattern in patterns.items():
        match = re.search(pattern, full_info, re.DOTALL)
        if match:
            if key == "Citations":
                citations = match.group(1).strip().split('. ')
                data[key] = [citation.strip() for citation in citations if citation]
            elif key == "BulletinNumber":
                data[key] = f"Бюл. №{match.group(1)}"
            else:
                data[key] = match.group(1).strip()

    # Обработка приоритетов и PCT
    if "Priority" in data:
        data["Priorities"] = {"ConventionPriority": data.pop("Priority")}
    if "PCTApplication" in data:
        data["PCTApplication"] = data["PCTApplication"].strip()
    if "PCTPublication" in data:
        data["PCTPublication"] = data["PCTPublication"].strip()

    # Преобразование словаря в JSON без переноса строк
    patent_info_json = json.dumps(data, ensure_ascii=False, indent=None)

    return patent_info_json