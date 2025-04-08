import asyncio
import re
from playwright.async_api import Playwright, async_playwright, expect


async def __page_down(page):
    await page.evaluate(
        """
                                const scrollStep = 200; // Размер шага прокрутки (в пикселях)
                                const scrollInterval = 100; // Интервал между шагами (в миллисекундах)

                                const scrollHeight = document.documentElement.scrollHeight;
                                let currentPosition = 0;
                                const interval = setInterval(() => {
                                    window.scrollBy(0, scrollStep);
                                    currentPosition += scrollStep;

                                    if (currentPosition >= scrollHeight) {
                                        clearInterval(interval);
                                    }
                                }, scrollInterval);
                            """
    )

url = "https://2gis.ru/spb"
category = ["Поесть", "Премия 2ГИС", "Красота", "ТРЦ", "Автосервис", "Заправочные станции", "Продукты", "Стоматолог", "Аптеки/rubricId/207", ""]
async def org_len_gen(len):
    for i in range(len):
        yield i


async def run(playwright: Playwright, url) -> None:
    browser = await playwright.firefox.launch(headless=False)
    context = await browser.new_context()
    page = await context.new_page()
    for i in category:
        await page.goto(
            f"{url}/search/{i}"
        )
        organization = await page.query_selector_all(
            'div[class="_1kf6gff"]'
        )
        for i, element_handle in enumerate(organization):
            element_text = await element_handle.text_content()
            print(f"Element {i}: {element_text}")


    # ---------------------
    await context.close()
    await browser.close()


async def main(url) -> None:
    async with async_playwright() as playwright:
        await run(playwright, url)


asyncio.run(main(url))
