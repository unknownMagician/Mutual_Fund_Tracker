import re
import calendar
import json
import os
import pandas as pd
import aiohttp
import asyncio
from datetime import datetime
from dateutil.relativedelta import relativedelta
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import sys

class Funds:
    def __init__(self):
        self._filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'funds.json')
        self._const = self.init_const()
        self._url = "https://www.moneycontrol.com/mutual-funds/axis-long-term-equity-fund-direct-plan/portfolio-holdings/"
        self.datasets = pd.DataFrame()

        # Ensure the 'csv' directory exists
        os.makedirs("csv", exist_ok=True)

    def init_const(self):
        with open(self._filepath, 'r') as f:
            return json.load(f)

    @staticmethod
    def slugify(string):
        return re.sub(r'[-\s]+', '-', re.sub(r'[^\w\s-]', '', string).strip().lower())

    @staticmethod
    def remove_ordinal_suffix(date_str):
        return re.sub(r'(\d+)(st|nd|rd|th)', r'\1', date_str)

    async def get_stock_price(self, session, url):
        price_detail = {}
        async with session.get(url) as result:
            if result.status == 200:
                soup1 = BeautifulSoup(await self.decode_response_text(result), "html.parser")
                change_element = soup1.find("div", {"id": "nsechange"})
                if change_element is not None:
                    change_text = change_element.text
                    price_detail["change"] = change_text.split(" ")[0]
                    price_detail["change_per"] = change_text.split(" ")[1].strip("(").strip(")")
                    price = soup1.find("div", {"id": "nsecp"})["rel"]
                    price_detail["price"] = price
                else:
                    change_element = soup1.find("p", {"class": "gr_20 FL MT5 ML5"})
                    price_detail["change"] = change_element.text.split(" ")[0]
                    price_detail["change_per"] = change_element.text.split(" ")[1].strip("(").strip(")")
                    price_detail["price"] = soup1.find("p", {"class": "gr_28 FL"}).text
        return price_detail

    async def decode_response_text(self, response):
        """Decode the response text with the correct charset."""
        content_type = response.headers.get('Content-Type', '').lower()
        charset = None
        if 'charset=' in content_type:
            charset = content_type.split('charset=')[-1]
        try:
            return await response.text(encoding=charset)
        except UnicodeDecodeError:
            encodings = ['utf-8', 'ISO-8859-1', 'latin1']
            for encoding in encodings:
                try:
                    return (await response.read()).decode(encoding)
                except UnicodeDecodeError:
                    continue
            raise UnicodeDecodeError("Failed to decode response with common encodings.")

    async def parse_fund(self, fund_key, fund_name):
        url = self._url + fund_key
        print(f"URL of mutual fund {fund_name} is: {url}")
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as result:
                if result.status == 200:
                    soup = BeautifulSoup(await self.decode_response_text(result), "html.parser")
                    date_on = soup.find("span", class_="subtext TT").get_text().strip()
                    print("Result dated ", date_on)
                    date_on = self.remove_ordinal_suffix(date_on.strip("()").strip("as on"))
                    report_date = datetime.strptime(date_on, '%d %b,%Y')
                    current_month = datetime.today().month
                    report_month = (report_date + relativedelta(months=1)).month
                    if current_month == report_month:
                        print("Perfect! Report is for the current month.", calendar.month_name[report_month])
                    else:
                        print("Report is old for last month.", calendar.month_name[report_month])
                    stock_table = soup.find("table", id="equityCompleteHoldingTable")
                    headings = [th.get_text() for th in stock_table.find("tr").find_all("th")]
                    headings = ["id", "fund-name", "url"] + headings
                    head_slug = [self.slugify(h) for h in headings]
                    tbody = stock_table.find("tbody")
                    rows = []
                    for row in tbody.find_all("tr")[1:]:
                        temp = [td.get_text().strip() for td in row.find_all('td')]
                        row_url = row.find('a', href=True)['href']
                        row_data = [fund_key, fund_name, row_url] + temp
                        rows.append(row_data)
                    fund_df = pd.DataFrame(rows, columns=head_slug)
                    self.datasets = pd.concat([self.datasets, fund_df], ignore_index=True)

    def write_data_to_csv(self, filename='data.csv'):
        self.datasets.to_csv(filename, index=False)

    def convert_quantity(self, quantity_str):
        if 'L' in quantity_str:
            return float(quantity_str.replace('L', '')) * 1e5
        elif 'Cr' in quantity_str:
            return float(quantity_str.replace('Cr', '')) * 1e7
        elif 'k' in quantity_str:
            return float(quantity_str.replace('k', '')) * 1e3
        elif '-' in quantity_str:
            return 0
        else:
            return float(quantity_str)

    async def update_prices(self, df, file):
        async with aiohttp.ClientSession() as session:
            tasks = []
            for index, row in df.iterrows():
                tasks.append(self.get_stock_price(session, row['url']))

            prices = await asyncio.gather(*tasks)
            for index, price_detail in enumerate(prices):
                df.loc[index, 'price'] = price_detail.get('price', None)
                df.loc[index, 'change'] = price_detail.get('change', None)
                df.loc[index, 'change_per'] = price_detail.get('change_per', None)
                qty = self.convert_quantity(df.loc[index, 'quantity'])
                price = float(df.loc[index, 'price'])
                change = float(df.loc[index, 'change'])
                prev = qty * (price - change)
                current = qty * price
                if qty != 0:
                    df.loc[index, 'share_per_change'] = (current - prev) * 100 / prev
            df.to_csv("csv/" + file, index=False)
            print(f"Fund: {df.loc[1, 'fund-name']}, percent change = {df['share_per_change'].sum() / df.shape[0]}")

    def run_update_prices(self, df, file):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.update_prices(df, file))
        loop.close()

    def read_csv_with_multiple_encodings(self, filepath, encodings=['utf-8', 'ISO-8859-1', 'latin1']):
        for encoding in encodings:
            try:
                return pd.read_csv(filepath, encoding=encoding)
            except UnicodeDecodeError:
                print(f"Failed to read {filepath} with encoding {encoding}")
        raise UnicodeDecodeError(f"Failed to read {filepath} with provided encodings")

async def main():
    funds = Funds()
    for fund in funds.init_const()['funds']:
        print(fund['key'], fund['name'])
        await funds.parse_fund(fund['key'], fund['name'])
        funds.write_data_to_csv("csv/" + fund['key'] + ".csv")

    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor(max_workers=10) as executor:
        tasks = []
        for file in os.listdir("csv"):
            if file.endswith(".csv"):
                print(file)
                try:
                    df = funds.read_csv_with_multiple_encodings("csv/" + file)
                    print(df.shape)
                    tasks.append(loop.run_in_executor(executor, funds.run_update_prices, df, file))
                except Exception as e:
                    print(f"Skipping file {file} due to an error: {e}")

        await asyncio.gather(*tasks)

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
