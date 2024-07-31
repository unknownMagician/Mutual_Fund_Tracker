import calendar
import os
import bs4.element
from bs4 import BeautifulSoup
import requests
import csv
import re
import operator
from collections import OrderedDict
import json
from datetime import datetime
# from dateutil.relativedelta import relativedelta

class Funds:
    datasets = []

    def slugify(string):
        return re.sub(r'[-\s]+', '-', (re.sub(r'[^\w\s-]', '', string).strip().lower()))

    def __init__(self):
        self._session = requests.session()
        self.set_proxy(proxy=None)  # set proxy directly "http://user:pass@10.10.1.0:1080"
        self._filepath = str(os.path.dirname(os.path.abspath(__file__))) + '/funds.json'
        self._const = self.init_const()
        self._url = "https://www.moneycontrol.com/mutual-funds/axis-long-term-equity-fund-direct-plan/portfolio-holdings/"
        # more items will get added later

    def init_const(self):
        with open(self._filepath, 'r') as f:
            return json.load(f)

    def set_proxy(self, proxy):
        """
        This is optional method to work with proxy server before getting any data.
        :param proxy: provide dictionary for proxies setup as
                proxy = { 'http': 'http://user:pass@10.10.1.0:1080',
                          'https': 'http://user:pass@10.10.1.0:1090'}
        :return: None
        """
        proxy_dict = {
            "http": proxy,
            "https": proxy,
            "ftp": proxy
        }
        try:
            result = requests.get("http://google.com", proxies=proxy_dict, timeout=5)
        except requests.exceptions.RequestException as e:
            print("Proxy is possibly not needed.", e)
            proxy_dict = None
        self._session.proxies = proxy_dict

    def get_stock_price(self, url):
        price_detail = {}
        url = "https://www.moneycontrol.com/india/stockpricequote/sugar/balrampurchinimills/BCM"
        result = self._session.get(url, timeout=30)
        if result.status_code == 200:
            soup1 = BeautifulSoup(result.content, "lxml")
            change_text = soup1.find("div", {"id": "nsechange"}).text
            price_detail["change"] = change_text.split(" ")[0]
            price_detail["change_per"] = change_text.split(" ")[1].strip("(").strip(")")

            # print("div: " + div)
            # print("Change: " +  div.split(" ")[0] + " Change % : " + div.split(" ")[1].strip("(").strip(")"))
            # print("Price is : " + soup1.find("div", {"id": "nsecp"})["rel"] )
            price = soup1.find("div", {"id": "nsecp"})["rel"]
            price_detail["price"] = price
        return price_detail

    def get_tag_parse(self, tag_str):
        soup = BeautifulSoup(tag_str, 'html.parser')
        print(soup.find("div", {"id": "nsechange"}).text)

    def parse_fund(self, fund_key, fund_name):
        url = self._url + fund_key
        print("URL of mutual fund" + fund_name + " is: " + url)
        result = self._session.get(url, timeout=30)
        if result.status_code == 200:
            soup = BeautifulSoup(result.content, "lxml")
            date_on = soup.find("span", attrs={"class": "subtext TT"}).get_text().strip()
            print("Result dated ", date_on)
            current_month = datetime.today().month
            report_month = (datetime.strptime(date_on.strip("()").strip("as on"), '%dst %b,%Y') + relativedelta(months=1)).month
            if current_month == report_month:
                print("Perfect! Report is for current month.", calendar.month_name[report_month])
            else:
                print("Report is old for last month.", calendar.month_name[report_month])
            stock_table = soup.find("table", id="equityCompleteHoldingTable")
            headings = [th.get_text() for th in stock_table.find("tr").find_all("th")]
            headings = ["id", "fund-name", "url"] + headings
            head_slug = [self.slugify(h) for h in headings]
            tbody = stock_table.find("tbody")

            for row in tbody.find_all("tr")[1:]:
                temp = [td.get_text().strip() for td in row.find_all('td')]
                url = row.find('a', href=True)['href']
                dataset = dict(zip(head_slug, [fund_key, fund_name, url] + temp))
                self.datasets.append(dataset)

    def write_data_to_csv(self):
        keys = list(self.datasets[0].keys())
        with open('data.csv', 'w') as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(self.datasets)

if __name__ == "__main__":
    funds = Funds()
    # 1. Get every fund and open detail portfolio
    for fund in funds.init_const()['funds']['mid']:
        print(fund['key'], fund['name'])
        funds.parse_fund(fund['key'], fund['name'])
        funds.write_data_to_csv()
