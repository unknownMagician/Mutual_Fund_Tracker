import requests
from bs4 import BeautifulSoup

url = "https://www.moneycontrol.com/mutual-funds/performance-tracker/returns/large-cap-fund.html"
response = requests.get(url)
soup = BeautifulSoup(response.text, "html.parser")

table_body = soup.select_one("#dataTableId > tbody")
rows = table_body.select("tr")

data = []
for row in rows:
    name = row.select_one("td:nth-child(1)").text
    category = row.select_one("td:nth-child(2)").text
    

    row_data = {
        "name": name,
        "category": category,
    }
    data.append(row_data)

# Print the data for all rows
for row_data in data:
    print(row_data)
