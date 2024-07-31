import requests
from bs4 import BeautifulSoup
import pandas as pd
url = 'https://www.moneycontrol.com/mutual-funds/idbi-india-top-100-equity-fund-direct-plan/portfolio-holdings/MIB092'
response = requests.get(url)
soup = BeautifulSoup(response.content, 'html.parser')
header = [th.get_text(strip=True) for th in soup.select('#equityCompleteHoldingTable thead th')]
rows = soup.select('#equityCompleteHoldingTable > tbody > tr')
data_array = []
for row in rows:
    cols = [td.get_text(strip=True) for td in row.find_all('td')]
    data_array.append(cols)
df = pd.DataFrame(data_array,columns=header)
print(df.head)
stock_names = df['Stock Name'].tolist()  # Adjust column name as per your DataFrame

def get_moneycontrol_stock_url(stock_name):
    base_url = 'https://www.moneycontrol.com/india/stockpricequote/'
    search_url = f'{base_url}{stock_name.lower().replace(" ", "-")}'
    return search_url

def get_stock_change(stock_name):
    url = get_moneycontrol_stock_url(stock_name)
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    try:
        # Finding the price change and percentage change
        price_change = soup.find('div', {'id': 'nsechange'}).get_text(strip=True)
        percentage_change = soup.find('div', {'id': 'nseperc_change'}).get_text(strip=True)
    except AttributeError:
        price_change = 'N/A'
        percentage_change = 'N/A'
    
    return price_change, percentage_change

# Process each stock
results = []

for stock_name in stock_names:
    try:
        price_change, percentage_change = get_stock_change(stock_name)
        results.append({
            'StockName': stock_name,
            'PriceChange': price_change,
            'PercentageChange': percentage_change
        })
    except Exception as e:
        print(f"Error fetching data for {stock_name}: {e}")
        results.append({
            'StockName': stock_name,
            'PriceChange': 'N/A',
            'PercentageChange': 'N/A'
        })

# Create a DataFrame with the results
result_df = pd.DataFrame(results)
print(result_df)