import json
import pandas as pd
import numpy as np
from datetime import date
import os
import subprocess
import sys
import boto3
from decimal import Decimal

subprocess.check_call([sys.executable, "-m", "pip", "install", "--target", "/tmp", 'yfinance'])
sys.path.append('/tmp')
import yfinance

def lambda_handler(event, context):
    
    from stocksymbol import StockSymbol
    api_key = 'YourApiKey'
    ss = StockSymbol(api_key)
    symbol_list_italy = ss.get_symbol_list(market="italy")
    df_symbol_list_italy = pd.DataFrame(symbol_list_italy)
    
    n = len(df_symbol_list_italy)//2
    df_symbol_list_italy_1 = df_symbol_list_italy.iloc[:n]
    df_symbol_list_italy_2 = df_symbol_list_italy.iloc[n:]
    
    # 2. Retrieve new data
    today = date.today()
    
    stock_milan_today_1 = yfinance.download(df_symbol_list_italy_1.symbol.to_list(),
                                            today,
                                            auto_adjust=True)['Close']

    stock_milan_today_2 = yfinance.download(df_symbol_list_italy_2.symbol.to_list(),
                                            today,
                                            auto_adjust=True)['Close']

    stock_milan_today = pd.concat([stock_milan_today_1, stock_milan_today_2],
                                  axis=1)
    
    # 3. Data clensing
    stock_milan_today.reset_index(inplace=True)
    stock_milan_today_long = stock_milan_today.rename(columns = {'index':'Date'})
    stock_milan_today_long = pd.melt(stock_milan_today, id_vars=['Date'], value_name='close')
    
    stock_milan_today_long = stock_milan_today_long.rename(columns={'Date': 'date', 'variable': 'symbol', 'close': 'close'})
    
    stock_milan_today_long = stock_milan_today_long[['symbol', 'date', 'close']]
    
    stock_milan_today_long = stock_milan_today_long.replace("nan", np.nan)
    
    stock_milan_today_long['date'] = stock_milan_today_long['date'].dt.date
    stock_milan_today_long = stock_milan_today_long[stock_milan_today_long['date'] == today]
    
    stock_milan_today_long_filtered = stock_milan_today_long[stock_milan_today_long['close'].notnull()]
    stock_milan_today_long_filtered['date'] = stock_milan_today_long_filtered['date'].astype(str)
    stock_milan_today_long_filtered['date'] = stock_milan_today_long_filtered['date'].str[:10]
    
    stock_milan_today_json = json.loads(stock_milan_today_long_filtered.to_json(orient='records'),
                                        parse_float=Decimal)
    
    # 4. Batch write the data to DynamoDB
    dynamoDB = boto3.resource('dynamodb')
    stock_milan = dynamoDB.Table('stock_milan')
    
    with stock_milan.batch_writer() as batch:
        # Loop through the JSON objects
        for i in stock_milan_today_json:
            batch.put_item(Item=i)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Data successfully processed and stored in DynamoDB.')
    }
