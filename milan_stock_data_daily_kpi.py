import pandas as pd
import boto3
import os
from datetime import date, timedelta
import json
import subprocess
import sys
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from oauth2client.service_account import ServiceAccountCredentials

def lambda_handler(event, context):

    # 1. Get the symbols listed on Borsa Italiana
    from stocksymbol import StockSymbol
    api_key = '8b6d5f46-a1fb-4daf-8cac-5ca3db4d8748'
    ss = StockSymbol(api_key)
    
    symbol_list_italy = ss.get_symbol_list(market="italy")
    
    df_symbol_list_italy = pd.DataFrame(symbol_list_italy)
    
    dynamoDB = boto3.resource('dynamodb')
    stock_milan = dynamoDB.Table('stock_milan')
    
    symbol_list = df_symbol_list_italy.symbol.to_list()
    today = date.today()
    three_years_ago = today - timedelta(days=(365*3)+2)
    
    stock_milan_db = []
    from boto3.dynamodb.conditions import Key
    
    for symbol in symbol_list:
        stock_milan_response = stock_milan.query(
            KeyConditionExpression=Key('symbol').eq(symbol) & Key('date').gte(three_years_ago.strftime('%Y-%m-%d'))
            )
        stock_milan_db.extend(stock_milan_response['Items'])
    
    stock_milan_db = pd.DataFrame.from_records(stock_milan_db)
    stock_milan_db['close'] = pd.to_numeric(stock_milan_db['close'])
    stock_milan_db['date'] = pd.to_datetime(stock_milan_db['date'])
    
    # 2. KPI computing
    # Ordina il dataframe in base al simbolo e alla data
    stock_milan_db.sort_values(by=['symbol', 'date'], inplace=True)
    
    # Rimpimento date mancanti
    stock_milan_db = stock_milan_db.groupby('symbol').apply(lambda group: group.fillna(method='ffill'))
    
    # Crea un elenco di tutte le date uniche presenti nel dataframe
    all_dates = stock_milan_db['date'].unique()
    
    # Crea una nuova lista di record completi per ogni simbolo
    completed_records = []
    
    for symbol in stock_milan_db['symbol'].unique():
        symbol_data = stock_milan_db[stock_milan_db['symbol'] == symbol]
        
        # Crea un elenco di tutte le date per questo simbolo
        symbol_dates = symbol_data['date'].unique()
        
        # Trova la data massima per questo simbolo
        max_date = symbol_dates.max()
        
        # Crea un elenco di tutte le date mancanti tra la data minima e massima
        missing_dates = pd.date_range(start=symbol_dates.min(), end=max_date).difference(symbol_dates)
        
        # Crea nuovi record completi per le date mancanti
        for missing_date in missing_dates:
            last_close = symbol_data[symbol_data['date'] < missing_date]['close'].iloc[-1]
            new_record = {
                'symbol': symbol,
                'date': missing_date,
                'close': last_close
            }
            completed_records.append(new_record)
    
    # Aggiungi i nuovi record completi al dataframe
    completed_df = pd.DataFrame(completed_records)
    
    # Unisci il dataframe completato con quello originale
    stock_milan_db_completed = pd.concat([stock_milan_db, completed_df], ignore_index=True)
    
    stock_milan_db_completed.sort_values(by=['symbol', 'date'], inplace=True)
    
    # DB con i risultati
    today = stock_milan_db['date'].max()
    stock_milan_kpi = stock_milan_db_completed[stock_milan_db_completed['date'].isin(
        [today])]
    
    # Effettua il left join tra stock_milan_kpi e df_symbol_list_italy basato sulla colonna "symbol"
    stock_milan_kpi = stock_milan_kpi.merge(df_symbol_list_italy[['symbol', 'shortName']], on='symbol', how='left')
    stock_milan_kpi.rename(columns={'shortName': 'name'}, inplace=True)
    
    # Calcola le date di riferimento
    one_day_ago = today - pd.DateOffset(days=1)
    one_week_ago = today - pd.DateOffset(weeks=1)
    one_month_ago = today - pd.DateOffset(months=1)
    three_months_ago = today - pd.DateOffset(months=3)
    six_months_ago = today - pd.DateOffset(months=6)
    one_year_ago = today - pd.DateOffset(years=1)
    three_years_ago = today - pd.DateOffset(years=3)
    
    # Filtra il dataframe solo per le date di interesse
    stock_milan_db_filtered = stock_milan_db_completed[stock_milan_db_completed['symbol'].isin(
        stock_milan_kpi['symbol'])]
    stock_milan_db_filtered = stock_milan_db_filtered[stock_milan_db_filtered['date'].isin(
        [today, one_day_ago, one_week_ago, one_month_ago, three_months_ago,
        six_months_ago, one_year_ago, three_years_ago])]
    
    delta_columns = ['1d_delta', '1w_delta', '1m_delta', '3m_delta', '6m_delta', '1y_delta', '3y_delta']
    
    # Calcola i delta values e aggiungili come colonne a stock_milan_kpi
    for delta_col, delta_date in zip(delta_columns, [one_day_ago, one_week_ago, one_month_ago, three_months_ago,
                                                     six_months_ago, one_year_ago, three_years_ago]):
        delta_values = []
        
        for index, row in stock_milan_kpi.iterrows():
            symbol = row['symbol']
            symbol_data = stock_milan_db_filtered[(stock_milan_db_filtered['symbol'] == symbol) & (stock_milan_db_filtered['date'] == delta_date)]
            today_data = stock_milan_kpi[(stock_milan_kpi['symbol'] == symbol)]
    
            if not symbol_data.empty and not today_data.empty:
                delta = (today_data['close'].values[0] - symbol_data['close'].values[0]) / symbol_data['close'].values[0] * 100
                delta_values.append(delta)
            else:
                delta_values.append(None)
        
        stock_milan_kpi[delta_col] = delta_values
        
    stock_milan_kpi = stock_milan_kpi[['symbol', 'name', 'date', 'close'] + [col for col in stock_milan_kpi.columns if col not in ['symbol', 'name', 'date', 'close']]]
    stock_milan_kpi.sort_values(by=['symbol', 'date'], inplace=True)
    
    # Calcola i periodi di riferimento
    periods = [one_week_ago, one_month_ago, three_months_ago, six_months_ago, one_year_ago, three_years_ago]
    
    # Itera attraverso i periodi e calcola i flag
    for period in periods:
        flag_column = f'{period}_flag'
        stock_milan_kpi[flag_column] = None
        
        for symbol in stock_milan_kpi['symbol'].unique():
            symbol_data = stock_milan_db_completed[(stock_milan_db_completed['symbol'] == symbol) & (stock_milan_db_completed['date'] >= period)]
            
            if not symbol_data.empty:
                max_close = symbol_data['close'].max()
                min_close = symbol_data['close'].min()
                today_close = stock_milan_kpi[(stock_milan_kpi['symbol'] == symbol)]['close'].values[0]
                
                if today_close == max_close:
                    stock_milan_kpi.loc[stock_milan_kpi['symbol'] == symbol, flag_column] = 'max'
                elif today_close == min_close:
                    stock_milan_kpi.loc[stock_milan_kpi['symbol'] == symbol, flag_column] = 'min'
    
    flag_column_mapping = {
        f'{one_week_ago}_flag': 'max_min_1w',
        f'{one_month_ago}_flag': 'max_min_1m',
        f'{three_months_ago}_flag': 'max_min_3m',
        f'{six_months_ago}_flag': 'max_min_6m',
        f'{one_year_ago}_flag': 'max_min_1y',
        f'{three_years_ago}_flag': 'max_min_3y'
    }
    
    stock_milan_kpi.rename(columns=flag_column_mapping, inplace=True)
    
    
    # 3. Scrittura su Google Drive
    
    # Carica le credenziali JSON da un file
    google_drive_credentials = os.environ.get('GOOGLE_DRIVE_CREDENTIALS_JSON')
    
    try:
        credentials_dict = json.loads(google_drive_credentials)
        
        # Crea l'oggetto delle credenziali utilizzando ServiceAccountCredentials
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, ['https://www.googleapis.com/auth/drive'])
        
        # Autenticazione con le credenziali
        gauth = GoogleAuth()
        gauth.credentials = credentials
        drive = GoogleDrive(gauth)
    
        # Carica il DataFrame su Google Drive
        data = stock_milan_kpi.to_csv(index=False)  # Converti il DataFrame in CSV
        file = drive.CreateFile({'title': 'stock_milan_kpi.csv',
                                 'id': '1RqrFeQ6w1YF8ZWAcSzCVsg6zbf5PU0u0',
                                 'parents': [{'id': '1Z2X3UyLRhTu0p4NCmedeVnPVzhHJ4F2h'}]})
        file.SetContentString(data)
        file.Upload()
        print('File caricato su Google Drive con ID:', file.get('id'))
    
    except json.JSONDecodeError as e:
        print("Errore nella decodifica JSON:", e)
    
    return {
        'statusCode': 200,
        'body': json.dumps('KPIs successfully processed and published in Google Drive.')
    }
