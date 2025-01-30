import pandas as pd
import json
import requests
import warnings
warnings.filterwarnings('ignore')

from prophet import Prophet
from joblib import dump


def train_model(symbol, target_interval):
    req = requests.get(f"http://127.0.0.1:8000/candlesticks/{symbol}/{target_interval}")

    stock_data = pd.DataFrame(json.loads(req.content.decode()))
    stock_data['ds'] = pd.to_datetime(stock_data["open_time"])
    stock_data['ds'] = stock_data['ds'].dt.tz_localize(None)
    stock_data['y'] = stock_data.close
    stock_data.drop(columns=['open_time', 'open', 'close', 'high', 'low', 'number_of_trades'], inplace=True)

    m = Prophet()
    m.fit(stock_data)

    return m

def save_model(m, target_interval):
    dump(m, f'../../ml_models/prophet_model_{target_interval}.joblib')


if __name__ == '__main__':
    symbol = "LINKUSDT"
    target_interval = "15m"

    m = train_model(symbol, target_interval)

    save_model(m, target_interval)