from fastapi import FastAPI, Query
from pathlib import Path
from datetime import date
from typing import Optional
from joblib import load

from src.config.config_loader import load_config
from src.db.database_handler import connect_to_database
from src.db.postgres_operations import PostgresOperations
from src.helper.interval import search_suitable_interval


api = FastAPI(openapi_tags=[
    {
        'name': 'home',
        'description': 'Basic functionality of API'
    },
    {
        'name': 'candlestick',
        'description': 'Candlestick Data. Open, High, Low, Close, Volume'
    },
    {
        'name': 'predictions',
        'description': 'Predicts next cloxing prices via ML'
    }
])


@api.get('/check', tags=['home'])
def check_availability():
    """Check if the app is running."""
    return {"data": "success"}


@api.get("/candlesticks/{symbol}/{target_interval}", tags=['candlestick'])
def get_candlesticks(symbol: str, target_interval: str, start_date: Optional[date] = Query(None)):
    psql_ops = PostgresOperations()

    conf_path = Path(__file__).resolve().parent / "config.yml"
    config = load_config(conf_path)
    conn = connect_to_database(config['postgres'])
    available_intervals = psql_ops.get_available_intervals_for_trading_pair(conn, symbol)

    chosen_interval = search_suitable_interval(available_intervals=available_intervals,
                                                target_interval=target_interval)

    data = psql_ops.get_candlestick_data(conn, symbol, target_interval, chosen_interval, start_date)

    return data


@api.get("/predict/{symbol}", tags=['predictions'])
def predict(symbol: str, periods: int, freq: str, model_version: str='15m', history: bool=False):
    loaded_model = load(f"/app/ml_models/prophet_model_{model_version}.joblib")
    #loaded_model = load(f"../../ml_models/prophet_model_{model_version}.joblib")
    future = loaded_model.make_future_dataframe(periods=periods, freq=freq, include_history=history)
    forecast = loaded_model.predict(future)
    f = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]

    return f.to_dict(orient='records')