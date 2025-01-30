import warnings
warnings.filterwarnings('ignore')
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.tsa.arima.model import ARIMA
from sklearn.metrics import mean_squared_error, mean_absolute_error
import math
from pmdarima.arima import auto_arima

def load_data(frequency, local=False):
    if local == True:
        df = pd.read_json(f"../../sample_data/LINKUSDT_{frequency}.json")

    else:
        print("TODO: create logic to load data from database here")

def clean_data(df):
    df["Date"] = pd.to_datetime(df["open time"], unit='ms')
    df.index = df["Date"]
    df.drop(columns=['open time', 'close time', 'delete', 'Date'], inplace=True)

    df_close = df['close']

    df_log = np.log(df_close)

    return df_log


def arima_model(df_log):

    train_data, test_data = df_log[3:int(len(df_log) * 0.8)], df_log[int(len(df_log) * 0.8):]


    model_autoARIMA = auto_arima(train_data, start_p=0, start_q=0,
                                 test='kpss',  # use adftest to find optimal 'd'
                                 max_p=3, max_q=3,  # maximum p and q
                                 m=1,  # frequency of series
                                 d=None,  # let model determine 'd'
                                 seasonal='ocsb',  # No Seasonality
                                 start_P=0,
                                 D=0,
                                 trace=True,
                                 error_action='ignore',
                                 suppress_warnings=True,
                                 stepwise=True)
    print(model_autoARIMA.summary())
    model_autoARIMA.plot_diagnostics(figsize=(15, 8))
    plt.show()

    model = ARIMA(train_data, freq='D', order=(1, 1, 0))

    fitted = model.fit()
    print(fitted.summary())

    # Generate forecast and confidence intervals
    pred = fitted.get_forecast(steps=321, alpha=0.05)

    # Extract forecast and confidence intervals
    fc_series = pd.Series(pred.predicted_mean, index=test_data.index)
    conf = pred.conf_int(alpha=0.05)
    lower_series = pd.Series(conf.iloc[:, 0], index=test_data.index)
    upper_series = pd.Series(conf.iloc[:, 1], index=test_data.index)

    # Plot
    plt.figure(figsize=(10, 5), dpi=100)
    plt.plot(train_data, label='Training Data')
    plt.plot(test_data, color='blue', label='Actual Stock Price')
    plt.plot(fc_series, color='orange', label='Predicted Stock Price')
    plt.fill_between(lower_series.index, lower_series, upper_series,
                     color='k', alpha=.10, label='Confidence Interval')
    plt.title('Stock Price Prediction')
    plt.xlabel('Time')
    plt.ylabel('Stock Price')
    plt.legend(loc='upper left', fontsize=8)
    plt.show()

    # Ensure forecast length matches test_data
    forecast_steps = len(test_data)
    pred = fitted.get_forecast(steps=forecast_steps)

    # Extract forecast and align indices
    fc = pred.predicted_mean
    fc = fc.iloc[:len(test_data)]  # Trim forecast if it exceeds test_data length

    # Performance metrics
    mse = mean_squared_error(test_data, fc)
    print('MSE: ' + str(mse))

    mae = mean_absolute_error(test_data, fc)
    print('MAE: ' + str(mae))

    rmse = math.sqrt(mse)
    print('RMSE: ' + str(rmse))

    mape = np.mean(np.abs(fc - test_data) / np.abs(test_data))
    print('MAPE: ' + str(mape))

    return model

