import os
import warnings
warnings.filterwarnings('ignore')
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.tsa.stattools import adfuller
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.arima.model import ARIMA
from pmdarima.arima import auto_arima
from sklearn.metrics import mean_squared_error, mean_absolute_error
import math

stock_data = pd.read_json("../../sample_data/LINKUSDT_1d.json")


stock_data["Date"] = pd.to_datetime(stock_data["open time"],unit='ms')
stock_data.index = stock_data["Date"]
# stock_data.index = pd.DatetimeIndex(stock_data.index).to_period('D')


stock_data.drop(columns=['open time', 'close time', 'delete', 'Date'], inplace=True)


#plot close price
plt.figure(figsize=(10,6))
plt.grid(True)
plt.xlabel('Date')
plt.ylabel('Close Prices')
plt.plot(stock_data['close'])
plt.title('Closing price')
plt.show()

#Distribution of the dataset
df_close = stock_data['close']
df_close.plot(kind='kde')


#Test for staionarity
def test_stationarity(timeseries):
    #Determing rolling statistics
    rolmean = timeseries.rolling(12).mean()
    rolstd = timeseries.rolling(12).std()
    #Plot rolling statistics:
    plt.plot(timeseries, color='blue',label='Original')
    plt.plot(rolmean, color='red', label='Rolling Mean')
    plt.plot(rolstd, color='black', label = 'Rolling Std')
    plt.legend(loc='best')
    plt.title('Rolling Mean and Standard Deviation')
    plt.show(block=False)
    print("Results of dickey fuller test")
    adft = adfuller(timeseries,autolag='AIC')
    # output for dft will give us without defining what the values are.
    #hence we manually write what values does it explains using a for loop
    output = pd.Series(adft[0:4],index=['Test Statistics','p-value','No. of lags used','Number of observations used'])
    for key,values in adft[4].items():
        output['critical value (%s)'%key] =  values
    print(output)
test_stationarity(df_close)

#To separate the trend and the seasonality from a time series, 
# we can decompose the series using the following code.
result = seasonal_decompose(df_close, model='multiplicative', period = 30)
fig = plt.figure()  
fig = result.plot()  
fig.set_size_inches(16, 9)

#if not stationary then eliminate trend
#Eliminate trend
from pylab import rcParams
rcParams['figure.figsize'] = 10, 6
df_log = np.log(df_close)
moving_avg = df_log.rolling(12).mean()
std_dev = df_log.rolling(12).std()
plt.legend(loc='best')
plt.title('Moving Average')
plt.plot(std_dev, color ="black", label = "Standard Deviation")
plt.plot(moving_avg, color="red", label = "Mean")
plt.legend()
plt.show()

#split data into train and training set
train_data, test_data = df_log[3:int(len(df_log)*0.8)], df_log[int(len(df_log)*0.8):]
plt.figure(figsize=(10,6))
plt.grid(True)
plt.xlabel('Dates')
plt.ylabel('Closing Prices')
plt.plot(df_log, 'green', label='Train data')
plt.plot(test_data, 'blue', label='Test data')
plt.legend()

from pmdarima.arima import auto_arima

model_autoARIMA = auto_arima(train_data, start_p=0, start_q=0,
                      test='kpss',       # use adftest to find optimal 'd'
                      max_p=3, max_q=3, # maximum p and q
                      m=1,              # frequency of series
                      d=None,           # let model determine 'd'
                      seasonal='ocsb',   # No Seasonality
                      start_P=0, 
                      D=0, 
                      trace=True,
                      error_action='ignore',  
                      suppress_warnings=True, 
                      stepwise=True)
print(model_autoARIMA.summary())
model_autoARIMA.plot_diagnostics(figsize=(15,8))
plt.show()

#As a result, the Auto ARIMA model assigned the values 1, 1, and 0 to, p, d, and q, respectively.

#Modeling
# Build Model
model = ARIMA(train_data, freq = 'D', order=(1,1,0))  

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





# Create DataFrame for performance metrics
metrics = pd.DataFrame({
    'Metric': ['MSE', 'MAE', 'RMSE', 'MAPE'],
    'Value': [mse, mae, rmse, mape]
})

# Define output file path
output_path = "services/machine_learning/src/forecast_ARIMA_results.csv"
metrics_output_path = "performance_metrics.csv"

# Save metrics to CSV
metrics.to_csv(output_path, index=False)

print(f"Performance metrics saved to {output_path}")