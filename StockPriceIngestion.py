import json
import boto3
import yfinance as yf
from decimal import Decimal
import pandas as pd


# Your goal is to get per-hour stock price data for a time range for the ten stocks specified in the doc. 
# Further, you should call the static info api for the stocks to get their current 52WeekHigh and 52WeekLow values.
# You should craft individual data records with information about the stockid, price, price timestamp, 52WeekHigh and 52WeekLow values and push them individually on the Kinesis stream


client = boto3.client('kinesis',region_name='us-east-1')

companies = ["MSFT", "MVIS", "GOOG", "SPOT", "INO", "OCGN", "ABML", "RLLCF", "JNJ", "PSFE"]


## Add code to pull the data for the stocks specified in the doc
def stock_info(stock_name):
    stock = yf.Ticker(stock_name)
    print(stock.info)
    return(stock.info['fiftyTwoWeekLow'],stock.info['fiftyTwoWeekHigh'])

# for i in companies:
#     stock = yf.Ticker(i)
#     print(stock.info)

## Add additional code to call 'info' API to get 52WeekHigh and 52WeekLow refering this this link - https://pypi.org/project/yfinance/


# Add your code here to push data records to Kinesis stream.
for i in (companies):
    data = yf.download(i, start= "2021-10-25", end= "2021-10-31", interval = '1h' )
    data_frame = pd.DataFrame(data=data)
    data_frame.reset_index(drop=True,inplace=True)
    json_close_data = data_frame['Close'].to_json()
    my_stockinfo_data = {"company":i,"52WeekLow":stock_info(i)[0],"52Weekhigh":stock_info(i)[1],'stockPrice':json_close_data}
    json_data = json.dumps(my_stockinfo_data)
    new_data = json.loads(json_data.replace("\'",""))
    new_data['stockPrice'] = json.loads(new_data['stockPrice'])
    for key,value in new_data['stockPrice'].items():
        if (value >=new_data['52Weekhigh']-0.2*new_data['52Weekhigh']) or (value <= new_data['52WeekLow']+0.2*new_data['52WeekLow']):
            anamoly_detected = {'company':new_data['company'],'52Weekhigh':new_data['52Weekhigh'],'52WeekLow':new_data['52WeekLow'],'POI':value}
            anamoly_json_data = json.dumps(anamoly_detected)
            response = client.put_record(
                StreamName = 'stream',
                Data = anamoly_json_data,
                PartitionKey = 'company'
            )
            print(anamoly_json_data)
print('Data Pushed to kinesis stream')


