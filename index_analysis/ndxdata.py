import pandas as pd
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract

import threading
import time

reqId_to_symbol = {}

class IBapi(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.stocks_data = {}  
    def historicalData(self, reqId, bar):
        
        #print(f'Time: {bar.date} Close: {bar.close}')
        if reqId in  self.stocks_data.keys():
            self.stocks_data[reqId].append([reqId_to_symbol[reqId],bar.date, bar.close])
        else:
            self.stocks_data[reqId]=[[reqId_to_symbol[reqId], bar.date, bar.close]]

def run_loop():
    app.run()

app = IBapi()
app.connect('127.0.0.1', 7496, 123)

#Start the socket in a thread
api_thread = threading.Thread(target=run_loop, daemon=True)
api_thread.start()

time.sleep(1) #Sleep interval to allow time for connection to server

ndx_df = pd.read_csv('nasdaq100.csv')


reqId= 1
reqId_to_symbol = {}
for symbol in ndx_df['Symbol']:
    print(symbol)
    stk_contract = Contract()
    stk_contract.symbol = symbol
    stk_contract.secType = 'STK'
    stk_contract.currency = 'USD'
    stk_contract.exchange = 'ISLAND'
    app.reqHistoricalData(reqId, stk_contract, '', '1 y', '1 day', 'ASK', 0, 1, False, [])
    reqId_to_symbol[reqId]=symbol

   
    time.sleep(3)
          
   
    reqId = reqId + 1

time.sleep(50)

df = pd.DataFrame(columns=['Symbol','DateTime', 'Price'])

for req_id, stock_data in app.stocks_data.items():
    print(req_id)
    print(stock_data)
    df=df.append(pd.DataFrame(stock_data, columns=['Symbol','DateTime', 'Price']))

print(df)

df.to_csv('ndxdata.csv')  





      
