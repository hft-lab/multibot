# multibot functions format and outcomes

#BELOW ARE THE FUNCTIONS, EVERY SINGLE CLIENT MUST HAVE ALL OF THEM. AFTER EACH EQUAL SYMBOL IS
#VALUE FORMAT WHICH FUNCTION RETURNS

client.symbol = str
#str value of trading pair name, which being used within API requests. For instance ETH-USD

get_orderbook() = {symbol:
  {'asks': [[price, size, etc], [price2, size2, etc2]],
  'bids': [[price, size, etc], [price2, size2, etc2]],
  'timestamp': timestamp}
}
#timestamp type float, seconds | price, size - float | etc - some other useful information which we can get via API

positions() = {
  'BTC-USD':
    {'amount': amount, 'entry_price': entry_price, 'unrealized_pnl_usd': unrealized_pnl_usd, 'side': 'LONG'/'SHORT',
    'amount_usd': amount_usd, 'realized_pnl_usd': realized_pnl_usd},
  'ETH-USD':
    {'amount': amount, 'entry_price': entry_price, 'unrealized_pnl_usd': unrealized_pnl_usd, 'side': 'LONG'/'SHORT',
    'amount_usd': amount_usd, 'realized_pnl_usd': realized_pnl_usd}
}
#every parameter type float except 'side'

get_real_balance() = balance
#balance | type float | in USD

get_available_balance(side="buy"/"sell") = av_balance
#amount available to trade in certain direction | type float | in USD

create_order(amount, price, side, type, expire=5000, client_ID=None) = response
#amount in coin, float (BTC from BTC-USD) | side str in lower (buy/sell) | type str in lower (limit/market)

get_last_price(side) = price
#float value of last order execution price | side (buy/sell)

cancel_all_orders() = response
#cancels all open orders

async def get_funding_payments(session) = [{'market': 'BTCUSDT', 'payment': '-0.12578175', 'datetime': '2023-06-16T05:00:00.000Z', 'time': 1686297600000, 'rate': '0.00007916', 'price': '26481.005738095', 'position': +-'0.060003608918484885'}, {'market': 'BTCUSDT', 'payment': '-0.12578175', 'datetime': '2023-06-16T05:00:00.000Z', 'time': 1686297600000, 'rate': '0.00007916', 'price': '26481.005738095', 'position': +-'0.060003608918484885'} ]

taker_fee = taker_fee
#taker fee value in absolute, float

async def get_funding_payments(session) = [{'market': 'BTCUSDT',
      'payment': '-0.12578175',
      'datetime': '2023-06-16T05:00:00.000Z',
      'time': 1686297600000,
      'rate': '0.00007916',
      'price': '26481.005738095',
      'position': +-'0.060003608918484885'},
     {'market': 'BTCUSDT',
      'payment': '-0.12578175',
      'datetime': '2023-06-16T05:00:00.000Z',
      'time': 1686297600000,
      'rate': '0.00007916',
      'price': '26481.005738095',
      'position': +-'0.060003608918484885'}
     ]




