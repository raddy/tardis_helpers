import numpy as np
import pandas as pd

trades_norm_dict = {
    'bitmex':{
        'timestamp': ('trd_time',str),
        'symbol': ('symbol',str),
        'side': ('trd_side',str),
        'size': ('qty',int),
        'price': ('px',float),
        'tickDirection': ('tick_dir',str),
        'trdMatchID': ('trd_match_id',str),
        'grossValue': ('gross_val',int),
        'homeNotional': ('home_ntnl',float),
        'foreignNotional': ('foreign_ntnl',float)
    },
    'binance':{
        'e': ('evt_type', str),
        'E': ('evt_time', int),
        's': ('symbol', str),
        't': ('trd_id', int),
        'p': ('px', float),
        'q': ('qty', float),
        'b': ('buyer_order_id', int),
        'a': ('seller_order_id', int),
        'T': ('trd_time', int),
        'm': ('is_buyer_mkt_mkr?', bool),
        'M': ('ignore', bool)
    },    
    'binance-futures':{
        'e': ('evt_type', str),
        'E': ('evt_time', int),
        'T': ('trd_time', int),
        's': ('symbol', str),
        't': ('trd_id', int),
        'p': ('px', float),
        'q': ('qty', float),
        'X': ('cur_order_status', str),
        'm': ('is_buyer_mkt_mkr?', bool)
    },
    'binance-delivery':{
        'e': ('evt_type', str),
        'E': ('evt_time', int),
        'T': ('trd_time', int),
        's': ('symbol', str),
        't': ('trd_id', int),
        'p': ('px', float),
        'q': ('qty', float),
        'X': ('cur_order_status', str),
        'm': ('is_buyer_mkt_mkr?', bool)
    },
    'okex-swap':{
        'side': ('trd_side', str),
        'trade_id': ('trd_id', str),
        'price': ('px', float),
        'size': ('qty', float),
        'instrument_id': ('symbol', str),
        'timestamp': ('trd_time', str)
    },
    'okex-futures':{
        'side': ('trd_side', str),
        'trade_id': ('trd_id', str),
        'price': ('px', float),
        'qty': ('qty', float),
        'instrument_id': ('symbol', str),
        'timestamp': ('trd_time', str)
        
    },
    'huobi':{
        'id': ('ord_id',str),
        'ts': ('trd_time', int),
        'tradeId': ('trd_id', int),
        'amount': ('qty', float),
        'price': ('px', float),
        'direction': ('trd_aggr_dir', str),
        'ch': ('symbol', str)
    },    
    'huobi-dm':{
        'amount': ('trd_ntnl', int),
        'quantity': ('qty', float),
        'ts': ('trd_time', int),
        'id': ('trd_id', int),
        'price': ('px', float),
        'direction': ('trd_aggr_dir', str),
        'ch': ('symbol', str)
    },
    'huobi-dm-swap':{
        'amount': ('trd_ntnl', int),
        'quantity': ('qty', float),
        'ts': ('trd_time', int),
        'id': ('trd_id', int),
        'price': ('px', float),
        'direction': ('trd_aggr_dir', str),
        'ch': ('symbol', str)
    },
    'ftx':{
        'id': ('trd_id', int),
        'price': ('px', float),
        'size': ('qty', float),
        'side': ('trd_aggr_dir', str),
        'liquidation': ('is_liq?', bool),
        'time': ('trd_time', str),
        'symbol': ('symbol', str)
    },
    'coinbase':{
        'type': ('msg_type', str),
        'side': ('mkt_mkr_dir', str),
        'product_id': ('symbol', str),
        'time': ('trd_time', str),
        'sequence': ('trd_sequence_no', int),
        'trade_id': ('trd_id', int),
        'maker_order_id': ('maker_ord_id', str),
        'taker_order_id': ('taker_ord_id', str),
        'size': ('qty', float),
        'price': ('px', float)
    },
    'deribit':{
        'trade_seq': ('trd_seq', int),
        'trade_id': ('trd_id', str),
        'timestamp': ('trd_time', int),
        'tick_direction': ('tick_dir', int),
        'price': ('px', float),
        'mark_price': ('mkt_px', float),
        'instrument_name': ('symbol', str),
        'index_price': ('idx_px', float),
        'direction': ('trd_ntnl', str),
        'amount': ('qty', float)
    },
    'kraken':{
        'price': ('px', float),
        'volume': ('qty', float),
        'time': ('trd_time', str),
        'side': ('trd_dir', str),
        'orderType': ('trig_ord_type', str),
        'misc': ('mkt_px', str),
        'symbol': ('symbol', str)
    }
}

examples_dict = {
    'bitmex': {'dl_dtype':'trade', 'dl_symbols':['XBTUSD']},
    'binance': {'dl_dtype':'trade', 'dl_symbols':['btcusdt']},
    'binance-futures': {'dl_dtype':'trade', 'dl_symbols':['ethusdt']},
    'binance-delivery': {'dl_dtype':'trade', 'dl_symbols':['btcusd_200925']},
    'okex-swap': {'dl_dtype':'swap/trade', 'dl_symbols':['BTC-USD-SWAP']},
    'okex-futures': {'dl_dtype':'futures/trade', 'dl_symbols':['BTC-USD-200103']},
    'huobi-dm': {'dl_dtype':'trade', 'dl_symbols':['BTC_CW']},
    'huobi-dm-swap': {'dl_dtype':'trade', 'dl_symbols':['BTC-USD']},
    'huobi': {'dl_dtype':'trade', 'dl_symbols':['btcusdt']},
    'ftx': {'dl_dtype':'trades', 'dl_symbols':['BTC-PERP']},
    'coinbase': {'dl_dtype':'match', 'dl_symbols':['BTC-USD']},
    'kraken':{'dl_dtype':'trade', 'dl_symbols':['XBT/USD']},
    'deribit':{'dl_dtype':'trades', 'dl_symbols':['ETH-PERPETUAL']}
}

def normalize_df_timestamps(df, exch):
    
    if exch in ['bitmex', 'ftx', 'coinbase', 'okex-swap', 'okex-futures']:
        if 'trd_time' in df.columns:
            df['trd_time'] = pd.to_datetime(df['trd_time'])
   
    elif exch in ['binance-delivery', 'binance', 'binance-futures',
                    'huobi-dm', 'huobi-dm-swap', 'huobi', 'deribit']:
        if 'trd_time' in df.columns:
            df['trd_time'] = pd.to_datetime(df['trd_time'].astype(np.int64),origin='unix',unit='ms')
        if 'evt_time' in df.columns:
            df['evt_time'] = pd.to_datetime(df['evt_time'].astype(np.int64),origin='unix',unit='ms')

    elif exch in ['kraken']:
        if 'trd_time' in df.columns:
            df['trd_time'] = pd.to_datetime(df['trd_time'].astype(np.float64),origin='unix',unit='s')
        
    return 0