import argparse
import pandas as pd
import numpy as np
import time
from dateutil import tz

# process tardis messages
import asyncio
from tardis_client import TardisClient, Channel

# i/o
import os
import ujson
import json
import gzip
import pyarrow.feather as feather
from pathlib import Path

# normalization dictionary in separate file to keep code cleaner
from tardis_msg_normalization import *
# cythonized file
import tardis_msg_counter
from tardis_msg_counter import ret_all_subdir_file_paths

### Default Global Settings
from decouple import config  
api_key = config('TARDIS_KEY')
default_cache_dir = config('CACHE_DIR')
msg_thrsh_process_file_by_file = 1000000

### Helper Functions

def setup_tardis_request(exch, dl_date, dl_dtype, dl_symbols, cache_dir_root, tardis_key):
    """
    Currently hardcoded to get one day worth of data
    Creates separate cache directory for each download 
    
    Returns dictionary {'cache_dir':_,'data_async_gen':_}    
    """
    
    # Create Cache Directory For Download
    
    cur_time_str = pd.Timestamp.now().strftime('%Y%m%d%H%M%S')
    new_cache_dir_lbl = exch + pd.Timestamp(dl_date).strftime('%Y%m%d') + '_' + cur_time_str
    cache_dir_full_path = Path(cache_dir_root) / new_cache_dir_lbl
    os.makedirs(cache_dir_full_path)
    print('New Root Directory For Tardis Data: ' + str(cache_dir_full_path))

    # Tardis API Request

    from_date_str = pd.Timestamp(dl_date).strftime('%Y-%m-%d')
    to_date_str = (pd.Timestamp(dl_date)+pd.Timedelta(days=1)).strftime('%Y-%m-%d')

    tardis_client = TardisClient(api_key=tardis_key,cache_dir=cache_dir_full_path)
    messages = tardis_client.replay(
            exchange=exch,
            from_date=from_date_str,
            to_date=to_date_str,
            filters=[Channel(name=dl_dtype, symbols=dl_symbols)],
        )
    
    return {'cache_dir_full_path': cache_dir_full_path,
            'data_async_gen': messages}    

def format_houbi_ch_field(ch_field_str):
    if ch_field_str.startswith('market.') & ch_field_str.endswith('.trade.detail'):
        ch_field_str = ch_field_str.replace('market.','')
        ch_field_str = ch_field_str.replace('.trade.detail','')
    return ch_field_str

def tardis_cache_trade_zip_file_into_arr(file_path, arr_save_into, start_idx, exch):
    """
    For one Tardis gzip file:    
    -> Iterate through messages
    -> Load each one into pre-created 2D numpy array starting at row start_idx
    
    Returns dictionary:
    {
     'start_idx': updated value start_idx (latest numpy array row inserted into),
     'col_headings': data column headings
    }
    """
    
    line_data = None
    sub_data = None
    
    # read the gzip file
    with gzip.open(file_path, "r") as z:
        json_bytes = z.read()

    json_str = json_bytes.decode('utf-8')
    lines = json_str.split('\n')
    col_headings = []
    
    for l in lines:
        
        if len(l)==0:
            continue
        
        line_data = {}
        try:
            loaded_line = ujson.loads(l[l.find(' ')+1:])
        except ValueError:
            loaded_line = json.loads(l[l.find(' ')+1:])

        # extra fields sometimes found in deribit messages
        # when this happens, for now they are ignored
        is_deribit = False 
      
        # Generic
        if 'data' in loaded_line:
            line_data = loaded_line['data']
            # For FTX, look up symbol separately b/c it's not included in data dictionary
            if exch == 'ftx':
                if type(line_data) is list:
                    for sub_data in line_data:
                        if 'market' in loaded_line:
                            sub_data['symbol'] = loaded_line['market']
                        else:
                            sub_data['symbol'] = ''
        # For Houbi Data
        elif 'tick' in loaded_line:
            if 'data' in loaded_line['tick']:
                line_data = loaded_line['tick']['data']
                # look up symbol separately b/c it's not included in data dictionary
                if  type(line_data) is dict:
                    if 'ch' in loaded_line:
                        line_data['ch'] = format_houbi_ch_field(loaded_line['ch'])
                elif type(line_data) is list:
                    for sub_data in line_data:
                        if 'ch' in loaded_line:
                            sub_data['ch'] = format_houbi_ch_field(loaded_line['ch'])
                        else:
                            sub_data['ch'] = ''
        # For Deribit Data
        elif 'params' in loaded_line:
            if 'data' in loaded_line['params']:
                line_data = loaded_line['params']['data']
                is_deribit = True
        # For Coinbase & Bitstamp data
        elif type(loaded_line) is dict:
            if 'type' in loaded_line:
                if loaded_line['type']=='match':
                    line_data = loaded_line
        # For Kraken data
        elif type(loaded_line) is list:
            if len(loaded_line)>=3:
                if loaded_line[2]=='trade':
                    line_data = [{'price': sub_line[0],
                                  'volume': sub_line[1],
                                  'time': sub_line[2],
                                  'side': sub_line[3],
                                  'orderType': sub_line[4],
                                  'misc': sub_line[5],
                                  'symbol': loaded_line[3]}
                                 for sub_line in loaded_line[1]]

        if len(line_data)==0:
            continue

        if type(line_data) is dict:
            # Unfortunately need hack to drop extra fields which 
            # occur in deribit messages sometimes
            if is_deribit: 
                if 'liquidation' in line_data:
                    line_data.pop('liquidation')
                if 'block_trade_id' in line_data:
                    line_data.pop('block_trade_id')
            arr_save_into[start_idx] = np.array(list(line_data.items()))[:,1]
            start_idx += 1
            if len(col_headings)==0:
                col_headings = list(line_data.keys())
                
        elif type(line_data) is list:
            for sub_data in line_data:
                if is_deribit:
                    if 'liquidation' in sub_data:
                        sub_data.pop('liquidation')
                    if 'block_trade_id' in sub_data:
                        sub_data.pop('block_trade_id')
                arr_save_into[start_idx] = np.array(list(sub_data.items()))[:,1]
                start_idx += 1
                if len(col_headings)==0:
                    col_headings = list(sub_data.keys())
   
    return {
        'start_idx': start_idx, 
        'col_headings': col_headings
    }


def tardis_parse_zip_dir_and_cache_into_arr(dir_path, exch, dl_date):
    """
    For one Tardis (sub)directory:    
    Iterate through message files and cache into Feather File
    """

    # Count number of messages in all tardis gz files in directory
    num_messages = tardis_msg_counter.c_tardis_count_trade_msgs_cache_dir(dir_path)
    num_fields = len(trades_norm_dict[exch].keys())
    # Set up numpy array to hold them
    np_cache_array = np.ndarray(shape=(num_messages, num_fields),dtype='O')

    glbl_msg_id = 0
    dir_files = ret_all_subdir_file_paths(dir_path)
    
    for file_path in dir_files:
        if not(str(file_path).endswith('.json.gz')):
            continue                  
        run_out = tardis_cache_trade_zip_file_into_arr(file_path, np_cache_array, glbl_msg_id, exch)
        glbl_msg_id = run_out['start_idx']
    
    # convert numpy array to DataFrame
    col_headings = run_out['col_headings']
    if exch == 'kraken':
        col_headings = trades_norm_dict[exch].keys()
    if len(col_headings)==0:
        col_headings_norm = [trades_norm_dict[exch][k][0] for k in trades_norm_dict[exch].keys()]
    else:
        col_headings_norm = [trades_norm_dict[exch][c][0] for c in col_headings]
    type_dict = {t[0]:t[1] for t in trades_norm_dict[exch].values()}

    df_result = pd.DataFrame(np_cache_array,columns=col_headings_norm)
    df_result.index.name = 'msg_num'

    # Clean up DataFrame
    for k in type_dict.keys():
        type_to_convert_to = type_dict[k]
        if type_to_convert_to == int:
            type_to_convert_to = float
        df_result[k] = df_result[k].astype(type_to_convert_to)

    normalize_df_timestamps(df_result, exch)

    # Save as (intermediate) Feather file
    feather_save_path = dir_path + 'trd_tmp_'+pd.Timestamp(dl_date).strftime('%Y%m%d')
    feather.write_feather(df_result, feather_save_path, compression='lz4')
        
def tardis_parse_root_cache_dir(root_cache_dir, exch, dl_date):   
    """
    For all Tardis sub-directories (one level up from cached gzip files):    
    -> aggregate messages from sub-directory files
    -> combine and save to intermediate Feather file
    
    Then iterate through all intermediate cached Feather files:
    -> combine data and save to final output file    
    """

    all_file_paths = ret_all_subdir_file_paths(root_cache_dir)
    all_file_paths_onelvlup = [path_str[:(path_str[::-1].find('\\'))*-1] for path_str in all_file_paths]
    all_file_paths_onelvlup = list(pd.value_counts(all_file_paths_onelvlup).sort_index().index)
    
    for dir_path in all_file_paths_onelvlup:
        tardis_parse_zip_dir_and_cache_into_arr(dir_path, exch, dl_date)
        print(dir_path)
    
    print('**** Recombining DataFrames ****\n')
    feather_files_list = [f for f in ret_all_subdir_file_paths(root_cache_dir) if not(f.endswith('.json.gz'))]
    df_list = []
    for ff in feather_files_list:
        temp_df = pd.read_feather(ff)
        df_list.append(temp_df)
    
    df_result = pd.concat(df_list)
    feather_save_path = root_cache_dir + 'trd_'+pd.Timestamp(dl_date).strftime('%Y%m%d')    
    feather.write_feather(df_result, feather_save_path, compression='lz4')
    
    print('Done Saving Aggregated DataFrame As Feather File: ')
    print('-> output path: '+feather_save_path)
    print('-> # of rows: '+str(len(df_result)))
    print('---')

def main():

    # Process Arguments
    parser = argparse.ArgumentParser()

    parser.add_argument("exchange", help='['+' | '.join(trades_norm_dict.keys())+' ]')
    parser.add_argument("date", help="date at YYYY-MM-DD")    
    parser.add_argument("symbols", help="remote exchange symbols (comma separated)")
    parser.add_argument("--cache_dir", help="Cache Source", default=default_cache_dir)
    parser.add_argument("--api_key", help="Api Key", default=api_key)

    args = parser.parse_args()

    exch = args.exchange
    dl_date = args.date
    dl_dtype = examples_dict[exch]['dl_dtype']
    dl_symbols = args.symbols.split(',')
    if type(dl_symbols) is not list:
        dl_symbols = [dl_symbols]
    cache_dir_root = args.cache_dir
    tardis_key = args.api_key

    print('\nParsed Arguments:\n***')
    print('exch: '+exch)
    print('dl_date: '+str(dl_date))
    print('dl_dtype: '+dl_dtype)
    print('dl_symbols: '+str(dl_symbols))
    print('cache_dir_root: '+cache_dir_root)
    print('***\n')

    # Create Tardis Trade Request

    tardis_req_info = setup_tardis_request(exch=exch, dl_date=dl_date, 
        dl_dtype=dl_dtype, dl_symbols=dl_symbols, 
        cache_dir_root=cache_dir_root, tardis_key=tardis_key)

    cache_dir_full_path = tardis_req_info['cache_dir_full_path']
    messages = tardis_req_info['data_async_gen']

    # Cache & Count Messages

    num_messages = asyncio.run(tardis_msg_counter.c_tardis_count_and_save_async_gen_msgs(messages))
    print('-> total messages = '+str(num_messages))
    if num_messages==0:
        print('Query Failed and Returned Empty Data Set. Exiting.')
        return 0

    print("\nProcess The Data...")
    
    # Process each cached zip file

    if num_messages > msg_thrsh_process_file_by_file:
        
        print("More than "+str(msg_thrsh_process_file_by_file)+' total messages => PROCESS EACH HOUR CACHE DIR SEPARATELY\n')
        
        # Process each cached zip file

        t1 = time.time()

        all_file_paths = ret_all_subdir_file_paths(cache_dir_full_path)
        all_file_paths_onelvlup = [path_str[:(path_str[::-1].find('\\'))*-1] for path_str in all_file_paths]
        all_file_paths_onelvlup = list(pd.value_counts(all_file_paths_onelvlup).sort_index().index)

        tardis_parse_root_cache_dir(cache_dir_full_path, exch, dl_date)

        t2 = time.time()
        
        print('\nProcessing Data Took '+str(np.round(t2-t1,3))+' sec')

    else:
    
        print("Less than "+str(msg_thrsh_process_file_by_file)+' total messages => PROCESS ALL CACHED MESSAGES TOGETHER\n')
        
        t1 = time.time()

        all_file_paths = ret_all_subdir_file_paths(cache_dir_full_path)
        all_file_paths_onelvlup = [path_str[:(path_str[::-1].find('\\'))*-1] for path_str in all_file_paths]
        all_file_paths_onelvlup = list(pd.value_counts(all_file_paths_onelvlup).sort_index().index)

        # Set up numpy array to messages

        num_fields = len(trades_norm_dict[exch].keys())
        np_cache_array = np.ndarray(shape=(num_messages, num_fields),dtype='O')
        
        glbl_msg_id = 0

        for file_path in all_file_paths:
            run_out = tardis_cache_trade_zip_file_into_arr(file_path, np_cache_array, glbl_msg_id, exch)
            glbl_msg_id = run_out['start_idx']

        # Convert numpy array (that messages were cached into) to DataFrame

        col_headings = run_out['col_headings']
        if exch == 'kraken':
            col_headings = trades_norm_dict[exch].keys()
        if len(col_headings)==0:
            col_headings_norm = [trades_norm_dict[exch][k][0] for k in trades_norm_dict[exch].keys()]
        else:
            col_headings_norm = [trades_norm_dict[exch][c][0] for c in col_headings]
        type_dict = {t[0]:t[1] for t in trades_norm_dict[exch].values()}

        df_result = pd.DataFrame(np_cache_array,columns=col_headings_norm)
        df_result.index.name = 'msg_num'

        # Clean up DataFrame

        for k in type_dict.keys():
            type_to_convert_to = type_dict[k]
            if type_to_convert_to == int:
                type_to_convert_to = float
            df_result[k] = df_result[k].astype(type_to_convert_to)

        normalize_df_timestamps(df_result, exch)

        t2 = time.time() 

        feather_save_path = cache_dir_full_path / ('trd_'+pd.Timestamp(dl_date).strftime('%Y%m%d'))
        feather.write_feather(df_result, feather_save_path, compression='lz4')
        print('Done Saving Aggregated DataFrame As Feather File: ')
        print('-> output path: '+ str(feather_save_path))
        print('-> # of rows: '+str(len(df_result)))
        print('---')
        print('\nProcessing Data Took: '+str(np.round(t2-t1,3))+' sec')

if __name__ == '__main__':
    main()
