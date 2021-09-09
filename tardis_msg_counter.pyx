import numpy as np
import asyncio
import os
import json
import ujson
import gzip
import time

def read_gzip_file(file_path):
    with gzip.open(file_path, "r") as z:
        json_bytes = z.read()
    return json_bytes

def parse_msg_line(line):
    try:
        parsed_line = ujson.loads(line[line.find(' ')+1:])
    except ValueError:
        parsed_line = json.loads(line[line.find(' ')+1:])
    return parsed_line

def ret_all_subdir_file_paths(root_dir):
    """
    Return list of full paths of all files in all sub dirs of a root dir
    """    
    all_paths = []
    for subdir, dirs, files in os.walk(root_dir):
        for file in files:
            all_paths.append(os.path.join(subdir, file))
    return all_paths

async def c_tardis_count_and_save_async_gen_msgs(msg_gen, log=True):
    """
    Tardis Python API returns an async_generator.
    When messages returned by this generator are iterated through, 
    they are saved to disk in .gz files
    
    This function:
    (1) forces this caching to take place and 
    (2) also counts the total # of messages: So that a numpy array of the right size 
    can be created to hold these messages
    
    Using Cython helps speed up the code.
    Further possible speedup to try is using "uvloop" library - need linux or MacOS however
    """
  
    t1 = time.time()

    cdef int num_msgs = 0    
    
    async for local_timestamp, message in msg_gen:
        ## 'message' is a JSON object that has a 'data' key containing the trade info
        ## 'data' can be a dict, a list of dicts, sometimes 'data' is a sub_key in a dict

        msg_data = {}
        # Generic
        if 'data' in message:
            msg_data = message['data']
        # For Houbi data
        elif 'tick' in message:
            if 'data' in message['tick']:
                msg_data = message['tick']['data']
        # For Deribit data
        elif 'params' in message: 
            if 'data' in message['params']:
                msg_data = message['params']['data']
        # For Coinbase & Bitstamp data
        elif type(message) is dict:
            if 'type' in message:
                if message['type']=='match':
                    msg_data = message
        # For Kraken data
        elif type(message) is list:
            if len(message)>=3:
                if message[2]=='trade':
                    msg_data = message[1]
        
        if len(msg_data) > 0:
            # Message data can be a dictionary, or a list (usually one item) of dicts
            if type(msg_data) is dict:
                num_msgs += 1
            elif type(msg_data) is list:
                if len(msg_data)==1:
                    num_msgs += 1
                else:
                    for sub_dict in msg_data:
                        num_msgs += 1  

                        
    t2 = time.time()                   
    
    if log:
        print('\nCounting and Caching All Tardis Trade Msgs Took: '+str(np.round(t2-t1,3))+' sec')
    
    return num_msgs


def c_tardis_count_trade_msgs_cache_file(file_path):
    """
    Return number of Tardis trade messages in a .gz file
    """   

    cdef list lines = []
    cdef str json_str
    cdef int num_file_msgs = 0
    
    # read the gzip file   
    json_bytes = read_gzip_file(str(file_path))
    json_str = json_bytes.decode('utf-8')
    lines = json_str.split('\n')
   
    for l in lines:
        
        if len(l)==0:
            continue
        
        line_data = {}
        loaded_line = parse_msg_line(l)
    
        # Generic
        if 'data' in loaded_line:
            line_data = loaded_line['data']
        # For Houbi Data
        elif 'tick' in loaded_line:
            if 'data' in loaded_line['tick']:
                line_data = loaded_line['tick']['data']
        # For Deribit Data
        elif 'params' in loaded_line:
            if 'data' in loaded_line['params']:
                line_data = loaded_line['params']['data']
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
            num_file_msgs += 1
                
        elif type(line_data) is list:
            for sub_data in line_data:
                num_file_msgs += 1
    
    return num_file_msgs


def c_tardis_count_trade_msgs_cache_dir(dir_path):
    """
    Return number of Tardis trade messages in a directory 
    which contains Tardis cached .gz files 
    """      
    
    cdef int num_dir_msgs = 0
    cdef int num_file_msgs = 0
    cdef list file_paths = []
    
    file_paths = ret_all_subdir_file_paths(dir_path)
    
    for file_path in file_paths:
        if not(str(file_path).endswith('.json.gz')):
            continue
        num_file_msgs = c_tardis_count_trade_msgs_cache_file(file_path)
        num_dir_msgs += num_file_msgs
    
    return num_dir_msgs