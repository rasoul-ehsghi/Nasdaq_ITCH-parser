'''
Main ITCH parser class
'''
import struct                  # converts between python and C-style data structures represents as python byte objects.
import pandas as pd
import numpy as np
from pathlib import Path       # It's considered the recommended way to handle file paths instead of the older os.path module.
import pytz                    # a third-party Python library for working with time zones. It provides the Olson timezone database (also known as the IANA Time Zone Database) which is the most comprehensive timezone database available. 
import warnings
from typing import Dict, List, Optional, Union  # FIXED: uppercase Union
import logging                 # The logging module in Python provides a flexible framework for emitting log messages from Python programs. It's far more powerful than using print() statements and is essential for production applications.
warnings.filterwarnings('ignore')


# Configure logging
logging.basicConfig(level=logging.INFO) # only show messages that are INFO or more important
logger = logging.getLogger(__name__)

'''
logging--> secure way of printing
basicconfig --> setup function that configures how logging behaves for entire program.

logging.DEBUG --> ignore
INFO, WARNING, ERROR, CRITICAL --> shown
'''

class ITCHParser:
    # Nasdaq ITCH binary file parser

    def __init__(self, itch_file_path: Union[str, Path],  # FIXED: uppercase Union
                 date: str, 
                 timezone: str = 'US/Eastern'):

        self.itch_file_path = Path(itch_file_path)
        self.date = date
        self.timezone = pytz.timezone(timezone)

        # Message type definitions for ITCH 5.0
        self.MESSAGE_TYPES = {  # FIXED: uppercase
            'S': 'System Event',
            'R': 'Stock Directory',
            'H': 'Stock Trading Action',
            'Y': 'Reg SHO Restriction',
            'L': 'Market Participant Position',
            'V': 'MWCB Decline Level',
            'W': 'MWCB Status',
            'K': 'IPO Quoting Period Update',
            'A': 'Add Order No MPID',
            'F': 'Add Order With MPID',
            'E': 'Order Executed',
            'C': 'Order Executed With Price',
            'X': 'Order Cancel',
            'D': 'Order Delete',
            'U': 'Order Replace',
            'P': 'Trade',
            'Q': 'Cross Trade',
            'B': 'Broken Trade',
            'I': 'NOII',
            'N': 'RPII'
        }

        logger.info(f'ITCHParser initialized for file: {self.itch_file_path}')
        logger.info(f'Date: {date}, TimeZone: {timezone}')

    def parse_file_to_hdf5(self,
                           output_file_path: Union[str, Path],  # FIXED: uppercase Union
                           limit: Optional[int] = None,  # limit = None means process the whole records, limits = 100 process only 100 records
                           chunksize: int = 100000) -> Path:   # ← Returns a Path object!
        
        # parse ITCH file and save to HDF5 in chunks
        if not self.itch_file_path.exists():
            raise FileNotFoundError(f'ITCH file not found: {self.itch_file_path}')
        
        output_path = Path(output_file_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        '''
            outputpath -> creates the directory if it doesn't exist
            parents = true -> means create all missing parent directory
        '''
        logger.info(f'starting ITCH Parsing...')
        logger.info(f'output HDF5: {output_path}')
        logger.info(f"chunk size: {chunksize}, Limit: {limit or 'No limit'}")

        with pd.HDFStore(str(output_path), mode='w', complevel=9, complib='zlib') as store:  # FIXED: removed duplicate complevel
            message_data = self._initialize_message_buffers()
            message_count = 0
            chunk_count = 0

            with open(self.itch_file_path, 'rb') as f:  # FIXED: added space
                '''rb -> stands for read binary'''
                # read and skip headers (44 bytes)
                header = f.read(44)
                '''
                 ITCH 5.0 HEADER -> this is the topic of the header
                 and it's 44 bytes
                '''
                logger.info(f"File header: {header[:10].decode('ascii', errors='ignore')}...")  # FIXED: double quotes for f-string

                while True:
                    if limit and message_count >= limit:
                        logger.info(f'Reached limit of {limit} messages')
                        break

                    try:
                        # read message length (2bytes, big endian)
                        length_bytes = f.read(2)
                        if not length_bytes or len(length_bytes) < 2:
                            logger.info('end of file reached!')
                            break

                        message_length = struct.unpack('>H', length_bytes)[0]
                        # converts 2 raw bytes into a python integer representing the message length.

                        # read the message
                        message = f.read(message_length)
                        if not message or len(message) < message_length:
                            logger.warning(f'Incomplete message at position {message_count}')
                            break

                        # parse the message
                        parsed = self._parse_message(message)
                        if parsed:
                            self._categorize_message(parsed, message_data)

                        message_count += 1

                        # store in chunks
                        if message_count % chunksize == 0:
                            ''' % -> shows the remainder of division'''
                            self._save_chunk_to_store(store, message_data, chunk_count)
                            chunk_count += 1

                            self._clear_message_buffers(message_data)

                            if message_count % (chunksize * 10) == 0:
                                logger.info(f' Processed {message_count:,} messages...')
                        
                    except EOFError:  # error for ending of the file.
                        logger.info('End of File Reached!')
                        break
                    except struct.error as e:  # error for parsing file failed.
                        logger.error(f'Struct error parsing message {message_count}: {e}')
                        continue
                    except Exception as e:  # other error
                        logger.error(f'unexpected error parsing message {message_count}: {e}')
                        continue
            
            # save any remaining data
            if message_count % chunksize != 0:
                self._save_chunk_to_store(store, message_data, chunk_count)

            logger.info(f'Total messages processed: {message_count:,}')
            logger.info(f'Created {chunk_count + 1} chunk(s) in HDF5 file')

        return output_path

    def _initialize_message_buffers(self) -> Dict[str, List[Dict]]:
        '''Initialize empty buffers for different message types.'''
        
        return {
            'system_events': [],
            'stock_directory': [],
            'trading_actions': [],
            'add_orders': [],
            'trades': [],
            'order_executions': [],
            'order_cancels': [],
            'order_deletes': [],
            'order_replaces': []
        }
    
    def _clear_message_buffers(self, messages_data: Dict[str, List[Dict]]) -> None:
        """Clear all message buffers."""
        for key in messages_data:
            messages_data[key] = []
    
    def _parse_message(self, message: bytes) -> Optional[Dict]:
        '''Parse individual ITCH message.'''
        if len(message) < 1:
            return None
        
        message_type = chr(message[0])

        try:
            # common header fields for all messages
            stock_locate = struct.unpack('>H', message[1:3])[0]
            tracking_number = struct.unpack('>H', message[3:5])[0]
            timestamp = int.from_bytes(message[5:11], 'big')  # FIXED: 5:11 not 5:4

            # Parse based on message type
            parser_method = self._get_parser_method(message_type)  # FIXED: single underscore

            if parser_method:
                return parser_method(message, stock_locate, tracking_number, timestamp)
            else:
                return {
                    'message_type': message_type,
                    'stock_locate': stock_locate,
                    'tracking_number': tracking_number,
                    'timestamp': timestamp,
                    'raw_length': len(message)
                }

        except Exception as e:
            logger.error(f'Error Parsing {message_type} message: {e}')
            return None
    
    def _get_parser_method(self, message_type: str):
        # Get the appropriate parser method for a message type.
        parsers = {  # FIXED: variable name
            'S': self._parse_system_event,  # FIXED: method name
            'R': self._parse_stock_directory,
            'A': self._parse_add_order_no_mpid,  # FIXED: method name
            'F': self._parse_add_order_with_mpid,  # FIXED: method name
            'E': self._parse_order_executed,  # FIXED: method name
            'P': self._parse_trade,
            'X': self._parse_order_cancel,  # FIXED: method name
            'D': self._parse_order_delete,
            'U': self._parse_order_replace,
            'H': self._parse_stock_trading_action,
        }
        return parsers.get(message_type)
    
    def _parse_system_event(self, message: bytes, stock_locate: int,  # FIXED: method name
                            tracking_number: int, timestamp: int) -> Dict:
        # Parse System Event Message (type 'S').
        event_code = chr(message[11])

        return {
            'message_type': 'S',
            'stock_locate': stock_locate,
            'tracking_number': tracking_number,
            'timestamp': timestamp,
            'event_code': event_code
        }
    
    def _parse_stock_directory(self, message: bytes, stock_locate: int, 
                               tracking_number: int, timestamp: int) -> Dict:
        # Parse Stock Directory Message (type 'R')
        stock = message[11:19].strip(b'\x00').decode('ascii', errors='ignore')
        market_category = chr(message[19])
        financial_status = chr(message[20])
        round_lot_size = struct.unpack('>I', message[40:44])[0]
        
        return {
            'message_type': 'R',
            'stock_locate': stock_locate,
            'tracking_number': tracking_number,
            'timestamp': timestamp,
            'stock': stock,
            'market_category': market_category,
            'financial_status': financial_status,
            'round_lot_size': round_lot_size
        }

    def _parse_add_order_no_mpid(self, message: bytes, stock_locate: int,  # FIXED: method name
                                 tracking_number: int, timestamp: int) -> Dict:
        # Parse Add order no MPID message (type 'A')
        order_ref = struct.unpack('>Q', message[19:27])[0]
        buy_sell = 'B' if chr(message[11]) == 'B' else 'S'
        shares = struct.unpack('>I', message[27:31])[0]  # FIXED: '>I' not '>Q'
        stock = message[12:20].strip(b'\x00').decode('ascii', errors='ignore')  # FIXED: b'\x00' not 'b\x00'
        price = struct.unpack('>I', message[31:35])[0] / 10000.0
        
        return {
            'message_type': 'A',
            'stock_locate': stock_locate,
            'tracking_number': tracking_number,
            'timestamp': timestamp,
            'order_ref': order_ref,
            'buy_sell': buy_sell,
            'shares': shares,
            'stock': stock,
            'price': price
        }

    def _parse_add_order_with_mpid(self, message: bytes, stock_locate: int,  # FIXED: method name
                                   tracking_number: int, timestamp: int) -> Dict:
        # Parse Add order with MPID Message (type 'F')
        order_ref = struct.unpack('>Q', message[19:27])[0]
        buy_sell = 'B' if chr(message[11]) == 'B' else 'S'
        shares = struct.unpack('>I', message[27:31])[0]
        stock = message[12:20].strip(b'\x00').decode('ascii', errors='ignore')  # FIXED: b'\x00'
        price = struct.unpack('>I', message[31:35])[0] / 10000.0
        mpid = message[35:39].strip(b'\x00').decode('ascii', errors='ignore')
        return {
            'message_type': 'F',
            'stock_locate': stock_locate,
            'tracking_number': tracking_number,
            'timestamp': timestamp,
            'order_ref': order_ref,
            'buy_sell': buy_sell,
            'shares': shares,
            'stock': stock,
            'price': price,
            'mpid': mpid 
        }
    
    def _parse_order_executed(self, message: bytes, stock_locate: int,  # FIXED: method name
                              tracking_number: int, timestamp: int) -> Dict:
        # Parse orders executed message (type 'E')
        order_ref = struct.unpack('>Q', message[11:19])[0]
        executed_shares = struct.unpack('>I', message[19:23])[0]
        match_number = struct.unpack('>Q', message[23:31])[0]

        return {
            'message_type': 'E',
            'stock_locate': stock_locate,
            'tracking_number': tracking_number,
            'timestamp': timestamp,
            'order_ref': order_ref,
            'executed_shares': executed_shares,
            'match_number': match_number
        }
    
    def _parse_trade(self, message: bytes, stock_locate: int,  # ADDED: missing method
                     tracking_number: int, timestamp: int) -> Dict:
        # Parse Trade Message (type 'P')
        order_ref = struct.unpack('>Q', message[19:27])[0]
        shares = struct.unpack('>Q', message[27:35])[0]
        stock = message[11:19].strip(b'\x00').decode('ascii', errors='ignore')
        price = struct.unpack('>I', message[35:39])[0] / 10000.0
        match_number = struct.unpack('>Q', message[39:47])[0]
        
        return {
            'message_type': 'P',
            'stock_locate': stock_locate,
            'tracking_number': tracking_number,
            'timestamp': timestamp,
            'order_ref': order_ref,
            'shares': shares,
            'stock': stock,
            'price': price,
            'match_number': match_number
        }
    
    def _parse_order_cancel(self, message: bytes, stock_locate: int,  # FIXED: method name
                            tracking_number: int, timestamp: int) -> Dict:
        # Parse Orders Cancel message (type 'X')
        order_ref = struct.unpack('>Q', message[11:19])[0]
        canceled_shares = struct.unpack('>I', message[19:23])[0]

        return {
            'message_type': 'X',
            'stock_locate': stock_locate,
            'tracking_number': tracking_number,
            'timestamp': timestamp,
            'order_ref': order_ref,
            'canceled_shares': canceled_shares 
        }
    
    def _parse_order_delete(self, message: bytes, stock_locate: int, 
                            tracking_number: int, timestamp: int) -> Dict:
        # Parse order delete message (type 'D')
        order_ref = struct.unpack('>Q', message[11:19])[0]

        return {
            'message_type': 'D',
            'stock_locate': stock_locate,
            'tracking_number': tracking_number,
            'timestamp': timestamp,
            'order_ref': order_ref
        }
    
    def _parse_order_replace(self, message: bytes, stock_locate: int, 
                             tracking_number: int, timestamp: int) -> Dict:
        # Parse order replaced message (type 'U')
        orig_order_ref = struct.unpack('>Q', message[11:19])[0]
        new_order_ref = struct.unpack('>Q', message[19:27])[0]
        shares = struct.unpack('>I', message[27:31])[0]
        price = struct.unpack('>I', message[31:35])[0] / 10000.0

        return {
            'message_type': 'U',
            'stock_locate': stock_locate,
            'tracking_number': tracking_number,
            'timestamp': timestamp,
            'orig_order_ref': orig_order_ref,
            'new_order_ref': new_order_ref,
            'shares': shares,
            'price': price
        }
    
    def _parse_stock_trading_action(self, message: bytes, stock_locate: int,
                                    tracking_number: int, timestamp: int) -> Dict:
        # Parse Stock Trading Action Message (type 'H')
        stock = message[11:19].strip(b'\x00').decode('ascii', errors='ignore')  # FIXED: b'\x00'
        trading_state = chr(message[19])
        reserved = chr(message[20])
        reason = message[21:25].strip(b'\x00').decode('ascii', errors='ignore')

        return {
            'message_type': 'H',
            'stock_locate': stock_locate,
            'tracking_number': tracking_number,
            'timestamp': timestamp,
            'stock': stock,
            'trading_state': trading_state,
            'reason': reason
        }

    def _categorize_message(self, parsed: Dict, messages_data: Dict[str, List[Dict]]) -> None:  # FIXED: return None
        # Categorize Parsed Message into appropriate buffer
        msg_type = parsed.get('message_type')

        if msg_type == 'S':
            messages_data['system_events'].append(parsed)
        elif msg_type == 'R':
            messages_data['stock_directory'].append(parsed)
        elif msg_type == 'H':
            messages_data['trading_actions'].append(parsed)
        elif msg_type in ['A', 'F']:
            messages_data['add_orders'].append(parsed)
        elif msg_type == 'P':
            messages_data['trades'].append(parsed)
        elif msg_type in ['E', 'C']:
            messages_data['order_executions'].append(parsed)
        elif msg_type == 'X':
            messages_data['order_cancels'].append(parsed)
        elif msg_type == 'D':
            messages_data['order_deletes'].append(parsed)
        elif msg_type == 'U':
            messages_data['order_replaces'].append(parsed)

    def _save_chunk_to_store(self, store: pd.HDFStore, message_data: Dict[str, List[Dict]],
                             chunk_num: int) -> None:
        # Save a chunk of data to HDF5 store.
        for data_type, data_list in message_data.items():
            if data_list:
                df = pd.DataFrame(data_list)
                df['chunk'] = chunk_num
                df['date'] = self.date

                # convert timestamp to datetime
                if 'timestamp' in df.columns:
                    df['datetime'] = pd.to_datetime(df['timestamp'], unit='ns')

                # Store in HDF5
                key = f'/{data_type}/chunk_{chunk_num:04d}'  # FIXED: :04d not :04
                store.put(key, df, format='table', data_columns=True)
                logger.debug(f'Saved chunk {chunk_num} for {data_type}: {len(df)} messages')  # FIXED: debug not info