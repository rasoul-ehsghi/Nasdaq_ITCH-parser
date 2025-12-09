"""
Utility function for ITCH data processing and Analysis
"""
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional, List, Dict, Union
import matplotlib.pyplot as plt
import logging

logger = logging.getLogger(__name__)


def read_itch_hdf5(hdf5_path: Union[str, Path], 
                   data_type: str = 'trades',
                   chunks: Optional[List[int]] = None) -> pd.DataFrame:
    '''
    Read Data from ITCH HDF5 file

    Args:
        hdf5_path: Path to HDF5 file
        data_type: Type of data to read(e.g. 'trades', 'add_orders')
        chunks: Specific chunks to read(None for All)

    Returns:
        DataFrame with requested Data
    
    Raises:
        FileNotFoundError: if file doesn't exist
        ValueError: if data_type is invalid
    '''

    hdf5_path = Path(hdf5_path)
    if not hdf5_path.exists():
        raise FileNotFoundError(f'HDF5 file not found: {hdf5_path}')  
    
    valid_data_types = [
        'system_events', 'stock_directory', 'trading_actions',
        'add_orders', 'trades', 'order_executions',
        'order_cancels', 'order_deletes', 'order_replaces'
    ]

    if data_type not in valid_data_types:
        raise ValueError(f'Invalid data_type. Must be one of {valid_data_types}')
    
    logger.info(f'Reading {data_type} from {hdf5_path}')

    with pd.HDFStore(str(hdf5_path), mode='r') as store:
        data_frames = []

        for key in store.keys():
            if data_type in key:
                if chunks is not None:
                    # Extract chunk number from key
                    try:
                        chunk_num = int(key.split('_')[-1])  
                        if chunk_num in chunks:
                            df = store[key]
                            data_frames.append(df)
                    except (ValueError, IndexError):
                        # If can't parse chunk number, include it anyway
                        df = store[key]
                        data_frames.append(df)
                else:
                    df = store[key]
                    data_frames.append(df)
        
        if not data_frames:
            logger.warning(f'No {data_type} data found in {hdf5_path}')
            return pd.DataFrame()
        
        combined_df = pd.concat(data_frames, ignore_index=True)
        logger.info(f'Loaded {len(combined_df):,} {data_type} messages')  
        return combined_df


def query_trades(hdf5_path: Union[str, Path], 
                 stock_symbol: Optional[str] = None,
                 start_time: Optional[pd.Timestamp] = None,
                 end_time: Optional[pd.Timestamp] = None) -> pd.DataFrame:
    '''
    Query trades from ITCH HDF5 file with filtering options

    Args:
        hdf5_path: Path to HDF5 file
        stock_symbol: Filter by stock symbol (e.g., 'AAPL')
        start_time: Filter trades after this time
        end_time: Filter trades before this time

    Returns:
        DataFrame with filtered trades
    '''

    trades_df = read_itch_hdf5(hdf5_path, data_type='trades')

    if trades_df.empty:
        return trades_df
    
    # Apply filters
    if stock_symbol:
        trades_df = trades_df[trades_df['stock'] == stock_symbol]

    if start_time:
        trades_df = trades_df[trades_df['datetime'] >= start_time]
    
    if end_time:
        trades_df = trades_df[trades_df['datetime'] <= end_time]

    # Sort by timestamp
    if not trades_df.empty and 'datetime' in trades_df.columns:
        trades_df = trades_df.sort_values('datetime')

    logger.info(f'Query returned {len(trades_df):,} trades')
    return trades_df


def analyze_hdf5_files(itch_h5_path: Union[str, Path],
                       order_book_h5_path: Optional[Union[str, Path]] = None) -> Dict:
    '''
    Analyze HDF5 files and return statistics.
    
    Args:
        itch_h5_path: Path to ITCH HDF5 file
        order_book_h5_path: Path to order book HDF5 file (optional)
        
    Returns:
        Dictionary with analysis results
    '''
    analysis = {'itch_file': {}, 'order_book_file': {}}

    # Analyze ITCH file
    itch_path = Path(itch_h5_path)
    if itch_path.exists():
        with pd.HDFStore(str(itch_path), mode='r') as store:
            analysis['itch_file']['keys'] = list(store.keys())  

            total_messages = 0
            message_counts = {}

            for key in store.keys():  
                df = store[key]
                data_type = key.split('/')[1] if '/' in key and len(key.split('/')) > 1 else 'unknown'  
                message_counts[data_type] = message_counts.get(data_type, 0) + len(df)
                total_messages += len(df)
        
        analysis['itch_file']['total_messages'] = total_messages
        analysis['itch_file']['message_counts'] = message_counts
    
    # Analyze order book file
    if order_book_h5_path:
        ob_path = Path(order_book_h5_path)
        if ob_path.exists():
            with pd.HDFStore(str(ob_path), mode='r') as store:
                analysis['order_book_file']['keys'] = list(store.keys())  

                stocks = set()
                for key in store.keys():
                    if '/' in key:
                        parts = key.split('/')
                        if len(parts) > 2:
                            stocks.add(parts[1])
                
                analysis['order_book_file']['stocks'] = sorted(stocks)
                analysis['order_book_file']['num_stocks'] = len(stocks)  
    
    return analysis


def plot_trade_prices(trades_df: pd.DataFrame,
                      title: str = 'Trade Prices',
                      figsize: tuple = (14, 7)) -> plt.Figure:
    '''Plot trade prices over time'''
    if trades_df.empty or 'datetime' not in trades_df.columns or 'price' not in trades_df.columns:
        logger.warning('No valid trade data to plot.')
        return None
    
    fig, ax = plt.subplots(figsize=figsize)
    ax.plot(trades_df['datetime'], trades_df['price'], 'b.', markersize=2, alpha=0.7)
    ax.set_xlabel('Time')
    ax.set_ylabel('Price ($)')
    ax.set_title(title)
    ax.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()

    return fig


def build_order_book_from_store(itch_h5_path: Union[str, Path],
                                output_h5_path: Union[str, Path],
                                stock_symbols: Optional[List[str]] = None) -> Path:
    '''Build order book from ITCH HDF5 store.'''
    logger.info('Building order book from HDF5 store...')

    output_path = Path(output_h5_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Read all add orders
    add_orders_df = read_itch_hdf5(itch_h5_path, data_type='add_orders')

    if add_orders_df.empty:
        logger.warning('No add orders found in ITCH file')
        return output_path
    
    # Filter by stock symbol if provided
    if stock_symbols:
        add_orders_df = add_orders_df[add_orders_df['stock'].isin(stock_symbols)]

    # Group by stock
    stocks = add_orders_df['stock'].unique()
    logger.info(f'Building order book for {len(stocks)} stocks')

    with pd.HDFStore(str(output_path), mode='w', complevel=9, complib='zlib') as store:
        for stock in stocks:
            stock_orders = add_orders_df[add_orders_df['stock'] == stock]  

            # Separate bids and asks
            bids = stock_orders[stock_orders['buy_sell'] == 'B'].copy()
            asks = stock_orders[stock_orders['buy_sell'] == 'S'].copy()

            if not bids.empty:
                bids = bids[['order_ref', 'price', 'shares', 'timestamp']]
                bids.set_index('order_ref', inplace=True)
                store.put(f'/{stock}/bids', bids, format='table')  

            if not asks.empty:
                asks = asks[['order_ref', 'price', 'shares', 'timestamp']]
                asks.set_index('order_ref', inplace=True)
                store.put(f'/{stock}/asks', asks, format='table')  
    
    logger.info(f'Order book saved to: {output_path}')
    return output_path