# NASDAQ ITCH Parser 📈

A high-performance Python package for parsing **NASDAQ ITCH 5.0 binary market data** and storing it in efficient **HDF5** format for financial analysis and research.

![Python](https://img.shields.io/badge/python-3.8%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)
![HDF5](https://img.shields.io/badge/format-HDF5-orange)

---

## 📋 Features

- **Binary ITCH 5.0 Parsing:** Parse raw NASDAQ ITCH 5.0 binary files  
- **HDF5 Storage:** Store parsed data in efficient, compressed HDF5 format  
- **Chunked Processing:** Handle huge files (10GB+) in memory-efficient chunks  
- **Message Type Support:**
  - System Events (`S`)
  - Stock Directory (`R`)
  - Add Orders (`A`, `F`)
  - Trades (`P`)
  - Order Executions (`E`, `C`)
  - Cancels / Deletes / Replaces (`X`, `D`, `U`)
  - …and more  
- **Order Book Reconstruction:** Build limit order books  
- **Time-Series Queries:** Query by symbol and time range  
- **Visualization Tools:** Built-in trade and order-book plotting utilities  

---

## 🚀 Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/nasdaq-itch-parser.git
cd nasdaq-itch-parser

# Install dependencies
pip install -r requirements.txt

# Install the package in development mode
pip install -e .
```

---
### Basic Usage

```python
from itch_parser import ITCHParser
from itch_parser.utils import analyze_hdf5_files, query_trades

# Initialize parser with ITCH file
parser = ITCHParser('10302019.NASDAQ_ITCH50', date='10302019')

# Parse to HDF5 (first 10,000 messages for testing)
result = parser.parse_file_to_hdf5(
    output_file_path='processed_data/itch.h5',
    limit=10000,
    chunksize=5000
)

# Analyze parsed data
analysis = analyze_hdf5_files('processed_data/itch.h5')
print(f"Total messages: {analysis['itch_file']['total_messages']:,}")

# Query trades for a specific stock
aapl_trades = query_trades(
    'processed_data/itch.h5',
    stock_symbol='AAPL',
    start_time='2019-10-30 09:30:00',
    end_time='2019-10-30 10:00:00'
)
print(f"AAPL trades: {len(aapl_trades):,}")
```

---

## 📁 Project Structure

```pgsql
nasdaq-itch-parser/
├── itch_parser/
│   ├── __init__.py
│   ├── parser.py
│   └── utils.py
├── examples/
│   ├── basic_usage.py
│   ├── order_book_builder.py
│   └── visualization.py
├── tests/
│   ├── test_parser.py
│   └── test_utils.py
├── requirements.txt
├── setup.py
└── README.md

```

---

## 🔧 Requirements
```txt
pandas>=1.0.0
numpy>=1.18.0
tables>=3.6.0
pytz>=2020.1
matplotlib>=3.2.0
```
---

## 📊 Data Flow

```java
NASDAQ ITCH Binary File (.bin)
        ↓
    ITCHParser
        ↓
    HDF5 Storage (.h5)
        ↓
    Query / Analysis
        ↓
    Visualization / Research
```
---

## 🎯 Key Methods:
`ITCHParser.parse_file_to_hdf5()`
```python
parser.parse_file_to_hdf5(
    output_file_path: Union[str, Path],
    limit: Optional[int] = None,
    chunksize: int = 100000
) -> Path

```
`read_itch_hdf5()`

```python
read_itch_hdf5(
    hdf5_path: Union[str, Path],
    data_type: str = 'trades',
    chunks: Optional[List[int]] = None
) -> pd.DataFrame

```

`query_trades()`

```python
query_trades(
    hdf5_path: Union[str, Path],
    stock_symbol: Optional[str] = None,
    start_time: Optional[pd.Timestamp] = None,
    end_time: Optional[pd.Timestamp] = None
) -> pd.DataFrame

```

`build_order_book_from_store()`

```python
build_order_book_from_store(
    itch_h5_path: Union[str, Path],
    output_h5_path: Union[str, Path],
    stock_symbols: Optional[List[str]] = None
) -> Path
```

---


## 📈 Example: Full Processing Pipeline

```python
import pandas as pd
from pathlib import Path
from itch_parser import ITCHParser
from itch_parser.utils import *

# Configuration
ITCH_FILE = Path('data/10302019.NASDAQ_ITCH50')
DATA_DIR = Path('processed')
DATE = '10302019'

# 1. Parse ITCH file
parser = ITCHParser(ITCH_FILE, DATE)
itch_h5 = parser.parse_file_to_hdf5(
    output_file_path=DATA_DIR / 'itch.h5',
    limit=None,
    chunksize=100000
)

# 2. Build order book
order_book_h5 = build_order_book_from_store(
    itch_h5,
    DATA_DIR / 'order_book.h5',
    stock_symbols=['AAPL', 'MSFT', 'GOOGL']
)

# 3. Analyze & visualize
trades = query_trades(itch_h5, stock_symbol='AAPL')
if not trades.empty:
    fig = plot_trade_prices(trades, "AAPL Trade Prices")
    fig.savefig('aapl_trades.png')

# 4. Get statistics
stats = analyze_hdf5_files(itch_h5, order_book_h5)
print(f"Total messages: {stats['itch_file']['total_messages']:,}")

```

## 🧪 Running Tests

```bash
# Run all tests
python -m pytest tests/

# Run specific test file
python -m pytest tests/test_parser.py -v

# Run with coverage
python -m pytest tests/ --cov=itch_parser

```


---

## 👨‍💻 Author

**Rasoul Esghi**

✉️[Github](https://github.com/rasoul-ehsghi)

💼[LinkedIn](https://www.linkedin.com/in/rasoul-eshghi)

📧[Gmail](cfte.mehr@gmail.com)
