# List of column names for the quandl price table.
# Used as references so that strange values for col names can be changed later with less pain.
TICKER_COL = "ticker"
DATE_COL = "date"
OPEN_COL = "open"
HIGH_COL = "high"
LOW_COL = "low"
CLOSE_COL = "close"
VOLUME_COL = "volume"

PRICES_COLUMNS_TO_KEEP = [
    TICKER_COL,
    DATE_COL,
    OPEN_COL,
    CLOSE_COL,
    HIGH_COL,
    LOW_COL,
    VOLUME_COL
]

EX_DIVIDEND_COL = "ex-dividend"
SPLIT_RATIO = "split_ratio"
ADJ_OPEN = "adj_open"
ADJ_HIGH = "adj_high"
ADJ_LOW = "adj_low"
ADJ_CLOSE = "adj_close"
ADJ_VOLUME = "adj_volume"

PRICES_COLUMNS_TO_DROP = [
    EX_DIVIDEND_COL,
    SPLIT_RATIO,
    ADJ_OPEN,
    ADJ_HIGH,
    ADJ_LOW,
    ADJ_CLOSE,
    ADJ_VOLUME
]
