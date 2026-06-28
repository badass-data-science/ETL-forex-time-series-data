schema_measurement_name = 'candlestick'

ALLOWED_TAGS = set(['instrument', 'granularity'])

ALLOWED_FIELDS = {
    'volume' : int,
    'complete' : bool,
    
    'bid_open' : float,
    'bid_high' : float,
    'bid_low' : float,
    'bid_close' : float,

    'ask_open' : float,
    'ask_high' : float,
    'ask_low' : float,
    'ask_close' : float,
}

schema_measurement_name_ff = 'forward-filled candlestick'

ALLOWED_TAGS_ff = set(['instrument', 'granularity'])

ALLOWED_FIELDS_ff = {
    'volume' : int,
    
    'bid_open' : float,
    'bid_high' : float,
    'bid_low' : float,
    'bid_close' : float,

    'mid_open' : float,
    'mid_high' : float,
    'mid_low' : float,
    'mid_close' : float,
    
    'ask_open' : float,
    'ask_high' : float,
    'ask_low' : float,
    'ask_close' : float,

    'spread_open' : float,
    'spread_high' : float,
    'spread_low' : float,
    'spread_close' : float,
}