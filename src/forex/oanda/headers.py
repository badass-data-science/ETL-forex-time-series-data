
def get_oanda_headers(config):
    headers = {
        'Content-Type' : 'application/json',
        'Authorization' : 'Bearer ' + config['token'],
        'Accept-Datetime-Format' : config['oanda_date_time_format'],
    }
    return headers
