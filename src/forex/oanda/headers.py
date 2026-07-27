from forex.oanda.config import oanda_config


def get_oanda_headers() -> dict:
    return {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + oanda_config.OANDA_TOKEN,
        'Accept-Datetime-Format': oanda_config.OANDA_DATE_TIME_FORMAT,
    }
