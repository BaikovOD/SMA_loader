from datetime import datetime

# config parser
import os, configparser

# LOGGER
from functools import wraps
import logging
import csv
import io
from timeit import default_timer as timer

# alpha_vantage
from alpha_vantage.techindicators import TechIndicators

# --------------------------------------------------------------------
# LOGGER
class CsvFormatter(logging.Formatter):
    def __init__(self):
        super().__init__()
        self.output = io.StringIO()
        self.writer = csv.writer(self.output, quoting=csv.QUOTE_ALL)

    def format(self, record):
        self.writer.writerow([record.levelname, datetime.now(), record.msg])
        data = self.output.getvalue()
        self.output.truncate(0)
        self.output.seek(0)
        return data.strip()


def get_logger(file_name):
    logging.basicConfig(level=logging.DEBUG)

    logger = logging.getLogger("SMA")
    frmt = CsvFormatter()
    logging.root.handlers[0].setFormatter(frmt)

    fh = logging.FileHandler(file_name)
    fh.setLevel(logging.DEBUG)  # ensure all messages are logged to file
    fh.setFormatter(frmt)
    logger.addHandler(fh)

    return logger


def close_log_file(logger):
    handlers = logger.handlers[:]
    for handler in handlers:
        handler.close()
        logger.removeHandler(handler)


def log_error():
    def error_log(func):

        @wraps(func)
        def wrapper(*args, **kwargs):

            start_time = timer()
            success_status, info_message = False, ""

            logger = get_logger(f"logs\\{func.__name__}-{datetime.now():%Y%m%d_%H%M%S-%f}.csv")
            logger.debug(f"Function {func.__name__} called with args:{args}{kwargs}")

            try:
                success_status, info_message = func(*args, **kwargs)
                if success_status == False:
                    logger.exception(info_message)
            except Exception as e:
                info_message = 'Unknown error occured /' + func.__name__ + '\n' + str(e)
                logger.exception(info_message)

            end_time = timer()
            logger.debug(
                f"Function {func.__name__} call ended with status '{success_status}' and run time: {end_time - start_time} seconds")

            close_log_file(logger)

            return (success_status, info_message)

        return wrapper

    return error_log


# -------------------------------------------------
# ALPHA VANTAGE

def _get_api_key():
    if not os.path.isfile("config.ini"):
        return ("", "config.ini file doesn't exist")

    api_key = ""
    with open("config.ini", "r") as config_file:
        config = configparser.ConfigParser()
        config.read("config.ini")
        try:
            api_key = config["ALPHA_VANTAGE"]["api_key"]
        except:
            return ("", "api_key not found in the config.ini")

    return (api_key, "")

API_KEY = ""
API_KEY, error_message = _get_api_key()
if API_KEY == "":
    print("API key wasn't initialised!")
    raise SystemExit


@log_error()
def av_import_sma(ticker):
    """
    1.Fetches the SMA values using alpha_vantage framework
    2.Processes the data
    3.Saves to parquetee file with snappy compression
    !!!MAX QUERIES = 5/minute AND 500/day
    :param ticker:
    :param api_key:
    :return: tuple(SuccessStatus(bool), info_message)
    """

    # initial variables
    interval, time_period, series_type='daily', '90', 'close'

    # --------------------
    # 1. fetching the data
    if type(ticker) != type("") or ticker.strip() == "":
        return (False, "Empty ticker!")

    try:
        tech_indctr = TechIndicators(key=API_KEY, output_format='pandas')
        sma_df, sma_mf = tech_indctr.get_sma(symbol=ticker, interval=interval,
                                                time_period=time_period, series_type=series_type)
    except ValueError as val_error:
        if "if you would like to target a higher API call frequency" in str(val_error):
            error_msg = "Exception. Too many queries in a low period of time:" + str(val_error)
        else:
            error_msg = "Exception. Check API key or the ticker. error code: " + str(val_error)
        return (False, error_msg)

    # --------------------
    # 2. Preprocess the data (change some columns name's)
    sma_df = sma_df.rename(columns={'SMA':'SMA_close'})

    # --------------------
    # 3. Save to paqruete (compress with Snappy)
    try:
        file_name_tmp = f"SMA-{datetime.now():%Y%m%d}-{ticker}-{series_type}-{interval}-{time_period}"
        df_file_name = "data_frames\\" + file_name_tmp + "-df.parquet"
        sma_df.to_parquet(df_file_name, compression='snappy')

    except AttributeError as attr_error:
        return (False, "Couldn't save the parquet file: " + str(attr_error))

    except Exception as exc:
        return (False, "Couldn't save the parquet file: " + str(exc))

    return (True, f"Saved {len(sma_df)} rows into file")