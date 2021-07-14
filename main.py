import SMA_loader

def get_info_on_tickers():
    tickers = ['FAKETKR', 'SPCE',  'AAPL', 'AMC', 'NIO', 'GE', 'CLOV', 'DIDI', 'F', 'NOKPF', 'WISH', 'BAC', 'ITUB',
               'TME', 'AMD', 'TSLA',
               'CLF', 'BBD', 'T', 'CCL', 'PLTR', 'GSAT', 'AAL', 'C', 'PLUG', 'PBR']

    for ticker in tickers:
        status, message = SMA_loader.av_import_sma(ticker)
        print(f"Ticker: {ticker}. SMA import status = {status}. With message: -- {message}")

if __name__=='__main__':
    get_info_on_tickers()
