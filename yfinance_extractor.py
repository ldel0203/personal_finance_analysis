import logging
import pandas as pd
import yfinance as yf

class YFinanceExtractor:
    def __init__(self):
        pass
        
    def extract_securities_info(self, tickers: list):
        data = []
        for ticker in tickers:
            asset = yf.Ticker(ticker)
            data.append({
                "ticker": ticker,
                "type": asset.fast_info.get("quoteType"),
                "market": asset.fast_info.get("exchange")
            })
        return pd.DataFrame(data)
    
    def extract_security_prices(self, securities_df: pd.DataFrame) ->pd.DataFrame:
        data = []
        for _, row in securities_df.iterrows():
            try:
                ticker = row["ticker"]
                start = row["start_import_date"]
                end = row["end_import_date"]
                isin = row["isin"]
                
                hist = yf.download(
                    tickers= ticker,
                    start= start,
                    end= end,
                    progress= False,
                    group_by="column",
                    auto_adjust=True
                )
                hist.reset_index(inplace=True)
                hist["isin"] = isin
                
                hist.columns = ["_".join([str(c) for c in col if c != ""]) for col in hist.columns]
                
                hist = hist.rename(columns={
                    f"Date": "date",
                    f"Open_{ticker}": "open_price",
                    f"Close_{ticker}": "close_price",
                    f"High_{ticker}": "high",
                    f"Low_{ticker}": "low",
                    f"Volume_{ticker}": "volume"
                })
                
                hist = hist[["isin", "date", "open_price", "close_price", "high", "low", "volume"]]
                
                data.append(hist)
                logging.info(f"Data found for isin={isin}/ticker={ticker}")
            except Exception as e:
                logging.error(f"Unable to download data for isin={isin}/ticker={ticker} : {e}")
        
        return pd.concat(data, ignore_index=True)
