import os

try:
    from .password import ALPHAVANTAGE_API_KEY 
    os.environ["ALPHAVANTAGE_API_KEY"] = ALPHAVANTAGE_API_KEY
except ImportError as error:
    print("no alpha vantage key available", error)
    pass

