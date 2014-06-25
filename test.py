from pandas.io.data import DataReader
from datetime import datetime
from pandas.io.data import Options

goog = DataReader("GOOG",  "yahoo", start=datetime(2000,1,1))
print goog["Adj Close"]
print goog

appl = Options('AAPL', 'yahoo')
puts,calls = appl.get_options_data()
print puts
print calls


