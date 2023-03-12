from . import filecollect
from . import dbconnect

class Context(object):
    def __init__(self, config):
        self.taisyaku = filecollect.TaisyakuCollector(config=config["resources"])
        self.softhompo = filecollect.SofthompoCollector(config=config["resources"])
        self.softhompoShinyo = filecollect.SofthompoShinyoCollector(config=config["resources"])
        self.symbolsconnector = dbconnect.SymbolsConnector(config=config["database"])

    def download(self):
        self.taisyaku.download()
        self.softhompo.download()
        self.softhompoShinyo.download()

    def daily_collect(self):
        openingdays = list(map(lambda e: e["opening_date"], self.symbolsconnector.find_openingdays()))
        pasts = self.taisyaku.get_pasts()
        pasts.extend(self.softhompo.get_pasts())
        targets = list(filter(lambda d: d not in openingdays, [x for x in set(pasts) if pasts.count(x) > 1]))
        for target in targets:
            symbols = self.symbolsconnector.find_symbols()
            t_data = self.taisyaku.read(target)
            s_data = self.softhompo.read(target)
            for symbol in symbols:
                symbol["opening_date"] = target
                for s in s_data:
                    if symbol["symbol_code"] == s["symbol_code"]:
                        symbol["first_opening_price"] =   s["first_opening_price"]
                        symbol["first_high_price"] =      s["first_high_price"]
                        symbol["first_low_price"] =       s["first_low_price"]
                        symbol["first_closing_price"] =   s["first_closing_price"]
                        symbol["latter_opening_price"] =  s["latter_opening_price"]
                        symbol["latter_high_price"] =     s["latter_high_price"]
                        symbol["latter_low_price"] =      s["latter_low_price"]
                        symbol["latter_closing_price"] =  s["latter_closing_price"]
                        symbol["change_previous_close"] = s["change_previous_close"]
                        symbol["trading_volume"] =        s["trading_volume"]
                        symbol["trading_value"] =         s["trading_value"]
                        symbol["vwap"] =                  s["vwap"]
                for t in t_data:
                    if symbol["symbol_code"] == t["symbol_code"]:
                        symbol["loaning_amount"] =      t["loaning_amount"]
                        symbol["paid_loaning_amount"] = t["paid_loaning_amount"]
                        symbol["loaning_balance"] =     t["loaning_balance"]
                        symbol["lending_amount"] =      t["lending_amount"]
                        symbol["paid_lending_amount"] = t["paid_lending_amount"]
                        symbol["lending_balance"] =     t["lending_balance"]
                        symbol["credit_balance"] =      t["credit_balance"]
            for symbol in symbols:
                self.symbolsconnector.save_symbol(symbol)

    def weekly_collect(self):
        weekenddays = list(map(lambda e: e["weekend_date"], self.symbolsconnector.find_weekenddays()))
        pasts = self.softhompoShinyo.get_pasts()
        targets = list(filter(lambda d: d not in weekenddays, pasts))
        for target in targets:
            s_data = self.softhompoShinyo.read(target)
            for data in s_data:
                self.symbolsconnector.save_symbol_week(data)
