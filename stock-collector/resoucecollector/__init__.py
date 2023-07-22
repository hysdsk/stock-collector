from . import filecollect
from .collecter import jsda
from . import dbconnect
import collections

class Context(object):
    def __init__(self, config):
        self.taisyaku = filecollect.TaisyakuCollector(config=config["resources"])
        self.softhompo = filecollect.SofthompoCollector(config=config["resources"])
        self.softhompoShinyo = filecollect.SofthompoShinyoCollector(config=config["resources"])
        self.jsdanewdeal = jsda.JSDANewDealCollector(config=config["resources"])
        self.jsdabalance = jsda.JSDABalanceCollector(config=config["resources"])
        self.symbolsconnector = dbconnect.SymbolsConnector(config=config["database"])

    def download(self):
        self.taisyaku.download()
        self.softhompo.download()
        self.softhompoShinyo.download()
        self.jsdanewdeal.download()
        self.jsdabalance.download()

    def daily_collect(self):
        openingdays = [e["opening_date"] for e in self.symbolsconnector.find_openingdays()]
        pasts = self.taisyaku.get_pasts()
        pasts.extend(self.softhompo.get_pasts())
        targets = [x for x in set(pasts) if pasts.count(x) > 1 and x not in openingdays]
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
        # 各ファイル日付と登録済み週末日付を突合し未登録の日付を対象日とする
        weekenddays = [e["weekend_date"] for e in self.symbolsconnector.find_weekenddays()]
        symbols_pasts = self.softhompoShinyo.get_pasts()
        newdeal_pasts = self.jsdanewdeal.get_pasts()
        balance_pasts = self.jsdabalance.get_pasts()
        pasts = collections.Counter(symbols_pasts + newdeal_pasts + balance_pasts)
        pasts = [p for p in pasts if pasts[p] == 3]
        targets = [d for d in pasts if d not in weekenddays]

        for target in targets:
            symbols = self.softhompoShinyo.read(target)
            newdeals = self.jsdanewdeal.read(target)
            balances = self.jsdabalance.read(target)
            for symbol in symbols:
                code = symbol["symbol_code"]
                if code not in newdeals or code not in balances:
                    continue
                newdeal = {
                    "lend_contract":                  newdeals[code][1],
                    "lend_contract_value":            newdeals[code][2],
                    "borrow_contract_self":           newdeals[code][3],
                    "borrow_contract_self_value":     newdeals[code][4],
                    "borrow_contract_sublease":       newdeals[code][5],
                    "borrow_contract_sublease_value": newdeals[code][6],
                }
                balance = {
                    "lend_balance":                  balances[code][1],
                    "lend_balance_value":            balances[code][2],
                    "borrow_balance_self":           balances[code][3],
                    "borrow_balance_self_value":     balances[code][4],
                    "borrow_balance_sublease":       balances[code][5],
                    "borrow_balance_sublease_value": balances[code][6],
                }
                merged = symbol | newdeal | balance
                self.symbolsconnector.save_symbol_week(merged)
