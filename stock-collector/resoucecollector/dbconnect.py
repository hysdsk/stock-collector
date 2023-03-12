from mysql import connector as mc

class Connector(object):
    def __init__(self, config):
        self.host = config["db_host"]
        self.user = config["db_user"]
        self.pswd = config["db_pswd"]
        self.name = config["db_name"]

    def find(self, sql, params=None):
        with mc.connect(host=self.host, user=self.user, password=self.pswd, database=self.name) as connection:
            cursor = connection.cursor()
            cursor.execute(sql, params)
            return cursor.fetchall()
    
    def save(self, sql, params):
        with mc.connect(host=self.host, user=self.user, password=self.pswd, database=self.name) as connection:
            cursor = connection.cursor()
            cursor.execute(sql, params)
            cursor.close()
            connection.commit()


class SymbolsConnector(Connector):
    def __init__(self, config):
        super().__init__(config)

    def find_openingdays(self):
        sql = """
            SELECT DISTINCT
                opening_date
            FROM
                symbol_daily_info
        """
        rows = super().find(sql)
        return list(map(lambda row: {"opening_date": str(row[0])}, rows))

    def find_weekenddays(self):
        sql = """
            SELECT DISTINCT
                weekend_date
            FROM
                symbol_weekly_info
        """
        rows = super().find(sql)
        return list(map(lambda row: {"opening_date": str(row[0])}, rows))


    def find_symbols(self):
        sql = """
            SELECT
                code,
                exchange_code
            FROM
                symbols
        """
        rows = super().find(sql)
        return list(map(lambda row: {
            "symbol_code":           row[0],
            "opening_date":          None,
            "first_opening_price":   None,
            "first_high_price":      None,
            "first_low_price":       None,
            "first_closing_price":   None,
            "latter_opening_price":  None,
            "latter_high_price":     None,
            "latter_low_price":      None,
            "latter_closing_price":  None,
            "change_previous_close": None,
            "trading_volume":        None,
            "trading_value":         None,
            "vwap":                  None,
            "loaning_amount":        None,
            "paid_loaning_amount":   None,
            "loaning_balance":       None,
            "lending_amount":        None,
            "paid_lending_amount":   None,
            "lending_balance":       None,
            "credit_balance":        None,
            "exchange_code":         row[1]
        }, rows))

    def save_symbol(self, symbol):
        sql = """
            INSERT INTO symbol_daily_info (
                symbol_code,
                opening_date,
                first_opening_price,
                first_high_price,
                first_low_price,
                first_closing_price,
                latter_opening_price,
                latter_high_price,
                latter_low_price,
                latter_closing_price,
                change_previous_close,
                trading_volume,
                trading_value,
                vwap,
                loaning_amount,
                paid_loaning_amount,
                loaning_balance,
                lending_amount,
                paid_lending_amount,
                lending_balance,
                credit_balance
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            ) ON DUPLICATE KEY UPDATE
                first_opening_price = VALUES(first_opening_price),
                first_high_price = VALUES(first_high_price),
                first_low_price = VALUES(first_low_price),
                first_closing_price = VALUES(first_closing_price),
                latter_opening_price = VALUES(latter_opening_price),
                latter_high_price = VALUES(latter_high_price),
                latter_low_price = VALUES(latter_low_price),
                latter_closing_price = VALUES(latter_closing_price),
                change_previous_close = VALUES(change_previous_close),
                trading_volume = VALUES(trading_volume),
                trading_value = VALUES(trading_value),
                vwap = VALUES(vwap),
                loaning_amount = VALUES(loaning_amount),
                paid_loaning_amount = VALUES(paid_loaning_amount),
                loaning_balance = VALUES(loaning_balance),
                lending_amount = VALUES(lending_amount),
                paid_lending_amount = VALUES(paid_lending_amount),
                lending_balance = VALUES(lending_balance),
                credit_balance = VALUES(credit_balance)
        """
        super().save(sql, params=(
            symbol["symbol_code"],
            symbol["opening_date"],
            symbol["first_opening_price"],
            symbol["first_high_price"],
            symbol["first_low_price"],
            symbol["first_closing_price"],
            symbol["latter_opening_price"],
            symbol["latter_high_price"],
            symbol["latter_low_price"],
            symbol["latter_closing_price"],
            symbol["change_previous_close"],
            symbol["trading_volume"],
            symbol["trading_value"],
            symbol["vwap"],
            symbol["loaning_amount"],
            symbol["paid_loaning_amount"],
            symbol["loaning_balance"],
            symbol["lending_amount"],
            symbol["paid_lending_amount"],
            symbol["lending_balance"],
            symbol["credit_balance"]))

    def save_symbol_week(self, symbol):
        sql = """
            INSERT INTO symbol_weekly_info (
                symbol_code,
                weekend_date,
                sell_balance,
                sell_balance_per,
                buy_balance,
                buy_balance_per,
                sell_balance_general_credit,
                sell_balance_general_credit_per,
                sell_balance_system_credit,
                sell_balance_system_credit_per,
                buy_balance_general_credit,
                buy_balance_general_credit_per,
                buy_balance_system_credit,
                buy_balance_system_credit_per
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            ) ON DUPLICATE KEY UPDATE
                sell_balance = VALUES(sell_balance),
                sell_balance_per = VALUES(sell_balance_per),
                buy_balance = VALUES(buy_balance),
                buy_balance_per = VALUES(buy_balance_per),
                sell_balance_general_credit = VALUES(sell_balance_general_credit),
                sell_balance_general_credit_per = VALUES(sell_balance_general_credit_per),
                sell_balance_system_credit = VALUES(sell_balance_system_credit),
                sell_balance_system_credit_per = VALUES(sell_balance_system_credit_per),
                buy_balance_general_credit = VALUES(buy_balance_general_credit),
                buy_balance_general_credit_per = VALUES(buy_balance_general_credit_per),
                buy_balance_system_credit = VALUESbuy_balance_system_credit),
                buy_balance_system_credit_per = VALUES(buy_balance_system_credit_per)
        """
        super().save(sql, params=(
            symbol["symbol_code"],
            symbol["weekend_date"],
            symbol["sell_balance"],
            symbol["sell_balance_per"],
            symbol["buy_balance"],
            symbol["buy_balance_per"],
            symbol["sell_balance_general_credit"],
            symbol["sell_balance_general_credit_per"],
            symbol["sell_balance_system_credit"],
            symbol["sell_balance_system_credit_per"],
            symbol["buy_balance_general_credit"],
            symbol["buy_balance_general_credit_per"],
            symbol["buy_balance_system_credit"],
            symbol["buy_balance_system_credit_per"]
        ))
