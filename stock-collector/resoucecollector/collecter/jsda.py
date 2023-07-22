import os
import re
from datetime import datetime, timedelta
from urllib.request import urlopen
from urllib.error import HTTPError
import pandas as pd

class JSDACollector(object):
    def __init__(
            self,
            xlsname_template,
            dire_path,
            exclude_days_filename):
        self.dire_path: str = dire_path
        self.xlsname_template: str = xlsname_template

        # 除外日リストを取得する
        f = open(exclude_days_filename, mode="r")
        exclude_days = [datetime.strptime(l.replace("\n", ""), "%Y-%m-%d") for l in f.readlines()]
        f.close

        # 次の対象営業日を取得する
        self.target_day = datetime.strptime(sorted(self.get_pasts(), reverse=True)[0], "%Y%m%d") + timedelta(days=7)
        while self.target_day in exclude_days:
            self.target_day = self.target_day - timedelta(days=1)

        self.xlsname = self.xlsname_template.format(self.get_target())
        self.url = "https://www.jsda.or.jp/shiryoshitsu/toukei/kabu-taiw/files/" + self.xlsname

    # 過去のファイル群から過去日リストを取得する
    def get_pasts(self):
        filepattern = re.compile("([0-9]{8})[sz]{1}\.xls")
        files = os.listdir(path=self.dire_path)
        return [filepattern.search(f).group(1) for f in files]
    
    def get_target(self):
        return self.target_day.strftime("%Y%m%d")

    def download(self):
        try:
            source = urlopen(self.url, timeout=3).read()
        except HTTPError as e:
            print("Failed to get jsda data.")
            return

        dist = self.dire_path + self.xlsname
        with open(dist, mode="wb") as f:
            f.write(source)

        if 1000 > os.path.getsize(dist):
            print("Failed to get jsda data.")
            os.remove(dist)
            return

    def read(self, target_day: str) -> dict:
        book = pd.ExcelFile(self.dire_path + self.xlsname_template.format(target_day))
        df = book.parse(book.sheet_names[0], skiprows=6, usecols=[1, 3, 5, 7, 9, 11, 13])
        df.columns = [
            "symbol_code",          # 銘柄コード
            "lend_qty",             # 貸付数量
            "lend_value",           # 貸付金額
            "borrow_self_qty",      # 自己借入数量
            "borrow_self_value",    # 自己借入金額
            "borrow_sublease_qty",  # 転貸借入数量
            "borrow_sublease_value" # 転貸借入金額
        ]
        # 担保有無で分割されているため集計する
        # 銘柄コードから下一桁の予備コードを削除する
        df = df.groupby("symbol_code").sum()
        df.reset_index(inplace=True)
        df["symbol_code"] = df["symbol_code"] / 10
        df["symbol_code"] = df["symbol_code"].astype("int")
        return {str(data[0]): data for data in df.values.tolist()}

class JSDANewDealCollector(JSDACollector):
    def __init__(self, config: dict):
        super().__init__(
            xlsname_template="{}s.xls",
            dire_path=config["jsda_newdeal_dire_path"],
            exclude_days_filename=config["exclude_days_filename"])

class JSDABalanceCollector(JSDACollector):
    def __init__(self, config: dict):
        super().__init__(
            xlsname_template="{}z.xls",
            dire_path=config["jsda_balance_dire_path"],
            exclude_days_filename=config["exclude_days_filename"])
