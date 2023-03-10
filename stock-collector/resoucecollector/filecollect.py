import os, re
import zipfile
import csv
from datetime import datetime, timedelta
from urllib.request import urlopen
from urllib.error import HTTPError

class Collector(object):
    def __init__(self, dire_path, filepattern, exclude_days_filename):
        self.dire_path = dire_path
        self.filepattern = re.compile(filepattern)

        # 除外日リストを取得する
        f = open(exclude_days_filename, mode="r")
        exclude_days = list(map(lambda d: datetime.strptime(d, "%Y-%m-%d"), list(map(lambda l: l.replace("\n", ""), f.readlines()))))
        f.close

        # 次の対象営業日を取得する
        self.target_day = datetime.strptime(sorted(self.get_pasts(), reverse=True)[0], "%Y%m%d") + timedelta(days=1)
        while not(not self.target_day in exclude_days and not self.target_day.strftime("%a") in ["Sun", "Sat"]):
            self.target_day = self.target_day + timedelta(days=1)
    
    # 過去のファイル群から過去日リストを取得する
    def get_pasts(self):
        filenames = os.listdir(path=self.dire_path)
        return list(map(lambda f: self.filepattern.search(f).group(1), filenames))
    
    def get_target(self):
        return self.target_day.strftime("%Y%m%d")


class TaisyakuCollector(Collector):
    def __init__(self, config):
        self.csvnametemplate = "zandaka{}.csv"
        super().__init__(
            dire_path=config["taisyaku_dire_path"],
            filepattern="zandaka([0-9]{8}).csv",
            exclude_days_filename=config["exclude_days_filename"])
        self.url = "https://www.taisyaku.jp/search_admin/comp/balance/" + self.csvnametemplate.format(self.get_target())

    def download(self):
        try:
            source = urlopen(self.url, timeout=3).read()
        except HTTPError as e:
            print("Failed to get taisyaku data.")
            return
        
        dist = self.dire_path + self.csvnametemplate.format(self.get_target())
        with open(dist, mode="w") as f:
            f.write(source.decode("shift_jis"))
        
        if 1000 > os.path.getsize(dist):
            print("Failed to get taisyaku data.")
            os.remove(dist)
            return

    def read(self, target_day):
        with open(self.dire_path + self.csvnametemplate.format(target_day)) as f:
            rows = csv.reader(f)
            rows = list(filter(lambda row: re.compile("^[0-9]{4}$").match(row[1]), rows))
            rows = list(filter(lambda row: "1" == self.name_to_code(row[3]), rows))
            return list(map(lambda row: {
                "symbol_code":         row[1],
                "exchange_code":       self.name_to_code(row[3]),
                "loaning_amount":      int(row[4]),
                "paid_loaning_amount": int(row[5]),
                "loaning_balance":     int(row[6]),
                "lending_amount":      int(row[7]),
                "paid_lending_amount": int(row[8]),
                "lending_balance":     int(row[9]),
                "credit_balance":      int(row[10])
            }, rows))

    def name_to_code(self, exchange_name):
        if "東証およびＰＴＳ" == exchange_name:
            return "1"
        elif "名証" == exchange_name:
            return "3"
        elif "福証" == exchange_name:
            return "5"
        elif "礼証" == exchange_name:
            return "6"
        else:
            return "10"


class SofthompoCollector(Collector):
    def __init__(self, config):
        self.csvnametemplate = "stq_{}.csv"
        self.zipnametemplate = "stq_{}.zip"
        super().__init__(
            dire_path=config["softhompo_kabuka_dire_path"],
            filepattern="stq_([0-9]{8}).csv",
            exclude_days_filename=config["exclude_days_filename"])
        self.url = "http://softhompo.a.la9.jp/Data/stock/thisMonth/" + self.zipnametemplate.format(self.get_target())
 
    def download(self):
        try:
            source = urlopen(self.url, timeout=3).read()
        except HTTPError as e:
            print("Failed to get softhomp kabuka data.")
            return

        dist = self.dire_path + self.zipnametemplate.format(self.get_target())
        with open(dist, mode="wb") as f:
            f.write(source)

        if 1000 > os.path.getsize(dist):
            print("Failed to get softhomp kabuka data.")
            os.remove(dist)
            return

        with zipfile.ZipFile(dist) as inputFile:
            inputFile.extractall(self.dire_path)
            os.remove(dist)

    def read(self, target_day):
        with open(self.dire_path + self.csvnametemplate.format(target_day)) as f:
            rows = csv.reader(f)
            rows = list(filter(lambda row: re.compile("^[0-9]{4}$").match(row[0]), rows))
            return list(map(lambda row: {
                "symbol_code":           row[0],
                "first_opening_price":   None if "－" == row[4] else float(row[4]),
                "first_high_price":      None if "－" == row[5] else float(row[5]),
                "first_low_price":       None if "－" == row[6] else float(row[6]),
                "first_closing_price":   None if "－" == row[7] else float(row[7]),
                "latter_opening_price":  None if "－" == row[8] else float(row[8]),
                "latter_high_price":     None if "－" == row[9] else float(row[9]),
                "latter_low_price":      None if "－" == row[10] else float(row[10]),
                "latter_closing_price":  None if "－" == row[11] else float(row[11]),
                "change_previous_close": None if "－" == row[13] else float(row[13]),
                "trading_volume":        None if "－" == row[15] else float(row[15]),
                "trading_value":         None if "－" == row[16] else float(row[16]),
                "vwap":                  None if "－" == row[14] else float(row[14]) 
            }, rows))


class SofthompoShinyoCollector(object):
    def __init__(self, config):
        self.dire_path = config["softhompo_shinyo_dire_path"]
        self.filepattern = re.compile("syumatsu([0-9]{8})00.csv")

        # 除外日リストを取得する
        f = open(config["exclude_days_filename"], mode="r")
        exclude_days = list(map(lambda d: datetime.strptime(d, "%Y-%m-%d"), list(map(lambda l: l.replace("\n", ""), f.readlines()))))
        f.close

        # 次の対象営業日を取得する
        self.target_day = datetime.strptime(sorted(self.get_pasts(), reverse=True)[0], "%Y%m%d") + timedelta(days=7)
        while self.target_day in exclude_days:
            self.target_day = self.target_day - timedelta(days=1)

        self.zipname = "syumatsu{}00.zip".format(self.get_target())
        self.url = "http://softhompo.a.la9.jp/Data/margin/thisMonth/" + self.zipname

    # 過去のファイル群から過去日リストを取得する
    def get_pasts(self):
        files = os.listdir(path=self.dire_path)
        return list(map(lambda f: self.filepattern.search(f).group(1), files))

    def get_target(self):
        return self.target_day.strftime("%Y%m%d")

    def download(self):
        try:
            source = urlopen(self.url, timeout=3).read()
        except HTTPError as e:
            print("Failed to get softhomp shinyo data.")
            return

        dist = self.dire_path + self.zipname
        with open(dist, mode="wb") as f:
            f.write(source)

        if 1000 > os.path.getsize(dist):
            print("Failed to get softhomp shinyo data.")
            os.remove(dist)
            return

        with zipfile.ZipFile(dist) as inputFile:
            inputFile.extractall(self.dire_path)
            os.remove(dist)
