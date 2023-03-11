import os, re
import zipfile
from datetime import datetime, timedelta
from urllib.request import urlopen
from urllib.error import HTTPError

class Collector(object):
    def __init__(self, dire_path, filepattern, exclude_days_filename):
        # 過去のファイル群から過去日リストを取得する
        self.dire_path = dire_path
        files = os.listdir(path=dire_path)
        pattern = re.compile(filepattern)
        pasts = list(map(lambda f: pattern.search(f).group(1), files))

        # 除外日リストを取得する
        f = open(exclude_days_filename, mode="r")
        exclude_days = list(map(lambda d: datetime.strptime(d, "%Y-%m-%d"), list(map(lambda l: l.replace("\n", ""), f.readlines()))))
        f.close

        # 次の対象営業日を取得する
        self.target_day = datetime.strptime(sorted(pasts, reverse=True)[0], "%Y%m%d") + timedelta(days=1)
        while not(not self.target_day in exclude_days and not self.target_day.strftime("%a") in ["Sun", "Sat"]):
            self.target_day = self.target_day + timedelta(days=1)
    
    def get_target(self):
        return self.target_day.strftime("%Y%m%d")


class TaisyakuCollector(Collector):
    def __init__(self, config):
        super().__init__(
            dire_path=config["taisyaku_dire_path"],
            filepattern="zandaka([0-9]{8}).csv",
            exclude_days_filename=config["exclude_days_filename"])
        
        self.csvname = "zandaka{}.csv".format(self.get_target())
        self.url = "https://www.taisyaku.jp/search_admin/comp/balance/" + self.csvname

    def download(self):
        try:
            source = urlopen(self.url, timeout=3).read()
        except HTTPError as e:
            print("Failed to get taisyaku data.")
            return
        
        dist = self.dire_path + self.csvname
        with open(dist, mode="w") as f:
            f.write(source.decode("shift_jis"))
        
        if 1000 > os.path.getsize(dist):
            print("Failed to get taisyaku data.")
            os.remove(dist)
            return


class SofthompoCollector(Collector):
    def __init__(self, config):
        super().__init__(
            dire_path=config["softhompo_kabuka_dire_path"],
            filepattern="stq_([0-9]{8}).csv",
            exclude_days_filename=config["exclude_days_filename"])
        
        self.zipname = "stq_{}.zip".format(self.get_target())
        self.url = "http://softhompo.a.la9.jp/Data/stock/thisMonth/" + self.zipname
    
    def download(self):
        try:
            source = urlopen(self.url, timeout=3).read()
        except HTTPError as e:
            print("Failed to get softhomp data.")
            return

        dist = self.dire_path + self.zipname
        with open(dist, mode="wb") as f:
            f.write(source)

        if 1000 > os.path.getsize(dist):
            print("Failed to get softhomp data.")
            os.remove(dist)
            return

        with zipfile.ZipFile(dist) as inputFile:
            inputFile.extractall(self.dire_path)
            os.remove(dist)
        