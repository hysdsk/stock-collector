from configparser import ConfigParser
from . import resoucecollector

def main():
    config = ConfigParser()
    config.read("config.ini")
    clt = resoucecollector.Context(config)
    clt.download()
    clt.daily_collect()
    clt.weekly_collect()

if __name__ == '__main__':
    main()
