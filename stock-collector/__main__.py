from configparser import ConfigParser
from . import resoucecollector

def main():
    config = ConfigParser()
    config.read("config.ini")

    clt = resoucecollector.Context(config)
    clt.taisyaku.download()
    clt.softhompo.download()
    clt.softhompoShinyo.download()


if __name__ == '__main__':
    main()
