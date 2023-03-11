from configparser import ConfigParser
from . import resoucecollector

def main():
    config = ConfigParser()
    config.read("config.ini")

    clt = resoucecollector.Context(config)


if __name__ == '__main__':
    main()