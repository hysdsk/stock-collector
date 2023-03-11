from . import collect

class Context(object):
    def __init__(self, config):
        self.taisyaku = collect.TaisyakuCollector(config=config["resources"])
        self.taisyaku.download()

        self.softhompo = collect.SofthompoCollector(config=config["resources"])
        self.softhompo.download()