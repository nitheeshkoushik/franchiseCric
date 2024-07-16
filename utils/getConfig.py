import configparser

class GetConfig:
    def __init__(self, file = 'pipeline.config'):
        self.file = file
        self.parser = configparser.ConfigParser()
        self.parser.read(self.file)

    def getConfig(self, section, option):
        result = self.parser.get(section, option)
        return result