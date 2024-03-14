import json


class App:
    __conf = {}

    @staticmethod
    def load(config_file):
        with open(config_file, 'r') as file:
            App.__conf = json.load(file)

    @staticmethod
    def config(name: str):
        return App.__conf[name]

    @staticmethod
    def all():
        return App.__conf
