from dlflow.features.feature_base import *


class LocalFeParser(FeParser):
    def __init__(self, *args, **kwargs):
        super(LocalFeParser, self).__init__(*args, **kwargs)

    def fit(self):
        raise NotImplementedError()

    def transform(self, *args, **kwargs):
        raise NotImplementedError()
