from dlflow.mgr import model, config
from dlflow.model import ModelBase


@model.reg("model register name")
class DemoModel(ModelBase):

    cfg = config.setting(
        config.opt("DemoParam", "DemoDefaultValue")
    )

    def __init__(self, fmap):
        super(DemoModel, self).__init__(fmap)

    def build(self):
        ...

    def train(self, feature, label):
        ...

    def evaluate(self, feature, label):
        ...

    def predict(self, feature):
        ...
