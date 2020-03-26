from dlflow.mgr import model, config
from dlflow.model import InputBase


@model.reg("input register name")
class DemoInput(InputBase):

    cfg = config.setting(
        config.opt("DemoParam", "DemoDefaultValue")
    )

    def __init__(self, fmap):
        super(DemoInput, self).__init__(fmap)

    def tfr_inputs(self, files):
        ...

    def rdd_inputs(self, rdd):
        ...
