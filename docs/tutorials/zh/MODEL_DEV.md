# 模型开发

DLFlow中模型主要由两部分组成，模型本身和模型输入。模型为tensorflow模型，模型输入为tfrecords，对于分布式预测，则需单独开发针对RDD的输入。

模型和输入都需要接收fmap作为输入，其构建与fmap直接相关，关于Fmap请参见 [Fmap说明](FMAP.md)。

## 模型

构建模型需要导入如下两个模块
```python
from dlflow.mgr import model
from dlflow.model import ModelBase
```

用户开发的模型类应继承`ModelBase`，并实现 `build()` `train()` `evaluate()` `predict()` 四个方法。

```python
from typing import List, Tuple
import tensorflow as tf


@model.reg("name_1", "name_2", ...)
class MyModel(ModelBase):

    def __init__(self, fmap):
        super(MyModel, self).__init__(fmap)
        
    def build(self) -> tf.keras.Model:
        # 构建模型
        ...
        
    def train(self, feature: List[tf.Tensor], label: List[tf.Tensor]):
        # 训练一个step
        ...
        
    def evaluate(self, feature: List[tf.Tensor], label: List[tf.Tensor]):
        # 评估一个step
        ...
        
    def predict(self, feature: List[tf.Tensor]) -> Tuple[tf.Tensor]:
        # 预测一个step
        ...
```

模型开发完成需要利用装饰器 `@model.reg()` 对模型进行注册，否则DLFlow将无法读入模型。

模型注册的名字允许有多个，注册名称将是用户访问模型的Key。

## 输入

构建输入需要导入如下模块
```python
from dlflow.mgr import model
from dlflow.model import InputBase
```

输入类应继承`InputBase`，主要实现两个方法 `tfr_inputs()`  `rdd_inputs()`。
```python
import tensorflow as tf


@model.reg("name_1", "name_2", ...)
class MyInput(InputBase):

    def __init__(self, fmap):
        super(MyInput, self).__init__(fmap)
        
    def tfr_inputs(self, files: List[str]) -> tf.data.Dataset:
        # 读取tfrecords Dataset
        ...
        
    def rdd_inputs(self, rdd) -> tf.data.Dataset:
        # 读取RDD转成Dataset
        ...
```

输入开发同样需要利用装饰器 `@model.reg()` 进行注册，其用法与模型注册完全相同。

## 配置
模型和输入都可以使用 `dlflow.mgr.config` 的全部功能，使用方式请参 [配置说明](CONFIGURATION.md)。

