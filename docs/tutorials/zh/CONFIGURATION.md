# 配置说明

## 基本配置

配置文件格式为hocon。hocon有着和json相类似格式，但是支持更灵活的格式、可以自由地添加注释甚至引用其他配置文件，更多关于hocon的介绍请参考 [HOCON](https://github.com/lightbend/config/blob/master/HOCON.md)。
DLFlow中顶级配置默认使用大些英文字母和下划线做为配置名。配置路径建议使用绝对路径，以免出现不必要的错误。HDFS路径允许使用非URI格式，用户自定义配置需要确保其访问的正确性。


如下是典型的配置：

```hocon
// Hocon for DLFlow

STEPS : "merge, encode, predict, evaluate, train, UDTs",

MODEL_TAG : "model tag",
MODEL_DATE : "20200101",
FEATURE_TAG : "feature tag",
FEATURE_DATE : "20200101",
PRIMARY_KEYS : "id",
LABELED_KEYS : "label",

MODELS_DIR : "/<path>/static_models",
TASKS_DIR : "/<path>/static_tasks",
LOCAL_WORKSPACE : "<path>/local_workspace",
HDFS_WORKSPACE : "/<hdfs_path>/hdfs_workspace",

SPARK : { include file("/<conf_path>/spark_conf/spark.conf") },
MODEL : { include file("/<conf_path>/model_conf/model.conf") },
BUCKET : { include file("/<conf_path>/bucket_conf/bucket.conf") },
MERGE : {
    config_file : "/<conf_path>/merge_conf/merge.conf",
    seed_sql : "/<conf_path>/merge_conf/seed.sql",
    fit : true,
}
```

### STEPS

指定DLFlow中需要执行的任务，框架内置5个预定义任务：

* merg - 特征融合
* encode / encoding - 特征编码
* train / training - 训练
* evaluate / evaluating / eval / test - 评估
* predict / predicting / pred - 预测

此外，框架允许用户自定义任务(UDT, User Define Task)，UDT的使用同样需要STEPS进行设置。STEPS任务通过逗号分割，任务顺序可以是无序的，框架会通过任务之间的依赖关系自动排列任务的执行顺序。

### MODEL_TAG

模型标签，用于和`MODEL_DATE`组成机器学习模型的唯一标识。

### MODEL_DATE

模型日期，用于和`MODEL_TAG`组成机器学习模型的唯一标识。

### FEATURE_TAG

特征标签，表示特征的一个版本。其与特征选择和特征的处理方式直接相关。

### FEATURE_DATE

特征日期，表示某个版本特征的不同分区。

### PRIMARY_KEYS

特征的主键。多个主键可用逗号分割。

### LABELED_KEYS

训练的标签。多个标签可用逗号分割。

### MODELS_DIR

用户自定义模型的存放目录。

### TASKS_DIR

用户自定义任务的存放目录。若没有可不指定。

### LOCAL_WORKSPACE

本地工作目录，建议设置，否则会使用默认值 `~/dlflow_workspace`。

### HDFS_WORKSPACE

HDFS工作目录，建议设置，否则会使用默认值 `~/dlflow_workspace`

### SPARK

Spark相关配置，使用Spark原生的配置名。例如：
```hocon
SPARK : {
    // 设置spark任务模式
    spark.submit.deployMode: "client",
    
    // 设置executor内存大小
    spark.executor.memory: "8G",
    
    ...
}
```

### MODEL

用户自定义模型配置。
```hocon
MODEL :  {
    model_name: "model_name",
    input_name: "input_name"
}
```

更多详细信息请参考 [模型开发](MODEL_DEV.md)

### BUCKET

用户自定义特征分桶配置。
```hocon
BUCKET : {
    <bucket_name> : {
        features: [
            "feature_name_1",
            "feature_name_2",
            ...
        ],
        is_encode: true,
        method: "pnorm",
        param: {"p": 1.0}
    },
    
    ...
}
```

 - &lt;bucket_name&gt; - 特征分桶名称 
 - features - 该域包含的特征 
 - is_encode - 该域是否进行编码和归一化处理 （默认为true）
 - method - 归一化方法（默认为minmax）
 - param - 归一化方法参数（默认值见下表）

| 归一化方法 | 参数和默认值 |
|   :---:  | :---: |
| minmax | "min": 0.0, "max": 1.0 |
| zscore | "withMean": true, "withStd": true |
| pnorm  | "p": 2.0 |
 
### MERGE

特征融合的相关配置。

```hocon
MERGE : {
    config_file : "/<conf_path>/merge_conf/merge.conf",
    seed_sql : "/<conf_path>/merge_conf/seed.sql",
    fit : true
}
```
 - config_file - 特征融合的配置文件 
 - seed_sql - 特征融合的seed
 - fit - 设置为true，则每次都会进行fit操作，false则会利用已经fit过的融合信息。

特征融合更为详细的信息请参考 [特征融合](../../../../dmflow/README.md)

## 自定义配置

由于用户自定义模型和自定义任务可能需要更多的配置项，因此DLFlow提供了用户自定义配置项的功能。

自定义配置首先需要导入配置模块
```python
from dlflow.mgr import config
```

**配置定义**

定义模型或任务时，添加类属性cfg并通过`config.setting()`方法设置配置项

```python
@model.reg("my_model")
class MyModel:
    
    cfg = config.setting(
        config.req("<CONF_NAME_1>"), 
        config.opt("<CONF_NAME_2>"),
        config.opt("<CONF_NAME_3>", "DEFAULT_VALUE")
    )
    
    ...
```

**配置级别**

设置配置项时需指定指定配置级别，DLFlow中的配置被分为三个级别：

 - system：框架运行的必要参数，用户在启动框架时必须提供参数值，该类型参数不允许引用其他参数
```python
config.setting(
    config.sys("CONF_NAME")
)
```

 - require：任务或模型运行必要的参数，该类型参数的值推荐用户通过配置文件或命令行参数提供
```python
config.setting(
    config.req("CONF_NAME")
)
```

 - option：可选参数，推荐在定义时提供默认值，用户无需在配置文件中提供值
```python
config.setting(
    config.opt("CONF_NAME", "DEFAULT_VALUE")
)
```

**配置引用**

```python
# 已存在的配置项 config.LOCAL_WORKSPACE
cfg = config.setting(
    config.opt("MY_CONF_DIR", "$LOCAL_WORKSPACE/my_conf_dir")
)
```

当配置生效后，引用的配置项`$LOCAL_WORKSPACE`会自动被替换成相应等值
```python
print(config.LOCAL_WORKSPACE)
# /local_dir

# "$LOCAL_WORKSPACE/my_conf_dir"
print(config.MY_CONF_DIR)
# /local_dir/my_conf_dir
```

## 配置使用

在导入配置后，通过 `config.<CONF_KEY>` 或 `config[<CONF_KEY>]` 进行调用，多级配置使用方式相同。
```python
from dlflow.mgr import config


print(config.MODEL_TAG)
# "model tag"

print(config.SPARK.spark.submit.deployMode)
# "client"

print(config["SPARK.spark.submit.deployMode"])
# "client"

print(config.MODEL.batch_size)
# 128
```
