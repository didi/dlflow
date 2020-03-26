# 任务开发

## 任务

构建模型需要导入如下两个模块
```python
from dlflow.tasks import TaskNode
from dlflow.mgr import task
```

用户开发的任务类应继承`TaskNode`，并实现 `run()` 方法。

```python
from dlflow.mgr import config


@task.reg("my_task_name", ...)
class MyTask(TaskNode):
    parent_tags = ("p_tag_1", "p_tag_2", ...)
    output_tags = ("o_tag_1", "o_tag_2", ...) 

    cfg = config.setting(
        config.req("MODEL.model_name"),
        config.req("NEW_LOCAL_WORKSPACE", "$LOCAL_WORKSPACE/my_dir")
    )

    def __init__(self):
        super(self.__class__, self).__init__()
        
    @TaskNode.timeit
    def run(self):
        # 任务代码
        ...
```

任务类通过装饰器 `@task.reg()` 进行注册，从而DFLlow可以正确识别任务。使用任务只需根据注册名设置相应的 `STEPS` 即可，例如使用上述任务：
```hocon
STEPS : "enocde, train, my_task_name"
```

## 任务依赖

任务依赖通过任务类属性 `parent_tags` 进行设置，任务执行成功后，同样需要产出相应的标签，通过类属性 `output_tags` 进行设置。框架会根据任务的依赖和产出标记自动构建任务的工作流。

## 配置

任务可以使用 `dlflow.mgr.config` 的全部功能，使用方式请参考 [配置说明](CONFIGURATION.md)。

