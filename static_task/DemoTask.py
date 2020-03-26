from dlflow.tasks import TaskNode
from dlflow.mgr import task, config

from absl import logging


@task.reg("model register name")
class DemoTask(TaskNode):

    parent_tag = TaskNode.set_tag("PARENT_TAG")
    output_tag = TaskNode.set_tag("OUTPUT_TAG")

    bind_tasks = "task name or list of tasks"

    cfg = config.setting(
        config.req("DemoParam")
    )

    def __init__(self):
        super(DemoTask, self).__init__()

    @TaskNode.timeit
    def run(self):
        logging.info("Running {}".format(self.__class__.__name__))
