# Copyright (C) 2017 Beijing Didi Infinity Technology and Development Co.,Ltd.
# All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================


from dlflow.utils.logger import logging_initialize
from dlflow.engine import Engine
from dlflow.mgr import MOTW

from absl import flags
from absl import app


FLAGS = flags.FLAGS


def _main(*args, **kwargs):
    if not FLAGS.config:
        raise ValueError("Parameter '--config' must be set.")

    engine = Engine()
    engine.initialize(file=FLAGS.config,
                      udc=FLAGS.conf,
                      log_level=FLAGS.log_level,
                      log_dir=FLAGS.log_dir,
                      lang=FLAGS.lang,
                      mode="local")
    engine.run()


def run_app():
    print(MOTW)
    logging_initialize(log_level="info")

    flags.DEFINE_string("config", None, "* <Require> Path for config file.")
    flags.DEFINE_string("log_level", None, "<Option> Changing log level.")
    flags.DEFINE_string("lang", None, "<Option> Changing log language.")
    flags.DEFINE_multi_string("conf",
                              None,
                              "<Option> User Define Runtime Parameters.")

    from dlflow.utils.check import env_version_check
    env_version_check()
    app.run(_main)


if __name__ == "__main__":
    run_app()
