from dlflow.utils.locale import i18n

import tensorflow
import sys


def env_version_check():
    v_major = sys.version_info[0]
    v_minor = sys.version_info[1]
    assert (v_major == 3 and v_minor >= 6) or v_major > 3, \
        i18n("This program requires at least Python 3.6")

    assert tensorflow.__version__.startswith("2."), \
        i18n("This program require Tensorflow2.0")
