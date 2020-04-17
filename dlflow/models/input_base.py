import abc
import tensorflow as tf


class InputBase(metaclass=abc.ABCMeta):

    @staticmethod
    def _int_feature(shape=[], default_value=None):
        return tf.io.FixedLenFeature(shape,
                                     tf.int64,
                                     default_value=default_value)

    @staticmethod
    def _float_feature(shape=[], default_value=None):
        return tf.io.FixedLenFeature(shape,
                                     tf.float32,
                                     default_value=default_value)

    @staticmethod
    def _string_feature(shape=[], default_value=None):
        return tf.io.FixedLenFeature(shape,
                                     tf.string,
                                     default_value=default_value)

    def __init__(self, fmap):
        self._fmap = fmap

        self._TF_FEATURE = {
            "FeInt": self._int_feature,
            "FeFloat": self._float_feature,
            "FeStr": self._string_feature,
        }

        self._TF_TYPE = {
            "FeInt": tf.int64,
            "FeFloat": tf.float32,
            "FeStr": tf.string
        }

    @property
    def fmap(self):
        return self._fmap

    @abc.abstractmethod
    def tfr_inputs(self, files):
        pass

    @abc.abstractmethod
    def rdd_inputs(self, rdd, batch_size):
        pass
