from dlflow.mgr import model, config
from dlflow.models import InputBase

from dlflow.features import PRESET_BUCKETS
from collections import OrderedDict

import tensorflow as tf


@model.reg("NNDenseInput")
class NNDenseInput(InputBase):

    cfg = config.setting(
        config.opt("MODEL.epochs", None),
        config.opt("MODEL.batch_size", 1),
        config.opt("MODEL.parallel", 4),
        config.opt("MODEL.shuffle_size", None),
        config.opt("MODEL.drop_remainder", False),
        config.opt("MODEL.buffer_size", None)
    )

    def __init__(self, fmap):
        super(NNDenseInput, self).__init__(fmap)

    def tfr_inputs(self, files):
        feature_dict = OrderedDict()

        for fe in self.fmap.primary_keys.get_features():
            feature_dict[fe.name] = self._TF_FEATURE[fe.fetype]([1])

        for fe in self.fmap.labels.get_features():
            feature_dict[fe.name] = self._TF_FEATURE[fe.fetype]([1])

        buckets = self.fmap.get_buckets(drop=PRESET_BUCKETS)
        for bucket in buckets:
            nums_size = bucket.nums.fe_size
            ctgs_size = bucket.ctgs.fe_count

            if nums_size > 0:
                name = "_".join([bucket.name, "nums"])
                feature_dict[name] = self._float_feature([nums_size])

            if ctgs_size > 0:
                name = "_".join([bucket.name, "ctgs"])
                feature_dict[name] = self._int_feature([ctgs_size])

        def _parse_single_example(example):
            feature = tf.io.parse_single_example(example, feature_dict)

            return feature

        parallel = config.MODEL.parallel
        dataset = tf.data \
                    .TFRecordDataset(filenames=files,
                                     buffer_size=config.MODEL.buffer_size,
                                     num_parallel_reads=parallel) \
                    .map(map_func=_parse_single_example,
                         num_parallel_calls=parallel) \
                    .batch(batch_size=config.MODEL.batch_size,
                           drop_remainder=config.MODEL.drop_remainder) \
                    .repeat(count=config.MODEL.epochs)

        if config.MODEL.shuffle_size:
            dataset = dataset.shuffle(config.MODEL.shuffle_size)

        return dataset

    def rdd_inputs(self, rdd, batch_size):
        primary_keys = []
        features = []

        out_dtype = []
        out_shape = []

        for fe in self.fmap.primary_keys.get_features():
            primary_keys.append(fe.name)
            out_dtype.append(self._TF_TYPE[fe.fetype])
            out_shape.append(tf.TensorShape([fe.size]))

        buckets = self.fmap.get_buckets(drop=PRESET_BUCKETS)
        for bucket in buckets:
            nums_size = bucket.nums.fe_size
            ctgs_size = bucket.ctgs.fe_count

            if nums_size > 0:
                name = "_".join([bucket.name, "nums"])
                features.append(name)
                out_dtype.append(tf.float32)
                out_shape.append(tf.TensorShape(nums_size))

            if ctgs_size > 0:
                name = "_".join([bucket.name, "ctgs"])
                features.append(name)
                out_dtype.append(tf.int64)
                out_shape.append(tf.TensorShape(ctgs_size))

        def rdd_generator():
            for row in rdd:
                row_data = []

                for k in primary_keys:
                    row_data.append([row[k]])

                for k in features:
                    row_data.append(list(row[k]))

                yield tuple(row_data)

        dataset = tf.data.Dataset \
                         .from_generator(generator=rdd_generator,
                                         output_shapes=tuple(out_shape),
                                         output_types=tuple(out_dtype)) \
                         .batch(batch_size, drop_remainder=False)

        return dataset
