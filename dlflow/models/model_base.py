from dlflow.features import PRESET_BUCKETS, PKEY_NAME, LABEL_NAME

from pathlib import Path
from collections import OrderedDict
from absl import logging
import tensorflow as tf
import numpy as np
import abc


class ModelBase(metaclass=abc.ABCMeta):

    def __init__(self, fmap):
        super(ModelBase, self).__init__()

        self.metrics = {}
        self.msg_frac = 100

        self._model = None
        self.pkey_names = []
        self.label_names = []
        self.feature_names = []
        self.output_names = []

        self.inputs = OrderedDict()
        self.outputs = OrderedDict()

        self._build(fmap)

    @property
    def model(self):
        return self._model

    @abc.abstractmethod
    def build(self, *args, **kwargs):
        pass

    def train(self, feature, label):
        pass

    def predict(self, feature):
        pass

    def evaluate(self, feature, label):
        pass

    def train_act(self, dataset):
        step = 0
        for data in dataset:
            step += 1

            _, label, feature = self.unpack_data(data)
            self.train(feature, label)

            if step % self.msg_frac == 0:
                self.show_metrics(step)

        self.show_metrics(step)

    def predict_act(self, dataset):

        len_pkey = len(self.pkey_names)
        len_output = len(self.output_names)

        pred_pkeys = [[] for _ in range(len_pkey)]
        pred_outputs = [[] for _ in range(len_output)]

        for _data in dataset:
            data = list(_data)
            pkey = data[:len_pkey]
            feature = data[len_pkey:]

            if len(feature) == 1:
                feature = feature[0]

            outputs = self.predict(feature)
            if isinstance(outputs, tf.Tensor):
                outputs = [outputs]

            for l, k in zip(pred_pkeys, pkey):
                if k.shape[-1] == 1:
                    k_np = k.numpy().squeeze(axis=len(k.shape) - 1)
                else:
                    k_np = k.numpy()
                l.append(k_np)

            for l, p in zip(pred_outputs, outputs):
                if p.shape[-1] == 1:
                    p_np = p.numpy().squeeze(axis=len(p.shape) - 1)
                else:
                    p_np = p.numpy()
                l.append(p_np)

        np_list = []
        for i in pred_pkeys:
            _keys = np.concatenate(i, axis=0).tolist()
            if isinstance(_keys[0], bytes):
                _keys = [e.decode("utf-8") for e in _keys]

            np_list.append(_keys)

        for i in pred_outputs:
            np_list.append(np.concatenate(i, axis=0).tolist())

        res = []
        for item in zip(*np_list):
            res.append([i for i in item])

        return res

    def evaluate_act(self, dataset):
        step = 0
        for data in dataset:
            step += 1

            _, label, feature = self.unpack_data(data)
            self.evaluate(feature, label)

            if step % self.msg_frac == 0:
                self.show_metrics(step)

        self.show_metrics(step)

    def _build(self, fmap):
        self.build_inputs(fmap)
        inputs = self.get_inputs()

        self.build()
        outputs = self.get_outputs()

        self._model = tf.keras.Model(inputs=inputs, outputs=outputs)

    def save(self, save_dir):
        save_dir = Path(save_dir)
        h5dir = save_dir.joinpath("h5weights")
        if not h5dir.is_dir():
            h5dir.mkdir(parents=True)

        self._model.save(save_dir.as_posix())
        self._model.save_weights(h5dir.joinpath("weights.h5").as_posix())

    def load_model(self, load_dir):
        load_dir = Path(load_dir)
        self._model = tf.keras.models.load_model(load_dir.as_posix())

    def load_weights(self, load_dir):
        weight_path = Path(load_dir)

        if weight_path.suffix == ".h5":
            self._model.load_weights(weight_path.as_posix())

        else:
            self._model.load_weights(
                weight_path.joinpath("h5weights", "weights.h5").as_posix())

    def unpack_data(self, data):
        pkey = [data[n] for n in self.pkey_names]
        label = {n: data[n] for n in self.label_names}
        feature = [data[n] for n in self.feature_names]

        return pkey, label, feature

    def build_inputs(self, fmap):
        for fe in fmap[PKEY_NAME].get_features():
            self.pkey_names.append(fe.name)

        for fe in fmap[LABEL_NAME].get_features():
            self.label_names.append(fe.name)

        buckets = fmap.get_buckets(drop=PRESET_BUCKETS)
        for bucket in buckets:
            nums_size = bucket.nums.fe_size
            ctgs_size = bucket.ctgs.fe_count

            self.inputs[bucket.name] = OrderedDict()

            if nums_size > 0:
                num_name = "_".join([bucket.name, "nums"])
                num_input = tf.keras.Input(shape=nums_size,
                                           dtype=tf.float32,
                                           name=num_name)
                self.feature_names.append(num_name)
                info = {
                    "input": num_input,
                    "depth": nums_size
                }
                self.inputs[bucket.name]["nums"] = info

            if ctgs_size > 0:
                ctgs_name = "_".join([bucket.name, "ctgs"])
                ctg_input = tf.keras.Input(shape=ctgs_size,
                                           dtype=tf.int64,
                                           name=ctgs_name)

                emb_depth = bucket.ctgs.fe_size
                self.feature_names.append(ctgs_name)
                info = {
                    "input": ctg_input,
                    "depth": emb_depth
                }
                self.inputs[bucket.name]["ctgs"] = info

    def get_inputs(self, tp=None, with_depth=False):
        inputs = []

        if tp is None:
            for field_inputs in self.inputs.values():
                for _field in field_inputs.values():

                    if with_depth:
                        _item = (_field["input"], _field["depth"])
                    else:
                        _item = _field["input"]

                    inputs.append(_item)

        else:
            for field_inputs in self.inputs.values():
                if tp not in field_inputs:
                    continue

                _field = field_inputs[tp]

                if with_depth:
                    _item = (_field["input"], _field["depth"])
                else:
                    _item = _field["input"]

                inputs.append(_item)

        return inputs

    def get_outputs(self):
        outputs = []
        for _, output in self.outputs.items():
            outputs.append(output)

        if len(outputs) == 1:
            outputs = [outputs]

        return outputs

    def show_metrics(self, step):
        msg = "Step: {}".format(step)
        for name, metric in self.metrics.items():
            msg += ", {}: {:>.4f}".format(name, metric.result())

        logging.info(msg)

    def set_output(self, output, name):
        self.outputs[name] = output
        self.output_names.append(name)
