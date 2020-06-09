from dlflow.mgr import model, config
from dlflow.models import ModelBase

import tensorflow as tf


class _Embedding(tf.keras.layers.Layer):
    def __init__(self, input_dim, output_dim):
        super(_Embedding, self).__init__()
        self.input_dim = input_dim
        self.output_dim = output_dim

    def build(self, input_shape):
        self.embedding = self.add_weight(name="emb_w",
                                         shape=[self.input_dim,
                                                self.output_dim],
                                         initializer='uniform')

    def call(self, inputs, **kwargs):
        emb = tf.nn.embedding_lookup(self.embedding, inputs)
        out_dim = inputs.shape[-1] * self.output_dim
        return tf.reshape(emb, [-1, out_dim])


@model.reg("DNNRegression")
class DNNRegression(ModelBase):

    cfg = config.setting(
        config.req("MODEL.learning_rate"),
        config.req("MODEL.layers"),

        config.opt("MODEL.batch_size", 128)
    )

    def __init__(self, fmap):
        super(DNNRegression, self).__init__(fmap)

        self.optimizer = tf.keras.optimizers.Adam(
            learning_rate=config.MODEL.learning_rate)
        self.compute_loss = tf.keras.losses.MeanSquaredError()

        self.mean_loss = tf.keras.metrics.Mean()

        self.metrics = {
            "mean_loss": self.mean_loss,
        }

    def build(self):

        concat_list = self.get_inputs(tp="nums")
        for ctg_inp, depth in self.get_inputs(tp="ctgs", with_depth=True):
            _emb = _Embedding(depth, 4)(ctg_inp)
            concat_list.append(_emb)

        net = tf.concat(concat_list, axis=1)

        for size in config.MODEL.layers:
            net = tf.keras.layers.Dense(size, activation=tf.nn.relu)(net)

        output = tf.keras.layers.Dense(1)(net)
        sigmoid = tf.nn.sigmoid(output)

        self.set_output(output, "output")
        self.set_output(sigmoid, "sigmoid")

    @tf.function
    def train(self, feature, label):
        _label = label["label"]

        with tf.GradientTape() as tape:
            logits, _ = self.model(feature)
            loss = self.compute_loss(_label, logits)

        grads = tape.gradient(loss, self.model.trainable_variables)
        self.optimizer.apply_gradients(
            zip(grads, self.model.trainable_variables))

        self.mean_loss(loss)

    @tf.function
    def evaluate(self, feature, label):
        _label = label["label"]

        logits, _ = self.model(feature)
        loss = self.compute_loss(_label, logits)
        self.mean_loss(loss)

    @tf.function
    def predict(self, feature):
        pred = self.model(feature)
        return pred
