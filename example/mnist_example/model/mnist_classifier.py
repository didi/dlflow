from dlflow.mgr import model, config
from dlflow.models import ModelBase

import tensorflow as tf


from model.model import CNN


@model.reg("mnist_cnn")
class MyModel(ModelBase):

    cfg = config.setting(
        config.req("MODEL.learning_rate"),
        config.req("MODEL.classes"),
        config.req("MODEL.layers"),
        config.opt("MODEL.batch_size", 8)
    )

    def __init__(self, fmap):
        super(MyModel, self).__init__(fmap)

        self.optimizer = tf.keras.optimizers.Adam(learning_rate=0.001)
        self.compute_loss = tf.keras.losses.SparseCategoricalCrossentropy()

        self.mean_loss = tf.keras.metrics.Mean()
        self.acc = tf.keras.metrics.SparseCategoricalAccuracy()

        self.metrics = {
            "mean_loss": self.mean_loss,
            "acc": self.acc
        }
        self.msg_frac = 10

    def build(self):

        concat_list = self.get_inputs(tp="nums")
        images = tf.concat(concat_list, axis=1)
        images = tf.reshape(images,(-1,28,28,1))

        output = CNN(n_class=10)(images)

        arg_max = tf.argmax(output, axis=1)
        self.set_output(output, "softmax")
        self.set_output(arg_max, "argmax")

    @tf.function
    def train(self, feature, label):
        _label = label["label"]

        with tf.GradientTape() as tape:
            output, _ = self.model(feature)
            loss = self.compute_loss(_label, output)

        grads = tape.gradient(loss, self.model.trainable_variables)
        self.optimizer.apply_gradients(
            zip(grads, self.model.trainable_variables))

        self.mean_loss(loss)
        self.acc(_label, output)

    @tf.function
    def evaluate(self, feature, label):
        _label = label["label"]

        output, _ = self.model(feature)
        loss = self.compute_loss(_label, output)
        self.mean_loss(loss)
        self.acc(_label, output)

    @tf.function
    def predict(self, feature):
        pred = self.model(feature)
        return pred
