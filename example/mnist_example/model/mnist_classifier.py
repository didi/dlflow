from dlflow.mgr import model, config
from dlflow.models import ModelBase

import tensorflow as tf
from tensorflow.keras.models import Model
from tensorflow.keras.layers import Dense, Conv2D
from tensorflow.keras.layers import ReLU, Dropout, Flatten
from tensorflow.keras.layers import BatchNormalization, MaxPooling2D


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
        images = tf.reshape(images, (-1, 28, 28, 1))

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


class CNN(Model):
    def __init__(self, n_class=10):
        super(CNN, self).__init__()

        self.conv1 = Conv2D(32, (3, 3), activation='relu')
        self.conv2 = Conv2D(64, (3, 3), activation='relu')
        self.max_pooing2d = MaxPooling2D((2, 2))
        self.flatten = Flatten()
        self.dense1 = Dense(64, activation='relu')
        self.dense2 = Dense(n_class, activation='softmax')

    def call(self, inputs):
        x = self.conv1(inputs)
        x = self.max_pooing2d(x)
        x = self.conv2(x)
        x = self.max_pooing2d(x)
        x = self.flatten(x)
        x = self.dense1(x)
        x = self.dense2(x)
        return x
