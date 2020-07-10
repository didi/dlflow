from tensorflow.keras.models import Model
from tensorflow.keras.layers import Dense, Conv2D
from tensorflow.keras.layers import BatchNormalization, MaxPooling2D
from tensorflow.keras.layers import ReLU, Dropout, Flatten


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
