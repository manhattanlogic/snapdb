import numpy as np
import tensorflow as tf

class Encoder:
    def __init__(self, input_size=100, output_size=2):
        self.activation_function = tf.tanh
        self.shape = [input_size, 64, 8, output_size]
        self.weights = []
        self.input = tf.placeholder(tf.float32, [None, input_size])
        for i in range(0, len(self.shape) - 1):
            m = 1 if i < (len(self.shape) - 2) else 2
            w = [tf.Variable(tf.random_normal([self.shape[i], m * self.shape[i+1]], stddev=0.01)),
                tf.Variable(tf.zeros([m * self.shape[i+1]]))]
            self.weights.append(w)

        output = self.input
        for i in range(0, len(self.shape) - 1):
            output = tf.matmul(output, self.weights[i][0]) + self.weights[i][1]
            if i < (len(self.shape) - 2):
                output =  self.activation_function(output)
            else:
                self.mean = mean = output[:, :self.shape[-1]]
                self.stddev = stddev = tf.sqrt(tf.exp(output[:, self.shape[-1]:]))

                self.epsilon = epsilon = tf.random_normal([tf.shape(mean)[0], self.shape[-1]])
                output = mean + epsilon * stddev

                self.vae_loss = tf.reduce_mean(0.5 * (tf.square(mean) + tf.square(stddev) -
                                    2.0 * tf.log(stddev + 1e-8) - 1.0))

        self.output = output

class Decoder:
    def __init__(self, input_size=2, output_size=100, encoder=None):
        self.encoder = encoder
        self.activation_function = tf.tanh
        self.shape = [input_size, 8, 64, output_size]
        self.weights = []
        self.target = tf.placeholder(tf.float32, [None, output_size])
        self.learning_rate = tf.placeholder(tf.float32)
        self.optimizer = tf.train.AdamOptimizer(self.learning_rate)
        
        for i in range(0, len(self.shape) - 1):
            w = [tf.Variable(tf.random_normal([self.shape[i], self.shape[i+1]], stddev=0.01)),
                tf.Variable(tf.zeros([self.shape[i+1]]))]
            self.weights.append(w)
        
        output = encoder.output
        
        for i in range(0, len(self.shape) - 1):
            output = tf.matmul(output, self.weights[i][0]) + self.weights[i][1]
            if i != (len(self.shape) - 2):
                output = self.activation_function(output)

        self.output = output
        
        self.loss = tf.reduce_mean(tf.square(output - self.target)) + encoder.vae_loss * 0.01
        # encoder.vae_loss
        self.learn = (self.optimizer.minimize(self.loss),
                          self.loss, encoder.vae_loss)
        
