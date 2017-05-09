import numpy as np
import tensorflow as tf
from model import Encoder, Decoder
if __name__ == "__main__":
    data = np.load(open("data.np","rb"))
    encoder = Encoder()
    decoder = Decoder(encoder=encoder)

    sess = tf.Session()
    sess.run(tf.global_variables_initializer())
    
    batch_size = 1024 * 16
    shuffler = np.arange(0, data.shape[0])
    np.random.shuffle(shuffler)


    for e in range(0, 100):
        error_1 = []
        error_2 = []
        for i in range(0, data.shape[0] // batch_size):
            batch_data_idx = shuffler[i * batch_size : (i+1) * batch_size].tolist()
            batch_data = data[batch_data_idx][:,2:]
            _, e1, e2 = sess.run(decoder.learn, feed_dict={decoder.encoder.input: batch_data,
                                                               decoder.target: batch_data,
                                                               decoder.learning_rate: 0.001})
            error_1.append(e1)
            error_2.append(e2)
        print (e, np.mean(error_1), np.mean(error_2))
