import numpy as np
import tensorflow as tf
from model import Encoder, Decoder
import matplotlib.pyplot as plt

if __name__ == "__main__":
    data = np.load(open("data.np","rb"))
    encoder = Encoder()
    decoder = Decoder(encoder=encoder)

    sess = tf.Session()
    sess.run(tf.global_variables_initializer())
    writer = tf.summary.FileWriter('logs', sess.graph)
    
    batch_size = 1024 * 16 * 4
    shuffler = np.arange(0, data.shape[0])
    

    converters = np.where(data[:,1]==1)[0]
    non_converters = np.where(data[:,1]==0)[0]

    for e in range(1, 1000):
        np.random.shuffle(shuffler)
        error_1 = []
        error_2 = []
        for i in range(0, data.shape[0] // batch_size):
            batch_data_idx = shuffler[i * batch_size : (i+1) * batch_size].tolist()
            batch_data = data[batch_data_idx][:,2:]
            _, e1, e2 = sess.run(decoder.learn, feed_dict={decoder.encoder.input: batch_data,
                                                               decoder.target: batch_data,
                                                               decoder.learning_rate: 0.001,
                                                               decoder.encoder.keep_prob: 0.5})
            error_1.append(e1)
            error_2.append(e2)
        print (e, np.mean(error_1), np.mean(error_2))
        if e % 10 != 0:
            continue
        _projection = sess.run(decoder.encoder.output, feed_dict={decoder.encoder.input: data[:, 2:],
                                                                      decoder.encoder.keep_prob: 1.0})

       
        f1 = plt.figure(figsize=(10, 10))
        projection = _projection[non_converters,:]
        plt.scatter(projection[:,0],projection[:,1], s=1, marker="," ,color="black")
        projection = _projection[converters,:]
        plt.scatter(projection[:,0],projection[:,1], s=1, marker=",",  color="red")
        plt.savefig('graph_'+("%04d" % e)+'.png')
        plt.close(f1)
        
