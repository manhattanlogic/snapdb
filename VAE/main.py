import numpy as np
import tensorflow as tf
from model import Encoder, Decoder
import matplotlib.pyplot as plt
import pickle
import sys

if __name__ == "__main__":
    data = np.load(open("data.np","rb"))
    encoder = Encoder()
    decoder = Decoder(encoder=encoder)

    sess = tf.Session()
    sess.run(tf.global_variables_initializer())
    writer = tf.summary.FileWriter('logs', sess.graph)
    
    batch_size = 1024
    shuffler = np.arange(0, data.shape[0])
    

    converters = np.where(data[:,1]==1)[0]
    non_converters = np.where(data[:,1]==0)[0]

    try:
        w = pickle.load(open("weights.pkl", "rb"))
        for i in range(0, len(decoder.encoder.weights)):
            print ("loading weight:", i)
            sess.run(decoder.encoder.weights[i][0].assign(w[0][i][0]))
            sess.run(decoder.encoder.weights[i][1].assign(w[0][i][1]))

            sess.run(decoder.weights[i][0].assign(w[1][i][0]))
            sess.run(decoder.weights[i][1].assign(w[1][i][1]))

    except:
        print ("weights not loaded")


    start = 0
    if len(sys.argv) > 1:
        start = int(sys.argv[1])

    print ("images start at", start)
        
    for e in range(1, 1000000):
        np.random.shuffle(shuffler)
        error_1 = []
        error_2 = []
        for i in range(0, data.shape[0] // batch_size):
            batch_data_idx = shuffler[i * batch_size : (i+1) * batch_size].tolist()
            batch_data = data[batch_data_idx][:,2:]
            _, e1, e2 = sess.run(decoder.learn, feed_dict={decoder.encoder.input: batch_data,
                                                               decoder.target: batch_data,
                                                               decoder.learning_rate: 0.0001,
                                                               decoder.encoder.keep_prob: 0.5})
            error_1.append(e1)
            error_2.append(e2)
        print (e, np.mean(error_1), np.mean(error_2))
        if e % 10 != 0:
            continue
        _projection = sess.run(decoder.encoder.output, feed_dict={decoder.encoder.input: data[:, 2:],
                                                                      decoder.encoder.keep_prob: 1.0})
        
        f1 = plt.figure(figsize=(20, 20))
        projection = _projection[non_converters,:]
        plt.scatter(projection[:,0],projection[:,1], s=1, marker="," ,color="black")
        projection = _projection[converters,:]
        plt.scatter(projection[:,0],projection[:,1], s=1, marker=",",  color="red")
        plt.savefig('graph_'+("%06d" % (start + e))+'.png')
        plt.close(f1)
        
        w = sess.run([decoder.encoder.weights, decoder.weights])
        pickle.dump(w, open("weights.pkl", "wb"))
