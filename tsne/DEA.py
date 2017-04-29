import tensorflow as tf
import numpy as np
import matplotlib.pyplot as plt
import pickle
import sys
'''
       0  1 2   3
[100,32,2,32,100]
'''
class DEA:
    def __init__(self, layer_shapes=[100, 64, 32, 2], pretrain = [],
                     batch_size=1024 * 16,
                     learing_rate=0.001, p_epochs=2, t_epochs=2, projection_function=tf.tanh, projection_factor=1):

        self.pretrain = pretrain
        self.keep_prob = tf.placeholder(tf.float32)
        self.projection_function = projection_function
        self.projection_factor = projection_factor
        self.batch_size = batch_size
        self.lr = learing_rate
        self.p_epochs = p_epochs
        self.t_epochs = t_epochs
        self.aes = []
        self.weights = []
        self.input = tf.placeholder(tf.float32, [None, layer_shapes[0]])
        self.target = tf.placeholder(tf.float32, [None, layer_shapes[0]])
        
        self.learning_rate = tf.placeholder(tf.float32)
        self.optimizer = tf.train.AdamOptimizer(self.learning_rate)
        backward_weights = []
        
        for i in range(1, len(layer_shapes)):
            self.weights.append([tf.Variable(tf.random_normal([layer_shapes[i-1], layer_shapes[i]], stddev=0.01)),
                tf.Variable(tf.zeros([layer_shapes[i]]))])
            backward_weights.append([tf.Variable(tf.random_normal([layer_shapes[i], layer_shapes[i-1]], stddev=0.01)),
                tf.Variable(tf.zeros([layer_shapes[i-1]]))])
            
        self.weights += backward_weights[::-1]
        
        for a in range (1, len(layer_shapes)):
            ae = {}
            ae["input"] = tf.placeholder(tf.float32, [None, layer_shapes[a - 1]])
            ae["target"] = tf.placeholder(tf.float32, [None, layer_shapes[a - 1]])
            ae["hidden"] =  tf.matmul(ae["input"], self.weights[a-1][0]) + self.weights[a-1][1]
            if (a != len(layer_shapes) -1):
                ae["hidden"] = tf.tanh(ae["hidden"])
            else:
                ae["hidden"] = projection_function(ae["hidden"] * self.projection_factor)
            t_idx = len(layer_shapes)*2-a-2
            ae["output"]  =  tf.matmul(ae["hidden"], self.weights[t_idx][0]) + self.weights[t_idx][1]
            if a > 1:
                ae["output"] = tf.tanh(ae["output"])
            ae["error"] = tf.reduce_mean(tf.square(ae["output"] - ae["target"]))
            ae["learn"] = (self.optimizer.minimize(ae["error"], var_list=[
                self.weights[a-1][0],
                self.weights[a-1][1],
                self.weights[t_idx][0],
                self.weights[t_idx][1]
                ]), ae["error"])
            self.aes.append(ae)

            
        self.ae = {"layers":[self.input]}
        for i in range(0, len(self.weights)):
            hidden = tf.matmul(self.ae["layers"][-1], self.weights[i][0]) + self.weights[i][1]
            if i != (len(self.weights) - 1):
                if i != (len(layer_shapes) - 2):
                    hidden = tf.tanh(hidden)
                else:
                    hidden = projection_function(hidden)
            if i == 0:
                #hidden = tf.nn.dropout(hidden, self.keep_prob)
                nop = 0
            self.ae["layers"].append(hidden)
        self.ae["error"] = tf.reduce_mean(tf.square(self.ae["layers"][-1] - self.target))
        self.ae["learn"] = (self.optimizer.minimize(self.ae["error"]), self.ae["error"])
        self.ae["projection"] = self.ae["layers"][len(layer_shapes)-1]
        self.ae["preprojection"] = self.ae["layers"][len(layer_shapes)-3]

        
        
    def train(self, data, pretrain=True):
        
        shuffler = np.array(range(0, data.shape[0]))

        if pretrain:
            for a in range(0, len(self.aes)):
              if len(self.pretrain) == 0 or a in self.pretrain:
                print ("training ae:", a)
                for e in range(0, self.p_epochs):
                    print ("  epoch:", e)
                    np.random.shuffle(shuffler)
                    epoch_erros = []
                    for i in range(0, shuffler.shape[0] // self.batch_size):
                        print ("   " + str(i *100.0 / (shuffler.shape[0] // self.batch_size) ), "\033[1A")
                        batch_index = shuffler[i * self.batch_size : (i+1) * self.batch_size]
                        #batch_input = (data[batch_index] - self.means) / self.stds
                        batch_input = data[batch_index]
                        if (a > 0):
                            batch_input=self.sess.run(self.ae["layers"][a], feed_dict={self.ae["layers"][0]:batch_input, self.keep_prob: 1.0})
                        batch_noise = np.random.randint(low=0,high=2,size=batch_input.shape)
                        batch_input_noised = batch_input * batch_noise
                        _, error = self.sess.run(self.aes[a]["learn"], feed_dict={self.aes[a]["input"]: batch_input_noised,
                                                                                      self.aes[a]["target"]: batch_input,
                                                                                      self.learning_rate: self.lr,
                                                                                      self.keep_prob: 1.0})
                        epoch_erros.append(error)
                    print ("   error:", np.mean(epoch_erros))

        
        for e in range(0, self.t_epochs):
            print ("global epoch:", e)
            np.random.shuffle(shuffler)
            epoch_erros = []
            for i in range(0, shuffler.shape[0] // self.batch_size):
                print ("   " + str(i *100.0 / (shuffler.shape[0] // self.batch_size) ), "\033[1A")
                batch_index = shuffler[i * self.batch_size : (i+1) * self.batch_size]
                batch_input = data[batch_index]
                _,error = self.sess.run(self.ae["learn"], feed_dict={self.input: batch_input,
                                                                         self.target: batch_input,
                                                                         self.learning_rate: self.lr,
                                                                         self.keep_prob: 0.5})
                epoch_erros.append(error)
            print ("   error:", np.mean(epoch_erros))

    def get_preprojection(self, data):
        return self.sess.run(self.ae["preprojection"], feed_dict={self.input: data, self.keep_prob: 1.0})

    def get_projection(self, data):
        return self.sess.run(self.ae["projection"], feed_dict={self.input: data, self.keep_prob: 1.0})

    def get_projections(self, data):
        return self.sess.run([self.ae["preprojection"], self.ae["projection"]], feed_dict={self.input: data, self.keep_prob: 1.0})
    
    def save_weights(self, filename):
        weights = self.sess.run(self.weights)
        pickle.dump(weights, open(filename,"wb"))

    def load_weights(self, filename):
        try:
            weights = pickle.load(open(filename,"rb"))
            for i in range(0, len(weights)):
                print ("loading weight ", i)
                self.sess.run(self.weights[i][0].assign(weights[i][0]))
                self.sess.run(self.weights[i][1].assign(weights[i][1]))
        except:
            print ("weights not loaded")

            
if __name__ == "__main__":


    colors = [(255, 200, 220),(170, 110, 40),(255, 150, 0),
              (255, 215, 180),(128, 128, 0),(255, 235, 0),
              (255, 250, 200),(190, 255, 0),(0, 190, 0),
              (170, 255, 195),(0, 128, 128),(100, 255, 255),
              (0, 0, 128),(67, 133, 255), (130, 0, 150),
              (230, 190, 255),(255, 0, 255),(128, 128, 128)]

    
    try:
        data = np.load(open("data.np","rb"))
        print ("numpy data loaded")
    except:
        data = np.loadtxt("short.csv", delimiter="\t")
        means = np.mean(data[:,2:], axis=0)
        stds = np.std(data[:,2:], axis=0)
        data[:,2:] = (data[:,2:] - means) / stds
        np.save(open("data.np","wb"), data)
        print ("csv data loaded. numpy data saved")

    dea = DEA(layer_shapes = [100, 32, 2, 32, 16], pretrain = [0,1,2],
                  p_epochs=5, t_epochs=10, projection_function=tf.nn.softmax, projection_factor=100)
    dea.sess = tf.Session()
    #writer = tf.summary.FileWriter('logs', self.sess.graph)
    dea.sess.run(tf.global_variables_initializer())
    
    dea.load_weights("weights.pkl")

    for epoch in range(0, 100000):
        if epoch > 0 or (len(sys.argv) > 1 and sys.argv[1] == "skip"):
            dea.train(data[:,2:], pretrain=False)
        else:
            dea.train(data[:,2:], pretrain=True)

        print ("saving weights")
        dea.save_weights("weights.pkl")

        converters = np.where(data[:,1]==1)[0]
        non_converters = np.where(data[:,1]==0)[0]

        f1 = plt.figure(figsize=(20, 20))
        #_projection = dea.get_preprojection(data[:,2:])
        


        _projection, postprojection = dea.get_projections(data[:,2:])
        
        projection = _projection[non_converters,:]
        plt.scatter(projection[:,0],projection[:,1], s=1, marker="," ,color="black")
        projection = _projection[converters,:]
        plt.scatter(projection[:,0],projection[:,1], s=1, marker=",",  color="red")
        plt.savefig('graph_'+("%04d" % epoch)+'.png')
        
        f2 = plt.figure(figsize=(20, 20))
        
        projection = _projection
        tmp1 = np.argmax(postprojection, axis=1)
        color_projection = np.array(colors)[tmp1] / 256
        
        plt.scatter(projection[:,0],projection[:,1], s=1, marker=",",  c=color_projection)
        plt.savefig('clust_'+("%04d" % epoch)+'.png')

        plt.close(f1)
        plt.close(f2)
        
