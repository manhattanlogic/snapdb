import tensorflow as tf
import numpy as np
import matplotlib.pyplot as plt
import pickle
'''
       0  1 2   3
[100,32,2,32,100]
'''
class DEA:
    def __init__(self, layer_shapes=[100, 64, 32, 2], batch_size=4096*4,
                     learing_rate=0.0001, p_epochs=2, t_epochs=2):

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
            self.weights.append([tf.Variable(tf.random_normal([layer_shapes[i-1], layer_shapes[i]])),
                tf.Variable(tf.zeros([layer_shapes[i]]))])
            backward_weights.append([tf.Variable(tf.random_normal([layer_shapes[i], layer_shapes[i-1]])),
                tf.Variable(tf.zeros([layer_shapes[i-1]]))])
            
        self.weights += backward_weights[::-1]
        
        for a in range (1, len(layer_shapes)):
            ae = {}
            ae["input"] = tf.placeholder(tf.float32, [None, layer_shapes[a - 1]])
            ae["target"] = tf.placeholder(tf.float32, [None, layer_shapes[a - 1]])
            ae["hidden"] =  tf.matmul(ae["input"], self.weights[a-1][0]) + self.weights[a-1][1]
            if (a != len(layer_shapes) -1):
                ae["hidden"] = tf.tanh(ae["hidden"])
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
            if i != (len(self.weights) - 1) and i != (len(layer_shapes) - 2):
                hidden = tf.tanh(hidden)
            self.ae["layers"].append(hidden)
        self.ae["error"] = tf.reduce_mean(tf.square(self.ae["layers"][-1] - self.target))
        self.ae["learn"] = (self.optimizer.minimize(self.ae["error"]), self.ae["error"])
        self.ae["projection"] = self.ae["layers"][len(layer_shapes)-1]

        self.sess = tf.Session()
        writer = tf.summary.FileWriter('logs', self.sess.graph)
        self.sess.run(tf.global_variables_initializer())
        
    def train(self, data):
        
        shuffler = np.array(range(0, data.shape[0]))
        for a in range(0, len(self.aes)):
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
                        batch_input=self.sess.run(self.ae["layers"][a], feed_dict={self.ae["layers"][0]:batch_input})
                    batch_noise = np.random.randint(low=0,high=2,size=batch_input.shape)
                    batch_input_noised = batch_input * batch_noise
                    _, error = self.sess.run(self.aes[a]["learn"], feed_dict={self.aes[a]["input"]: batch_input_noised,
                                                                                  self.aes[a]["target"]: batch_input,
                                                                                  self.learning_rate: self.lr})
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
                                                                         self.learning_rate: self.lr})
                epoch_erros.append(error)
            print ("   error:", np.mean(epoch_erros))
                        
    def get_projection(self, data):
        return self.sess.run(self.ae["projection"], feed_dict={self.input: data})

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

    dea = DEA(p_epochs=5, t_epochs=20)
    dea.load_weights("weights.pkl")
    
    dea.train(data[:,2:])

    dea.save_weights("weights.pkl")
    
    projection = dea.get_projection(data[:,2:])
    plt.scatter(projection[:,0],projection[:,1], s=1, c=data[:,1])
    plt.savefig('graph.png')
