import tensorflow as tf
import numpy as np
import matplotlib.pyplot as plt
import pickle
import sys
from DEA import DEA
from sklearn.cluster import KMeans
import png

image_width = 1024
image_height = 1024


def draw_circle(_x, _y, data, diameter, color=[255,255,255]):
    angles = np.arange(0, 360) / 360.0 * np.pi
    x = _x + np.cos(angles) * diameter
    y = _y + np.sin(angles) * diameter
    for i in range(0, len(x)):
        data[int(x[i]), int(y[i]), :] = color
        


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

    dea = DEA(p_epochs=20, t_epochs=100)
    dea.load_weights("weights.pkl")

    projection = dea.get_projection(data[:,2:])
    kmeans = KMeans(n_clusters=8, random_state=0, n_jobs=-1).fit(projection)

    image_data = np.zeros([image_width, image_height, 3])
    f = open('clusters.png', 'wb')
    w = png.Writer(image_width, image_height, greyscale=False)
    for p in range(0, projection.shape[0]):
        y = int(projection[p,0] * image_width / 2 + image_width / 2)
        x = int(-projection[p,1] * image_height / 2 + image_height / 2)
        image_data[x,y,0] = 255


    for i in range(0, kmeans.cluster_centers_.shape[0]):
        y = int(kmeans.cluster_centers_[i,0] * image_width / 2 + image_width / 2)
        x = int(-kmeans.cluster_centers_[i,1] * image_height / 2 + image_height / 2)
        draw_circle(x, y, image_data, 100)

    print (kmeans.cluster_centers_)
        
    w.write(f, np.reshape(image_data, [image_height,-1]))
    f.close()
