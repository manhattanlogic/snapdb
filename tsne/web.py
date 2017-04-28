import cherrypy
import numpy as np
import png
r = 10
data = np.load(open("image_id_matrix.np", "rb"))
#print (data[np.where(data == 4714899388067487645)][0])
class Root(object):
    
    @cherrypy.expose
    def get_vid(self, x, y):
        results = []
        for i in range(-r, r):
            for j in range(-r, r):
                results.append(data[int(y) + i, int(x) + j])

        
        results = np.array(results)
        print (results)
        return str(max(results[np.where(results > 0)[0]]))
        
                
        if len(np.where(np.array(results) != 0)) > r:
            return str(np.max(results))
        else:
            return "0"

        
       


f = open('clusters_test.png', 'wb')
w = png.Writer(data.shape[0], data.shape[1], greyscale=True)
w.write(f, (data > 0) * 250)
f.close()

cherrypy.config.update({'server.socket_host': '0.0.0.0',
                        'server.socket_port': 8090,
                       })
cherrypy.quickstart(Root())
