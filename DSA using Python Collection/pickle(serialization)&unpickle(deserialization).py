import pickle


dataObj = {'name':"harry"}

byteStream = pickle.dumps(dataObj)

print(byteStream)

print(pickle.loads(byteStream))

