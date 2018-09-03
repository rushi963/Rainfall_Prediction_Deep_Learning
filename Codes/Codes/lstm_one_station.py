from keras.models import Sequential
from keras.layers import Dense
from keras.layers import LSTM
from keras.layers import Masking
from sklearn.model_selection import train_test_split
import numpy as np
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
import os
import csv

dataPath = "/home/geospacial2/PycharmProjects/BTP/"
with open(os.path.join(dataPath, "one_station.csv"), 'rt') as file:
    reader = csv.reader(file)
    X = []
    Y = []
    content = []
    #actual_length = 7345
    Data = [[[0 for i in range(8)] for j in range(24)] for k in range(332)]
    data = np.array(Data, dtype=float)
    Target = [[0 for i in range(24)] for j in range(332)]
    target = np.array(Target, dtype=float)
    #print(data.shape)
    next(reader, None)
    for row in reader:
        content.append(row)
        X.append(row[9:17])
        Y.append(row[22])

    X = StandardScaler().fit_transform(X, Y)
    count = 0
    for i in range(len(content)):
        #for j in range(len(content[0])):
        data[count][int(content[i][3])][:] = X[i]
        target[count][int(content[i][3])] = content[i][22]
        row1 = content[i]
        try:
            row2 = content[i+1]
        except IndexError:
            continue
        if row1[4] != row2[4]:
            count = count + 1
    #print(target[1])
    print(data.shape, target.shape)

    x_train, x_test, y_train, y_test = train_test_split(data, target, test_size=0.2, random_state=4)
    model = Sequential()
    #model.add(Masking(mask_value=0., input_shape=(24, 8)))
    model.add(LSTM(24, batch_input_shape=(None, 24, 8), return_sequences=True))
    model.add(LSTM(24, return_sequences=True))
    model.add(LSTM(24, return_sequences=True))
    model.add(LSTM(24, return_sequences=True))
    model.add(LSTM(24, return_sequences=False))
    model.compile(loss='mean_squared_error', optimizer='rmsprop', metrics=['accuracy'])
    print(model.summary())

    history = model.fit(x_train, y_train, epochs=500, validation_data=(x_test, y_test))
    print(history)
    results = model.predict(x_test)

    #plt.scatter(range(len(results)), results, c='r')
    #plt.scatter(range(len(y_test)), y_test, c='g')
    #plt.show()
    print(history.history)
    one, = plt.plot(history.history['loss'], label='Training Loss')
    two, = plt.plot(history.history['val_loss'], label='Validation Loss')
    plt.xlabel('Epochs')
    plt.ylabel('MSE')
    plt.title('rmsprop 24 units 5 LSTM')
    plt.legend(handles=[one, two])
    plt.show()