import h5py
import csv
import numpy as np
import os
import numpy as np
from joblib import Parallel, delayed
import multiprocessing


def trmm_generator(file, subdir):

    filepath = subdir + os.sep + file
    if filepath.endswith(".HDF5"):
        file_name = file.split('.')
        d = file_name[4]
        day = d[4:6] + d[6:8] + d[0:4]
        time_split = d.split('-')
        t = time_split[1]
        time_crop = t[1:3]
        print(day, time_crop)
        f = h5py.File(filepath, 'r')
        # heading = list(f.keys())
        # heading_1 = list(f['Grid'].keys())

        # precipitation_Cal values
        prep_cal = f['Grid']['precipitationCal']
        #print(prep_cal.shape)

        # storing lat, long of each pixel
        dic_lat = f['Grid']['lat']
        dic_lon = f['Grid']['lon']
        #print(dic_lat.shape[0], dic_lon.shape[0])

        lat1 = []
        lat2 = []
        long1 = []
        long2 = []

        # storing precipitation in a list
        rain = []
        for i in range(len(prep_cal)):
            for j in range(len(prep_cal[0])):
                rain.append(round(float(prep_cal[i][j]), 2))
                long1.append(round(float(dic_lon[i]) - 0.05, 2))
                long2.append(round(float(dic_lon[i]) + 0.15, 2))
                lat1.append(round(float(dic_lat[j]) - 0.05, 2))
                lat2.append(round(float(dic_lat[j]) + 0.15, 2))

        #print(len(rain))
        #print(len(long1))
        #print(len(lat1))

        time = []
        for i in range(len(rain)):
            time.append(time_crop)

            # storing date
        date = []
        for i in range(len(rain)):
            date.append(day)

        # Writing to a csv file
        data = np.empty((7, len(rain),))
        data[0] = lat1
        data[1] = lat2
        data[2] = long1
        data[3] = long2
        data[4] = time
        data[5] = date
        data[6] = rain
        data = list(map(list, zip(*data)))
        # print(len(data))

        dataset = np.zeros((len(data), 7))
        count = 0
        for i in range(len(data)):
            dataset[count][0] = data[i][0]
            dataset[count][1] = data[i][1]
            dataset[count][2] = data[i][2]
            dataset[count][3] = data[i][3]
            dataset[count][4] = data[i][4]
            dataset[count][5] = data[i][5]
            dataset[count][6] = data[i][6]
            count = count + 1
        # print(len(dataset))
        out_path = '/home/geospatial-3/Desktop/BTP/TRMM_1/output_new/2016/' + '%s/' % d[4:6] + '%s/' % d[6:8]
        out_file = day + '_' + time_crop
        with open(out_path + '%s.csv' % out_file, 'w+') as f1:
            writer = csv.writer(f1)
            writer.writerow(["Top Latitude", "Bottom Latitude", "Left Longitude", "Right Longitude", "Date", "Time",
                             "Rain"])
            writer.writerows(dataset)
            f1.close()
        f.close()


num_cores = multiprocessing.cpu_count()
# iterating through all files
path = '/home/geospatial-3/Desktop/BTP/TRMM_1/2016'

for subdir, dirs, files in os.walk(path):
    Parallel(n_jobs=num_cores - 1)(delayed(trmm_generator)(file, subdir) for file in files)

