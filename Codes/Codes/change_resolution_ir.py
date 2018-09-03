import csv
import numpy as np
import statistics
import os

def prepare(temp3, data, count):

    data[count] = temp3
    count = count + 1
    return data, count


def changeres(temp1, data, count):
    temp2 = np.zeros(25)
    temp3 = np.zeros(3500)
    index = [-2, -1, 0, 1, 2]
    count2 = 0
    for j in range(2, len(temp1[0]) - 2, 5):
        for i in range(2, len(temp1) - 2, 5):
            count1 = 0
            for x in index:
                for y in index:
                    temp2[count1] = temp1[i + x][j + y]
                    count1 = count1 + 1
            temp3[count2] = statistics.mean(temp2)
            count2 = count2 + 1

    data, count = prepare(temp3, data, count)
    return data, count


def convert2d(temp, data, count):
    temp1 = np.zeros((250, 350, ), dtype=float)
    for i in range(248):
        for j in range(334):
            temp1[i][j] = float(temp[j + (i * 334)])

    for x in range(16):
        for y in range(248):
            temp1[y][334 + x] = temp1[y][333]

    for x in range(2):
        for y in range(350):
            temp1[248 + x][y] = temp1[247][y]

    data, count = changeres(temp1, data, count)
    return data, count




path = '/media/geospatial-3/Data/output_IR/July_2014'
for subdir, dirs, files in os.walk(path):
    for file in files:
        filepath = subdir + os.sep + file
        file_name = file.split('_')
        time = file_name[1]
        day = file_name[0]
        date = day[2:4]
#with open('06012014_05.csv', 'r') as f:
        columns = [[] for j in range(43)]
        with open(filepath, 'r') as f:
            reader = csv.reader(f, delimiter=',')
            first = next(reader)
            for row in reader:
                for (i, v) in enumerate(row):
                    columns[i].append(v)

            count = 0
            data = np.zeros((35, 3500), dtype=float)
            temp = []
            for j in range(6, 43):
                if j == 13 or j == 14:
                    continue
                else:
                    temp = columns[j]
                    data, count = convert2d(temp, data, count)

            out_path = '/media/geospatial-3/Data/Out_Res/July_2014/' + '%s/' % date
            out_file = day + '_' + time
            data = list(map(list, zip(*data)))
            #with open('resolution_change.csv', 'w+') as f1:
            with open(out_path + '%s' % out_file, 'w+') as f1:
                writer = csv.writer(f1)
                writer.writerow(["TIR1_T", "TIR2_T", "MIR_T", "SWIR_R", "VIS_A", "WV_R", "WV_T",
                                 "TIR1_T_M3", "TIR1_T_S3", "TIR1_T_M5", "TIR1_T_S5",
                                 "TIR2_T_M3", "TIR2_T_S3", "TIR2_T_M5", "TIR2_T_S5", "MIR_T_M3", "MIR_T_S3",
                                 "MIR_T_M5", "MIR_T_S5", "SWIR_R_M3", "SWIR_R_S3", "SWIR_R_M5", "SWIR_R_S5",
                                 "VIS_R_M3", "VIS_R_S3", "VIS_R_M5", "VIS_R_S5", "WV_R_M3", "WV_R_S3", "WV_R_M5",
                                 "WV_R_S5", "WV_T_M3", "WV_T_S3", "WV_T_M5", "WV_T_S5"])
                writer.writerows(data)
                f1.close()
