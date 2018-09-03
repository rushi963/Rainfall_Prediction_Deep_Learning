import csv
import os
import math
import numpy as np
from joblib import Parallel, delayed
import multiprocessing


def combine(file, sun_elevation_1, sun_elevation_2, sun_elevation_3, sun_elevation_4):

    path_IR1 = '/media/geospatial-3/Data/Out_Res/2017/'
    path_TRMM = '/home/geospatial-3/Desktop/BTP/TRMM_1/output_new/2017/'
    out_path = '/media/geospatial-3/Data/Combined_output/2017/'
    file_name = file.split('_')
    date = file_name[0]
    month = date[0:2]
    day = date[2:4]
    temp = file_name[1].split('.')
    time = temp[0]
    scale = np.zeros(10, dtype=float)
    # ir file
    print(file)
    try :
        with open(os.path.join(path_IR1 + '%s/' % month + '%s/' % day, file), 'r') as ir:
            try:
                # trmm file
                # print(os.path.join(path_TRMM + '%s/' % month + '%s/' % day, file))
                with open(os.path.join(path_TRMM + '%s/' % month + '%s/' % day, file), 'r') as trmm:
                    reader_ir = csv.reader(ir, delimiter=',')
                    next(reader_ir)
                    reader_trmm = csv.reader(trmm, delimiter=',')
                    next(reader_trmm)
                    # new file in which the combined data is going to be written
                    # print(os.path.join(out_path + '%s/' % month + '%s/' % day, file))
                    with open(os.path.join(out_path + '%s/' % month + '%s/' % day, file), 'w+') as output:
                        writer = csv.writer(output)
                        writer.writerow(["Top Latitude", "Bottom Latitude", "Left Longitude", "Right Longitude", "Time",
                                         "Date", "TIR1_T", "TIR2_T", "MIR_T", "SWIR_R", "VIS_A", "WV_R", "WV_T",
                                         "TIR1_T_M3", "TIR1_T_S3", "TIR1_T_M5", "TIR1_T_S5", "TIR2_T_M3", "TIR2_T_S3",
                                         "TIR2_T_M5", "TIR2_T_S5", "MIR_T_M3", "MIR_T_S3", "MIR_T_M5", "MIR_T_S5",
                                         "SWIR_R_M3", "SWIR_R_S3", "SWIR_R_M5", "SWIR_R_S5", "VIS_R_M3", "VIS_R_S3",
                                         "VIS_R_M5", "VIS_R_S5", "WV_R_M3", "WV_R_S3", "WV_R_M5", "WV_R_S5", "WV_T_M3",
                                         "WV_T_S3", "WV_T_M5", "WV_T_S5", "Rain"])
                        for row_ir, row_trmm in zip(reader_ir, reader_trmm):
                            scale[0] = float(row_ir[3])
                            scale[1] = float(row_ir[4])
                            scale[2:10] = row_ir[19:27]
                            # multiplyng the VIS and SWIR data with the cos(sun_zenith) as the data correction
                            if 22.5 <= float(row_trmm[0]) <= 25 and 68 <= float(row_trmm[2]) <= 71.5:
                                scale = math.cos(90 - sun_elevation_1[int(float(time)) - 9][int(float(month)) - 6])*scale
                            elif 22.5 <= float(row_trmm[0]) <= 25 and 71.5 < float(row_trmm[2]) <= 75:
                                scale = math.cos(90 - sun_elevation_2[int(float(time)) - 9][int(float(month)) - 6])*scale
                            elif 20 <= float(row_trmm[0]) < 22.5 and 68 <= float(row_trmm[2]) <= 71.5:
                                scale = math.cos(90 - sun_elevation_3[int(float(time)) - 9][int(float(month)) - 6])*scale
                            else:
                                scale = math.cos(90 - sun_elevation_4[int(float(time)) - 9][int(float(month)) - 6])*scale
                            new_data = [row_trmm[0], row_trmm[1], row_trmm[2], row_trmm[3], row_trmm[4], row_trmm[5],
                                        row_ir[0], row_ir[1], row_ir[2], scale[0], scale[1], row_ir[5], row_ir[6],
                                        row_ir[7], row_ir[8], row_ir[9], row_ir[10], row_ir[11], row_ir[12], row_ir[13],
                                        row_ir[14], row_ir[15], row_ir[16], row_ir[17], row_ir[18], scale[2], scale[3],
                                        scale[4], scale[5], scale[6], scale[7], scale[8], scale[9], row_ir[27], row_ir[28],
                                        row_ir[29], row_ir[30], row_ir[31], row_ir[32], row_ir[33], row_ir[34], row_trmm[6]]
                            #new_data.append(scale)
                            #new_data.append(row_trmm[6])
                            writer.writerow(new_data)
            except FileNotFoundError:
                print('File not found: ', file)
    except FileNotFoundError:
        print('File not found: ', file)



path_IR = '/media/geospatial-3/Data/Out_Res/2017/'
# sun  elevation angles for the 4 spatial points from 9 to 18 hours.
sun_elevation_1 = np.array([[30.64, 28.94, 26.95, 24.84], [44.06, 42.44, 40.67, 38.3], [57.65, 56.09, 54.33, 51.09],
                            [71.32, 69.81, 67.58, 62.24], [85.04, 83.36, 78.55,	68.88], [81.18,	82.08, 77.33, 66.72],
                            [67.46,	68.46, 65.69, 57.47], [53.8, 54.74,	52.33, 45.37], [40.26, 41.1, 38.64, 32.21],
                            [26.9, 27.61, 24.92, 18.63]], np.float)
sun_elevation_2 = np.array([[33.78, 32.11, 30.19, 28.09], [47.25, 45.65, 43.91,	41.42], [60.86,	59.33, 57.53, 53.97],
                            [74.56,	73.05, 70.56, 64.46], [88.25, 86.3, 79.99, 69.36], [77.94, 78.93, 75.04, 65.09],
                            [64.23,	65.24, 62.66, 54.87], [50.59, 51.53, 49.17, 42.41], [37.08,	37.92, 35.45, 29.11],
                            [23.77, 24.47, 21.75, 15.47]], np.float)
sun_elevation_3 = np.array([[33.2, 31.59, 30.03, 28.48], [46.81, 45.3, 44, 42.15], [60.53, 59.13, 57.94, 55.18],
                            [74.3, 73.04, 71.55, 66.37], [87.35, 86.98, 82.36, 71.85], [77.65, 79.03, 76.43, 67.06],
                            [63.9, 65.1, 63.23, 56.11], [50.16, 51.22, 49.35, 43.16], [36.51,	37.45, 35.37, 29.51],
                            [23.05, 23.84, 21.44, 15.6]], np.float)
sun_elevation_4 = np.array([[30.03, 28.4, 26.77, 25.24], [43.06, 42.07, 40.72, 38.99], [57.31, 55.88, 54.68, 52.22],
                           [71.08, 69.78, 68.43, 64.05], [84.65, 83.72, 80.62, 71.43], [80.84, 82.3, 79.13, 68.97],
                           [67.13, 68.36, 66.43, 58.94], [53.37, 54.47, 52.62, 46.28], [39.7, 40.67, 38.65, 32.74],
                           [26.18, 27.01, 24.69, 18.87]], np.float)
num_cores = multiprocessing.cpu_count()
# num_cores = 2
# Code starts from here
for subdir, dirs, files in os.walk(path_IR):
    Parallel(n_jobs=num_cores - 1)(delayed(combine)(file, sun_elevation_1, sun_elevation_2, sun_elevation_3,
                                                    sun_elevation_4) for file in files)






