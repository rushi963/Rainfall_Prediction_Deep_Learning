import h5py
import csv
import numpy as np
import os
from PIL import Image
import statistics
from joblib import Parallel, delayed
import multiprocessing

# iterating through all files
def ir(file, elevation, vegetation1, vegetation2):
    filepath = subdir + os.sep + file
    if filepath.endswith(".h5"):
        file_name = file.split('_')
        d = file_name[1]
        day = "09" + d[0:2] + d[5:]
        t = file_name[2]
        time_crop = t[0:2]
        if 4 < int(time_crop) < 19:
            f = h5py.File(filepath, 'r')
            heading = list(f.keys())

            # projection information and lat, long
            projection = f['Projection_Information']
            proj_attr = list(projection.attrs)

            upper_left = projection.attrs.get('upper_left_lat_lon(degrees)')
            upper_left_lat = upper_left[0]
            upper_left_long = upper_left[1]
            upper_right = projection.attrs.get('upper_right_lat_lon(degrees)')
            upper_right_lat = upper_right[0]
            upper_right_long = upper_right[1]
            lower_left = projection.attrs.get('lower_left_lat_lon(degrees)')
            lower_left_lat = lower_left[0]
            lower_left_long = lower_left[1]
            lower_right = projection.attrs.get('lower_right_lat_lon(degrees)')
            lower_right_lat = lower_right[0]
            lower_right_long = lower_right[1]

            # # sun elevation
            # sun = f['Sun_Elevation']
            # sun_ele = sun[0]
            # sun_ele = np.array(sun_ele)
            #
            # count = 0
            # sun_azu = np.empty((1, len(sun_ele)))
            # for i in range(len(sun_ele)):
            #     sun_ele[i] = sun_ele[i]*0.01
            #     sun_azu[i] = 90 - sun_ele[i]
            #     if sun_azu[i] > 60:
            #         count = count + 1
            #
            # if count == len(sun_azu):
            #     break

            # tir1 gray count values
            tir1 = f['IMG_TIR1']
            tir1_gray = tir1[0]
            tir1_gray = np.array(tir1_gray)

            # water vapour gray count values
            wv = f['IMG_WV']
            wv_gray = wv[0]
            wv_gray = np.array(wv_gray)

            # tir2 gray count values
            tir2 = f['IMG_TIR2']
            tir2_gray = tir2[0]
            tir2_gray = np.array(tir2_gray)

            # mir gray count values
            mir = f['IMG_MIR']
            mir_gray = mir[0]
            mir_gray = np.array(mir_gray)

            # swir gray count values
            swir = f['IMG_MIR']
            swir_gray = swir[0]
            swir_gray = np.array(swir_gray)

            # swir gray count values
            vis = f['IMG_VIS']
            vis_gray = vis[0]
            vis_gray = np.array(vis_gray)




            # wv temp values
            wv_temp = f['IMG_WV_TEMP']
            wv_temp = list(wv_temp)

            # wv radiance values
            wv_rad = f['IMG_WV_RADIANCE']
            wv_rad = list(wv_rad)

            # tir1 temp values
            tir1_temp = f['IMG_TIR1_TEMP']
            tir1_temp = list(tir1_temp)

            # tir2 temp values
            tir2_temp = f['IMG_TIR2_TEMP']
            tir2_temp = list(tir2_temp)

            # mir temp values
            mir_temp = f['IMG_MIR_TEMP']
            mir_temp = list(mir_temp)

            # swir radiance values
            swir_rad = f['IMG_SWIR_RADIANCE']
            swir_rad = list(swir_rad)

            # vis albedo values
            vis_rad = f['IMG_VIS_ALBEDO']
            vis_rad = list(vis_rad)




            # creating count -> tir1_temp lookup table
            tir1_dic = {}
            for i in range(tir1_temp.__len__()):
                tir1_dic[i] = tir1_temp[i]

            # creating count -> tir2_temp lookup table
            tir2_dic = {}
            for i in range(tir2_temp.__len__()):
                tir2_dic[i] = tir2_temp[i]

            # creating count -> mir_temp lookup table
            mir_dic = {}
            for i in range(mir_temp.__len__()):
                mir_dic[i] = mir_temp[i]

            # creating count -> swir_radiance lookup table
            swir_dic = {}
            for i in range(swir_rad.__len__()):
                swir_dic[i] = swir_rad[i]

            # creating count -> vis_radiance (albedo) lookup table
            vis_dic = {}
            for i in range(vis_rad.__len__()):
                vis_dic[i] = vis_rad[i]

            # creating count -> wv_radiance lookup table
            wv_dic = {}
            for i in range(wv_rad.__len__()):
                wv_dic[i] = wv_rad[i]

            # creating count -> wv_temp lookup table
            wv_dic1 = {}
            for i in range(wv_temp.__len__()):
                wv_dic1[i] = wv_temp[i]




            # storing tir1 brightness temp values
            temp_tir1 = np.empty((tir1_gray.shape[0], tir1_gray.shape[1],))
            for i in range(tir1_gray.shape[0]):
                for j in range(tir1_gray.shape[1]):
                    temp_tir1[i][j] = tir1_dic[tir1_gray[i][j]]

            # storing tir2 brightness temp values
            temp_tir2 = np.empty((tir2_gray.shape[0], tir2_gray.shape[1],))
            for i in range(tir2_gray.shape[0]):
                for j in range(tir2_gray.shape[1]):
                    temp_tir2[i][j] = tir2_dic[tir2_gray[i][j]]

            # storing mir brightness temp values
            temp_mir = np.empty((mir_gray.shape[0], mir_gray.shape[1],))
            for i in range(mir_gray.shape[0]):
                for j in range(mir_gray.shape[1]):
                    temp_mir[i][j] = mir_dic[mir_gray[i][j]]

            # storing swir radiance values
            rad_swir = np.empty((swir_gray.shape[0], swir_gray.shape[1],))
            for i in range(swir_gray.shape[0]):
                for j in range(swir_gray.shape[1]):
                    rad_swir[i][j] = swir_dic[swir_gray[i][j]]

            # storing vis radiance (albedo) values
            rad_vis = np.empty((vis_gray.shape[0], vis_gray.shape[1],))
            for i in range(vis_gray.shape[0]):
                for j in range(vis_gray.shape[1]):
                    rad_vis[i][j] = vis_dic[vis_gray[i][j]]

            # storing wv brightness temp values
            temp_wv = np.empty((wv_gray.shape[0], wv_gray.shape[1],))
            for i in range(wv_gray.shape[0]):
                for j in range(wv_gray.shape[1]):
                    temp_wv[i][j] = wv_dic1[wv_gray[i][j]]

            # storing wv radiance values
            rad_wv = np.empty((wv_gray.shape[0], wv_gray.shape[1],))
            for i in range(wv_gray.shape[0]):
                for j in range(wv_gray.shape[1]):
                    rad_wv[i][j] = wv_dic[wv_gray[i][j]]




            # storing lat, long of each pixel
            lat1 = []
            lat2 = []
            long1 = []
            long2 = []
            upper_left_long1 = upper_left_long
            upper_left_long2 = upper_left_long
            upper_left_lat1 = upper_left_lat
            upper_left_lat2 = upper_left_lat

            # top lat and left long
            for i in range(tir1_gray.shape[0]):
                for j in range(tir1_gray.shape[1]):
                    lat1.append(upper_left_lat1)
                    long1.append(upper_left_long1)
                    upper_left_long1 = upper_left_long1 + (upper_right_long - upper_left_long1) / tir1_gray.shape[1]
                upper_left_long1 = upper_left_long
                upper_left_lat1 = upper_left_lat1 - (upper_left_lat1 - lower_left_lat) / tir1_gray.shape[0]

            # bottom lat and right long
            for i in range(tir1_gray.shape[0]):
                upper_left_lat2 = upper_left_lat2 - (upper_left_lat2 - lower_left_lat) / tir1_gray.shape[0]
                for j in range(tir1_gray.shape[1]):
                    upper_left_long2 = upper_left_long2 + (upper_right_long - upper_left_long2) / tir1_gray.shape[1]
                    lat2.append(upper_left_lat2)
                    long2.append(upper_left_long2)
                upper_left_long2 = upper_left_long





            # storing tir1 temperature in a list
            tir1_t = []
            for i in range(tir1_gray.shape[0]):
                for j in range(tir1_gray.shape[1]):
                    tir1_t.append(temp_tir1[i][j])

            # storing tir2 temperature in a list
            tir2_t = []
            for i in range(tir2_gray.shape[0]):
                for j in range(tir2_gray.shape[1]):
                    tir2_t.append(temp_tir2[i][j])

            # storing mir temperature in a list
            mir_t = []
            for i in range(mir_gray.shape[0]):
                for j in range(mir_gray.shape[1]):
                    mir_t.append(temp_mir[i][j])

            # storing swir radiance in a list
            swir_r = []
            for i in range(swir_gray.shape[0]):
                for j in range(swir_gray.shape[1]):
                    swir_r.append(rad_swir[i][j])

            # storing vis radiance (albedo) in a list
            vis_r = []
            for i in range(vis_gray.shape[0]):
                for j in range(vis_gray.shape[1]):
                    vis_r.append(rad_vis[i][j])

            # storing wv radiance in a list
            wv_r = []
            for i in range(wv_gray.shape[0]):
                for j in range(wv_gray.shape[1]):
                    wv_r.append(rad_wv[i][j])

            # storing wv temperature in a list
            wv_t = []
            for i in range(wv_gray.shape[0]):
                for j in range(wv_gray.shape[1]):
                    wv_t.append(temp_wv[i][j])





            # storing time
            time = []
            for i in range(len(tir1_t)):
                time.append(time_crop)

            # storing date
            date = []
            for i in range(len(tir1_t)):
                date.append(day)




            # Writing to a csv file
            data = np.empty((13, len(tir1_t),))
            data[0] = lat1
            data[1] = lat2
            data[2] = long1
            data[3] = long2
            data[4] = date
            data[5] = time
            data[6] = tir1_t
            data[7] = tir2_t
            data[8] = mir_t
            data[9] = swir_r
            data[10] = vis_r
            data[11] = wv_r
            data[12] = wv_t

            data = list(map(list, zip(*data)))
            index = []

            for i in range(len(data)):
                if data[i][0] < 25.0 and data[i][1] > 20.0 and data[i][2] > 68.0 and data[i][3] < 75.0:
                    index.append(i)




            dataset = np.zeros((len(index), 43))
            count = 0
            for i in index:
                dataset[count][0] = data[i][0]
                dataset[count][1] = data[i][1]
                dataset[count][2] = data[i][2]
                dataset[count][3] = data[i][3]
                dataset[count][4] = data[i][4]
                dataset[count][5] = data[i][5]
                dataset[count][6] = data[i][6]
                dataset[count][7] = data[i][7]
                dataset[count][8] = data[i][8]
                dataset[count][9] = data[i][9]
                dataset[count][10] = data[i][10]
                dataset[count][11] = data[i][11]
                dataset[count][12] = data[i][12]
                dataset[count][13] = elevation[count]
                if int(d[0:2]) < 16:
                    dataset[count][14] = vegetation1[count]
                else:
                    dataset[count][14] = vegetation2[count]
                count = count + 1




            # tir1 neighbourhood
            columns = 334
            rows = 248
            derived = np.empty((rows, columns,))
            for i in range(rows):
                for j in range(columns):
                    derived[i][j] = dataset[j + i*columns][6]


            # Deriving stats of 3x3 pixel window
            index1 = [-1, 0, 1]
            temp1 = []
            mean3 = []
            std3 = []

            for i in range(derived.shape[0]):
                for j in range(derived.shape[1]):
                    for x in index1:
                        for y in index1:
                            try:
                                temp1.append(derived[i+x][j+y])
                            except IndexError:
                                continue
                    m3 = statistics.mean(temp1)
                    s3 = statistics.stdev(temp1)
                    mean3.append(m3)
                    std3.append(s3)
                    temp1.clear()



            # Deriving stats of 5x5 pixel window
            index2 = [-2, -1, 0, 1, 2]
            temp2 = []
            mean5 = []
            std5 = []

            for i in range(derived.shape[0]):
                for j in range(derived.shape[1]):
                    for x in index2:
                        for y in index2:
                            try:
                                temp2.append(derived[i + x][j + y])
                            except IndexError:
                                continue
                    m5 = statistics.mean(temp2)
                    s5 = statistics.stdev(temp2)
                    mean5.append(m5)
                    std5.append(s5)
                    temp2.clear()


            for i in range(len(elevation)):
                dataset[i][15] = mean3[i]
                dataset[i][16] = std3[i]
                dataset[i][17] = mean5[i]
                dataset[i][18] = std5[i]





            # tir2 neighbourhood
            mean3.clear()
            std3.clear()
            mean5.clear()
            std5.clear()
            columns = 334
            rows = 248
            derived = np.empty((rows, columns,))
            for i in range(rows):
                for j in range(columns):
                    derived[i][j] = dataset[j + i * columns][7]

            # Deriving stats of 3x3 pixel window
            index1 = [-1, 0, 1]
            temp1 = []
            mean3 = []
            std3 = []

            for i in range(derived.shape[0]):
                for j in range(derived.shape[1]):
                    for x in index1:
                        for y in index1:
                            try:
                                temp1.append(derived[i + x][j + y])
                            except IndexError:
                                continue
                    m3 = statistics.mean(temp1)
                    s3 = statistics.stdev(temp1)
                    mean3.append(m3)
                    std3.append(s3)
                    temp1.clear()

            # Deriving stats of 5x5 pixel window
            index2 = [-2, -1, 0, 1, 2]
            temp2 = []
            mean5 = []
            std5 = []

            for i in range(derived.shape[0]):
                for j in range(derived.shape[1]):
                    for x in index2:
                        for y in index2:
                            try:
                                temp2.append(derived[i + x][j + y])
                            except IndexError:
                                continue
                    m5 = statistics.mean(temp2)
                    s5 = statistics.stdev(temp2)
                    mean5.append(m5)
                    std5.append(s5)
                    temp2.clear()

            for i in range(len(elevation)):
                dataset[i][19] = mean3[i]
                dataset[i][20] = std3[i]
                dataset[i][21] = mean5[i]
                dataset[i][22] = std5[i]





            # mir neighbourhood
            mean3.clear()
            std3.clear()
            mean5.clear()
            std5.clear()
            columns = 334
            rows = 248
            derived = np.empty((rows, columns,))
            for i in range(rows):
                for j in range(columns):
                    derived[i][j] = dataset[j + i * columns][8]

            # Deriving stats of 3x3 pixel window
            index1 = [-1, 0, 1]
            temp1 = []
            mean3 = []
            std3 = []

            for i in range(derived.shape[0]):
                for j in range(derived.shape[1]):
                    for x in index1:
                        for y in index1:
                            try:
                                temp1.append(derived[i + x][j + y])
                            except IndexError:
                                continue
                    m3 = statistics.mean(temp1)
                    s3 = statistics.stdev(temp1)
                    mean3.append(m3)
                    std3.append(s3)
                    temp1.clear()

            # Deriving stats of 5x5 pixel window
            index2 = [-2, -1, 0, 1, 2]
            temp2 = []
            mean5 = []
            std5 = []

            for i in range(derived.shape[0]):
                for j in range(derived.shape[1]):
                    for x in index2:
                        for y in index2:
                            try:
                                temp2.append(derived[i + x][j + y])
                            except IndexError:
                                continue
                    m5 = statistics.mean(temp2)
                    s5 = statistics.stdev(temp2)
                    mean5.append(m5)
                    std5.append(s5)
                    temp2.clear()

            for i in range(len(elevation)):
                dataset[i][23] = mean3[i]
                dataset[i][24] = std3[i]
                dataset[i][25] = mean5[i]
                dataset[i][26] = std5[i]





            # swir neighbourhood
            mean3.clear()
            std3.clear()
            mean5.clear()
            std5.clear()
            columns = 334
            rows = 248
            derived = np.empty((rows, columns,))
            for i in range(rows):
                for j in range(columns):
                    derived[i][j] = dataset[j + i * columns][9]

            # Deriving stats of 3x3 pixel window
            index1 = [-1, 0, 1]
            temp1 = []
            mean3 = []
            std3 = []

            for i in range(derived.shape[0]):
                for j in range(derived.shape[1]):
                    for x in index1:
                        for y in index1:
                            try:
                                temp1.append(derived[i + x][j + y])
                            except IndexError:
                                continue
                    m3 = statistics.mean(temp1)
                    s3 = statistics.stdev(temp1)
                    mean3.append(m3)
                    std3.append(s3)
                    temp1.clear()

            # Deriving stats of 5x5 pixel window
            index2 = [-2, -1, 0, 1, 2]
            temp2 = []
            mean5 = []
            std5 = []

            for i in range(derived.shape[0]):
                for j in range(derived.shape[1]):
                    for x in index2:
                        for y in index2:
                            try:
                                temp2.append(derived[i + x][j + y])
                            except IndexError:
                                continue
                    m5 = statistics.mean(temp2)
                    s5 = statistics.stdev(temp2)
                    mean5.append(m5)
                    std5.append(s5)
                    temp2.clear()

            for i in range(len(elevation)):
                dataset[i][27] = mean3[i]
                dataset[i][28] = std3[i]
                dataset[i][29] = mean5[i]
                dataset[i][30] = std5[i]





            # vis neighbourhood
            mean3.clear()
            std3.clear()
            mean5.clear()
            std5.clear()
            columns = 334
            rows = 248
            derived = np.empty((rows, columns,))
            for i in range(rows):
                for j in range(columns):
                    derived[i][j] = dataset[j + i * columns][10]

            # Deriving stats of 3x3 pixel window
            index1 = [-1, 0, 1]
            temp1 = []
            mean3 = []
            std3 = []

            for i in range(derived.shape[0]):
                for j in range(derived.shape[1]):
                    for x in index1:
                        for y in index1:
                            try:
                                temp1.append(derived[i + x][j + y])
                            except IndexError:
                                continue
                    m3 = statistics.mean(temp1)
                    s3 = statistics.stdev(temp1)
                    mean3.append(m3)
                    std3.append(s3)
                    temp1.clear()

            # Deriving stats of 5x5 pixel window
            index2 = [-2, -1, 0, 1, 2]
            temp2 = []
            mean5 = []
            std5 = []

            for i in range(derived.shape[0]):
                for j in range(derived.shape[1]):
                    for x in index2:
                        for y in index2:
                            try:
                                temp2.append(derived[i + x][j + y])
                            except IndexError:
                                continue
                    m5 = statistics.mean(temp2)
                    s5 = statistics.stdev(temp2)
                    mean5.append(m5)
                    std5.append(s5)
                    temp2.clear()

            for i in range(len(elevation)):
                dataset[i][31] = mean3[i]
                dataset[i][32] = std3[i]
                dataset[i][33] = mean5[i]
                dataset[i][34] = std5[i]






            # wv_rad neighbourhood
            mean3.clear()
            std3.clear()
            mean5.clear()
            std5.clear()
            columns = 334
            rows = 248
            derived = np.empty((rows, columns,))
            for i in range(rows):
                for j in range(columns):
                    derived[i][j] = dataset[j + i * columns][11]

            # Deriving stats of 3x3 pixel window
            index1 = [-1, 0, 1]
            temp1 = []
            mean3 = []
            std3 = []

            for i in range(derived.shape[0]):
                for j in range(derived.shape[1]):
                    for x in index1:
                        for y in index1:
                            try:
                                temp1.append(derived[i + x][j + y])
                            except IndexError:
                                continue
                    m3 = statistics.mean(temp1)
                    s3 = statistics.stdev(temp1)
                    mean3.append(m3)
                    std3.append(s3)
                    temp1.clear()

            # Deriving stats of 5x5 pixel window
            index2 = [-2, -1, 0, 1, 2]
            temp2 = []
            mean5 = []
            std5 = []

            for i in range(derived.shape[0]):
                for j in range(derived.shape[1]):
                    for x in index2:
                        for y in index2:
                            try:
                                temp2.append(derived[i + x][j + y])
                            except IndexError:
                                continue
                    m5 = statistics.mean(temp2)
                    s5 = statistics.stdev(temp2)
                    mean5.append(m5)
                    std5.append(s5)
                    temp2.clear()

            for i in range(len(elevation)):
                dataset[i][35] = mean3[i]
                dataset[i][36] = std3[i]
                dataset[i][37] = mean5[i]
                dataset[i][38] = std5[i]




            # wv neighbourhood
            mean3.clear()
            std3.clear()
            mean5.clear()
            std5.clear()
            columns = 334
            rows = 248
            derived = np.empty((rows, columns,))
            for i in range(rows):
                for j in range(columns):
                    derived[i][j] = dataset[j + i * columns][12]

            # Deriving stats of 3x3 pixel window
            index1 = [-1, 0, 1]
            temp1 = []
            mean3 = []
            std3 = []

            for i in range(derived.shape[0]):
                for j in range(derived.shape[1]):
                    for x in index1:
                        for y in index1:
                            try:
                                temp1.append(derived[i + x][j + y])
                            except IndexError:
                                continue
                    m3 = statistics.mean(temp1)
                    s3 = statistics.stdev(temp1)
                    mean3.append(m3)
                    std3.append(s3)
                    temp1.clear()

            # Deriving stats of 5x5 pixel window
            index2 = [-2, -1, 0, 1, 2]
            temp2 = []
            mean5 = []
            std5 = []

            for i in range(derived.shape[0]):
                for j in range(derived.shape[1]):
                    for x in index2:
                        for y in index2:
                            try:
                                temp2.append(derived[i + x][j + y])
                            except IndexError:
                                continue
                    m5 = statistics.mean(temp2)
                    s5 = statistics.stdev(temp2)
                    mean5.append(m5)
                    std5.append(s5)
                    temp2.clear()

            for i in range(len(elevation)):
                dataset[i][39] = mean3[i]
                dataset[i][40] = std3[i]
                dataset[i][41] = mean5[i]
                dataset[i][42] = std5[i]





            print(day, time_crop)
            out_path = '/media/geospacial2/Data/Out/Sept_2014/' + '%s/' % d[0:2]
            out_file = day + '_' + time_crop
            with open(out_path + '%s.csv' % out_file, 'w+') as f1:
                writer = csv.writer(f1)
                writer.writerow(["Top Latitude", "Bottom Latitude", "Left Longitude", "Right Longitude",
                                 "Date", "Time", "TIR1_T", "TIR2_T", "MIR_T", "SWIR_R", "VIS_A", "WV_R", "WV_T",
                                 "Elevation", "Vegetation", "TIR1_T_M3", "TIR1_T_S3", "TIR1_T_M5", "TIR1_T_S5",
                                 "TIR2_T_M3", "TIR2_T_S3", "TIR2_T_M5", "TIR2_T_S5", "MIR_T_M3", "MIR_T_S3",
                                 "MIR_T_M5", "MIR_T_S5", "SWIR_R_M3", "SWIR_R_S3", "SWIR_R_M5", "SWIR_R_S5",
                                 "VIS_R_M3", "VIS_R_S3", "VIS_R_M5", "VIS_R_S5", "WV_R_M3", "WV_R_S3", "WV_R_M5",
                                 "WV_R_S5", "WV_T_M3", "WV_T_S3", "WV_T_M5", "WV_T_S5"])
                writer.writerows(dataset)
                f1.close()
            f.close()






# retrieving srtm (elevation values)
im = Image.open('/home/geospacial2/PycharmProjects/BTP/srtm_map.tif')

imarray = np.array(im)

elevation = []
for i in range(imarray.shape[0]):
    for j in range(imarray.shape[1]):
        elevation.append(imarray[i][j])




# retrieving ndvi (vegetation values)
im1 = Image.open('/home/geospacial2/PycharmProjects/BTP/ndvi/ocm2_ndvi_filt_01to15_sep2014_v01_01/09012014.tif')
im2 = Image.open('/home/geospacial2/PycharmProjects/BTP/ndvi/ocm2_ndvi_filt_16to30_sep2014_v01_01/09162014.tif')

imarray1 = np.array(im1)
imarray2 = np.array(im2)

vegetation1 = []
for i in range(imarray1.shape[0]):
    for j in range(imarray1.shape[1]):
        if imarray1[i][j] == 255 or imarray1[i][j] == 240 or imarray1[i][j] == 230:
            vegetation1.append(-9999)
        else:
            vegetation1.append(imarray2[i][j])

vegetation2 = []
for i in range(imarray2.shape[0]):
    for j in range(imarray2.shape[1]):
        if imarray2[i][j] == 255 or imarray2[i][j] == 240 or imarray2[i][j] == 230:
            vegetation2.append(-9999)
        else:
            vegetation2.append(imarray2[i][j])

path = '/media/geospacial2/Data/IR/September_2014'
num_cores = multiprocessing.cpu_count()
for subdir, dirs, files in os.walk(path):
    Parallel(n_jobs=num_cores-1)(delayed(ir)(file, elevation, vegetation1, vegetation2) for file in files)

