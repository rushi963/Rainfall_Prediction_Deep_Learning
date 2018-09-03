# this code reads all the file that is there in the folder of a year and combine them all in sorted manner according
#  to the month, date, time
import csv
import os

input_path = '/media/geospatial-3/Data/Combined_output/2017/'
year = '2017'
month = ['06', '07', '08', '09']
date_1 = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18',
          '19', '20',  '21',  '22',  '23',  '24',  '25',  '26',  '27',  '28',  '29',  '30']
date_2 = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18',
          '19', '20',  '21',  '22',  '23',  '24',  '25',  '26',  '27',  '28',  '29',  '30', '31']
time = ['09', '10', '11', '12', '13', '14', '15', '16', '17', '18']

fileout = csv.writer(open('/media/geospatial-3/Data/Final_combined_output/2017.csv', 'w+'))

fileout.writerow(["Top Latitude", "Bottom Latitude", "Left Longitude", "Right Longitude", "Time", "Date", "TIR1_T",
                  "TIR2_T", "MIR_T", "SWIR_R", "VIS_A", "WV_R", "WV_T", "TIR1_T_M3", "TIR1_T_S3", "TIR1_T_M5",
                  "TIR1_T_S5", "TIR2_T_M3", "TIR2_T_S3", "TIR2_T_M5", "TIR2_T_S5", "MIR_T_M3", "MIR_T_S3", "MIR_T_M5",
                  "MIR_T_S5", "SWIR_R_M3", "SWIR_R_S3", "SWIR_R_M5", "SWIR_R_S5", "VIS_R_M3", "VIS_R_S3", "VIS_R_M5",
                  "VIS_R_S5", "WV_R_M3", "WV_R_S3", "WV_R_M5", "WV_R_S5", "WV_T_M3", "WV_T_S3", "WV_T_M5", "WV_T_S5",
                  "Rain"])
for m in month:
    # for month with 30 days
    if m == '06' or m == '09':
        for d in date_1:
            for t in time:
                file = m + d + year + '_' + t + '.csv'
                try:
                    #print(os.path.join(input_path + '%s/' % m + '%s/' % d, file))
                    with(open(os.path.join(input_path + '%s/' % m + '%s/' % d, file), 'r')) as input1:
                        reader_input = csv.reader(input1, delimiter=',')
                        next(reader_input)
                        for row1 in reader_input:
                            fileout.writerow(row1)
                        print('done ' + file)
                except FileNotFoundError:
                    print('File not found: ', file)
    # for month with 31 days
    else:
        for d in date_2:
            for t in time:
                file = m + d + year + '_' + t + '.csv'
                try:
                    with(open(os.path.join(input_path + '%s/' % m + '%s/' % d, file), 'r')) as input1:
                        reader_input = csv.reader(input1, delimiter=',')
                        next(reader_input)
                        for row1 in reader_input:
                            fileout.writerow(row1)
                            print('done ' + file)
                except FileNotFoundError:
                    print('File not found: ', file)
