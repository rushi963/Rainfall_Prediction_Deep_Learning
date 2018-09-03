import csv
import matplotlib.pyplot as plt
rain = list()
with open('rain2014.csv', 'r') as input1:
    reader = csv.reader(input1, delimiter=',')
    # Skipping header row
    header = next(reader)
    for row in reader:
        rain.append(float(row[22]))


plt.hist(rain, range=(0, 50), bins=50, density=True)
plt.xlabel('rain (mm/hour)')
plt.title('PDF for rainfall in 2014')
plt.show()
