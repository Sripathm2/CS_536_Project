import matplotlib.pyplot as plt
import pandas as pd





df = pd.read_csv('Results_0.85.tab',sep = '\t')
df95 = df[df['percentile']==0.85]
df951000 = df95[df95['packet_threshold']==1000]
df951500  = df95[df95['packet_threshold']==1500]
df952000  = df95[df95['packet_threshold']==2000]
df952500  = df95[df95['packet_threshold']==2500]
df953000  = df95[df95['packet_threshold']==3000]



# plt.title('comparision on cache size for percentile = 0.85')
# plt.xlabel('cache size')
# plt.ylabel('accuracy')
# x= df951000['cache_size'].to_list()
# y= df951000['accuracy_lru'].to_list()
# plt.plot(x,y,label='packet threshold=1000')


# y1= df951500['accuracy_lru'].to_list()
# plt.plot(x,y1,label='packet threshold=1500')

# y2= df952000['accuracy_lru'].to_list()
# plt.plot(x,y2,label='packet threshold=2000')

# y3= df952500['accuracy_lru'].to_list()
# plt.plot(x,y3,label='packet threshold=2500')

# y4= df953000['accuracy_lru'].to_list()
# plt.plot(x,y4,label='packet threshold=3000')
# plt.legend()


# plt.savefig('percentile0.85.png')
# plt.show()

# Importing modules
import csv
from xlsxwriter.workbook import Workbook
  
# Input file path
tsv_file = 'Results.tab'
# Output file path
xlsx_file = 'Results.xlsx'
  
# Creating an XlsxWriter workbook object and adding 
# a worksheet.
workbook = Workbook(xlsx_file)
worksheet = workbook.add_worksheet()
  
# Reading the tsv file.
read_tsv = csv.reader(open(tsv_file, 'r', encoding='utf-8'), delimiter='\t')
  
# We'll use a loop with enumerate to pass the data 
# together with its row position number, which we'll
# use as the cell number in the write_row() function.
for row, data in enumerate(read_tsv):
    worksheet.write_row(row, 0, data)
  
# Closing the xlsx file.
workbook.close()


# Input file path
tsv_file = 'Results_0.85.tab'
# Output file path
xlsx_file = 'Results_0.85.xlsx'
  
# Creating an XlsxWriter workbook object and adding 
# a worksheet.
workbook = Workbook(xlsx_file)
worksheet = workbook.add_worksheet()
  
# Reading the tsv file.
read_tsv = csv.reader(open(tsv_file, 'r', encoding='utf-8'), delimiter='\t')
  
# We'll use a loop with enumerate to pass the data 
# together with its row position number, which we'll
# use as the cell number in the write_row() function.
for row, data in enumerate(read_tsv):
    worksheet.write_row(row, 0, data)
  
# Closing the xlsx file.
workbook.close()
