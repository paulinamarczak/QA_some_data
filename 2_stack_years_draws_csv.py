#Stack output csvs together
#copy files to one master QA dataframe
#Paulina Marczak November 27 2019

# Import system modules
import os
import time
import csv
from os import listdir
from os.path import isfile, isdir, join
import pandas as pd 

#set workspace to workspace of script
script_dir = os.path.dirname(os.path.realpath(__file__))
qa_dir= os.path.join(script_dir, "qa_dir")
qa_raw_output_folder= os.path.join(qa_dir, "qa_raw_output_folder")

print ("Getting list of subdirectories from each local biomass directory")
csv_list = [f for f in listdir(qa_raw_output_folder) if isfile(join(qa_raw_output_folder, f))]
print (csv_list)

print ("Merging all QA files together")
qa_dataframe= pd.DataFrame()

for file in csv_list[:]:
	if file.endswith("QA.csv"):
	    new_path= os.path.join(qa_raw_output_folder, file)
	    with open(new_path, mode='r') as infile:
	        reader = csv.reader(infile)
	        df= pd.DataFrame(reader)
	        df= df.drop(df.index[0])
	        df.columns= ['LAYER_NAME', 'DRAW', 'YEAR', 'NUMBER_OF_PIXELS', 'NUMBER_OF_PIXELS_WITH_DATA', 'NUMBER_OF_PIXELS_NODATA', 'AVERAGE_VALUE_HA_LYR_ONLY', 'MIN_VALUE', 'MAX_VALUE', 'SUM_OF_VALUES_ABS_LYR', 'DATA_TYPE', 'NODATA_VALUE']
	        qa_dataframe = qa_dataframe.append(df)
	        #print (qa_dataframe)

masterDFsave=os.path.join(qa_dir, "GCBM_Spatial_Ouputs_QA_file.csv")
qa_dataframe.to_csv(masterDFsave, index=False)
