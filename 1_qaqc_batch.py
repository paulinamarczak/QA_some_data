#... spatial outputs qa/qc 
#Script outputs table that checks for certain number of pixels/sum of various output ... folders

#Paulina Marczak,November 19 2019
# Import system modules
import os
from os import listdir
from os.path import isfile, join
import time

#Import necessary modules
import csv
import sys
import numpy as np
import shutil
#import errno
import rasterio
try:
    from numba import njit, prange
except ImportError:
    print ("Numba not found-install from pip")
import numba as nb
try:
    from multiprocessing import Pool
except ImportError:
    print ("Pool not found")
import click
try:
    import pandas as pd
except ImportError:
    print ("No pandas found- must install from pip")


#set workspace to workspace of script
script_dir = os.path.dirname(os.path.realpath(__file__))
qa_dir= os.path.join(script_dir, "qa_dir")
qa_raw_output_folder= os.path.join(qa_dir, "qa_raw_output_folder")
print ("Starting at:", (time.strftime('%a %H:%M:%S')))
print ("Importing modules.")

if not os.path.exists(qa_raw_output_folder):
    os.makedirs(qa_raw_output_folder)

#check_folder= "M:\\..."
#check_folder= "O:\\..."

#print "Accessing network folder to copy QA layer data from", check_folder

# # #Remove unecessary folders
# mylist_qaqc = ['log_summary.txt', 'config', 'compiled']

# # def copy_qa(src, dest):
# #     try:
# #         if os.path.exists(qa_dir):
# #             shutil.rmtree(qa_dir)
# #         shutil.copytree(src, dest, ignore=shutil.ignore_patterns(*mylist_qaqc))
# #         #and ignore matching file extensions
# #     except OSError as e:
# #         # If the error was caused because the source wasn't a directory
# #         if e.errno == errno.ENOTDIR:
# #             shutil.copy(src, dest)
# #         else:
# #             print('Directory not copied. Error: %s' % e)

# # copy_qa(check_folder, qa_dir)

# print ("Getting list of subdirectories from each local biomass directory")
# file_list = [f for f in listdir(qa_dir) if isdir(join(qa_dir, f))]

# value_raster_dir= []

# print ("Appending subdirectory locations to one list")
# # get value_raster_dir which is list of the 4 directories in biomass folder
# #list of 4 draw folders so that i can iterate within them and get tif names
# for file in file_list[:]:
#         value_path = os.path.join(qa_dir, file)
#         value_list= os.listdir(value_path)
#         for value in value_list[:]:
#             value= os.path.join(value_path, value)
#             value_raster_dir.append(value)

# ####gives all tifs in subdirectories
# biomass_list=[]
# for i in value_raster_dir[:]:
#     filelist= []
#     for r,d,f in os.walk(i):
#         #for file in each raster directory
#         for file in f:
#             filelist.append(os.path.join(r, file))
#         for entry in filelist[:]:
#                 if not entry.endswith(".tif"):
#                     filelist.remove(entry)
#     biomass_list.append(filelist)

def qa_qc_process(data_folder, layer_type, scenario, year, MC_Spatial_Layer, projected_scenario, draw):

    qa_dataframe= pd.DataFrame(columns= ['LAYER_NAME', 'DRAW', 'YEAR', 'NUMBER_OF_PIXELS', 'NUMBER_OF_PIXELS_WITH_DATA', 'NUMBER_OF_PIXELS_NODATA', 'AVERAGE_VALUE_HA_LYR_ONLY', 'MIN_VALUE', 'MAX_VALUE', 'SUM_OF_VALUES_ABS_LYR', 'DATA_TYPE', 'NODATA_VALUE'])

    draw_number = str("%03d" % draw)
    draw_name = "draw{}".format(draw_number)
    # Start time
    print(draw_name, "started at:", time.strftime('%a %H:%M:%S'))
    
    year = str(year)
    print("Loading the first layer for year", year)

    src_dir = os.path.join(data_folder, layer_type + scenario +  draw_name + projected_scenario, "output")
    print ("src_dir is", src_dir)
    

    start_tif = os.path.join(src_dir, "{}_{}.tif".format(MC_Spatial_Layer, year))
    with rasterio.open(start_tif,'r') as src:
        print ("start of number crunching", (time.strftime('%a %H:%M:%S')))
        #for masking nodata min
        ARRAY_for_MIN = src.read(1, masked=True)
        print ((time.strftime('%a %H:%M:%S')))
        ARRAY_MIN = np.min(ARRAY_for_MIN)
        print ((time.strftime('%a %H:%M:%S')))
        nodata= src.nodata
        total_sum= np.sum(ARRAY_for_MIN, dtype=np.float64)
        print ((time.strftime('%a %H:%M:%S')))

        #COUNT TRUE NUMBER OF PIXELS SLOWER 
        # vals, counts= np.unique(ARRAY_for_MIN, return_counts=True)
        # pixel_count = np.sum(counts[0:(len(counts) - 1)])
        # print ("pixel_count", pixel_count)
        mean = np.mean(ARRAY_for_MIN)
        print( "mean is", mean)
        #re: as float 64 a little explanation, when doing naive recursive summation
        #at some point you end up adding 1e18 + 1e8, 
        #the difference in exponent is beyond the number of digits a float32 has 
        #and your accumulator simply won't change anymore if you add more elements
        print ((time.strftime('%a %H:%M:%S')))
        arr=src.read()
        #WITHOUT SETTING NODATA TO ZERO, I GET TOTAL AMOUNT OF PIXELS (ROW*COL)
        total_pixels= np.prod(arr.shape)
        print("total pixels", total_pixels)

        print ((time.strftime('%a %H:%M:%S')))
        @njit(parallel=True)
        def parallel_nonzero_count(arr):
            flattened = arr.ravel()
            sum_ = 0
            for i in prange(flattened.size):
                sum_ += flattened[i] != 0
            return sum_

        print (parallel_nonzero_count(arr), "count_loop") #gives 283131425
        print ( (time.strftime('%a %H:%M:%S')))
        no_data_and_data_but_not_zero_pixels= parallel_nonzero_count(arr)

        ####
        arr[arr == src.nodata] = 0
        #count non zero decreases because nodata is now zero
        count_pixels_no_zero_no_no_nodata= parallel_nonzero_count(arr)
        print ("Count non zero and non nodata is", count_pixels_no_zero_no_no_nodata)
        #to get number of nodata do difference between the two non zeroes
        No_data_count = no_data_and_data_but_not_zero_pixels- count_pixels_no_zero_no_no_nodata
        print ("Amount of nodata pixels is", No_data_count)
        #HERE I GET number of nodata
        #now get number of pixels without no_data but with zeroes
        total_non_no_data= total_pixels- No_data_count
        print ("Total number of non_no_data_pixels but including zeroes is", total_non_no_data)
        print ("end of number crunching", (time.strftime('%a %H:%M:%S')))


####
        tempdataframe= pd.DataFrame(columns= ['LAYER_NAME', 'DRAW', 'YEAR', 'NUMBER_OF_PIXELS', 'NUMBER_OF_PIXELS_WITH_DATA', 'NUMBER_OF_PIXELS_NODATA', 'AVERAGE_VALUE_HA_LYR_ONLY', 'MIN_VALUE', 'MAX_VALUE', 'SUM_OF_VALUES_ABS_LYR',  'DATA_TYPE', 'NODATA_VALUE'])
        name=start_tif.split("output\\")[1]

        tempdataframe['LAYER_NAME']= [name]
        tempdataframe['NUMBER_OF_PIXELS'] = total_pixels
        tempdataframe['MAX_VALUE']= np.max(arr) 
        tempdataframe['MIN_VALUE']= float(ARRAY_MIN)
        tempdataframe['NUMBER_OF_PIXELS_NODATA'] = No_data_count
        tempdataframe['NUMBER_OF_PIXELS_WITH_DATA'] = total_non_no_data
        #tempdataframe['AREA']
        tempdataframe['DATA_TYPE'] = src.dtypes
        tempdataframe['NODATA_VALUE'] = src.nodata

        folder_type= start_tif.split("\\output")[0]
        print (folder_type)
        tempdataframe['DRAW']= folder_type.split("bc_fire_uncertainty_harvest_base_fire_high_")[1] + "_high"
        
        if name.startswith("abs_"):
            file2_parts = name.split(".")
            name2_parts = file2_parts[0].split("_")
            year2 = name2_parts[(len(name2_parts) - 1)]
            #must convert to "list type" item thats why square brackets
            tempdataframe['YEAR'] = [year2]
            tempdataframe['SUM_OF_VALUES_ABS_LYR'] = total_sum

        elif name.startswith("ha_"):
            file2_parts = name.split(".")
            name2_parts = file2_parts[0].split("_")
            year2 = name2_parts[(len(name2_parts) - 1)]
            tempdataframe['YEAR'] = [year2]
            AVERAGE_VALUE= mean
            tempdataframe['AVERAGE_VALUE_HA_LYR_ONLY'] = AVERAGE_VALUE
        else:
            
            tempdataframe['YEAR']= name.split("Age_")[1].split(".tif")[0]

        print (tempdataframe)
        print("name is", name)
        tempsave=os.path.join(qa_dir, qa_raw_output_folder, name.split(".tif")[0] + "_"  + draw_name + projected_scenario + "_QA.csv")
        print ("saving outputs to", tempsave)
        tempdataframe.to_csv(tempsave, index=False)
        # qa_dataframe = qa_dataframe.append(tempdataframe)

        print ("Finished at:", (time.strftime('%a %H:%M:%S')))

# draw = 1

# qa_qc_process(data_folder, layer_type, scenario, startyear, endyear, draw)

@click.command()
@click.argument("data_folder")
@click.argument("layer_type")
@click.argument("scenario")
@click.argument("startyear", type=click.INT)
@click.argument("endyear", type=click.INT)
@click.argument("mc_list")
@click.argument("projected_scenario_list")
@click.option("--draw_min", type=click.INT)
@click.option("--draw_max", type=click.INT)
@click.option("--draw_list")
@click.option("-n", "--n_cores", default=3)


def batch(data_folder, layer_type, scenario, startyear, endyear, mc_list, projected_scenario_list, draw_min, draw_max, draw_list, n_cores):
    data_folder = "{}".format(data_folder)
    with Pool(n_cores) as p:
        var_list = []
        if draw_min:
            MC_Spatial_Layers = mc_list.split(" ")
            MC_Spatial_Layers = [str(j) for j in MC_Spatial_Layers]

            projected_scenarios = projected_scenario_list.split(" ")
            projected_scenarios = [str(j) for j in projected_scenarios]

            for year in range(startyear, endyear +1):
                for i in range(draw_min, draw_max + 1):
                    for projected_scenario in projected_scenarios:
                        for MC_Spatial_Layer in MC_Spatial_Layers:
                            iterables = (data_folder, layer_type, scenario, year, MC_Spatial_Layer, projected_scenario, i)
                            var_list.append(iterables)

        elif draw_list:
            draws = draw_list.split(" ")
            draws = [int(j) for j in draws]

            MC_Spatial_Layers = mc_list.split(" ")
            MC_Spatial_Layers = [str(j) for j in MC_Spatial_Layers]

            projected_scenarios = projected_scenario_list.split(" ")
            projected_scenarios = [str(j) for j in projected_scenario]

            for i in draws:
                for projected_scenario in projected_scenarios:
                    for MC_Spatial_Layer in MC_Spatial_Layers:
                        iterables = (data_folder, layer_type, scenario, startyear, endyear, MC_Spatial_Layer, projected_scenario, i)
                        var_list.append(iterables)

        p.starmap(qa_qc_process, var_list)

#if i want draw_list in output, then --draw_list "1 5 6"

if __name__ == "__main__":
    batch()
