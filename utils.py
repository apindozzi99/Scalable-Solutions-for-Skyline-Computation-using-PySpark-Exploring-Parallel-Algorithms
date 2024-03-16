import random
import pandas as pd
import time
import cdd
import enum
from math import sqrt, atan, floor, ceil, pi
import numpy as np
import os
import matplotlib.pyplot as plt
import csv

import openpyxl
from openpyxl import Workbook
from openpyxl.utils import get_column_letter

#function that normalize the data in the dataset
def normalize_data(data):
    if not isinstance(data, list):
        data = list(data)
    return ( 0.999999*(data - np.min(data))) / (np.max(data) - np.min(data))

#function to check if a tuple dominates another tuple
def dominates(a, b):
    hasStrict = False
    
    for i in range(len(a)):
        if a[i] < b[i]:
            return False
        elif a[i] > b[i]:
            hasStrict = True
    return hasStrict

#sort function based on weights (e.g. centroid coordinates)
def sort_function(data, weight):
    value = 0
    for i in range(len(data)):
        value += data[i]*weight[i]
    return value


def euclidean_dist(point):
    tot = 0
    for p in point:
        tot = tot + p**2
    return tot 

def area_function(point):
    area_covered = 1
    for value in point:
        area_covered = area_covered * (1-value)
    return area_covered

def computeList(l):

    p =l.strip().split(" ")
    point=[]
    for x in p:
        point.append(float(x))
    return point

#Function that computes the index of the tuples

#Angular partitioning
def get_angular_partitionIndex(datapoint, dimensions, numSlices = 2):
    
    angle = 0
    for i in range(dimensions):
        angle = angle + datapoint[i]**2
        
    anglesArray = []
    ## first is radius then all angles
    for i in range(dimensions):
        if i == 0:
            # radius
            continue
        else:
            angle = angle - (datapoint[i-1]**2) 
            angle = max(0,angle)
            if datapoint[i-1] == 0:
                value = sqrt(angle) / (0.0001)
            else:
                value = sqrt(angle) /  datapoint[i-1]
        anglesArray.append(value)
        
    index = 0
    for i in range(len(anglesArray)):
        index = index + floor(atan(anglesArray[i])*(2/pi)*numSlices) * (numSlices**i)
    return index

#Grid partitioning
def get_grid_partition_index(datapoint, numSlices = 2):
    
    index = 0
    for i in range(len(datapoint)):
        # Maps space from 0 to numSlices ^ dimensions - 1
        if datapoint[i] >= 1:
            index = index + (numSlices-1) * (numSlices**i)
        else:
            index = index + floor(datapoint[i] * numSlices) * (numSlices**i)
    return index



def write_partition_info_to_excel(excel_file, partition_info, finRes):
    points_in_global_skyline = compute_number_of_points_in_global_skyline(partition_info, finRes)
    if os.path.exists(excel_file):
        # If the excel file exists, load its DataFrame
        df = pd.read_excel(excel_file)
    else:
        # If it does not exists, create a new DataFrame
        df = pd.DataFrame(columns=['PartitionID', 'Time', 'Num of Datapoints', 'Local Skyline Length', 'Local skyline points in the global skyline'])

    i = 0
    j = 0
    #handle the list
    while i < len(partition_info):
        row = [partition_info[i], partition_info[i+1], partition_info[i+2], partition_info[i+3], points_in_global_skyline[j]]
        #print("row for the df: ")
        #print(row)
        #df = pd.concat([df, pd.DataFrame(row)], ignore_index=True)
        df.loc[len(df)] = row
        
        i +=5
        j +=1
    # print("To write:")
    # print(df)

    # Scrivi il DataFrame aggiornato nel file Excel
    df.to_excel(excel_file, index=False)


def compute_number_of_points_in_global_skyline(excel_rows, global_sky):
        i = 0
        percentages = []
        #print(global_sky)
        while i < len(excel_rows):
            count = 0
            #print(excel_rows[i+4])
            for point in excel_rows[i+4]:
                if point in global_sky:
                    count+=1
            if excel_rows[i+3] == 0:
                percentages.append(0)
            else:
                #perc = count*100/excel_rows[i+3]
                percentages.append(count)
            i+=5
        return percentages

def create_grouped_df(excel_file):
    df = pd.read_excel(excel_file)
    df = df.sort_values(by='PartitionID')

    # create a DataFrame which will be used for the plot
    grouped_df = pd.DataFrame(columns=['Partitions', 'Id', 'Num of Data', 'Execution Time', 'Skyline Data'])
    i = 0

    # group partitions in set of 100
    while i < 1000:
        grouped_df.loc[len(grouped_df.index)] = [str(i+1) + '-' + str(i+100), i+100, 0, 0, 0]
        i+=100

    # iter through the original df in order to obtain the sum of the number of data each 100 partitions 
    num_of_datapoints = []
    execution_times = []
    num_of_skyline_data = []
    for index1,row_to_update in grouped_df.iterrows():
        data_count = 0
        time_count = 0
        sky_data_count = 0
        for index,row in df.iterrows():
            current_partition = row['PartitionID']
            if current_partition > (row_to_update['Id']-100):
                data_count += row['Num of Datapoints']
                time_count += row['Time']
                sky_data_count +=row['Local Skyline Length']
                if current_partition == row_to_update['Id']:
                    num_of_datapoints.append(data_count)
                    execution_times.append(time_count)
                    num_of_skyline_data.append(sky_data_count)

                    break

    grouped_df['Num of Data'] = num_of_datapoints
    grouped_df['Execution Time'] = execution_times
    grouped_df['Skyline Data'] = num_of_skyline_data

    return grouped_df

def create_df(excel_file):
    df = pd.read_excel(excel_file)
    df = df.sort_values(by='PartitionID')

    return df
   
   

def draw_info_per_partitions_plot(excel_file, title, data=True, time=True, skyline_data = True):
    df = create_grouped_df(excel_file)
    # draw the bar plot
    if data:
        #plt.figure(figsize=(10, 5))
        plt.bar(df['Partitions'], df['Num of Data'], color='blue')
        if skyline_data:
            plt.bar(df['Partitions'], df['Skyline Data'], color='red', alpha=0.5, label="Points in the local skyline")
        plt.title('Number of Data per Partition: ' + title)
        plt.xlabel('Partitions Groups')
        plt.ylabel('Number of Data')
        plt.xticks(rotation=45, ha='right')
        plt.legend()
        plt.show()
    if time:
        #plt.figure(figsize=(10, 5))
        plt.bar(df['Partitions'], df['Execution Time'], color='orange')
        plt.title('Execution Time per Partition: ' + title)
        plt.xlabel('Partitions Groups')
        plt.ylabel('Execution Time (s)')
        plt.xticks(rotation=45, ha='right')
        plt.show()

def draw_info_per_partitions_plot_detailed(excel_file, title, data=True, time=True, skyline_data = True):
    df = create_df(excel_file)
    # draw the bar plot
    if data:
        #plt.figure(figsize=(10, 5))
        plt.bar(df['PartitionID'], df['Num of Datapoints'], color='blue')
        if skyline_data:
            plt.bar(df['PartitionID'], df['Local Skyline Length'], color='red', alpha=0.5, label="Points in the local skyline")
        plt.title('Number of Data per Partition: ' + title)
        plt.xlabel('Partitions Groups')
        plt.ylabel('Number of Data')
        plt.xticks(rotation=45, ha='right')
        plt.legend()
        plt.show()
    if time:
        #plt.figure(figsize=(10, 5))
        plt.bar(df['PartitionID'], df['Time'], color='orange')
        plt.title('Execution Time per Partition: ' + title)
        plt.xlabel('Partitions Groups')
        plt.ylabel('Execution Time (s)')
        plt.xticks(rotation=45, ha='right')
        plt.show()



def to_hyperplane(data, dimension_for_boundaries):
    hyperspace_dataset = []
    partitions_boundaries = []

    dimension = len(data[0])
    for point in data:
        denom = sum(point)
        hyperspace_point = [point_i / denom for point_i in point]
        hyperspace_dataset.append(hyperspace_point)
        
    max_value = max(hyperspace_dataset, key=lambda x: x[dimension_for_boundaries])
    increment = max_value[0]/2
    i = 0
    while i<=max_value[0]:
        partitions_boundaries.append(i)
        i+= increment

    return hyperspace_dataset, partitions_boundaries

def assign_partition(point, partitions_boundaries):
    denom = sum(point)
    hyperspace_point = [point_i / denom for point_i in point]
    for i in range(0, len(partitions_boundaries)):
        if hyperspace_point[0] <= partitions_boundaries[i]:
            return i
        
    return 0

def generate_csv_files_skyline_time(arrays, dataset_type, points_array = None, dimensions_array = None, number_of_partitions = None, number_of_cores = None):
    name_dependency = None
    first_column_name = None
    first_column = None
    if points_array:
        name_dependency = "change_cardinality"
        first_column_name = "number of points"
        first_column = points_array
    if dimensions_array:
        name_dependency = "change_dimensionality"
        first_column_name = "number of dimensions"
        first_column = dimensions_array
    if number_of_partitions:
        name_dependency = "change_partitions"
        first_column_name = "number of partitions"
        first_column = number_of_partitions
    if number_of_cores:
        name_dependency = "change_cores"
        first_column_name = "number of cores"
        first_column = number_of_cores
    for array_name, array_data in arrays.items():
        file_name = f"{dataset_type}_execution_time_{name_dependency}_{array_name}.csv"
        with open(file_name, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([first_column_name, 'execution time'])
            for i, point in enumerate(array_data):
                writer.writerow([first_column[i], point])

def generate_csv_files_skyline_caridnality(arrays, dataset_type, points_array = None, dimensions_array = None, number_of_partitions = None, number_of_cores = None):
    name_dependency = None
    first_column_name = None
    first_column = None
    if points_array:
        name_dependency = "change_cardinality"
        first_column_name = "number of points"
        first_column = points_array
    if dimensions_array:
        name_dependency = "change_dimensionality"
        first_column_name = "number of dimensions"
        first_column = dimensions_array
    if number_of_partitions:
        name_dependency = "change_partitions"
        first_column_name = "number of partitions"
        first_column = number_of_partitions
    if number_of_cores:
        name_dependency = "change_cores"
        first_column_name = "number of cores"
        first_column = number_of_cores
    for array_name, array_data in arrays.items():
        file_name = f"{dataset_type}_skyline_cardinality_{name_dependency}_{array_name}.csv"
        with open(file_name, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([first_column_name, 'local skyline points'])
            for i, point in enumerate(array_data):
                writer.writerow([first_column[i], point])



