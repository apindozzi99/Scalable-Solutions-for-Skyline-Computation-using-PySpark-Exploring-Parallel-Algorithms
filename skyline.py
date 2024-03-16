import os
import random
import pandas as pd
import time
import enum
from math import sqrt, atan, floor, ceil, pi
import numpy as np
import sys


from utils import *
from grid_filtering import *

class Skyline:

    def __init__(self):
        pass

    """
    Serial Algorithms
    These are the serial algorithms to compute the skyline 
    In order we have:
    - BNL
    - SFS (with SFS2)
    - SalSa
    - Methods to execute serial algorithms through indexes (which will be used in the parallel algorithms)
    """

    # function that find the skyline using BNL
    def find_skyline_bnl(self, data):
        #Finds the skyline using a block-nested loop.
        if not isinstance(data,list):  #check if data is a list, if not it is casted to list
            data = list(data)
        skyline = []
        c = 0
        for i in data:
            is_dominated = False
            for j in skyline:
                c += 1
                if dominates(j,i):
                    is_dominated = True
                    break
            if is_dominated:
                continue
            # removing dominated points from the window
            to_drop = []
            for k in skyline:
                c += 1
                if dominates(i, k):
                    to_drop.append(k)

            for drop in to_drop:
                skyline.remove(drop)

            skyline.append(i)

        print('comparisons:'+str(c))
            
        return skyline

    def find_skyline_sfs(self, data, weights):

        if not isinstance(data,list):
            data = list(data)

        #sort the dataset using a sort function
        data.sort(key = lambda x: sort_function(x, weights))

        skyline = []
        #c = 0
        # Loop through the rest of the rows
        for i in data:
            is_dominated = False

            for j in skyline:
                #c+=1
                if dominates(j,i):
                    is_dominated = True
                    break

            if is_dominated:
                continue

            skyline.append(i)

        #print('comparisons:'+str(c))
        return skyline



    #function that finds the skyline using SaLSa
    def find_skyline_SaLSa(self, data):
        if not isinstance(data,list):
            data = list(data)
        c = 0
        if len(data)== 0:
            return []
        data.sort(key= lambda x: (min(x), sum(x)))
        
        skyline = []
        
        p_stop = data[0]
        
        for p in data:
            is_dominated = False
            if (min(p) > max(p_stop)) or ((min(p) == max(p_stop)) and p != p_stop):
                break
        
            for i in skyline:
                c += 1
                if dominates(i, p):
                    is_dominated = True
                    break
            
            if not is_dominated:
                if max(p) < max(p_stop):
                    p_stop = p
                
                skyline.append(p)
        print('comparisons: '+str(c))
        return skyline
    

    #unpack each data by it's first index
    def execute_sfs_indexed(self, input_list, weights):
        i = 0
        nd = []
        for el_list in input_list:
            nd.append(el_list[1])
        nd = self.find_skyline_sfs(nd,weights)
        return nd
    
    """
    SFS and Salsa multithread
    """
    def sfs_multithread(self, datapoints, global_set, weights):
        nd = []
        
        datapoints = list(datapoints)
            
        datapoints.sort(key=lambda x: sort_function(x, weights))
        
        for ps in datapoints:
            # ps : potentially non dominated point
            for other in global_set:
                if ps == other:
                    nd.append(ps)
                    break
                dominated = False
                if dominates(other, ps):
                    dominated = True
                    break
                
        return nd

    """
    Parallel Algorithms
    These are the parallel algorithms 
    In order we have:
    - random partitioning (with bnl, sfs and salsa)
    - grid partitioning (with sfs and salsa)
    - angular partitionoing (with sfs and salsa)
    - one-slice partitioning (with sfs)
    """
    
    def random_partitioning_sfs(self, spark, data, weights, numberOfSlices ):
        
        start1 = time.time()

        initialValues = spark.sparkContext.parallelize(data, numberOfSlices).mapPartitions(lambda x: self.find_skyline_sfs(x, weights)).collect()
        end = time.time() - start1
        print("Length of the local skylines after parallel section is : " + str(len(initialValues)) + ", time taken: " + str(end))

        skyline = self.find_skyline_sfs(initialValues, weights)
        end2 = time.time() - start1
        print("Length of the skyline is : " + str(len(skyline)) + ", total time taken: " + str(end2))
        return end2



    #Grid partitioning
    def parallel_grid_partitioning_sfs(self, spark, dataArray, weights, numSlices):
        dimensions = len(dataArray[0])
        start = time.time()
        initialResults = spark.sparkContext.parallelize(dataArray) \
                        .map(lambda x : ( get_grid_partition_index(x, numSlices), x ) )  \
                        .partitionBy(numSlices**dimensions) \
                        .mapPartitions(lambda x : self.execute_sfs_indexed(x, weights), preservesPartitioning=True) \
                        .collect()
        end = time.time()- start
        print('Length of skyline after parallel phase of Grid Partitioning is :' + str(len(initialResults)) + ",time taken: "+ str(end))
        seq_time = time.time()
        finRes = self.find_skyline_sfs(initialResults,weights)
        end_seq = time.time() - seq_time

        print('Length of the skyline is :' + str(len(finRes))+ ', total time: ' + str(end+end_seq))
        return end+end_seq

    
    def naive_grid_partitioning_saLSa(self, spark, dataArray, numSlices = 2):
        dimensions = len(dataArray[0])
        
        print('Number of slices := ' + str(numSlices**dimensions))
        
        start = time.time()
        m2 = spark.sparkContext.parallelize(dataArray, numSlices) \
                        .map(lambda x : ( get_grid_partition_index(x, numSlices), x ) )  \
                        .partitionBy(numSlices**dimensions) \
                        .mapPartitions(self.execute_saLSa_indexed, preservesPartitioning=True) \
                        .collect()
        end = time.time()- start
        print('GP: Length of skyline after parallel phase is :' + str(len(m2)) + ",time taken: "+ str(end))
        seq_time = time.time()
        finRes = self.find_skyline_SaLSa(m2)
        end_seq = time.time() - seq_time

        print('AP: Length of the skyline is :' + str(len(finRes)) + ', total time taken: '+str(end+end_seq))



    #Angular partitioning
    def parallel_angled_partitioning_sfs(self, spark, dataArray, weights, numSlices):
        
        dimensions = len(dataArray[0])

        #numberOfSlices = min(max(8,  ceil((sys.getsizeof(dataArray)/1024/1000) * 0.4 ) ), 24)
        start = time.time()
        # Partition By divides the dataset by the primary key of each tuple
        initialResult = spark.sparkContext.parallelize(dataArray) \
                        .map(lambda x : ( get_angular_partitionIndex(x, dimensions, numSlices)  , x) )  \
                        .partitionBy(numSlices**(dimensions-1)) \
                        .mapPartitions(lambda x : self.execute_sfs_indexed(x, weights), preservesPartitioning=True)  \
                        .collect()
        end = time.time()- start
        print('AP: Length of skyline after parallel phase is :' + str(len(initialResult))+ ", time taken: "+ str(end))
        seq_time = time.time()
        finRes = self.find_skyline_sfs(initialResult,weights)
        end_seq = time.time() - seq_time

        print('AP: Length of the skyline is :' + str(len(finRes))+ ', total time: ' + str(end+end_seq))
        return end+end_seq


    def parallel_angled_partitioning_saLSa(self, spark, dataArray, numSlices = 2):
        dimensions = len(dataArray[0])
        
        start = time.time()
        # Partition By divides the dataset by the primary key of each tuple
        initialResult = spark.sparkContext.parallelize(dataArray, numSlices) \
                        .map(lambda x : ( get_angular_partitionIndex(x, dimensions, numSlices)  , x) )  \
                        .partitionBy(numSlices**(dimensions-1)) \
                        .mapPartitions(self.execute_saLSa_indexed, preservesPartitioning=True)  \
                        .collect()
        end = time.time()- start
        print('AP: Length of skyline after parallel phase is :' + str(len(initialResult))+ ", time taken: "+ str(end))
        seq_time = time.time()
        finRes = self.find_skyline_SaLSa(initialResult)
        end_seq = time.time() - seq_time

        print('AP: Length of the skyline is :' + str(len(finRes)) + ', total time taken: '+str(end+end_seq) )


    #One-slice partitioning
    def sliced_partitioning_sfs(self, spark, datapoints, weights, numPartitions):
        print("Strarting")
        start = time.time()
        datapoints.sort(key = lambda x : x[0])
        sortedData = datapoints
        end = time.time() - start
        print('Time taken for sorting: '+ str(end))

        start1 = time.time()
        initialValues = spark.sparkContext.parallelize(sortedData, numPartitions).mapPartitions(lambda x: self.find_skyline_sfs(x, weights)).collect()
        end = time.time() - start1
        print("Length of the local skylines after parallel section is : " + str(len(initialValues)) + ", time taken: " + str(end))

        skyline = self.find_skyline_sfs(initialValues, weights)
        end2 = time.time() - start1
        print("Length of the skyline is : " + str(len(skyline)) + ", total time taken: " + str(end2))
        return end2
    
    
    """
    Improvement of Parallel Algorithms
    These are some improvements for the parallel algorithms
    In order we have:
    - grid filtering
    - rapresentatives filtering
    - all parallel 
    """

    """
    Grid filtering
    - grid partitioning (sfs, salsa)
    - angular partitioning (sfs, salsa)
    """

    # Grid partitioning with a serial grid filtering phase 
    def sfs_grid_partitioning_with_serial_grid_filtering(self, spark, datapoints, weights, numberOfSlices = 4):

        start = time.time()
        input_list = query_containers(datapoints, numberOfSlices)
        end = time.time() - start
        print('Time for the container serial query: ' + str(end))
        start_parallel = time.time()
        finalResult = self.parallel_grid_partitioning_sfs(spark, input_list, weights)
        end_parallel = time.time() - start_parallel
        print('Total time: '+ str(end_parallel+end))
        return end_parallel
    

    def sfs_angular_partitioning_with_serial_grid_filtering(self, spark, dataset, weights, numberOfSlices = 4):
        start = time.time()


        input_list = query_containers(dataset, numberOfSlices)
        end = time.time() - start
        print('Time for the container serial query: ' + str(end))
        start_parallel = time.time()
        finalResult = self.parallel_angled_partitioning_sfs(spark, input_list, weights)
        end_parallel = time.time() - start_parallel
        print('Total time: '+ str(end_parallel+end))
        return end_parallel
    
    def saLSa_angular_partitioning_with_serial_grid_filtering(self, spark, datapoints, numSlicesPerDimension = 12):
        start = time.time()
        input_list = query_containers(datapoints, numSlicesPerDimension)
        end = time.time() - start
        print(str(end) + ' for the container serial query')
        start_parallel = time.time()
        #finalResult = parallel_sfs(input_list, weights)
        finalResult = self.naive_grid_partitioning_saLSa(spark, input_list)
        end_parallel = time.time() - start_parallel
        print('Total time: '+ str(end_parallel+end))
        return end_parallel


    """
    These are some methods for find the rapresentatives
    In order we have:
    - get_best_rapresentatives
        (based on a certain dimesnions sepcified through the index, we found the rapresentatives so that tha area_covered is maximized)
    - filter_with_memory (and execution function)
    - filter_with_improvement
    - get_best_representatives_sort (it exploits the euclidian distance)
    - get_angular_representatives (and execution function)
    - get_angular_representatives2 (and execution function)
    - grid_repr (also 2 and 3)
        (they exploit the grid filtering to find the representatives)
    """

    def get_best_representatives(self, index, dataset, n):
        # index is the dimension which we should check
        # is a tuple with area_covered as the first element and the n-dimensional point as the second element

        best_n_points = []
        limit = 1/n # default is 0.01
        for i in range(n):
            best_n_points.append((0,[]))
        counter = 0
        for point in dataset:
            counter = counter + 1
            area_covered = 1
            for value in point:
                area_covered = area_covered * (1-value)
            rep_index = floor(point[index]/limit)

            #print("rep_index:", rep_index, "length of best_n_points:", len(best_n_points))
            
            if 0 <= rep_index < n:
                if best_n_points[rep_index][0] < area_covered:
                    best_n_points[rep_index] = (area_covered, point)
        best_n_points = [x[1] for x in best_n_points if x[1]]
        return best_n_points


    def filter_with_memory(self, datapoints, reps, weights, onlyFilter = True):
        
        nd = []
    #     comp = []
    #     comp.insert(0,0) 
        for ps in datapoints:
            print("Point is: ")
            print(ps)
            # ps : potentially non dominated point
            dominated = False
            for rep in reps:
                print("Rep is: ")
                print(rep)
                #comp[0] = comp[0] +1
                if dominates(rep, ps):
                    dominated = True # other point is dominated
                    break
            if not dominated:
                nd.append(ps)
        
        if onlyFilter:
            return nd
        return self.find_skyline_sfs(nd, weights)
    
    def execute_filter_with_memory(self, input_list, repr, weights, onlyFilter):
        i = 0
        nd = []
        for el_list in input_list:
            nd.append(el_list[1])
        nd = self.filter_with_memory(nd, repr, weights, onlyFilter)
        return nd

    def filter_with_improvement(self, datapoints, reps, weights, onlyFilter = False):
        
        nd = []
            
        comp = []
        comp.insert(0,0) 
        
        for ps in datapoints:
            # ps : potentially non dominated point
            dominated = False
            for rep in reps:
                comp[0] = comp[0] +1
                if ps == rep: 
                    break
                if dominates(rep, ps):
                    dominated = True # other point is dominated
                    break
                
            if not dominated:
                nd.append(ps)
        
        if onlyFilter:
            return comp
        return self.find_skyline_sfs(nd, weights)
    
    def get_best_representatives_sort(self, index, dataset, n = 30):
        # index is the dimension which we should check
        # is a tuple with area_covered as the first element and the n-dimensional point as the second element

        best_n_points = []
        
        for i in range(n):
            best_n_points.append((0,[]))
        counter = 0
        for point in dataset:
            value = euclidean_dist(point)
            rep_index = floor(point[index]*n)
            if best_n_points[rep_index][0] == 0 or best_n_points[rep_index][0] > value:
                best_n_points[rep_index] = (value, point)
        best_n_points = [x[1] for x in best_n_points if x[1]]
        return best_n_points
    
    def get_angular_representatives(self, dataset, n, weights):
        if not isinstance(dataset, list):
            dataset = list(dataset)

        dataset.sort(key=lambda x: sort_function(x,weights))
        if len(dataset) >= n:
            return dataset[0:n-1]
        return dataset
    
    
    def execute_get_angular_representatives(self, input_list, numRepr, weights):
        i = 0
        nd = []
        for el_list in input_list:
            nd.append(el_list[1])
        nd = self.get_angular_representatives(nd, numRepr, weights)
        return nd
    
    def get_angular_representatives2(self, dataset, n, weights):
        if not isinstance(dataset, list):
            dataset = list(dataset)

        dataset.sort(key=lambda x: area_function(x), reverse = True)
        if len(dataset) >= n:
            return dataset[0:n-1]
        return dataset
    
    
    def execute_get_angular_representatives2(self, input_list, numRepr, weights):
        i = 0
        nd = []
        for el_list in input_list:
            nd.append(el_list[1])
        nd = self.get_angular_representatives2(nd, numRepr, weights)
        return nd
    

    def grid_repr(self, datapoints, numSlicesPerDimension, weights, numRep):

        limit = 1 / (numSlicesPerDimension)
        #print('iterating - limit: ' +str(limit))
        dimensions = len(datapoints[0])
        num_slices_in_space = numSlicesPerDimension**dimensions
        containerList = []
        # create N square containers with each container having the datapoints contained and an index
        for i in range(num_slices_in_space):
            worst = []
            best =[]
            for j in range(dimensions):
                #inizializzazione worst tuple
                index_w = floor( i / (numSlicesPerDimension**j) ) % numSlicesPerDimension
                worst.insert(j, index_w * limit + limit)
                #inizializzazione best tuple
                index_b = floor( i / (numSlicesPerDimension**j) ) % numSlicesPerDimension
                best.insert(j, index_b * limit)

            containerList.insert(i, Container(worst, best, []))

        for dp in datapoints:
            index = 0
            for i in range(len(dp)):
                if dp[i] >= 1:
                    index = index + floor(0.99999 / limit) * (numSlicesPerDimension**i)
                else:
                    index = index + floor(dp[i] / limit) * (numSlicesPerDimension**i)
            (containerList[index].dataContained).append(dp)

        print("Number of containers before filtering: " + str(len(containerList)))

        print(len(containerList))
        repr = []
        k = 0
        while k < len(containerList):
            #print(k)
            #print(len(containerList[k].dataContained))
            if len(containerList[k].dataContained)==0:
                k += 1
                continue
            containerList[k].dataContained.sort(key=lambda x: sort_function(x,weights))
            #print(containerList[k].dataContained[0:10])
            r = self.find_skyline_sfs(containerList[k].dataContained[0:numRep], weights)
            repr= repr + r
            k =(floor(k/numSlicesPerDimension)+1)*numSlicesPerDimension
        return repr

    
    """
    These are method which exploit the representatives computed through the functions above to find the skyline.
    In order we have:
    - parallel_representative_filtering (uses get_best_rapresentatives in parallel and than filter_with memory to get the parallel skyline)
    - parallel_representative_filtering_angular (as the previous one, but expolits the get_angular_partitionIndex)
    - parallel_representative_filtering_imp (as the parallel_representative_filtering but with filter_with_imporvement)
    - representative_sorted_euclidean
    - representative_sorted_centroids
    - representative_sorted2
    - representative_smallest
    - grid representatives (also 2 and 3)
    """

    #Function that finds the representatives in parallel 
    def parallel_representative_filtering(self, spark, dataAsList, weights, slicesForSorting = 12, onlyFilter = True, numberReps = 30):
        
        start_indexing = time.time()

        representatives = spark.sparkContext.parallelize(dataAsList, len(dataAsList[0]) ) \
                                        .mapPartitionsWithIndex(lambda index, y: self.get_best_representatives(index, y, n = numberReps)) \
                                        .collect()
        end_indexing = time.time() - start_indexing
        print('Time taken to find best reps ' + str(end_indexing))
        print('Length of representatives: ' + str(len(representatives)))
        representatives = self.find_skyline_sfs(representatives, weights)
        print('Length of representatives after skyline query: ' + str(len(representatives)))
        
        start_parallel = time.time()
        parallel_skyline = []
        
        parallel_skyline = spark.sparkContext.parallelize(dataAsList, slicesForSorting)\
                                    .mapPartitions(lambda x : self.filter_with_memory(x, representatives, weights, onlyFilter)) \
                                    .collect()
        end_parallel = time.time() - start_parallel
        #print('Number of comparisons: '+ str(parallel_skyline))
        print('Time taken to filter: ' +str(end_parallel))
        print('Length of the data after filter: ' + str(len(parallel_skyline)))
        
        return parallel_skyline

    def parallel_representative_filtering_angular(self, spark, dataAsList, weights, numberReps = 30, numSlices = 5, onlyFilter = True):

        start_indexing = time.time()

        representatives = spark.sparkContext.parallelize(dataAsList, len(dataAsList[0]) ) \
                                        .mapPartitionsWithIndex(lambda index, y: self.get_best_representatives(index, y, n = numberReps)) \
                                        .collect()
        end_indexing = time.time() - start_indexing
        print('Time taken to find best reps ' + str(end_indexing))
        print('Length of representatives: ' + str(len(representatives)))
        representatives = self.find_skyline_sfs(representatives, weights)
        print('Length of representatives after skyline query: ' + str(len(representatives)))

        start_parallel = time.time()
        parallel_skyline = []
        dimensions = len(dataAsList[0])
        parallel_skyline = spark.sparkContext.parallelize(dataAsList) \
                        .map(lambda x : ( get_angular_partitionIndex(x, dimensions, numSlices)  , x) )  \
                        .partitionBy(numSlices**(dimensions-1)) \
                        .mapPartitions(lambda x : self.execute_filter_with_memory(x, representatives, weights, onlyFilter), preservesPartitioning=True)  \
                        .collect()
        end_parallel = time.time() - start_parallel
        #print('Number of comparisons: '+ str(parallel_skyline))
        print('Time taken to filter: ' +str(end_parallel))
        print('Length of the data after filter: ' + str(len(parallel_skyline)))

        return parallel_skyline
    

    """
    All Parallel
    """

    """
    Parallel 3-pahses
    """
    def parallel_3P(self, spark, dataAsList, weights, numReps=30, slicesForSorting = 12):
        #numReps = ceil(len(dataAsList)/1000)
        sortedData = []
        print('Slices for representative skyline ' + str(slicesForSorting))
        start = time.time()
        #merge sort on the first 
        #if len(dataAsList) <= 10**5:
        #    dataAsList.sort(key = lambda x : x[0])
        #    sortedData = dataAsList
        #else:
        #    sortedData = spark.sparkContext.parallelize(dataAsList, max(12,slicesForSorting)).sortBy(lambda x: x[0]).collect()
        dataAsList.sort(key = lambda x : x[0])
        sortedData = dataAsList
        end = time.time() - start
        print('Time taken for sorting: '+ str(end))
        
        start_indexing = time.time()
        representatives = spark.sparkContext.parallelize(sortedData, len(dataAsList[0])) \
                                    .mapPartitionsWithIndex(lambda index, y: self.get_best_representatives(index, y, numReps)) \
                                    .collect()
        end_indexing = time.time() - start_indexing
        print('Length of representatives: ' + str(len(representatives)) + ', time taken to find: '+ str(end_indexing))

        start_parallel = time.time()
        parallel_skyline = spark.sparkContext.parallelize(sortedData, slicesForSorting)\
                                    .mapPartitions(lambda x : self.filter_with_memory(x, representatives, weights, onlyFilter = False)) \
                                    .collect()
        end_parallel = time.time() - start_parallel
        print('Length of the parallel section skyline: ' + str(len(parallel_skyline))+ ', time taken to find it: '+ str(end_parallel))
        
        return parallel_skyline

    

    """
    All parallel sfs
    """

    def AllParallel_sfs(self, spark, datapoints, weights, numPartFirst=100, numPartSecond=100, numReps = 30):

        start = time.time()
        parallel_skyline = self.parallel_3P(spark, datapoints, weights, numReps, numPartFirst)
        end = time.time()-start
        start_serial = time.time()
        #numberOfSlices = min(max(8,  ceil((sys.getsizeof(parallel_skyline)/1024/1000) * 0.4 ) ), 24)
        parallel_global_skyline = spark.sparkContext.parallelize(parallel_skyline, numPartSecond)\
                                    .mapPartitions(lambda x : self.sfs_multithread(x, parallel_skyline, weights)) \
                                    .collect()    

        end_serial = time.time() - start_serial
        print('Length of the skyline: ' + str(len(parallel_global_skyline)) + ', Time taken to find: ' + str(end_serial))
        print('Total time taken with representatives: ' + str(end_serial+end))
        return end_serial+end


    """
    One slice repr-filtering
    """
    
    def one_slice_repr_filtering(self, spark, dataset, weights, slices = 30, numOfRepr = 30):
        start_time = time.time()
        parallel_skyline = self.parallel_3P(spark, dataset, weights, slicesForSorting=slices, numReps=numOfRepr)
        global_skyline = self.find_skyline_sfs(parallel_skyline, weights)
        end_time = time.time() - start_time
        print("Total time for computing the gloabl_skyline is :" + str(end_time))
        return global_skyline, end_time
    

    """
    Here we have:
    - angular partitioning sfs applied to a filtered dataset thourgh representatives (found applying the angular method)
    - angular partitioning sfs applied to a filtered dataset thourgh representatives (found applying the dominance region method)
    - one slice partitioning sfs applied to a filtered dataset thourgh representatives (found applying the angular method)
    - one slice partitioning sfs applied to a filtered dataset thourgh representatives (found applying the dominance region method)
    - random partitioning sfs applied to a filtered dataset thourgh representatives (found applying the dominance region method)
    - angular partitioning sfs applied to a filtered dataset thourgh grid filtering
    - one slice partitioning sfs applied to a filtered dataset thourgh grid filtering
    """

    def angular_partition_with_sfs_representative_filtering_angular(self, spark, dataAsList, weights, numSlices, numberReps = 100, onlyFilter = True):
        dimensions = len(dataAsList[0])
        start_indexing = time.time()

        representatives = spark.sparkContext.parallelize(dataAsList) \
                    .map(lambda x : ( get_angular_partitionIndex(x, dimensions, numSlices)  , x) )  \
                    .partitionBy(numSlices**(dimensions-1)) \
                    .mapPartitions(lambda x : self.execute_get_angular_representatives(x, numberReps, weights), preservesPartitioning=True)  \
                    .collect()
        end_indexing = time.time() - start_indexing
        print('Time taken to find best reps ' + str(end_indexing))
        print('Length of representatives: ' + str(len(representatives)))
        representatives = self.find_skyline_sfs(representatives, weights)
        print('Length of representatives after skyline query: ' + str(len(representatives)))

        start_parallel = time.time()
        parallel_skyline = []
        
        parallel_skyline = spark.sparkContext.parallelize(dataAsList) \
                        .map(lambda x : ( get_angular_partitionIndex(x, dimensions, numSlices)  , x) )  \
                        .partitionBy(numSlices**(dimensions-1)) \
                        .mapPartitions(lambda x : self.execute_filter_with_memory(x, representatives, weights, onlyFilter), preservesPartitioning=True)  \
                        .collect()
        end_parallel = time.time() - start_parallel
        #print('Number of comparisons: '+ str(parallel_skyline))
        print('Time taken to filter: ' +str(end_parallel))
        print('Length of the data after filter: ' + str(len(parallel_skyline)))
        """
        start_serial = time.time()
        global_skyline = self.find_skyline_sfs(parallel_skyline, weights)
        end_serial = time.time() - start_serial
        end_time = end_serial + end_parallel
        print("Length of the global skyline is: " + str(len(global_skyline)))
        print("Time taken to find the global skyline: " + str(end_time))
        """
        return end_parallel
    

    def angular_partition_with_sfs_representative_filtering_dominance_region(self, spark, dataAsList, weights, numberOfSlices, numberOfReps = 30, onlyFilter1 = True):
        start_parallel = time.time()
        parallel_skyline = self.parallel_representative_filtering_angular(spark, dataAsList, weights,  numberOfSlices, onlyFilter1, numberOfReps)
        end_parallel = time.time() - start_parallel
        print('Length of the data after filter: ' + str(len(parallel_skyline)))
        """
        start_serial = time.time()
        global_skyline = self.find_skyline_sfs(parallel_skyline, weights)
        end_serial = time.time() - start_serial
        end_time = end_serial + end_parallel
        print("Length of the global skyline is: " + str(len(global_skyline)))
        print("Time taken to find the global skyline: " + str(end_time))
        """
        print("Time taken to find the parallel skyline: " + str(end_parallel))
        return end_parallel
    

    def one_slice_with_sfs_representative_filtering_angular(self, spark, dataAsList, weights, numPartitions, numSlices, numberReps = 100, onlyFilter = True):

        start_indexing = time.time()
        dimensions = len(dataAsList[0])

        representatives = spark.sparkContext.parallelize(dataAsList) \
                    .map(lambda x : ( get_angular_partitionIndex(x, dimensions, numSlices)  , x) )  \
                    .partitionBy(numSlices**(dimensions-1)) \
                    .mapPartitions(lambda x : self.execute_get_angular_representatives(x, numberReps, weights), preservesPartitioning=True)  \
                    .collect()
        end_indexing = time.time() - start_indexing
        print('Time taken to find best reps ' + str(end_indexing))
        print('Length of representatives: ' + str(len(representatives)))
        representatives = self.find_skyline_sfs(representatives, weights)
        print('Length of representatives after skyline query: ' + str(len(representatives)))

        start_parallel = time.time()

        dataAsList.sort(key = lambda x : x[0])
        sortedData = dataAsList

        parallel_skyline = []
        parallel_skyline = spark.sparkContext.parallelize(sortedData, numPartitions) \
                        .mapPartitions(lambda x : self.filter_with_memory(x, representatives, weights, onlyFilter), preservesPartitioning=True)  \
                        .collect()
        end_parallel = time.time() - start_parallel
        #print('Number of comparisons: '+ str(parallel_skyline))
        print('Time taken to filter: ' +str(end_parallel))
        print('Length of the data after filter: ' + str(len(parallel_skyline)))
        """
        start_serial = time.time()
        global_skyline = self.find_skyline_sfs(parallel_skyline, weights)
        end_serial = time.time() - start_serial
        end_time = end_serial + end_parallel
        print("Length of the global skyline is: " + str(len(global_skyline)))
        print("Time taken to find the global skyline: " + str(end_time))
        """
        return end_parallel

    def one_slice_with_sfs_representative_dominance_region(self, spark, dataAsList, weights, numPartitions, numSlices, numberReps = 100, onlyFilter = True):

            start_indexing = time.time()

            representatives = spark.sparkContext.parallelize(dataAsList, len(dataAsList[0]) ) \
                                        .mapPartitionsWithIndex(lambda index, y: self.get_best_representatives(index, y, n = numberReps)) \
                                        .collect()
            end_indexing = time.time() - start_indexing
            print('Time taken to find best reps ' + str(end_indexing))
            print('Length of representatives: ' + str(len(representatives)))
            representatives = self.find_skyline_sfs(representatives, weights)
            print('Length of representatives after skyline query: ' + str(len(representatives)))

            start_parallel = time.time()

            dataAsList.sort(key = lambda x : x[0])
            sortedData = dataAsList

            parallel_skyline = []
            #dimensions = len(dataAsList[0])
            parallel_skyline = spark.sparkContext.parallelize(sortedData, numPartitions) \
                            .mapPartitions(lambda x : self.filter_with_memory(x, representatives, weights, onlyFilter), preservesPartitioning=True)  \
                            .collect()
            end_parallel = time.time() - start_parallel
            #print('Number of comparisons: '+ str(parallel_skyline))
            print('Time taken to filter: ' +str(end_parallel))
            print('Length of the data after filter: ' + str(len(parallel_skyline)))
            """
            start_serial = time.time()
            global_skyline = self.find_skyline_sfs(parallel_skyline, weights)
            end_serial = time.time() - start_serial
            end_time = end_serial + end_parallel
            print("Length of the global skyline is: " + str(len(global_skyline)))
            print("Time taken to find the global skyline: " + str(end_time))
            """
            return end_parallel
    
    def random_with_sfs_representative_dominance_region(self, spark, dataAsList, weights, numPartitions, numSlices, numberReps = 100, onlyFilter = True):

            start_indexing = time.time()

            representatives = spark.sparkContext.parallelize(dataAsList, len(dataAsList[0]) ) \
                                        .mapPartitionsWithIndex(lambda index, y: self.get_best_representatives(index, y, n = numberReps)) \
                                        .collect()
            end_indexing = time.time() - start_indexing
            print('Time taken to find best reps ' + str(end_indexing))
            print('Length of representatives: ' + str(len(representatives)))
            representatives = self.find_skyline_sfs(representatives, weights)
            print('Length of representatives after skyline query: ' + str(len(representatives)))

            start_parallel = time.time()

            parallel_skyline = []
            #dimensions = len(dataAsList[0])
            parallel_skyline = spark.sparkContext.parallelize(dataAsList, numPartitions) \
                            .mapPartitions(lambda x : self.filter_with_memory(x, representatives, weights, onlyFilter), preservesPartitioning=True)  \
                            .collect()
            end_parallel = time.time() - start_parallel
            #print('Number of comparisons: '+ str(parallel_skyline))
            print('Time taken to filter: ' +str(end_parallel))
            print('Length of the data after filter: ' + str(len(parallel_skyline)))

            start_serial = time.time()
            global_skyline = self.find_skyline_sfs(parallel_skyline, weights)
            end_serial = time.time() - start_serial
            end_time = end_serial + end_parallel
            print("Length of the global skyline is: " + str(len(global_skyline)))
            print("Time taken to find the global skyline: " + str(end_time))

            return end_time
    

    # Angular partitioning with a serial grid filtering phase 
    def sfs_angular_partitioning_with_serial_grid_filtering(self, spark, datapoints, weights, numberOfSlices = 5):
        start = time.time()
        if numberOfSlices == -1 :
            numberOfSlices = min(max(8,  ceil((sys.getsizeof(datapoints)/1024/1000) * 0.4 ) ), 24)

        input_list = query_containers(datapoints)
        end = time.time() - start
        print('Time for the container serial query: ' + str(end))
        start_parallel = time.time()
        finalResult = self.parallel_angled_partitioning_sfs(spark, input_list, weights, numberOfSlices)
        end_parallel = time.time() - start_parallel
        print('Total time: '+ str(end_parallel+end))
        return end_parallel+end
    
    # Angular partitioning with a serial grid filtering phase 
    def sfs_sliced_partitioning_with_serial_grid_filtering(self, spark, datapoints, weights, numOfPartitions, numberOfSlices = 5):
        start = time.time()
        if numberOfSlices == -1 :
            numberOfSlices = min(max(8,  ceil((sys.getsizeof(datapoints)/1024/1000) * 0.4 ) ), 24)

        input_list = query_containers(datapoints)
        end = time.time() - start
        print('Time for the container serial query: ' + str(end))
        start_parallel = time.time()
        finalResult = self.sliced_partitioning_sfs(spark, input_list, weights, numOfPartitions)
        end_parallel = time.time() - start_parallel
        print('Total time: '+ str(end_parallel+end))
        return end_parallel+end
    
    """
    The following are just the classic partitioning algorithms (without the serial pahse)
    """

    ####################################################
    def random_partitioning(self, spark, data, weights, numberOfSlices ):
        
        start1 = time.time()

        initialValues = spark.sparkContext.parallelize(data, numberOfSlices).mapPartitions(lambda x: self.find_skyline_sfs(x, weights)).collect()
        end = time.time() - start1
        print("Length of the local skylines after parallel section is : " + str(len(initialValues)) + ", time taken: " + str(end))
        return end



    #Grid partitioning
    def parallel_grid_partitioning(self, spark, dataArray, weights, numSlices):
        dimensions = len(dataArray[0])
        start = time.time()
        initialResults = spark.sparkContext.parallelize(dataArray) \
                        .map(lambda x : ( get_grid_partition_index(x, numSlices), x ) )  \
                        .partitionBy(numSlices**dimensions) \
                        .mapPartitions(lambda x : self.execute_sfs_indexed(x, weights), preservesPartitioning=True) \
                        .collect()
        end = time.time()- start
        print('Length of skyline after parallel phase of Grid Partitioning is :' + str(len(initialResults)) + ",time taken: "+ str(end))
        return end



    #Angular partitioning
    def parallel_angled_partitioning(self, spark, dataArray, weights, numSlices):
        
        dimensions = len(dataArray[0])

        #numberOfSlices = min(max(8,  ceil((sys.getsizeof(dataArray)/1024/1000) * 0.4 ) ), 24)
        start = time.time()
        # Partition By divides the dataset by the primary key of each tuple
        initialResult = spark.sparkContext.parallelize(dataArray) \
                        .map(lambda x : ( get_angular_partitionIndex(x, dimensions, numSlices)  , x) )  \
                        .partitionBy(numSlices**(dimensions-1)) \
                        .mapPartitions(lambda x : self.execute_sfs_indexed(x, weights), preservesPartitioning=True)  \
                        .collect()
        end = time.time()- start
        print('AP: Length of skyline after parallel phase is :' + str(len(initialResult))+ ", time taken: "+ str(end))
        return end



    #One-slice partitioning
    def sliced_partitioning(self, spark, datapoints, weights, numPartitions):
        print("Strarting")
        start = time.time()
        datapoints.sort(key = lambda x : x[0])
        sortedData = datapoints
        end = time.time() - start
        print('Time taken for sorting: '+ str(end))

        start1 = time.time()
        initialValues = spark.sparkContext.parallelize(sortedData, numPartitions).mapPartitions(lambda x: self.find_skyline_sfs(x, weights)).collect()
        end = time.time() - start1
        print("Length of the local skylines after parallel section is : " + str(len(initialValues)) + ", time taken: " + str(end))

        return end