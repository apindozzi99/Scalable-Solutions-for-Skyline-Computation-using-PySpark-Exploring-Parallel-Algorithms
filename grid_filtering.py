import random
import pandas as pd
import time
import cdd
import enum
from math import sqrt, atan, floor, ceil, pi
import numpy as np
import sys

# libraries for LP (linear programming)
from pulp import LpMinimize, LpProblem, LpStatus, LpVariable, LpMaximize, PULP_CBC_CMD
import gurobipy as gp

from utils import *

class Container:
    def __init__(self, worstPoint = [], bestPoint = [], dataContained = []):
        #worst tuple
        self.worstPoint = worstPoint
        #best tuple
        self.bestPoint = bestPoint
        self.dataContained = dataContained
        
    def addPoint(self, dataPoint):
        if len(dataPoint) != len(self.worstPoint):
            raise Exception('Datapoint dimension not consistent with container point`s dimensions: ' \
                            + str(len(dataPoint)) + \
                            ' ' + str(len(self.worstPoint)))
        self.dataContained.add(dataPoint)

            
def filtering_containers(containerList):
    nd = []
    containerList.sort(key=lambda x: (min(x.bestPoint)))
    for container in containerList:
        if not container.dataContained: #if dataContained array is not empty
            continue
        bp = container.bestPoint
        dominated = False
        for other in nd:
            if container == other:
                continue
            #if the best point of the selected container is dominated by the worstPoint of one of the other containers 
            if dominates(other.worstPoint, bp):
                dominated = True
                break 
        if dominated:
            continue 
        nd.append(container)
    
    return nd
    
# Finds the skyline of grid containers based on its representative point min and max
def query_containers(datapoints, numSlicesPerDimension = 8):
    
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
    resultingContainers = filtering_containers(containerList)
    input_list = []
    for container in resultingContainers:
        input_list = input_list + container.dataContained
    print('Number of points after filtering: '+ str(len(input_list)))
    return input_list