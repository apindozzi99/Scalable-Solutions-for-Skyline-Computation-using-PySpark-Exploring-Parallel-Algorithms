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


### Data generation configuration

# Type of dataset generation
class DataGenEnum(enum.Enum):
    antiCorrelated = 1
    anticorrelated = 1
    Anticorrelated = 1
    AntiCorrelated = 1
    correlated = 2
    Correlated = 2
    Independent = 3
    independent = 3
    
    
class DataGenConfig():
    def __init__(self, typeOfCorrelation = DataGenEnum.independent, 
                 dataRange = [0,1], avg = 0.5, skylinePercentage = 1,
                 numberOfData = 10**6, numberOfDimensions = 4,
                 spreadPercentage = 10): 
        self.typeOfCorrelation = typeOfCorrelation
        self.dataRange = dataRange
        # UNUSED Variable
        self.avg = avg
        self.skylinePercentage = skylinePercentage
        self.numberOfData = numberOfData
        self.numberOfDimensions = numberOfDimensions
        self.spreadPercentage = spreadPercentage
        
    def setCorrelated(self):
            self.typeOfCorrelation = DataGenEnum.correlated
    def setAntiCorrelated(self):
            self.typeOfCorrelation = DataGenEnum.antiCorrelated
    def setIndependent(self):
            self.typeOfCorrelation = DataGenEnum.independent 
            
    def setNumberOfData(self, numData):
        self.numberOfData = numData

    def setNumberOfDimensions(self, numDim):
        self.numberOfDimensions = numDim
    
# Method that  creates the different types of datasets based on the distribution   
def dataGenerator(dataConfiguration = None):
    if dataConfiguration == None :
        dataConfiguration = DataGenConfig()
        
    typeOfCorrelation = dataConfiguration.typeOfCorrelation
    dataRange = dataConfiguration.dataRange
    avg = dataConfiguration.avg
    skylinePercentage = dataConfiguration.skylinePercentage
    numberOfData = dataConfiguration.numberOfData
    numberOfDimensions = dataConfiguration.numberOfDimensions
    spreadPercentage = dataConfiguration.spreadPercentage
    
    minDataValue = dataRange[0]
    maxDataValue = dataRange[1]
    data = []
    if typeOfCorrelation == DataGenEnum.independent:
        for i in range(numberOfData):
            datum = []
            for i in range(numberOfDimensions):
                datum.append(random.random()*(maxDataValue-minDataValue)+minDataValue)
            data.append(datum)
    elif typeOfCorrelation == DataGenEnum.correlated:
        for i in range(numberOfData):
            datum = []
            datum.append(random.random()*(maxDataValue-minDataValue)+minDataValue)
            relatedValue = datum[0]
            spread = spreadPercentage * 0.01
            for i in range(1, numberOfDimensions):
                datum.append(relatedValue + ((random.random()-0.5)*spread) )
            data.append(datum)
    else: #typeOfCorrelation = antiCorrelated
        for i in range(numberOfData):
            datum = []
            datum.append(random.random()*(maxDataValue-minDataValue)+minDataValue)
            relatedValue = maxDataValue-datum[0]
            spread = spreadPercentage * 0.01
            for i in range(1, numberOfDimensions):
                datum.append(relatedValue + (relatedValue*(random.random()-0.5)*spread) )
            data.append(datum)
    return data

#function that normalize the data in the dataset
def normalize_data(data):
    if not isinstance(data, list):
        data = list(data)
    return ( 0.999999*(data - np.min(data))) / (np.max(data) - np.min(data))