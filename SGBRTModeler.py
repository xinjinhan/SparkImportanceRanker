#!/usr/bin/env python
# -*- coding:utf-8 _*-

import csv

import numpy as np
from sklearn import ensemble, utils
from sklearn.model_selection import train_test_split as trainTestSplit

import EnvironmentParser as envParser

sparkTunerWarehouse = envParser.sparkTunerWarehouse
#sparkTunerWarehouse = "/home/BernardX"

# Load source modeling data
def loadModelingData(*, return_X_y=False):
    dataFileName = str(sparkTunerWarehouse) + "/SparkTunerReports/Normalized_confs.csv"
    with open(dataFileName) as f:
        dataFile = csv.reader(f)
        header = next(dataFile)
        samplingData = []
        for da in dataFile:
            samplingData.append(da)
        NumOfSamples = len(samplingData)
        NumOfFeatures = len(samplingData[0]) - 1
        data = np.empty((NumOfSamples, NumOfFeatures))
        target = np.empty((NumOfSamples,))
        featureNames = np.array(header)

        for i, d in enumerate(samplingData):
            data[i] = np.asarray(d[:-1], dtype=np.float64)
            target[i] = np.asarray(d[-1], dtype=np.float64)

    if return_X_y:
        return data, target

    return utils.Bunch(data=data,
                       target=target,
                       # last column == target value
                       featureNames=featureNames[:-1],
                       filename=dataFileName)


# Divide training set and test set
def divideTrainingSetAndTestSet():
    # diabetes = datasets.load_wine()
    diabetes = loadModelingData()
    # 1/4 testing set; 3/4 training set
    return trainTestSplit(diabetes.data, diabetes.target, test_size=0.1)


# Initialize GradientBoostingRegoressor model
def initializeGradientBoostingRegressor(*data):
    XTrain, XTest, YTrain, YTest = data
    regr = ensemble.GradientBoostingRegressor()
    regr.fit(XTrain, YTrain)
    return regr


# Return the prediction of performance
def predictPerformance(configs):
    XTrain, XTest, YTrain, YTest = divideTrainingSetAndTestSet()
    regr = initializeGradientBoostingRegressor(XTrain, XTest, YTrain, YTest)
    print("Train score ==", round(regr.score(XTrain, YTrain), 7))
    print("Test score == ", round(regr.score(XTest, YTest), 7))
    return regr.predict(configs)

# print(predictPerformance(
#
# [[0.2361111111111111,0.5,0.6141927083333333,1,0.5757575757575758,0.16547064305684994,0.022727272727272728,1,0.6898148148148148,0.20333333333333334,1,0,0,0.015873015873015872,0.6428571428571429,0,0.9583333333333334,0.8095238095238095,0,0,0.8749999999999999,0.7374999999999999,0.14285714285714285,0.2857142857142857,0.10111111111111111,0.6488888888888888,0.4848387096774194,0.7777777777777778,0.3958333333333333,1,0.9666666666666667,0.25,0.9,0.225,0,0.7325,0,1,1,0,0,0,1,1,0,0,1,0.67075,0,0.625,0,1,0.9777777777777777,0.75,0.34933035714285715,0.3333333333333333,0,0,0]]
# ))