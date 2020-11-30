#!/usr/bin/env python
# -*- coding:utf-8 _*-

import csv
import os
from yarn_api_client import ResourceManager
import EnvironmentParser as envParser

resourceManagerAddress = envParser.yarnResourceManagerWebAppAddress
if envParser.getYarnConfig('yarn.resourcemanager.webapp.address') != resourceManagerAddress:
    resourceManagerAddress = envParser.getYarnConfig('yarn.resourcemanager.webapp.address')
userName = envParser.getUsername()
sparkTunerWarehouse = envParser.sparkTunerWarehouse
hibenchHome = envParser.hibenchHome
benchmarkName = ""

def enableSparkStreaming():
    print("Need to optimize SparkStreaming? Please input yes or no:")
    inStr = input()
    enableSparkStreaming = False
    while inStr not in ["yes", "no", "Yes", "No", "YES", "NO"]:
        print("please input yes or no:")
        inStr = input()
    if inStr in ["yes", "Yes", "YES"]:
        enableSparkStreaming = True
    elif inStr in ["no", "No", "NO"]:
        enableSparkStreaming = False
    return enableSparkStreaming

def getApplicationName():
    applicationName = envParser.appName
    return applicationName

applicationName = getApplicationName()

def submitApplicationOnYarn(sparkHome, location, mainClass, propertiesFile = "", applicationName = applicationName):
    if propertiesFile == "":
        propertiesFile = str(sparkTunerWarehouse) + "/SparkTunerReports/tmp/sparktunerConfig.conf"
    command = str(sparkHome) \
              + "/bin/spark-submit " \
              + "--name " + str(applicationName) +" " \
              + "--master yarn " \
              + "--deploy-mode client " \
              + "--properties-file " + str(propertiesFile) + " " \
              + "--class " \
              + str(mainClass) + " " \
              + str(location)
    result = os.system(command)
    return result

def verifyApplicationStatus(yarnApplicationId, status):
    rm = ResourceManager(service_endpoints=['http://' + str(resourceManagerAddress)])
    applicationInformation = rm.cluster_application(application_id=yarnApplicationId).data
    if applicationInformation['app']['finalStatus'] == status:
        return True
    return False

def getApplicationExecutionTime(yarnApplicationId):
    rm = ResourceManager(service_endpoints=['http://' + str(resourceManagerAddress)])
    applicationInformation = rm.cluster_application(application_id=yarnApplicationId).data
    duration = int(applicationInformation['app']['elapsedTime']) / 1000
    return duration

class getClusterMetrics():
    def __init__(self):
        self.rm = ResourceManager(service_endpoints=['http://' + str(resourceManagerAddress)])
        clusterMetricsInJson = self.rm.cluster_metrics().data
        self.clusterMetrics = clusterMetricsInJson['clusterMetrics']

    def getMemoryUsage(self):
        totalMB = self.clusterMetrics['totalMB']
        allocatedMB = self.clusterMetrics['allocatedMB']
        memoryUsage = allocatedMB / totalMB
        return memoryUsage

    def getCpuCoreUsage(self):
        totalVirtualCore = self.clusterMetrics['totalVirtualCores']
        allocatedVirtualCore = self.clusterMetrics['allocatedVirtualCores']
        cpuCoreUsage = allocatedVirtualCore / totalVirtualCore
        return cpuCoreUsage

def killApplicationByApplicaitonId(applicationId):
    command = "yarn application -kill " + str(applicationId)
    os.system(command)
    print(str(applicationId) + " has been kill")

def killStreamingApplicationByProcessId():
    command = "kill -9 $(ps -ef|grep report/" + str(benchmarkName) + " |gawk '$0 !~/grep/ {print $2}' |tr -s '\n' ' ')"
    os.system(command)

def killApplicationsByApplicationName():
    command = "for i in `yarn application -list | grep -w "+ str(applicationName) +"| awk '{print $1}' | grep application_`; do yarn application -kill $i; done"
    os.system(command)

def killApplicationsByUserName():
    command = "for i in `yarn application -list | grep -w "+ str(userName) +"| awk '{print $1}' | grep application_`; do yarn application -kill $i; done"
    os.system(command)

def collectHibenchInformation(enableSparkStreaming):
    global applicationName, benchmarkName
    if enableSparkStreaming:
        modelsWithBenchmarks = envParser.streamingModelsWithBenchmarks
        hibenchModel = "streaming"
        print("select one benchmark of " + str(hibenchModel) + " to be optimized:" + "\n"
              + "(" + "/".join(str(i) for i in modelsWithBenchmarks[hibenchModel]) + ")")
        benchmark = input()
        while benchmark not in modelsWithBenchmarks[hibenchModel]:
            print("Error input, please check and input the right benchmark name:" + "\n"
                  + "(" + "/".join(str(i) for i in modelsWithBenchmarks[hibenchModel]) + ")")
            benchmark = input()
        applicationName = benchmark
    else:
        modelsWithBenchmarks = envParser.modelsWithBenchmarks
        print("select the model to be optimized:" + "\n"
              + "(graph/micro/ml/sql/websearch)")
        hibenchModel = input()
        while hibenchModel not in modelsWithBenchmarks:
            print("Error input, please check and input the right model name:" + "\n"
                  + "(graph/micro/ml/sql/websearch)")
            hibenchModel = input()
        print("select one benchmark of " + str(hibenchModel) + " to be optimized:" + "\n"
              + "(" + "/".join(str(i) for i in modelsWithBenchmarks[hibenchModel]) + ")")
        benchmark = input()
        while benchmark not in modelsWithBenchmarks[hibenchModel]:
            print("Error input, please check and input the right benchmark name:" + "\n"
                  + "(" + "/".join(str(i) for i in modelsWithBenchmarks[hibenchModel]) + ")")
            benchmark = input()
        applicationName = benchmark.capitalize()
    benchmarkName = benchmark
    if benchmark in envParser.benchmarkAppName:
        applicationName = envParser.benchmarkAppName[benchmark]
    hibenchInfoDic = {
        'hibenchModel': hibenchModel,
        'benchmark': benchmark,
    }
    return hibenchInfoDic

def genSeedDataSetForSparkStreamingHibench(benchmarkName):
    command = hibenchHome + "/bin/workloads/streaming/" + str(benchmarkName) + "/prepare/genSeedDataset.sh"
    os.system(command)

def getTopicNameForSparkStreamingHibench(benchmarkName):
    command = "kafka-topics.sh --zookeeper localhost:2181 --list"
    res = os.popen(command)
    allTopics = res.readlines()
    benchmarkNameTopics = []
    time = []
    for t in allTopics:
        if "SPARK_" + benchmarkName in t:
            benchmarkNameTopics.append(t.replace("\n", ""))
            time.append(t.replace("\n", "")[-13:])
    return benchmarkNameTopics[time.index(max(time))]

def genMetricsReportForSparkStreamingHibench(topicName, benchmarkName):
    command = hibenchHome + "/bin/workloads/streaming/" + benchmarkName + "/common/metrics_reader.sh " + topicName
    res = os.system(command)
    if res != 0:
        return False
    else:
        reportLocation = hibenchHome + "/report/" + topicName + ".csv"
        return reportLocation

def getRatioOfThroughputToLatency(metricsFileLocation):
    with open(metricsFileLocation, 'rt') as csvfile:
        next(csvfile)
        reader = csv.reader(csvfile)
        metrics = []
        for row in reader:
            metrics.append(row)
    throughput = float(metrics[0][2])
    meanLatency = float(metrics[0][4])
    return throughput / meanLatency * 100


class getRunningApplicationIdThread:
    def __init__(self):
        self._running = True

    def getApplicationId(self):
        self.applicationId = ""
        self.applicationId = self.run()

    def terminate(self):
        self._running = False

    def run(self):
        rm = ResourceManager(service_endpoints=['http://' + str(resourceManagerAddress)])
        yarnApplicationId = ""
        while self._running and yarnApplicationId == "":
            applicationInformations = rm.cluster_applications().data
            applications = applicationInformations['apps']['app']
            for i in range(len(applications)):
                if str(applications[i]['state']) == "RUNNING" and (str(applicationName) in applications[i]['name'] or str(applicationName.upper()) in applications[i]['name']):
                    yarnApplicationId = applications[i]['id']
                    return yarnApplicationId
