#!/usr/bin/env python
# -*- coding:utf-8 _*-

import csv
import os
import getpass
import socket
import sys

from xml.etree import ElementTree as elementTree
from numpy.core.defchararray import isdigit
from yarn_api_client import ResourceManager

sparkHome = os.environ.get('SPARK_HOME')
hadoopHome = os.environ.get('HADOOP_HOME')
hibenchHome = os.environ.get('HIBENCH_HOME')
sparkTunerWarehouse = os.environ.get("SPARKTUNER_WAREHOUSE")
if not os.path.exists(sparkTunerWarehouse):
    sparkTunerWarehouse = sys.path[0]
    print("SPARKTUNER_WAREHOUSE == invalid, set it to the root directory of SparkTuner")
currentWorkPath = os.getcwd()


def getYarnConfig(configName, hadoophome=hadoopHome):
    xmlFile = str(hadoophome) + "/etc/hadoop/yarn-site.xml"
    etobj = elementTree.parse(xmlFile)
    property_list = etobj.findall("./property")
    xmldata = []
    for property in property_list:
        property_dict = {}
        for child in list(property):
            property_dict[child.tag] = child.text
        xmldata.append(property_dict)
    for property in xmldata:
        if configName in property['name']:
            return property['value']


def getHdfsAddress(hadoophome=hadoopHome):
    xmlFile = str(hadoophome) + "/etc/hadoop/core-site.xml"
    etobj = elementTree.parse(xmlFile)
    property_list = etobj.findall("./property")
    xmldata = []
    for property in property_list:
        property_dict = {}
        for child in list(property):
            property_dict[child.tag] = child.text
        xmldata.append(property_dict)
    for property in xmldata:
        if property['name'] == "fs.defaultFS":
            return property['value']
        else:
            if property['name'] == "fs.default.name":
                return property['value']


resourceManagerAddress = getYarnConfig('yarn.resourcemanager.webapp.address')


def getYarnClusterNodeMetrics(metricsName):
    rm = ResourceManager(service_endpoints=['http://' + str(resourceManagerAddress)])
    nodesMetrics = rm.cluster_nodes().data['nodes']['node']
    for m in nodesMetrics:
        if int(m[metricsName]) != 0:
            return int(m[metricsName])


def getYarnClusterMetrics(metricsName):
    rm = ResourceManager(service_endpoints=['http://' + str(resourceManagerAddress)])
    clusterMetricsInJson = rm.cluster_metrics().data
    clusterMetrics = clusterMetricsInJson['clusterMetrics']
    return clusterMetrics[metricsName]


modelsWithBenchmarks = {
    'micro': ['sleep', 'sort', 'terasort', 'wordcount'],
    'sql': ['aggregation', 'join', 'scan'],
    'websearch': ['pagerank'],
    'ml': ['bayes', 'kmeans', 'lr', 'als', 'pca', 'gbt', 'rf', 'svd', 'linear', 'lda', 'svm'],
    'graph': ['nweight'],
}

streamingModelsWithBenchmarks = {
    'streaming': ['fixwindow', 'identity', 'repartition', 'wordcount']
}

benchmarksWithModels = {
    'sleep': 'micro/sleep',
    'sort': 'micro/sort',
    'terasort': 'micro/terasort',
    'wordcount': 'micro/wordcount',
    'aggregation': 'sql/aggregation',
    'join': 'sql/join',
    'scan': 'sql/scan',
    'pagerank': 'websearch/pagerank',
    'bayes': 'ml/bayes',
    'kmeans': 'ml/kmeans',
    'lr': 'ml/lr',
    'als': 'ml/als',
    'pca': 'ml/pca',
    'gbt': 'ml/gbt',
    'rf': 'ml/rf',
    'svd': 'ml/svd',
    'linear': 'ml/linear',
    'lda': 'ml/lda',
    'svm': 'ml/svm',
    'nweight': 'graph/nweight',
}

streamingBenchmarksWithModels = {
    'fixwindow': 'streaming/fixwindow',
    'identity': 'streaming/identity',
    'repartition': 'streaming/repartition',
    'wordcount': 'streaming/wordcount'
}

benchmarks = {
    'sleep': 'sleep',
    'sort': 'sort',
    'terasort': 'terasort',
    'wordcount': 'wordcount',
    'aggregation': 'aggregation',
    'join': 'join',
    'scan': 'scan',
    'pagerank': 'pagerank',
    'bayes': 'bayes',
    'kmeans': 'kmeans',
    'lr': 'lr',
    'als': 'als',
    'pca': 'pca',
    'gbt': 'gbt',
    'rf': 'rf',
    'svd': 'svd',
    'linear': 'linear',
    'lda': 'lda',
    'svm': 'svm',
    'nweight': 'nweight'
}

streamingBenchmarks = {
    'fixwindow': 'fixwindow',
    'identity': 'identity',
    'repartition': 'repartition',
    'wordcount': 'wordcount'
}

benchmarkAppName = {
    'sleep': 'ScalaSleep',
    'sort': 'ScalaSort',
    'terasort': 'ScalaTeraSort',
    'wordcount': 'ScalaWordCount',
    'aggregation': 'ScalaAggregation',
    'join': 'ScalaJoin',
    'scan': 'ScalaScan',
    'pagerank': 'ScalaPageRank',
    'bayes': 'SparseNaiveBayes',
    'kmeans': 'DenseKMeans',
    'lr': 'LogisticRegressionWithLBFGS',
    'als': 'ALS',
    'pca': 'PCAExample',
    'gbt': 'Gradient Boosted Tree',
    'rf': 'RFC',
    'svd': 'SVD',
    'linear': 'LinearRegressionWithSGD',
    'lda': 'LDA',
    'svm': 'SVM',
    'nweight': 'NWeightGraphX'
}

sparkParametersWithSql = [
    'spark.io.compression.zstd.level',
    'spark.io.compression.zstd.bufferSize',
    'spark.executor.instances',
    'spark.driver.cores',
    'spark.driver.memory',
    'spark.memory.offHeap.enabled',
    'spark.memory.offHeap.size',
    'spark.executor.memory',
    'spark.reducer.maxSizeInFlight',
    'spark.shuffle.compress',
    'spark.shuffle.file.buffer',
    'spark.shuffle.sort.bypassMergeThreshold',
    'spark.shuffle.spill.compress',
    'spark.broadcast.compress',
    'spark.kryoserializer.buffer.max',
    'spark.kryoserializer.buffer',
    'spark.rdd.compress',
    'spark.memory.fraction',
    'spark.memory.storageFraction',
    'spark.broadcast.blockSize',
    'spark.executor.cores',
    'spark.storage.memoryMapThreshold',
    'spark.locality.wait',
    'spark.scheduler.revive.interval',
    'spark.executor.memoryOverhead',
    'spark.default.parallelism',
    'spark.shuffle.io.numConnectionsPerPeer',
    'spark.sql.shuffle.partitions',
    'spark.sql.inMemoryColumnarStorage.compressed',
    'spark.sql.inMemoryColumnarStorage.batchSize',
    'spark.sql.inMemoryColumnarStorage.partitionPruning',
    'spark.sql.join.preferSortMergeJoin',
    'spark.sql.sort.enableRadixSort',
    'spark.sql.retainGroupColumns',
    'spark.sql.codegen.maxFields',
    'spark.sql.codegen.aggregate.map.twolevel.enable',
    'spark.sql.cartesianProductExec.buffer.in.memory.threshold',
    'spark.sql.autoBroadcastJoinThreshold',
]

sparkStreamingParameters = [
    'spark.streaming.backpressure.enabled',
    'spark.streaming.backpressure.initialRate',
    'spark.streaming.receiver.writeAheadLog.enable',
    'spark.streaming.unpersist',
    'spark.streaming.kafka.maxRatePerPartition',
    'spark.streaming.driver.writeAheadLog.closeFileAfterWrite',
    'spark.streaming.receiver.writeAheadLog.closeFileAfterWrite']

sparkParametersWithoutTuner = [
    'spark.task.maxFailures',
    'spark.eventLog.enabled',
    'spark.history.fs.logDirectory ',
    'spark.eventLog.dir',
    'spark.eventLog.compress',
    'spark.eventLog.enabled',
    'spark.yarn.maxAppAttempts ',
    'spark.dynamicAllocation.enabled'
]

hibenchApplicaitonExtralHeaders = [
    'Type',
    'Input_data_size',
    'Duration(s)'
]

sparkTunerConfigsDefaultValue = {
    "tuner.node.available.memory": int(getYarnClusterNodeMetrics("availMemoryMB")),
    "tuner.node.available.vcores": int(getYarnClusterNodeMetrics("availableVirtualCores")),
    "tuner.scheduler.maximum-allocation-mb": int(getYarnConfig("yarn.scheduler.maximum-allocation-mb")),
    "tuner.scheduler.maximum-allocation-vcores": int(getYarnConfig("yarn.scheduler.maximum-allocation-vcores")),
    "tuner.scheduler.minimum-allocation-mb": int(getYarnConfig("yarn.scheduler.minimum-allocation-mb")),
    "tuner.scheduler.minimum-allocation-vcores": int(getYarnConfig("yarn.scheduler.minimum-allocation-vcores")),
    "tuner.enable.defaltConfiguration": False,
    "tuner.enable.sparkStreamingTuner": False,
    "tuner.scheduler.numWorkers": int(getYarnClusterMetrics("totalNodes")),
    "tuner.application.name": "SparkTunerApp",
    "tuner.sample.scale": 200,
    "tuner.application.jar.location": None,
    "tuner.application.jar.mainClass": None,
    "tuner.yarn.resourcemanager.webapp.address": None,
    "tuner.root.directory": sys.path[0],
    "tuner.iteration.num": 60,
    "tuner.skip.dataCollection": False
}


def parseConfigurationXml(configName):
    xmlFile = sys.path[0] + "/tuner-site.xml"
    etobj = elementTree.parse(xmlFile)
    property_list = etobj.findall("./property")
    xmldata = []
    for property in property_list:
        property_dict = {}
        for child in list(property):
            property_dict[child.tag] = child.text
        xmldata.append(property_dict)
    for property in xmldata:
        if property['name'] == configName:
            if property['value'] is None:
                return sparkTunerConfigsDefaultValue[configName]
            if isdigit(property['value']) or property['value'] in ['True', 'true', 'False', 'false']:
                return property['value']
            if property['name'] in ['tuner.application.jar.location', 'tuner.application.jar.mainClass',
                                    'tuner.yarn.resourcemanager.webapp.address']:
                return property['value']
            return sparkTunerConfigsDefaultValue[configName]
    return sparkTunerConfigsDefaultValue[configName]


def verifyTrueOrFalse(statement):
    if statement in ["True", "true", "TRUE", "1", "YES", "yes", "Yes"]:
        return True
    else:
        return False


def sparkDefaultsConfParser(configPath=str(sparkHome) + "/conf/spark-defaults.conf"):
    sparktunerConfigTemplate = open(str(sparkTunerWarehouse) + "/SparkTunerReports/tmp/sparktunerConfigTemplate.conf",
                                    "w")
    if os.path.exists(configPath):
        sparkConfigFile = open(configPath, "r")
        allSparkParameters = sparkParametersWithSql + sparkParametersWithoutTuner + sparkStreamingParameters
        for p in sparkConfigFile:
            if ' ' in p:
                line = p[0:p.index(' ')]
                if line not in allSparkParameters:
                    sparktunerConfigTemplate.write(p)
        sparkConfigFile.close()
    sparktunerConfigTemplate.close()


def getBenchmarkNamewithoutModelName(BenchmarkNamewithModelName):
    for i in range(len(BenchmarkNamewithModelName)):
        if BenchmarkNamewithModelName[i] == "/":
            break
    benchmarkNamewithoutModelName = ""
    for j in range(i + 1, len(BenchmarkNamewithModelName)):
        benchmarkNamewithoutModelName += BenchmarkNamewithModelName[j]
    return benchmarkNamewithoutModelName


def getUsername():
    return getpass.getuser()


def getHostname():
    return socket.gethostname()


def getIP():
    return socket.gethostbyname(getHostname())


def getSampleScale():
    samples = 0
    with open(str(sparkTunerWarehouse) + "/SparkTunerReports/Normalized_confs.csv", 'r') as csvfile:
        next(csvfile)
        reader = csv.reader(csvfile)
        for row in reader:
            samples += 1
    return samples

def getConfigOrFeatureNum():
    dataFileName = str(sparkTunerWarehouse) + "/SparkTunerReports/Normalized_confs.csv"
    with open(dataFileName) as f:
        dataFile = csv.reader(f)
        samplingData = []
        for da in dataFile:
            samplingData.append(da)
        NumOfFeatures = len(samplingData[0]) - 1
    return NumOfFeatures

sparkTunerHome = parseConfigurationXml("tuner.root.directory")
yarnResourceManagerWebAppAddress = parseConfigurationXml("tuner.yarn.resourcemanager.webapp.address")
applicationJarMainClass = parseConfigurationXml("tuner.application.jar.mainClass")
applicationJarLoc = parseConfigurationXml("tuner.application.jar.location")
sampleScale = int(parseConfigurationXml("tuner.sample.scale"))
appName = parseConfigurationXml("tuner.application.name")
workerNum = int(parseConfigurationXml("tuner.scheduler.numWorkers"))
nodeAvailableMemory = int(parseConfigurationXml("tuner.node.available.memory"))
enableSparkStreamingTuner = verifyTrueOrFalse(parseConfigurationXml("tuner.enable.sparkStreamingTuner"))
nodeAvailableCore = int(parseConfigurationXml("tuner.node.available.vcores"))
maxAllocatedMemory = int(parseConfigurationXml("tuner.scheduler.maximum-allocation-mb"))
maxAllocatedCore = int(parseConfigurationXml("tuner.scheduler.maximum-allocation-vcores"))
minAllocatedMemory = int(parseConfigurationXml("tuner.scheduler.minimum-allocation-mb"))
minAllocatedCore = int(parseConfigurationXml("tuner.scheduler.minimum-allocation-vcores"))
useDefaultConfig = verifyTrueOrFalse(parseConfigurationXml("tuner.enable.defaltConfiguration"))
iterationTime = int(parseConfigurationXml("tuner.iteration.num"))
skipDataCollection = verifyTrueOrFalse(parseConfigurationXml("tuner.skip.dataCollection"))
