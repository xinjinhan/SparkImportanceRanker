#!/usr/bin/env python
# coding=utf-8

import csv
import os
import random
import threading
import time

import ApplicationController
import EnvironmentParser as envParser

timeVec = []
failedApplicationId = []
ratioOfThroughputToLatencyVec = []
applicationExecutionTime = 0
currentFailureTimes = 0
upperBoundTime = 180000

yarnApplicationId = ""
startTime = ""
applicationJarLocation = ""
applicationJarMainClass = ""

applicationName = envParser.appName
sparkHome = envParser.sparkHome
hibenchHome = envParser.hibenchHome
sparkTunerWarehouse = envParser.sparkTunerWarehouse
sparkTunerHome = envParser.sparkTunerHome
hdfsAddress = envParser.getHdfsAddress()
enableSparkStreaming = envParser.enableSparkStreamingTuner
workerNodeNum = envParser.workerNum
nodeAvailableMemory = envParser.nodeAvailableMemory
nodeAvailableCore = envParser.nodeAvailableCore
maxAllocatedMemory = envParser.maxAllocatedMemory
maxAllocatedCore = envParser.maxAllocatedCore
minAllocatedMemory = envParser.minAllocatedMemory
minAllocatedCore = envParser.minAllocatedCore
useDefaultConfig = envParser.useDefaultConfig
skipDataCollection = envParser.skipDataCollection


def makeConfig(enableSparkStreaming):
    # Generate parameters of the configuration file in the range of values
    configList = []  # Storage profile
    configVec = []  # Storage profile in vector
    # Fixed configs
    configList.append("----Fixed Configurations----\n")
    # Writer history-server config (Not in the scope of parameter optimization)
    configList.append("spark.eventLog.enabled             true\n")
    configList.append("spark.history.fs.logDirectory      " + str(hdfsAddress) + "/sparktunerLogs\n")
    configList.append("spark.eventLog.dir                 " + str(hdfsAddress) + "/sparktunerLogs\n")
    # configList.append("spark.history.fs.logDirectory      " + "hdfs://hacluster/sparkJobHistory\n")
    # configList.append("spark.eventLog.dir                 " + "hdfs://hacluster/sparkJobHistory\n")
    configList.append("spark.eventLog.compress            true\n")
    configList.append("spark.yarn.maxAppAttempts          1\n")

    configList.append("spark.io.compression.codec         zstd\n")
    configList.append("spark.kryo.referenceTracking       true\n")
    configList.append("spark.serializer                   org.apache.spark.serializer.KryoSerializer\n")
    configList.append("spark.network.timeout              120s\n")
    configList.append("spark.speculation                  false\n")
    configList.append("spark.task.maxFailures             4\n")

    if (useDefaultConfig):
        spark_executor_instances = 2
        configVec.append(spark_executor_instances)
        configList.append("spark.executor.instances      " + str(spark_executor_instances) + "\n")
        configList.append("hibench.yarn.executor.num    " + str(spark_executor_instances) + "\n")

        spark_driver_memory = 1024
        configVec.append(spark_driver_memory)
        configList.append("spark.driver.memory        " + str(spark_driver_memory) + "m\n")

        spark_executor_cores = 2
        configVec.append(spark_executor_cores)
        configList.append("spark.executor.cores      " + str(spark_executor_cores) + "\n")
        configList.append("hibench.yarn.executor.cores      " + str(spark_executor_cores) + "\n")

        spark_executor_memory = 2048
        configVec.append(spark_executor_memory)
        configList.append("spark.executor.memory        " + str(spark_executor_memory) + "m\n")

    else:
        configList.append("\n----SparkTuner Tuned Configurations----\n")
        spark_io_compression_zstd_level = random.randint(1, 5)
        configVec.append(spark_io_compression_zstd_level)
        configList.append("spark.io.compression.zstd.level      " + str(spark_io_compression_zstd_level) + "\n")

        spark_io_compression_zstd_bufferSize = random.randint(1, 6) * 16
        configVec.append(spark_io_compression_zstd_bufferSize)
        configList.append(
            "spark.io.compression.zstd.bufferSize	" + str(spark_io_compression_zstd_bufferSize) + "k\n")

        # The num of executors
        spark_executor_instances = random.randint(workerNodeNum, int(workerNodeNum * nodeAvailableCore * 0.1))
        configVec.append(spark_executor_instances)
        configList.append("spark.executor.instances      " + str(spark_executor_instances) + "\n")
        configList.append("hibench.yarn.executor.num    " + str(spark_executor_instances) + "\n")
        spark_executor_instances_pernode = spark_executor_instances / workerNodeNum

        # Application Propertie
        spark_driver_cores = min(random.randint(minAllocatedCore, int(nodeAvailableCore * 0.25)), maxAllocatedCore)
        configVec.append(spark_driver_cores)
        configList.append("spark.driver.cores         " + str(spark_driver_cores) + "\n")

        spark_driver_memory = min(random.randint(minAllocatedMemory, int(nodeAvailableMemory * 0.25)),
                                  maxAllocatedMemory)
        configVec.append(spark_driver_memory)
        configList.append("spark.driver.memory        " + str(spark_driver_memory) + "m\n")

        spark_memory_offHeap_enabled = random.choice(["true", "false"])
        configVec.append(spark_memory_offHeap_enabled)
        configList.append("spark.memory.offHeap.enabled      " + str(spark_memory_offHeap_enabled) + "\n")

        if nodeAvailableMemory * 0.2 / spark_executor_instances_pernode < 512:
            spark_memory_offHeap_size = 512
        else:
            spark_memory_offHeap_size = min(
                random.randint(512, int(nodeAvailableMemory * 0.2 / spark_executor_instances_pernode)),
                maxAllocatedMemory)
        configVec.append(spark_memory_offHeap_size)
        configList.append("spark.memory.offHeap.size      " + str(spark_memory_offHeap_size) + "m\n")

        if spark_memory_offHeap_enabled == "true":
            spark_executor_memory = min(int(int(
                (nodeAvailableMemory * 0.9 - spark_memory_offHeap_size * spark_executor_instances_pernode)
                / spark_executor_instances_pernode) * random.uniform(0.6, 1)), maxAllocatedMemory)
            spark_inuse_memory = spark_executor_memory + spark_memory_offHeap_size
        else:
            spark_executor_memory = min(int(int(nodeAvailableMemory * 0.9 / spark_executor_instances_pernode)
                                            * random.uniform(0.6, 1)), maxAllocatedMemory)
            spark_inuse_memory = spark_executor_memory
        configVec.append(spark_executor_memory)
        configList.append("spark.executor.memory        " + str(spark_executor_memory) + "m\n")

        # Shuffle Behavior
        spark_reducer_maxSizeInFlight = random.randint(40, 128)
        configVec.append(spark_reducer_maxSizeInFlight)
        configList.append("spark.reducer.maxSizeInFlight          " + str(spark_reducer_maxSizeInFlight) + "m\n")

        spark_shuffle_compress = random.choice(["true", "false"])
        configVec.append(spark_shuffle_compress)
        configList.append("spark.shuffle.compress        " + str(spark_shuffle_compress) + "\n")

        spark_shuffle_file_buffer = random.randint(1, 6) * 16
        configVec.append(spark_shuffle_file_buffer)
        configList.append("spark.shuffle.file.buffer      " + str(spark_shuffle_file_buffer) + "k\n")

        spark_shuffle_sort_bypassMergeThreshold = random.randint(100, 1000)
        configVec.append(spark_shuffle_sort_bypassMergeThreshold)
        configList.append(
            "spark.shuffle.sort.bypassMergeThreshold  " + str(spark_shuffle_sort_bypassMergeThreshold) + "\n")

        spark_shuffle_spill_compress = random.choice(["true", "false"])
        configVec.append(spark_shuffle_spill_compress)
        configList.append("spark.shuffle.spill.compress      " + str(spark_shuffle_spill_compress) + "\n")

        # Compression and Serialization
        spark_broadcast_compress = random.choice(["true", "false"])
        configVec.append(spark_broadcast_compress)
        configList.append("spark.broadcast.compress      " + str(spark_broadcast_compress) + "\n")

        spark_kryoserializer_buffer_max = random.randint(1, 8) * 16
        configVec.append(spark_kryoserializer_buffer_max)
        configList.append("spark.kryoserializer.buffer.max      " + str(spark_kryoserializer_buffer_max) + "m\n")

        spark_kryoserializer_buffer = random.randint(1, 16) * 8
        configVec.append(spark_kryoserializer_buffer)
        configList.append("spark.kryoserializer.buffer      " + str(spark_kryoserializer_buffer) + "k\n")

        spark_rdd_compress = random.choice(["true", "false"])
        configVec.append(spark_rdd_compress)
        configList.append("spark.rdd.compress      " + str(spark_rdd_compress) + "\n")

        # Memory Management
        spark_memory_fraction = round(random.uniform(0.5, 0.9), 2)
        configVec.append(spark_memory_fraction)
        configList.append("spark.memory.fraction      " + str(spark_memory_fraction) + "\n")

        spark_memory_storageFraction = round(random.uniform(0.1, 0.9), 2)
        configVec.append(spark_memory_storageFraction)
        configList.append("spark.memory.storageFraction      " + str(spark_memory_storageFraction) + "\n")

        spark_broadcast_blockSize = random.randint(1, 5)
        configVec.append(spark_broadcast_blockSize)
        configList.append("spark.broadcast.blockSize      " + str(spark_broadcast_blockSize) + "m\n")

        spark_executor_cores = max(min(
            random.randint(minAllocatedCore, int(nodeAvailableCore / spark_executor_instances_pernode)),
            maxAllocatedCore), 5)
        configVec.append(spark_executor_cores)
        configList.append("spark.executor.cores      " + str(spark_executor_cores) + "\n")
        configList.append("hibench.yarn.executor.cores      " + str(spark_executor_cores) + "\n")

        spark_storage_memoryMapThreshold = random.randint(50, 500)
        configVec.append(spark_storage_memoryMapThreshold)
        configList.append("spark.storage.memoryMapThreshold      " + str(spark_storage_memoryMapThreshold) + "m\n")

        # Scheduling
        spark_locality_wait = random.randint(1, 10)
        configVec.append(spark_locality_wait)
        configList.append("spark.locality.wait      " + str(spark_locality_wait) + "s\n")

        spark_scheduler_revive_interval = random.randint(100, 1000)
        configVec.append(spark_scheduler_revive_interval)
        configList.append("spark.scheduler.revive.interval      " + str(spark_scheduler_revive_interval) + "ms\n")

        if int((
                       nodeAvailableMemory - spark_inuse_memory * spark_executor_instances_pernode) / spark_executor_instances_pernode) > 2048:
            if spark_memory_offHeap_enabled == "true":
                spark_executor_memoryOverhead = random.randint(minAllocatedMemory, int(
                    (
                            nodeAvailableMemory - spark_inuse_memory * spark_executor_instances_pernode) / spark_executor_instances_pernode))
            else:
                spark_executor_memoryOverhead = random.randint(2048, int(
                    (
                            nodeAvailableMemory - spark_executor_memory * spark_executor_instances_pernode) / spark_executor_instances_pernode))
        elif int((
                         nodeAvailableMemory - spark_inuse_memory * spark_executor_instances_pernode) / spark_executor_instances_pernode) > 384:
            if spark_memory_offHeap_enabled == "true":
                spark_executor_memoryOverhead = random.randint(384, int(
                    (
                            nodeAvailableMemory - spark_inuse_memory * spark_executor_instances_pernode) / spark_executor_instances_pernode))
            else:
                spark_executor_memoryOverhead = random.randint(384, int(
                    (
                            nodeAvailableMemory - spark_executor_memory * spark_executor_instances_pernode) / spark_executor_instances_pernode))
        else:
            spark_executor_memoryOverhead = 384
        configVec.append(spark_executor_memoryOverhead)
        configList.append("spark.executor.memoryOverhead      " + str(spark_executor_memoryOverhead) + "m\n")

        spark_default_parallelism = random.randint(spark_executor_instances * spark_executor_cores,
                                                   int(spark_executor_instances * spark_executor_cores * 1.5))
        configVec.append(spark_default_parallelism)
        configList.append("spark.default.parallelism      " + str(spark_default_parallelism) + "\n")

        spark_shuffle_io_numConnectionsPerPeer = random.randint(1, 5)
        configVec.append(spark_shuffle_io_numConnectionsPerPeer)
        configList.append(
            "spark.shuffle.io.numConnectionsPerPeer      " + str(spark_shuffle_io_numConnectionsPerPeer) + "\n")

        spark_sql_shuffle_partitions = random.randint(spark_executor_instances * spark_executor_cores,
                                                      int(spark_executor_instances * spark_executor_cores * 1.5))
        configVec.append(spark_sql_shuffle_partitions)
        configList.append("spark.sql.shuffle.partitions      " + str(spark_sql_shuffle_partitions) + "\n")

        spark_sql_inMemoryColumnarStorage_compressed = random.choice(["true", "false"])
        configVec.append(spark_sql_inMemoryColumnarStorage_compressed)
        configList.append(
            "spark.sql.inMemoryColumnarStorage.compressed      " + str(
                spark_sql_inMemoryColumnarStorage_compressed) + "\n")

        spark_sql_inMemoryColumnarStorage_batchSize = random.randint(8000, 12000)
        configVec.append(spark_sql_inMemoryColumnarStorage_batchSize)
        configList.append(
            "spark.sql.inMemoryColumnarStorage.batchSize	" + str(spark_sql_inMemoryColumnarStorage_batchSize) + "\n")

        spark_sql_inMemoryColumnarStorage_partitionPruning = random.choice(["true", "false"])
        configVec.append(spark_sql_inMemoryColumnarStorage_partitionPruning)
        configList.append("spark.sql.inMemoryColumnarStorage.partitionPruning	" + str(
            spark_sql_inMemoryColumnarStorage_partitionPruning) + "\n")

        spark_sql_join_preferSortMergeJoin = random.choice(["true", "false"])
        configVec.append(spark_sql_join_preferSortMergeJoin)
        configList.append("spark.sql.join.preferSortMergeJoin	" + str(spark_sql_join_preferSortMergeJoin) + "\n")

        spark_sql_sort_enableRadixSort = random.choice(["true", "false"])
        configVec.append(spark_sql_sort_enableRadixSort)
        configList.append("spark.sql.sort.enableRadixSort	" + str(spark_sql_sort_enableRadixSort) + "\n")

        spark_sql_retainGroupColumns = random.choice(["true", "false"])
        configVec.append(spark_sql_retainGroupColumns)
        configList.append("spark.sql.retainGroupColumns	" + str(spark_sql_retainGroupColumns) + "\n")

        spark_sql_codegen_maxFields = random.randint(80, 120)
        configVec.append(spark_sql_codegen_maxFields)
        configList.append("spark.sql.codegen.maxFields  " + str(spark_sql_codegen_maxFields) + "\n")

        spark_sql_codegen_aggregate_map_twolevel_enable = random.choice(["true", "false"])
        configVec.append(spark_sql_codegen_aggregate_map_twolevel_enable)
        configList.append("spark.sql.codegen.aggregate.map.twolevel.enable	" + str(
            spark_sql_codegen_aggregate_map_twolevel_enable) + "\n")

        spark_sql_cartesianProductExec_buffer_in_memory_threshold = random.choice(["1024", "2048", "4096"])
        configVec.append(spark_sql_cartesianProductExec_buffer_in_memory_threshold)
        configList.append("spark.sql.cartesianProductExec.buffer.in.memory.threshold	" + str(
            spark_sql_cartesianProductExec_buffer_in_memory_threshold) + "\n")

        spark_sql_autoBroadcastJoinThreshold = round(random.uniform(0.5, 2.0), 5)
        configVec.append(int(spark_sql_autoBroadcastJoinThreshold * 10485760))
        configList.append(
            "spark.sql.autoBroadcastJoinThreshold      " + str(
                int(spark_sql_autoBroadcastJoinThreshold * 10485760)) + "\n")

        if enableSparkStreaming:
            spark_streaming_backpressure_enabled = random.choice(["true", "false"])
            configVec.append(spark_streaming_backpressure_enabled)
            configList.append(
                "spark.streaming.backpressure.enabled     " + str(spark_streaming_backpressure_enabled) + "\n")

            spark_streaming_backpressure_initialRate = random.randint(1, 100)
            configVec.append(spark_streaming_backpressure_initialRate * 10000)
            configList.append("spark.streaming.backpressure.initialRate     " + str(
                spark_streaming_backpressure_initialRate * 10000) + "\n")

            spark_streaming_receiver_writeAheadLog_enable = random.choice(["true", "false"])
            configVec.append(spark_streaming_receiver_writeAheadLog_enable)
            configList.append("spark.streaming.receiver.writeAheadLog.enable    " + str(
                spark_streaming_receiver_writeAheadLog_enable) + "\n")

            spark_streaming_unpersist = random.choice(["true", "false"])
            configVec.append(spark_streaming_unpersist)
            configList.append("spark.streaming.unpersist" + str(spark_streaming_unpersist) + "\n")

            spark_streaming_kafka_maxRatePerPartition = random.randint(1, 100)
            configVec.append(spark_streaming_kafka_maxRatePerPartition * 200)
            configList.append(
                "spark.streaming.kafka.maxRatePerPartition" + str(
                    spark_streaming_kafka_maxRatePerPartition * 200) + "\n")

            spark_streaming_driver_writeAheadLog_closeFileAfterWrite = random.choice(["true", "false"])
            configVec.append(spark_streaming_driver_writeAheadLog_closeFileAfterWrite)
            configList.append("spark.streaming.driver.writeAheadLog.closeFileAfterWrite    " + str(
                spark_streaming_driver_writeAheadLog_closeFileAfterWrite) + "\n")

            spark_streaming_receiver_writeAheadLog_closeFileAfterWrite = random.choice(["true", "false"])
            configVec.append(spark_streaming_receiver_writeAheadLog_closeFileAfterWrite)
            configList.append("spark.streaming.receiver.writeAheadLog.closeFileAfterWrite    " + str(
                spark_streaming_receiver_writeAheadLog_closeFileAfterWrite) + "\n")

    makeSparkConfigFileCommand = "scp " + str(
        sparkTunerWarehouse) + "/SparkTunerReports/tmp/sparktunerConfigTemplate.conf " + \
                                 str(sparkTunerWarehouse) + "/SparkTunerReports/tmp/sparktunerConfig.conf"
    os.system(makeSparkConfigFileCommand)
    confFile = open(str(sparkTunerWarehouse) + "/SparkTunerReports/tmp/sparktunerConfig.conf", "a")
    for line in configList:
        confFile.write(line)
    confFile.close()
    return configVec


def makeConfigForHibench(enableSparkStreaming):
    global useDefaultConfig
    configList = []  # Storage profile
    configVec = []  # Storage profile in vector
    # Fixed configs
    configList.append("----Fixed Configurations----\n")
    # Writer history-server config (Not in the scope of parameter optimization)

    if enableSparkStreaming:
        configList.append("hibench.streambench.spark.batchInterval         100\n")
        configList.append("hibench.streambench.spark.receiverNumber        " + str(envParser.workerNum) + "\n")
        configList.append("hibench.streambench.spark.storageLevel          2\n")
        configList.append("hibench.streambench.spark.enableWAL             false\n")
        configList.append("hibench.streambench.spark.checkpointPath        " + str(hdfsAddress) +
                          "/vartmp\n")
        configList.append("hibench.streambench.spark.useDirectMode         true\n")

        configList.append("hibench.streambench.datagen.intervalSpan            50\n")
        configList.append("hibench.streambench.datagen.recordsPerInterval      50000\n")
        configList.append("hibench.streambench.datagen.recordLength            1000\n")

    configList.append("hibench.spark.home                 " + str(sparkHome) + "\n")
    configList.append("hibench.spark.master               yarn\n")
    # Writer history-server config (Not in the scope of parameter optimization)
    configList.append("spark.eventLog.enabled             true\n")
    configList.append("spark.history.fs.logDirectory      " + str(hdfsAddress) + "/sparktunerLogs\n")
    configList.append("spark.eventLog.dir                 " + str(hdfsAddress) + "/sparktunerLogs\n")
    configList.append("spark.eventLog.compress            true\n")
    configList.append("spark.yarn.maxAppAttempts          1\n")

    configList.append("spark.io.compression.codec         zstd\n")
    configList.append("spark.kryo.referenceTracking       true\n")
    configList.append("spark.serializer                   org.apache.spark.serializer.KryoSerializer\n")
    configList.append("spark.network.timeout              120s\n")
    configList.append("spark.speculation                  false\n")
    configList.append("spark.task.maxFailures             4\n")

    if (useDefaultConfig):
        spark_executor_instances = 2
        configVec.append(spark_executor_instances)
        configList.append("spark.executor.instances      " + str(spark_executor_instances) + "\n")
        configList.append("hibench.yarn.executor.num    " + str(spark_executor_instances) + "\n")

        spark_driver_memory = 1024
        configVec.append(spark_driver_memory)
        configList.append("spark.driver.memory        " + str(spark_driver_memory) + "m\n")

        spark_executor_cores = 2
        configVec.append(spark_executor_cores)
        configList.append("sp ark.executor.cores      " + str(spark_executor_cores) + "\n")
        configList.append("hibench.yarn.executor.cores      " + str(spark_executor_cores) + "\n")

        spark_executor_memory = 2048
        configVec.append(spark_executor_memory)
        configList.append("spark.executor.memory        " + str(spark_executor_memory) + "m\n")

    else:
        configList.append("\n----SparkTuner Tuned Configurations----\n")
        spark_io_compression_zstd_level = random.randint(1, 5)
        configVec.append(spark_io_compression_zstd_level)
        configList.append("spark.io.compression.zstd.level      " + str(spark_io_compression_zstd_level) + "\n")

        spark_io_compression_zstd_bufferSize = random.randint(1, 6) * 16
        configVec.append(spark_io_compression_zstd_bufferSize)
        configList.append(
            "spark.io.compression.zstd.bufferSize	" + str(spark_io_compression_zstd_bufferSize) + "k\n")

        # The num of executors
        spark_executor_instances = random.randint(workerNodeNum, int(workerNodeNum * nodeAvailableCore * 0.1))
        configVec.append(spark_executor_instances)
        configList.append("spark.executor.instances      " + str(spark_executor_instances) + "\n")
        configList.append("hibench.yarn.executor.num    " + str(spark_executor_instances) + "\n")
        spark_executor_instances_pernode = spark_executor_instances / workerNodeNum

        # Application Propertie
        spark_driver_cores = min(random.randint(minAllocatedCore, int(nodeAvailableCore * 0.25)), maxAllocatedCore)
        configVec.append(spark_driver_cores)
        configList.append("spark.driver.cores         " + str(spark_driver_cores) + "\n")

        spark_driver_memory = min(random.randint(minAllocatedMemory, int(nodeAvailableMemory * 0.25)),
                                  maxAllocatedMemory)
        configVec.append(spark_driver_memory)
        configList.append("spark.driver.memory        " + str(spark_driver_memory) + "m\n")

        spark_memory_offHeap_enabled = random.choice(["true", "false"])
        configVec.append(spark_memory_offHeap_enabled)
        configList.append("spark.memory.offHeap.enabled      " + str(spark_memory_offHeap_enabled) + "\n")

        if nodeAvailableMemory * 0.2 / spark_executor_instances_pernode < 512:
            spark_memory_offHeap_size = 512
        else:
            spark_memory_offHeap_size = min(
                random.randint(512, int(nodeAvailableMemory * 0.2 / spark_executor_instances_pernode)),
                maxAllocatedMemory)
        configVec.append(spark_memory_offHeap_size)
        configList.append("spark.memory.offHeap.size      " + str(spark_memory_offHeap_size) + "m\n")

        if spark_memory_offHeap_enabled == "true":
            spark_executor_memory = min(int(int(
                (nodeAvailableMemory * 0.9 - spark_memory_offHeap_size * spark_executor_instances_pernode)
                / spark_executor_instances_pernode) * random.uniform(0.6, 1)), maxAllocatedMemory)
            spark_inuse_memory = spark_executor_memory + spark_memory_offHeap_size
        else:
            spark_executor_memory = min(int(int(nodeAvailableMemory * 0.9 / spark_executor_instances_pernode)
                                            * random.uniform(0.6, 1)), maxAllocatedMemory)
            spark_inuse_memory = spark_executor_memory
        configVec.append(spark_executor_memory)
        configList.append("spark.executor.memory        " + str(spark_executor_memory) + "m\n")

        # Shuffle Behavior
        spark_reducer_maxSizeInFlight = random.randint(40, 128)
        configVec.append(spark_reducer_maxSizeInFlight)
        configList.append("spark.reducer.maxSizeInFlight          " + str(spark_reducer_maxSizeInFlight) + "m\n")

        spark_shuffle_compress = random.choice(["true", "false"])
        configVec.append(spark_shuffle_compress)
        configList.append("spark.shuffle.compress        " + str(spark_shuffle_compress) + "\n")

        spark_shuffle_file_buffer = random.randint(1, 6) * 16
        configVec.append(spark_shuffle_file_buffer)
        configList.append("spark.shuffle.file.buffer      " + str(spark_shuffle_file_buffer) + "k\n")

        spark_shuffle_sort_bypassMergeThreshold = random.randint(100, 1000)
        configVec.append(spark_shuffle_sort_bypassMergeThreshold)
        configList.append(
            "spark.shuffle.sort.bypassMergeThreshold  " + str(spark_shuffle_sort_bypassMergeThreshold) + "\n")

        spark_shuffle_spill_compress = random.choice(["true", "false"])
        configVec.append(spark_shuffle_spill_compress)
        configList.append("spark.shuffle.spill.compress      " + str(spark_shuffle_spill_compress) + "\n")

        # Compression and Serialization
        spark_broadcast_compress = random.choice(["true", "false"])
        configVec.append(spark_broadcast_compress)
        configList.append("spark.broadcast.compress      " + str(spark_broadcast_compress) + "\n")

        spark_kryoserializer_buffer_max = random.randint(1, 8) * 16
        configVec.append(spark_kryoserializer_buffer_max)
        configList.append("spark.kryoserializer.buffer.max      " + str(spark_kryoserializer_buffer_max) + "m\n")

        spark_kryoserializer_buffer = random.randint(1, 16) * 8
        configVec.append(spark_kryoserializer_buffer)
        configList.append("spark.kryoserializer.buffer      " + str(spark_kryoserializer_buffer) + "k\n")

        spark_rdd_compress = random.choice(["true", "false"])
        configVec.append(spark_rdd_compress)
        configList.append("spark.rdd.compress      " + str(spark_rdd_compress) + "\n")

        # Memory Management
        spark_memory_fraction = round(random.uniform(0.5, 0.9), 2)
        configVec.append(spark_memory_fraction)
        configList.append("spark.memory.fraction      " + str(spark_memory_fraction) + "\n")

        spark_memory_storageFraction = round(random.uniform(0.1, 0.9), 2)
        configVec.append(spark_memory_storageFraction)
        configList.append("spark.memory.storageFraction      " + str(spark_memory_storageFraction) + "\n")

        spark_broadcast_blockSize = random.randint(1, 5)
        configVec.append(spark_broadcast_blockSize)
        configList.append("spark.broadcast.blockSize      " + str(spark_broadcast_blockSize) + "m\n")

        spark_executor_cores = max(min(
            random.randint(minAllocatedCore, int(nodeAvailableCore / spark_executor_instances_pernode)),
            maxAllocatedCore), 5)
        configVec.append(spark_executor_cores)
        configList.append("spark.executor.cores      " + str(spark_executor_cores) + "\n")
        configList.append("hibench.yarn.executor.cores      " + str(spark_executor_cores) + "\n")

        spark_storage_memoryMapThreshold = random.randint(50, 500)
        configVec.append(spark_storage_memoryMapThreshold)
        configList.append("spark.storage.memoryMapThreshold      " + str(spark_storage_memoryMapThreshold) + "m\n")

        # Scheduling
        spark_locality_wait = random.randint(1, 10)
        configVec.append(spark_locality_wait)
        configList.append("spark.locality.wait      " + str(spark_locality_wait) + "s\n")

        spark_scheduler_revive_interval = random.randint(100, 1000)
        configVec.append(spark_scheduler_revive_interval)
        configList.append("spark.scheduler.revive.interval      " + str(spark_scheduler_revive_interval) + "ms\n")

        if int((
                       nodeAvailableMemory - spark_inuse_memory * spark_executor_instances_pernode) / spark_executor_instances_pernode) > 2048:
            if spark_memory_offHeap_enabled == "true":
                spark_executor_memoryOverhead = random.randint(minAllocatedMemory, int(
                    (
                                nodeAvailableMemory - spark_inuse_memory * spark_executor_instances_pernode) / spark_executor_instances_pernode))
            else:
                spark_executor_memoryOverhead = random.randint(2048, int(
                    (
                                nodeAvailableMemory - spark_executor_memory * spark_executor_instances_pernode) / spark_executor_instances_pernode))
        elif int((
                         nodeAvailableMemory - spark_inuse_memory * spark_executor_instances_pernode) / spark_executor_instances_pernode) > 384:
            if spark_memory_offHeap_enabled == "true":
                spark_executor_memoryOverhead = random.randint(384, int(
                    (
                                nodeAvailableMemory - spark_inuse_memory * spark_executor_instances_pernode) / spark_executor_instances_pernode))
            else:
                spark_executor_memoryOverhead = random.randint(384, int(
                    (
                                nodeAvailableMemory - spark_executor_memory * spark_executor_instances_pernode) / spark_executor_instances_pernode))
        else:
            spark_executor_memoryOverhead = 384
        configVec.append(spark_executor_memoryOverhead)
        configList.append("spark.executor.memoryOverhead      " + str(spark_executor_memoryOverhead) + "m\n")

        spark_default_parallelism = random.randint(spark_executor_instances * spark_executor_cores,
                                                   int(spark_executor_instances * spark_executor_cores * 1.5))
        configVec.append(spark_default_parallelism)
        configList.append("spark.default.parallelism      " + str(spark_default_parallelism) + "\n")

        spark_shuffle_io_numConnectionsPerPeer = random.randint(1, 5)
        configVec.append(spark_shuffle_io_numConnectionsPerPeer)
        configList.append(
            "spark.shuffle.io.numConnectionsPerPeer      " + str(spark_shuffle_io_numConnectionsPerPeer) + "\n")

        spark_sql_shuffle_partitions = random.randint(spark_executor_instances * spark_executor_cores,
                                                      int(spark_executor_instances * spark_executor_cores * 1.5))
        configVec.append(spark_sql_shuffle_partitions)
        configList.append("spark.sql.shuffle.partitions      " + str(spark_sql_shuffle_partitions) + "\n")

        spark_sql_inMemoryColumnarStorage_compressed = random.choice(["true", "false"])
        configVec.append(spark_sql_inMemoryColumnarStorage_compressed)
        configList.append(
            "spark.sql.inMemoryColumnarStorage.compressed      " + str(
                spark_sql_inMemoryColumnarStorage_compressed) + "\n")

        spark_sql_inMemoryColumnarStorage_batchSize = random.randint(8000, 12000)
        configVec.append(spark_sql_inMemoryColumnarStorage_batchSize)
        configList.append(
            "spark.sql.inMemoryColumnarStorage.batchSize	" + str(spark_sql_inMemoryColumnarStorage_batchSize) + "\n")

        spark_sql_inMemoryColumnarStorage_partitionPruning = random.choice(["true", "false"])
        configVec.append(spark_sql_inMemoryColumnarStorage_partitionPruning)
        configList.append("spark.sql.inMemoryColumnarStorage.partitionPruning	" + str(
            spark_sql_inMemoryColumnarStorage_partitionPruning) + "\n")

        spark_sql_join_preferSortMergeJoin = random.choice(["true", "false"])
        configVec.append(spark_sql_join_preferSortMergeJoin)
        configList.append("spark.sql.join.preferSortMergeJoin	" + str(spark_sql_join_preferSortMergeJoin) + "\n")

        spark_sql_sort_enableRadixSort = random.choice(["true", "false"])
        configVec.append(spark_sql_sort_enableRadixSort)
        configList.append("spark.sql.sort.enableRadixSort	" + str(spark_sql_sort_enableRadixSort) + "\n")

        spark_sql_retainGroupColumns = random.choice(["true", "false"])
        configVec.append(spark_sql_retainGroupColumns)
        configList.append("spark.sql.retainGroupColumns	" + str(spark_sql_retainGroupColumns) + "\n")

        spark_sql_codegen_maxFields = random.randint(80, 120)
        configVec.append(spark_sql_codegen_maxFields)
        configList.append("spark.sql.codegen.maxFields  " + str(spark_sql_codegen_maxFields) + "\n")

        spark_sql_codegen_aggregate_map_twolevel_enable = random.choice(["true", "false"])
        configVec.append(spark_sql_codegen_aggregate_map_twolevel_enable)
        configList.append("spark.sql.codegen.aggregate.map.twolevel.enable	" + str(
            spark_sql_codegen_aggregate_map_twolevel_enable) + "\n")

        spark_sql_cartesianProductExec_buffer_in_memory_threshold = random.choice(["1024", "2048", "4096"])
        configVec.append(spark_sql_cartesianProductExec_buffer_in_memory_threshold)
        configList.append("spark.sql.cartesianProductExec.buffer.in.memory.threshold	" + str(
            spark_sql_cartesianProductExec_buffer_in_memory_threshold) + "\n")

        spark_sql_autoBroadcastJoinThreshold = round(random.uniform(0.5, 2.0), 5)
        configVec.append(int(spark_sql_autoBroadcastJoinThreshold * 10485760))
        configList.append(
            "spark.sql.autoBroadcastJoinThreshold      " + str(
                int(spark_sql_autoBroadcastJoinThreshold * 10485760)) + "\n")

        if enableSparkStreaming:
            spark_streaming_backpressure_enabled = random.choice(["true", "false"])
            configVec.append(spark_streaming_backpressure_enabled)
            configList.append(
                "spark.streaming.backpressure.enabled     " + str(spark_streaming_backpressure_enabled) + "\n")

            spark_streaming_backpressure_initialRate = random.randint(1, 100)
            configVec.append(spark_streaming_backpressure_initialRate * 10000)
            configList.append("spark.streaming.backpressure.initialRate     " + str(
                spark_streaming_backpressure_initialRate * 10000) + "\n")

            spark_streaming_receiver_writeAheadLog_enable = random.choice(["true", "false"])
            configVec.append(spark_streaming_receiver_writeAheadLog_enable)
            configList.append("spark.streaming.receiver.writeAheadLog.enable    " + str(
                spark_streaming_receiver_writeAheadLog_enable) + "\n")

            spark_streaming_unpersist = random.choice(["true", "false"])
            configVec.append(spark_streaming_unpersist)
            configList.append("spark.streaming.unpersist" + str(spark_streaming_unpersist) + "\n")

            spark_streaming_kafka_maxRatePerPartition = random.randint(1, 100)
            configVec.append(spark_streaming_kafka_maxRatePerPartition * 200)
            configList.append(
                "spark.streaming.kafka.maxRatePerPartition" + str(
                    spark_streaming_kafka_maxRatePerPartition * 200) + "\n")

            spark_streaming_driver_writeAheadLog_closeFileAfterWrite = random.choice(["true", "false"])
            configVec.append(spark_streaming_driver_writeAheadLog_closeFileAfterWrite)
            configList.append("spark.streaming.driver.writeAheadLog.closeFileAfterWrite    " + str(
                spark_streaming_driver_writeAheadLog_closeFileAfterWrite) + "\n")

            spark_streaming_receiver_writeAheadLog_closeFileAfterWrite = random.choice(["true", "false"])
            configVec.append(spark_streaming_receiver_writeAheadLog_closeFileAfterWrite)
            configList.append("spark.streaming.receiver.writeAheadLog.closeFileAfterWrite    " + str(
                spark_streaming_receiver_writeAheadLog_closeFileAfterWrite) + "\n")

    makeSparkConfigFileCommand = "scp " + str(
        sparkTunerWarehouse) + "/SparkTunerReports/tmp/sparktunerConfigTemplate.conf " + \
                                 str(hibenchHome) + "/conf/spark.conf"
    os.system(makeSparkConfigFileCommand)
    confFile = open(str(hibenchHome) + "/conf/spark.conf", "a")
    for line in configList:
        confFile.write(line)
    confFile.close()
    return configVec


def preData(benchmarks, enableSparkStreaming):
    if enableSparkStreaming:
        print("Need to generate seed dataset for sparkStreaming of hibench? Please input yes or no:")
        inStr = input()
        while inStr not in ["yes", "no", "Yes", "No", "YES", "NO"]:
            print("please input yes or no:")
            inStr = input()
        if inStr in ["yes", "Yes", "YES"]:
            predata = True
        elif inStr in ["no", "No", "NO"]:
            predata = False
        # Determine whether you need to prepare data based on the input.
        if predata == True:
            ApplicationController.genSeedDataSetForSparkStreamingHibench("fixwindow")
        print("generate seed dataset finished...")
    else:
        # prepare data
        print("Need to prepare hibench workload data? please input yes or no:")
        inStr = input()
        while inStr not in ["yes", "no", "Yes", "No", "YES", "NO"]:
            print("please input yes or no:")
            inStr = input()
        if inStr in ["yes", "Yes", "YES"]:
            predata = True
        elif inStr in ["no", "No", "NO"]:
            predata = False
        # Determine whether you need to prepare data based on the input.
        if predata == True:
            print("please select the data size:" + "\n"
                  + "(tiny; small; large; huge; gigantic; bigdata)")
            inStr = input()
            while inStr not in ['tiny', 'small', 'large', 'huge', 'gigantic', 'bigdata']:
                print("please select the legal data size:" + "\n"
                      + "(tiny; small; large; huge; gigantic; bigdata)")
                inStr = input()
            with open(str(hibenchHome) + "/conf/hibench.conf", "r") as hibench_ConfigFile:
                number = 0
                data = ''
                for p in hibench_ConfigFile:
                    number += 1
                    line = p
                    if number == 3:
                        line = 'hibench.scale.profile' + '      ' + str(inStr) + "\n"
                    data += line
            with open(str(hibenchHome) + "/conf/hibench.conf", "w") as hibench_ConfigFile:
                hibench_ConfigFile.writelines(data)
            print("prepare data of " + str(inStr) + " size ")
            for benchmark in benchmarks:
                command1 = str(hibenchHome) + "/bin/workloads/" + str(benchmark) + "/prepare/prepare.sh"
                result = os.system(command1)
                if result != 0:
                    print("Error preparing data in" + str(benchmark))
                    return False
            print("prepare data finished...")
    return True


def runApplication(location, mainClass):
    global timeVec, duration, applicationExecutionTime, yarnApplicationId, upperBoundTime, currentFailureTimes, failedApplicationId
    timer = threading.Timer(upperBoundTime, ApplicationController.killApplicationsByApplicationName)
    getId = ApplicationController.getRunningApplicationIdThread()
    getApplicationIdThread = threading.Thread(target=getId.getApplicationId)
    timer.start()
    getApplicationIdThread.start()

    print("\n Application == running...")
    result = ApplicationController.submitApplicationOnYarn(sparkHome, location, mainClass)

    yarnApplicationId = getId.applicationId
    print("reasult ==  ")
    print(result)
    print(str(yarnApplicationId))
    getId.terminate()
    timer.cancel()
    queryIdFile = open(str(sparkTunerWarehouse) + "/SparkTunerReports/queryLog.txt", "r")
    finalQueryId = queryIdFile.readline().replace(" ", "").replace("\n", "")
    if result == 0 and ApplicationController.verifyApplicationStatus(yarnApplicationId,
                                                                     "SUCCEEDED") and finalQueryId == "finished":
        if yarnApplicationId != "":
            duration = ApplicationController.getApplicationExecutionTime(yarnApplicationId)
            timeVec.append(duration)
        # if duration < upperBoundTime / 1.4:
        #     upperBoundTime = int(duration * 1.4)
        # currentFailureTimes = 0
    else:
        # if yarnApplicationId != "":
        #     failedApplicationId.append(yarnApplicationId)
        # currentFailureTimes += 1
        # if currentFailureTimes == 2:
        #     upperBoundTime *= 1.5
        result = 1
    flushQueryIdFileCmd = "cat /dev/null > " + str(sparkTunerWarehouse) + "/SparkTunerReports/queryLog.txt"
    os.system(flushQueryIdFileCmd)
    return result


def runHibenchApplication(benchmark):
    global applicationExecutionTime, currentFailureTimes, yarnApplicationId, upperBoundTime, timeVec
    command = str(hibenchHome) + "/bin/workloads/" + str(benchmark) + "/spark/run.sh"
    timer = threading.Timer(upperBoundTime, ApplicationController.killApplicationsByUserName)
    getId = ApplicationController.getRunningApplicationIdThread()
    getApplicationIdThread = threading.Thread(target=getId.getApplicationId)
    timer.start()
    getApplicationIdThread.start()

    print("\n Application == running...")
    result = os.system(command)

    yarnApplicationId = getId.applicationId
    print(str(yarnApplicationId))
    getId.terminate()
    timer.cancel()
    if result == 0 and ApplicationController.verifyApplicationStatus(yarnApplicationId, "SUCCEEDED"):
        if yarnApplicationId != "":
            duration = ApplicationController.getApplicationExecutionTime(yarnApplicationId)
            print("duration == " + str(duration))
            timeVec.append(duration)
            if duration < upperBoundTime / 1.2:
                upperBoundTime = int(duration * 1.3)
        currentFailureTimes = 0
    else:
        result = 1
        currentFailureTimes += 1
        if currentFailureTimes == 3:
            upperBoundTime *= 1.5
    return result


def runHibenchStreamingApplication(benchmarkWithModel):
    global yarnApplicationId

    result = 0
    startDataGeneratorCommand = str(hibenchHome) + "/bin/workloads/" + str(benchmarkWithModel) + "/prepare/dataGen.sh &"
    runStreamingApplicaitonCommand = str(hibenchHome) + "/bin/workloads/" + str(benchmarkWithModel) + "/spark/run.sh"
    print("\n Start streaming data generator")
    os.system(startDataGeneratorCommand)
    time.sleep(5)
    timer = threading.Timer(300, ApplicationController.killStreamingApplicationByProcessId)
    getId = ApplicationController.getRunningApplicationIdThread()
    getApplicationIdThread = threading.Thread(target=getId.getApplicationId)

    timer.start()
    getApplicationIdThread.start()

    print("\n Streaming application == running...")
    os.system(runStreamingApplicaitonCommand)

    yarnApplicationId = getId.applicationId
    getId.terminate()
    timer.cancel()

    if ApplicationController.verifyApplicationStatus(yarnApplicationId, "SUCCEEDED"):
        result = 1
    return result


def makeFailedApplicaitonToQueryId(appIdList):
    flag = 0
    applicationToQueryIdFile = open(str(sparkTunerWarehouse) + "/SparkTunerReports/errorDAC/appIdToQueryId.csv", "w")
    queryIdFile = open(str(sparkTunerWarehouse) + "/SparkTunerReports/failedQueryId.txt", "r")
    tmpVec = []
    for queryid in queryIdFile:
        if flag > len(appIdList) - 1:
            break
        tmpVec.append([queryid.replace(" ", "").replace("\n", ""), appIdList[flag]])
        flag += 1
    applicationToQueryIdFile_csv = csv.writer(applicationToQueryIdFile)
    applicationToQueryIdFile_csv.writerow(["QueryId", "ApplicationId"])
    for appIdToQueryId in tmpVec:
        applicationToQueryIdFile_csv.writerow(appIdToQueryId)
    applicationToQueryIdFile.close()
    queryIdFile.close()


def makeApplicationResult(total, headers):
    global timeVec, failedApplicationId
    # read the configuration from confVec.csv
    confVecFile = open(str(sparkTunerWarehouse) + "/SparkTunerReports/confVec.csv", "r")
    # create the combination file
    backupFilecmd = "scp " + str(sparkTunerWarehouse) + "/SparkTunerReports/time_conf.csv " + str(sparkTunerWarehouse) \
                    + "/SparkTunerReports/time_conf_bak.csv"
    os.system(backupFilecmd)
    comVecFile = open(str(sparkTunerWarehouse) + "/SparkTunerReports/time_conf.csv", "w")
    confVec = []  # store the configuration
    # dsizeVec = []  # stort input datasize
    comVec = []  # combination of the 4 vectors

    # read config from confVec.csv
    confVecFile_csv = csv.reader(confVecFile)
    for row in confVecFile_csv:
        confVec.append(row)

    # combine the config, type, datasize, time together
    for i in range(total):
        tmpVec = []
        tmpVec.append(timeVec[i])
        comVec.append(confVec[i] + tmpVec)
        # print(len(tmpVec))
    confVecFile.close()
    # write the combination of success task
    comVecFile_csv = csv.writer(comVecFile)
    comVecFile_csv.writerow(headers)
    for i in range(len(comVec)):
        comVecFile_csv.writerow(comVec[i])
    comVecFile.close()
    # makeFailedApplicaitonToQueryId(failedApplicationId)
    return


def makeHibenchStreamingApplicationResult(total, headers):
    global ratioOfThroughputToLatencyVec, failedApplicationId
    # read the configuration from confVec.csv
    confVecFile = open(str(sparkTunerWarehouse) + "/SparkTunerReports/confVec.csv", "r")
    # create the combination file
    backupFilecmd = "scp " + str(sparkTunerWarehouse) + "/SparkTunerReports/time_conf.csv " + str(sparkTunerWarehouse) \
                    + "/SparkTunerReports/time_conf_bak.csv"
    os.system(backupFilecmd)
    comVecFile = open(str(sparkTunerWarehouse) + "/SparkTunerReports/time_conf.csv", "w")
    confVec = []  # store the configuration
    comVec = []  # combination of the 4 vectors

    # read config from confVec.csv
    confVecFile_csv = csv.reader(confVecFile)
    for row in confVecFile_csv:
        confVec.append(row)

    # combine the config, type, datasize, time together
    for i in range(total):
        tmpVec = []
        tmpVec += ratioOfThroughputToLatencyVec[i]
        comVec.append(confVec[i] + tmpVec)
        # print(len(tmpVec))
    confVecFile.close()
    # write the combination of success task
    comVecFile_csv = csv.writer(comVecFile)
    comVecFile_csv.writerow(headers)
    for i in range(len(comVec)):
        comVecFile_csv.writerow(comVec[i])
    comVecFile.close()
    # makeFailedApplicaitonToQueryId(failedApplicationId)
    return


def testOptimalConfigurationFile():
    outputConfigurationFiles = os.listdir(str(sparkTunerWarehouse) + "/SparkTunerReports/outputConfigurationFiles")
    gAIterationTimes = 0
    for outputConfigurationFile in outputConfigurationFiles:
        if "-spark-defaults.conf" in outputConfigurationFile:
            gAIterationTimes = gAIterationTimes + 1
    optimalConfigurationFileLocation = str(sparkTunerWarehouse) + "/SparkTunerReports/outputConfigurationFiles/" \
                                       + str(gAIterationTimes - 1) + "-spark-defaults.conf"

    # ApplicationController.submitApplicationOnYarn(sparkHome, "/home/ljy/software/test.jar",
    # "SparkTunerTest.HwTestApplicationForSparkTuner", "optimalConfigurationTest", optimalConfigurationFileLocation)

    ApplicationController.submitApplicationOnYarn(sparkHome, applicationJarLocation, applicationJarMainClass,
                                                  "optimalConfigurationTest", optimalConfigurationFileLocation)


def buildModel():
    global applicationName, startTime, enableSparkStreaming, skipDataCollection
    rows = 0
    with open(str(sparkTunerWarehouse) + "/SparkTunerReports/time_conf.csv", 'r') as csvfile:
        next(csvfile)
        reader = csv.reader(csvfile)
        for _ in reader:
            rows += 1

    # Data preprocessing
    if enableSparkStreaming:
        processDataCommand = "python3 " + str(envParser.currentWorkPath) + "/DataProcessor.py"
    else:
        processDataCommand = "python3 " + str(envParser.currentWorkPath) + "/DataProcessor.py"
    os.system(processDataCommand)

    # if enableSparkStreaming:
    #     os.system("python3 " + str(sparkTunerHome) + "/GaAutoSearching.py")
    # else:
    #     os.system("python3 " + str(sparkTunerHome) + "/GaAutoSearching.py")

    print("==================================================" + "\n"
          + "       Generating Importance Ranking Report       " + "\n"
          + "--------------------------------------------------" + "\n")
    os.system("python3 " + str(sparkTunerHome) + "/ImportanceRanker.py")
    print("--------------------------------------------------" + "\n"
          + "           Importance Ranking Report Got" + "\n"
          + "==================================================" + "\n")

    if not skipDataCollection:
        if os.path.exists(str(sparkTunerWarehouse) + "/SparkTunerReports/confVec.csv"):
            removeRedundantFilesCommand = "rm " + str(sparkTunerWarehouse) + "/SparkTunerReports/confVec.csv"
            os.system(removeRedundantFilesCommand)

    if os.path.exists(str(sparkTunerWarehouse) + "/SparkTunerReports/tmp/spark-defaults.conf"):
        removeRedundantFilesCommand = "rm " + str(sparkTunerWarehouse) + "/SparkTunerReports/tmp/spark-defaults.conf"
        os.system(removeRedundantFilesCommand)

    finishedTime = time.strftime("%m%d-%H:%M", time.localtime())
    saveReportsCommand = "cp -r " + str(sparkTunerWarehouse) + "/SparkTunerReports " \
                         + str(sparkTunerWarehouse) + "/SparkTunerReports_" + str(applicationName) \
                         + "_" + str(rows) + "Samples" "_FinishedAt_" + str(finishedTime)
    os.system(saveReportsCommand)


def main():
    global upperBoundTime, applicationName, startTime, applicationJarLocation, \
        applicationJarMainClass, enableSparkStreaming, ratioOfThroughputToLatencyVec
    applicationName = ApplicationController.applicationName
    randomSeed = int(time.time())
    random.seed(randomSeed)
    Hibench = False

    # run job
    numOfFail = 0  # record the number of failed tasks
    numOfSuc = 0  # record the number of succeed tasks

    # remove the old hibench.report and touch a new hibench.report
    cmdrm = "rm -rf " + str(hibenchHome) + "/report/hibench.report"
    cmdtouch = "touch " + str(hibenchHome) + "/report/hibench.report"
    if hibenchHome:
        os.system(cmdrm)
        os.system(cmdtouch)

    cmdTestSparkHistoryServerDir = "hadoop fs -test -e /sparktunerLogs"
    if os.system(cmdTestSparkHistoryServerDir) != 0:
        cmdCreateTestSparkHistoryServerDir = "hadoop fs -mkdir /sparktunerLogs"
        os.system(cmdCreateTestSparkHistoryServerDir)

    if not os.path.exists(str(sparkTunerWarehouse) + "/SparkTunerReports"):
        cmdCreateReportsFolder = "mkdir " + str(sparkTunerWarehouse) + "/SparkTunerReports"
        os.system(cmdCreateReportsFolder)

    if os.path.exists(str(sparkTunerWarehouse) + "/SparkTunerReports/tmp"):
        # clear error log folder:errorDAC
        cmdrmlog = "rm -rf " + str(sparkTunerWarehouse) + "/SparkTunerReports/tmp"
        os.system(cmdrmlog)
    cmdCreateTmpFolder = "mkdir " + str(sparkTunerWarehouse) + "/SparkTunerReports/tmp"
    os.system(cmdCreateTmpFolder)

    if os.path.exists(str(sparkTunerWarehouse) + "/SparkTunerReports/outputConfigurationFiles"):
        # clear error log folder:errorDAC
        cmdrmlog = "rm -rf " + str(sparkTunerWarehouse) + "/SparkTunerReports/outputConfigurationFiles"
        os.system(cmdrmlog)
    cmdCreateOutputConfigurationFiles = "mkdir " + str(
        sparkTunerWarehouse) + "/SparkTunerReports/outputConfigurationFiles"
    os.system(cmdCreateOutputConfigurationFiles)

    if os.path.exists(str(sparkTunerWarehouse) + "/SparkTunerReports/errorDAC"):
        # clear error log folder:errorDAC
        cmdrmlog = "rm -rf " + str(sparkTunerWarehouse) + "/SparkTunerReports/errorDAC/"
        os.system(cmdrmlog)
    cmdCreateErrorDACFolder = "mkdir " + str(sparkTunerWarehouse) + "/SparkTunerReports/errorDAC"
    os.system(cmdCreateErrorDACFolder)

    if os.path.exists(str(sparkTunerWarehouse) + "/SparkTunerReports/confVec.csv"):
        cmdrm_conVecFile = "rm " + str(sparkTunerWarehouse) + "/SparkTunerReports/confVec.csv"
        os.system(cmdrm_conVecFile)

    if os.path.exists(str(sparkTunerWarehouse) + "/SparkTunerReports/time_conf.csv"):
        cmdrm_comVecFile = "rm " + str(sparkTunerWarehouse) + "/SparkTunerReports/time_conf.csv"
        os.system(cmdrm_comVecFile)

    if os.path.exists(str(sparkTunerWarehouse) + "/SparkTunerReports/Normalized_confs.csv"):
        cmdDeleteNormalizedFile = "rm " + str(sparkTunerWarehouse) + "/SparkTunerReports/Normalized_confs.csv"
        os.system(cmdDeleteNormalizedFile)

    if os.path.exists(str(sparkTunerWarehouse) + "/SparkTunerReports/failedQueryId.txt"):
        cmdDeleteNfailedQueryId = "rm " + str(sparkTunerWarehouse) + "/SparkTunerReports/failedQueryId.txt"
        os.system(cmdDeleteNfailedQueryId)

    confVecFile = open(str(sparkTunerWarehouse) + "/SparkTunerReports/confVec.csv", "a")
    confVecFile.close()

    if enableSparkStreaming:
        configHeaders = envParser.sparkParametersWithSql + envParser.sparkStreamingParameters
        benchmarkNames = envParser.streamingBenchmarks
        benchmarkWithModels = envParser.streamingBenchmarksWithModels
        hibenchSuccessfulApplicationHeaders = configHeaders + ['ratioOfThroughputToLatency']
        hibenchFailedApplcationHeaders = configHeaders + ["type", "number"]
    else:
        configHeaders = envParser.sparkParametersWithSql
        benchmarkWithModels = envParser.benchmarksWithModels
        benchmarkNames = envParser.benchmarks
        hibenchSuccessfulApplicationHeaders = configHeaders + ['duration']
        hibenchFailedApplcationHeaders = configHeaders + ["type", "number"]

    with open(str(sparkTunerWarehouse) + "/SparkTunerReports/errorDAC/err_ConfVec.csv", "a") as errorVecFile:
        errorVecFile_csv = csv.writer(errorVecFile)
        errorVecFile_csv.writerow(hibenchFailedApplcationHeaders)
    errorVecFile.close()

    start = int(time.time())

    if hibenchHome is None:
        inStr = "No"
    elif enableSparkStreaming:
        inStr = "Yes"
    else:
        print("Use Hibench workload? Please input yes or no:")
        inStr = input()

    while inStr not in ["yes", "no", "Yes", "No", "YES", "NO"]:
        print("please input yes or no:")
        inStr = input()
    if inStr in ["yes", "Yes", "YES"]:
        Hibench = True
    elif inStr in ["no", "No", "NO"]:
        Hibench = False

    if Hibench == True:
        envParser.sparkDefaultsConfParser()
        hibenchModelDic = ApplicationController.collectHibenchInformation(enableSparkStreaming)
        # prepare data for Hibench
        benchmarks = [benchmarkWithModels[hibenchModelDic["benchmark"]]]
        result = preData(benchmarks, enableSparkStreaming)
        if result == False:
            return
        totalTestTimes = envParser.sampleScale
        print("\nThe application's logs will be saved to hdfs path\n"
              + "(sparkTuner start sparkEventLog by default):\n"
              + str(hdfsAddress) + "/sparktunerLogs\n")

        startTime = time.strftime("%m%d-%H:%M", time.localtime())
        for j in range(len(benchmarks)):
            while (numOfSuc < totalTestTimes):
                print("upperTime = " + str(upperBoundTime))
                print("The num of finished benchmarks:" + str(numOfSuc + numOfFail))
                print("Succeed: " + str(numOfSuc))
                print("Failed: " + str(numOfFail))
                configVec = makeConfigForHibench(enableSparkStreaming)  # make config
                if enableSparkStreaming:
                    res = runHibenchStreamingApplication(benchmarkWithModels[hibenchModelDic["benchmark"]])
                else:
                    res = runHibenchApplication(benchmarkWithModels[hibenchModelDic["benchmark"]])
                if res == 0:
                    numOfSuc += 1
                    with open(str(sparkTunerWarehouse) + "/SparkTunerReports/confVec.csv", "a") as configVecFile:
                        confVecFile_csv = csv.writer(configVecFile)
                        confVecFile_csv.writerow(configVec)
                else:
                    numOfFail += 1
                    # save error log
                    cmdlog = "cp " + str(hibenchHome) + "/report/" + str(
                        benchmarkNames[hibenchModelDic["benchmark"]]) + "/spark/bench.log " + \
                             str(sparkTunerWarehouse) + "/SparkTunerReports/errorDAC/" + str(
                        benchmarkNames[hibenchModelDic["benchmark"]]) + "_" + str(
                        numOfSuc + numOfFail) + "_" + "error.log"
                    os.system(cmdlog)
                    with open(str(sparkTunerWarehouse) + "/SparkTunerReports/errorDAC/err_ConfVec.csv",
                              "a") as errorVecFile:
                        errorVecFile_csv = csv.writer(errorVecFile)
                        error_configVec = configVec + [benchmarkNames[hibenchModelDic["benchmark"]],
                                                       numOfSuc + numOfFail]
                        # save the excution time and confVec of failed job into a csv file
                        errorVecFile_csv.writerow(error_configVec)
                # Read run time Generate configuration-time data
                # Note: use numOfsuc ,not total,because some job may be failed
                if enableSparkStreaming:
                    topicName = ApplicationController.getTopicNameForSparkStreamingHibench(hibenchModelDic["benchmark"])
                    reportLoc = ApplicationController \
                        .genMetricsReportForSparkStreamingHibench(topicName, hibenchModelDic["benchmark"])
                    ratioOfThroughputToLatency = ApplicationController.getRatioOfThroughputToLatency(reportLoc)
                    ratioOfThroughputToLatencyVec.append(ratioOfThroughputToLatency)
                    makeHibenchStreamingApplicationResult(numOfSuc, hibenchSuccessfulApplicationHeaders)
                else:
                    makeApplicationResult(numOfSuc, hibenchSuccessfulApplicationHeaders)
    else:
        envParser.sparkDefaultsConfParser()
        applicationJarLocation = envParser.applicationJarLoc
        applicationJarMainClass = envParser.applicationJarMainClass

        if applicationJarLocation is None:
            print("You have not set Jar Location in tuner-site.xml!\n"
                  "Please input application(jar file)'s location now:")
            applicationJarLocation = input()
        while not os.path.exists(applicationJarLocation):
            print("Jar location " + applicationJarLocation + " specified in tuner-site.xml not exists,\n"
                                                             "please correct or input the right path:")
            applicationJarLocation = input()

        if applicationJarMainClass is None:
            print("You have not set Jar main class in tuner-site.xml!\n"
                  "Please input your main class now:")
            applicationJarMainClass = input()

        print("\nThe application's logs will be saved to hdfs path\n"
              + "(sparkTuner start sparkEventLog by default):\n"
              + str(hdfsAddress) + "/sparktunerLogs\n")

        totalTestTimes = envParser.sampleScale
        startTime = time.strftime("%m%d-%H:%M", time.localtime())
        while (numOfSuc < totalTestTimes):
            print("The num of finished Applications:" + str(numOfFail + numOfSuc))
            print("Succeed: " + str(numOfSuc))
            print("Failed: " + str(numOfFail))
            configVec = makeConfig(enableSparkStreaming)  # make config
            res = runApplication(applicationJarLocation, applicationJarMainClass)
            if res == 0:
                numOfSuc += 1
                with open(str(sparkTunerWarehouse) + "/SparkTunerReports/confVec.csv", "a") as configVecFile:
                    confVecFile_csv = csv.writer(configVecFile)
                    confVecFile_csv.writerow(configVec)
            else:
                numOfFail += 1
                with open(str(sparkTunerWarehouse) + "/SparkTunerReports/errorDAC/err_ConfVec.csv",
                          "a") as errorVecFile:
                    errorVecFile_csv = csv.writer(errorVecFile)
                    # save the excution time and confVec of failed job into a csv file
                    errorVecFile_csv.writerow(configVec)
            ApplicationSuccessHeaders = configHeaders + ['duration']
            makeApplicationResult(numOfSuc, ApplicationSuccessHeaders)
    confVecFile.close()
    errorVecFile.close()

    end = int(time.time())
    DataCollectionCostTime = end - start
    print("\n"
          + "--------------------------------------------------" + "\n"
          + "             DATA COLLECTION Finished" + "\n"
          + "==================================================" + "\n")
    # print data collection result
    sparktunerRecord = open(str(sparkTunerWarehouse) + "/SparkTunerReports/record.txt", "a")
    print("Data Collection costs Time   " + str(DataCollectionCostTime) + "s\n" +
          "Successful to Executed Applications  " + str(numOfSuc) + "\n" +
          "Failed to Executed Applications  " + str(numOfFail), file=sparktunerRecord)
    sparktunerRecord.close()


if not skipDataCollection:
    main()
buildModel()
