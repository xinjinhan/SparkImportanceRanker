#!/usr/bin/env python
# -*- coding:utf-8 _*-

import pandas as pd
import csv
from sklearn.preprocessing import LabelEncoder
import EnvironmentParser as envParser
import random
import os

hdfsAddress = envParser.getHdfsAddress()
sparkHome = envParser.sparkHome
enableStreaming = envParser.enableSparkStreamingTuner
sparkTunerWarehouse = envParser.sparkTunerWarehouse
workerNodeNum = envParser.workerNum
nodeAvailableMemory = envParser.nodeAvailableMemory
nodeAvailableCore = envParser.nodeAvailableCore
maxAllocatedMemory = envParser.maxAllocatedMemory
maxAllocatedCore = envParser.maxAllocatedCore
minAllocatedMemory = envParser.minAllocatedMemory
minAllocatedCore = envParser.minAllocatedCore
useDefaultConfig = envParser.useDefaultConfig


# Produce configuration vector
def makeConfVec(enableSparkStreaming):
    configVec = []

    spark_io_compression_zstd_level = random.randint(1, 5)
    configVec.append(spark_io_compression_zstd_level)

    spark_io_compression_zstd_bufferSize = random.randint(1, 6) * 16
    configVec.append(spark_io_compression_zstd_bufferSize)

    spark_executor_instances = random.randint(workerNodeNum, int(workerNodeNum * nodeAvailableCore * 0.25))
    configVec.append(spark_executor_instances)
    spark_executor_instances_pernode = spark_executor_instances / workerNodeNum

    spark_driver_cores = min(random.randint(minAllocatedCore, int(nodeAvailableCore * 0.25)), maxAllocatedCore)
    configVec.append(spark_driver_cores)

    spark_driver_memory = min(random.randint(minAllocatedMemory, int(nodeAvailableMemory * 0.25)), maxAllocatedMemory)
    configVec.append(spark_driver_memory)

    spark_memory_offHeap_enabled = random.choice(["true", "false"])
    configVec.append(spark_memory_offHeap_enabled)

    if nodeAvailableMemory * 0.2 / spark_executor_instances_pernode < 512:
        spark_memory_offHeap_size = 512
    else:
        spark_memory_offHeap_size = min(
            random.randint(512, int(nodeAvailableMemory * 0.2 / spark_executor_instances_pernode)),
            maxAllocatedMemory)
    configVec.append(spark_memory_offHeap_size)

    if spark_memory_offHeap_enabled == "true":
        spark_executor_memory = min(random.randint(minAllocatedMemory * 4, int(
            (nodeAvailableMemory * 0.8 - spark_memory_offHeap_size * spark_executor_instances_pernode)
            / spark_executor_instances_pernode)), maxAllocatedMemory)
        spark_inuse_memory = spark_executor_memory + spark_memory_offHeap_size
    else:
        spark_executor_memory = min(
            random.randint(minAllocatedMemory * 4, int(nodeAvailableMemory * 0.8 / spark_executor_instances_pernode)),
            maxAllocatedMemory)
        spark_inuse_memory = spark_executor_memory
    configVec.append(spark_executor_memory)

    spark_reducer_maxSizeInFlight = random.randint(40, 128)
    configVec.append(spark_reducer_maxSizeInFlight)

    spark_shuffle_compress = random.choice(["true", "false"])
    configVec.append(spark_shuffle_compress)

    spark_shuffle_file_buffer = random.randint(1, 6) * 16
    configVec.append(spark_shuffle_file_buffer)

    spark_shuffle_sort_bypassMergeThreshold = random.randint(100, 1000)
    configVec.append(spark_shuffle_sort_bypassMergeThreshold)

    spark_shuffle_spill_compress = random.choice(["true", "false"])
    configVec.append(spark_shuffle_spill_compress)

    spark_broadcast_compress = random.choice(["true", "false"])
    configVec.append(spark_broadcast_compress)

    spark_kryoserializer_buffer_max = random.randint(1, 8) * 16
    configVec.append(spark_kryoserializer_buffer_max)

    spark_kryoserializer_buffer = random.randint(1, 16) * 8
    configVec.append(spark_kryoserializer_buffer)

    spark_rdd_compress = random.choice(["true", "false"])
    configVec.append(spark_rdd_compress)

    spark_memory_fraction = round(random.uniform(0.5, 0.9), 2)
    configVec.append(spark_memory_fraction)

    spark_memory_storageFraction = round(random.uniform(0.1, 0.9), 2)
    configVec.append(spark_memory_storageFraction)

    spark_broadcast_blockSize = random.randint(1, 5)
    configVec.append(spark_broadcast_blockSize)

    spark_executor_cores = min(
        random.randint(minAllocatedCore, int(nodeAvailableCore / spark_executor_instances_pernode)), maxAllocatedCore)
    configVec.append(spark_executor_cores)

    spark_storage_memoryMapThreshold = random.randint(50, 500)
    configVec.append(spark_storage_memoryMapThreshold)

    spark_locality_wait = random.randint(1, 10)
    configVec.append(spark_locality_wait)

    spark_scheduler_revive_interval = random.randint(100, 1000)
    configVec.append(spark_scheduler_revive_interval)

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

    spark_default_parallelism = random.randint(spark_executor_instances * spark_executor_cores,
                                               spark_executor_instances * spark_executor_cores * 3)
    configVec.append(spark_default_parallelism)

    spark_shuffle_io_numConnectionsPerPeer = random.randint(1, 5)
    configVec.append(spark_shuffle_io_numConnectionsPerPeer)

    spark_sql_shuffle_partitions = random.randint(spark_executor_instances * spark_executor_cores,
                                                  spark_executor_instances * spark_executor_cores * 3)
    configVec.append(spark_sql_shuffle_partitions)

    spark_sql_inMemoryColumnarStorage_compressed = random.choice(["true", "false"])
    configVec.append(spark_sql_inMemoryColumnarStorage_compressed)

    spark_sql_inMemoryColumnarStorage_batchSize = random.randint(8000, 12000)
    configVec.append(spark_sql_inMemoryColumnarStorage_batchSize)

    spark_sql_inMemoryColumnarStorage_partitionPruning = random.choice(["true", "false"])
    configVec.append(spark_sql_inMemoryColumnarStorage_partitionPruning)

    spark_sql_join_preferSortMergeJoin = random.choice(["true", "false"])
    configVec.append(spark_sql_join_preferSortMergeJoin)

    spark_sql_sort_enableRadixSort = random.choice(["true", "false"])
    configVec.append(spark_sql_sort_enableRadixSort)

    spark_sql_retainGroupColumns = random.choice(["true", "false"])
    configVec.append(spark_sql_retainGroupColumns)

    spark_sql_codegen_maxFields = random.randint(80, 120)
    configVec.append(spark_sql_codegen_maxFields)

    spark_sql_codegen_aggregate_map_twolevel_enable = random.choice(["true", "false"])
    configVec.append(spark_sql_codegen_aggregate_map_twolevel_enable)

    spark_sql_cartesianProductExec_buffer_in_memory_threshold = random.choice(["1024", "2048", "4096"])
    configVec.append(spark_sql_cartesianProductExec_buffer_in_memory_threshold)

    spark_sql_autoBroadcastJoinThreshold = round(random.uniform(0.5, 2.0), 5)
    configVec.append(int(spark_sql_autoBroadcastJoinThreshold * 10485760))

    if enableSparkStreaming:
        spark_streaming_backpressure_enabled = random.choice(["true", "false"])
        configVec.append(spark_streaming_backpressure_enabled)

        spark_streaming_backpressure_initialRate = random.randint(1, 100)
        configVec.append(spark_streaming_backpressure_initialRate * 10000)

        spark_streaming_receiver_writeAheadLog_enable = random.choice(["true", "false"])
        configVec.append(spark_streaming_receiver_writeAheadLog_enable)

        spark_streaming_unpersist = random.choice(["true", "false"])
        configVec.append(spark_streaming_unpersist)

        spark_streaming_kafka_maxRatePerPartition = random.randint(1, 100)
        configVec.append(spark_streaming_kafka_maxRatePerPartition * 200)

        spark_streaming_driver_writeAheadLog_closeFileAfterWrite = random.choice(["true", "false"])
        configVec.append(spark_streaming_driver_writeAheadLog_closeFileAfterWrite)

        spark_streaming_receiver_writeAheadLog_closeFileAfterWrite = random.choice(["true", "false"])
        configVec.append(spark_streaming_receiver_writeAheadLog_closeFileAfterWrite)

    return configVec


# Write the optimal value found by GA to the spark configuration file
def writeConf(configVec, filename, enableSparkStreaming):
    configList = []  # Storage profile

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

    configList.append("spark.dynamicAllocation.enabled    false\n")
    configList.append("spark.io.compression.codec         zstd\n")
    configList.append("spark.kryo.referenceTracking       true\n")
    configList.append("spark.serializer                   org.apache.spark.serializer.KryoSerializer\n")
    configList.append("spark.network.timeout              120s\n")
    configList.append("spark.speculation                  false\n")
    configList.append("spark.task.maxFailures             4\n")

    spark_io_compression_zstd_level = int(configVec[0])
    configList.append("spark.io.compression.zstd.level      " + str(spark_io_compression_zstd_level) + "\n")

    spark_io_compression_zstd_bufferSize = int(configVec[1])
    configList.append("spark.io.compression.zstd.bufferSize	" + str(spark_io_compression_zstd_bufferSize) + "k\n")

    spark_executor_instances = int(configVec[2])
    configList.append("spark.executor.instances      " + str(spark_executor_instances) + "\n")
    configList.append("hibench.yarn.executor.num    " + str(spark_executor_instances) + "\n")

    spark_driver_cores = int(configVec[3])
    configList.append("spark.driver.cores         " + str(spark_driver_cores) + "\n")

    spark_driver_memory = int(configVec[4])
    configList.append("spark.driver.memory        " + str(spark_driver_memory) + "m\n")

    if configVec[5] == 1:
        spark_memory_offHeap_enabled = "true"
    elif configVec[5] == 0:
        spark_memory_offHeap_enabled = "false"
    else:
        spark_memory_offHeap_enabled = "error"
    configList.append("spark.memory.offHeap.enabled      " + str(spark_memory_offHeap_enabled) + "\n")

    # spark_memory_offHeap_size = random.randint(10,1000)
    spark_memory_offHeap_size = int(configVec[6])
    configList.append("spark.memory.offHeap.size      " + str(spark_memory_offHeap_size) + "m\n")

    # spark_executor_memory = random.randint(1024,36864)
    spark_executor_memory = int(configVec[7])
    configList.append("spark.executor.memory        " + str(spark_executor_memory) + "m\n")

    spark_reducer_maxSizeInFlight = int(configVec[8])
    configList.append("spark.reducer.maxSizeInFlight          " + str(spark_reducer_maxSizeInFlight) + "m\n")

    if configVec[9] == 1:
        spark_shuffle_compress = "true"
    elif configVec[9] == 0:
        spark_shuffle_compress = "false"
    else:
        spark_shuffle_compress = "error"
    configList.append("spark.shuffle.compress        " + str(spark_shuffle_compress) + "\n")

    spark_shuffle_file_buffer = int(configVec[10])
    configList.append("spark.shuffle.file.buffer      " + str(spark_shuffle_file_buffer) + "k\n")

    # spark_shuffle_sort_bypassMergeThreshold = random.randint(100,1000)
    spark_shuffle_sort_bypassMergeThreshold = int(configVec[11])
    configList.append("spark.shuffle.sort.bypassMergeThreshold  " + str(spark_shuffle_sort_bypassMergeThreshold) + "\n")

    if configVec[12] == 1:
        spark_shuffle_spill_compress = "true"
    elif configVec[12] == 0:
        spark_shuffle_spill_compress = "false"
    else:
        spark_shuffle_spill_compress = "error"
    configList.append("spark.shuffle.spill.compress      " + str(spark_shuffle_spill_compress) + "\n")

    # Compression and Serialization
    if configVec[13] == 1:
        spark_broadcast_compress = "true"
    elif configVec[13] == 0:
        spark_broadcast_compress = "false"
    else:
        spark_broadcast_compress = "error"
    configList.append("spark.broadcast.compress      " + str(spark_broadcast_compress) + "\n")

    # spark_kryoserializer_buffer_max = random.randint(8,128)
    spark_kryoserializer_buffer_max = int(configVec[14])
    configList.append("spark.kryoserializer.buffer.max      " + str(spark_kryoserializer_buffer_max) + "m\n")

    # spark_kryoserializer_buffer = random.randint(2,128)
    spark_kryoserializer_buffer = int(configVec[15])
    configList.append("spark.kryoserializer.buffer      " + str(spark_kryoserializer_buffer) + "k\n")

    if configVec[16] == 1:
        spark_rdd_compress = "true"
    elif configVec[16] == 0:
        spark_rdd_compress = "false"
    else:
        spark_rdd_compress = "error"
    configList.append("spark.rdd.compress      " + str(spark_rdd_compress) + "\n")

    spark_memory_fraction = round(float(configVec[17]), 2)
    configList.append("spark.memory.fraction      " + str(spark_memory_fraction) + "\n")

    spark_memory_storageFraction = round(float(configVec[18]), 2)
    configList.append("spark.memory.storageFraction      " + str(spark_memory_storageFraction) + "\n")

    spark_broadcast_blockSize = min(int(configVec[19]), random.randint(1, 5))
    configList.append("spark.broadcast.blockSize      " + str(spark_broadcast_blockSize) + "m\n")

    # spark_executor_cores = random.randint(1,30)
    spark_executor_cores = int(min(configVec[20], 300 / spark_executor_instances))
    configList.append("spark.executor.cores      " + str(spark_executor_cores) + "\n")
    configList.append("hibench.yarn.executor.cores      " + str(spark_executor_cores) + "\n")

    # spark_storage_memoryMapThreshold = random.randint(50,500)
    spark_storage_memoryMapThreshold = int(configVec[21])
    configList.append("spark.storage.memoryMapThreshold      " + str(spark_storage_memoryMapThreshold) + "m\n")

    # Scheduling
    # spark_locality_wait = random.randint(1,10)
    spark_locality_wait = int(configVec[22])
    configList.append("spark.locality.wait      " + str(spark_locality_wait) + "s\n")

    # spark_scheduler_revive_interval = random.randint(2,50)
    spark_scheduler_revive_interval = int(configVec[23])
    configList.append("spark.scheduler.revive.interval      " + str(spark_scheduler_revive_interval) + "ms\n")

    spark_executor_memoryOverhead = int(configVec[24])
    configList.append("spark.executor.memoryOverhead      " + str(spark_executor_memoryOverhead) + "m\n")

    spark_default_parallelism = int(configVec[25])
    configList.append("spark.default.parallelism      " + str(spark_default_parallelism) + "\n")

    spark_shuffle_io_numConnectionsPerPeer = int(configVec[26])
    configList.append(
        "spark.shuffle.io.numConnectionsPerPeer      " + str(spark_shuffle_io_numConnectionsPerPeer) + "\n")

    # spark_sql_shuffle_partitions = random.randint(8,50)
    spark_sql_shuffle_partitions = int(configVec[27])
    configList.append("spark.sql.shuffle.partitions      " + str(spark_sql_shuffle_partitions) + "\n")

    # Compress Behavior
    if configVec[28] == 1:
        spark_sql_inMemoryColumnarStorage_compressed = "true"
    elif configVec[28] == 0:
        spark_sql_inMemoryColumnarStorage_compressed = "false"
    configList.append(
        "spark.sql.inMemoryColumnarStorage.compressed      " + str(spark_sql_inMemoryColumnarStorage_compressed) + "\n")

    # 列缓存的批处理大小
    spark_sql_inMemoryColumnarStorage_batchSize = int(configVec[29])
    configList.append(
        "spark.sql.inMemoryColumnarStorage.batchSize	" + str(spark_sql_inMemoryColumnarStorage_batchSize) + "\n")

    # 启用内存中的列表分区剪枝
    if configVec[30] == 1:
        spark_sql_inMemoryColumnarStorage_partitionPruning = "true"
    elif configVec[30] == 0:
        spark_sql_inMemoryColumnarStorage_partitionPruning = "false"
    configList.append("spark.sql.inMemoryColumnarStorage.partitionPruning	" + str(
        spark_sql_inMemoryColumnarStorage_partitionPruning) + "\n")

    # When true, 使用sort merge join 代替 shuffle hash join
    if configVec[31] == 1:
        spark_sql_join_preferSortMergeJoin = "true"
    elif configVec[31] == 0:
        spark_sql_join_preferSortMergeJoin = "false"
    configList.append("spark.sql.join.preferSortMergeJoin	" + str(spark_sql_join_preferSortMergeJoin) + "\n")

    # 是否开启adaptive query execution（自适应查询执行）
    if configVec[32] == 1:
        spark_sql_sort_enableRadixSort = "true"
    elif configVec[32] == 0:
        spark_sql_sort_enableRadixSort = "false"
    configList.append("spark.sql.sort.enableRadixSort	" + str(spark_sql_sort_enableRadixSort) + "\n")

    # 是否保留分组列
    if configVec[33] == 1:
        spark_sql_retainGroupColumns = "true"
    elif configVec[33] == 0:
        spark_sql_retainGroupColumns = "false"
    configList.append("spark.sql.retainGroupColumns	" + str(spark_sql_retainGroupColumns) + "\n")

    # 在激活整个stage codegen之前支持的最大字段（包括嵌套字段)
    spark_sql_codegen_maxFields = int(configVec[34])
    configList.append("spark.sql.codegen.maxFields	" + str(spark_sql_codegen_maxFields) + "\n")

    # 启用两级聚合哈希映射。当启用时，记录将首先“插入/查找第一级、小、快的映射，然后在第一级满或无法找到键时回落到第二级、更大、较慢的映射。当禁用时，记录直接进入第二级。默认为真
    if configVec[35] == 1:
        spark_sql_codegen_aggregate_map_twolevel_enable = "true"
    elif configVec[35] == 0:
        spark_sql_codegen_aggregate_map_twolevel_enable = "false"
    configList.append("spark.sql.codegen.aggregate.map.twolevel.enable	" + str(
        spark_sql_codegen_aggregate_map_twolevel_enable) + "\n")

    # 笛卡尔乘积算子保证存储在内存中的行数的阈值
    spark_sql_cartesianProductExec_buffer_in_memory_threshold = configVec[36]
    configList.append("spark.sql.cartesianProductExec.buffer.in.memory.threshold	" + str(
        spark_sql_cartesianProductExec_buffer_in_memory_threshold) + "\n")

    spark_sql_autoBroadcastJoinThreshold = min(20971520, int(configVec[37]))
    configList.append("spark.sql.autoBroadcastJoinThreshold      " + str(spark_sql_autoBroadcastJoinThreshold) + "\n")

    if enableSparkStreaming:
        if configVec[38] == 1:
            spark_streaming_backpressure_enabled = "true"
        elif configVec[38] == 0:
            spark_streaming_backpressure_enabled = "false"
        configList.append(
            "spark.streaming.backpressure.enabled     " + str(spark_streaming_backpressure_enabled) + "\n")

        spark_streaming_backpressure_initialRate = int(configVec[39])
        configList.append(
            "spark.streaming.backpressure.initialRate     " + str(spark_streaming_backpressure_initialRate) + "\n")

        if configVec[40] == 1:
            spark_streaming_receiver_writeAheadLog_enable = "true"
        elif configVec[40] == 0:
            spark_streaming_receiver_writeAheadLog_enable = "false"
        configList.append("spark.streaming.receiver.writeAheadLog.enable    " + str(
            spark_streaming_receiver_writeAheadLog_enable) + "\n")

        if configVec[41] == 1:
            spark_streaming_unpersist = "true"
        elif configVec[41] == 0:
            spark_streaming_unpersist = "false"
        configList.append("spark.streaming.unpersist    " + str(spark_streaming_unpersist) + "\n")

        spark_streaming_kafka_maxRatePerPartition = int(configVec[42])
        configList.append("spark.streaming.kafka.maxRatePerPartition    " + str(
            spark_streaming_kafka_maxRatePerPartition * 200) + "\n")

        if configVec[43] == 1:
            spark_streaming_driver_writeAheadLog_closeFileAfterWrite = "true"
        elif configVec[43] == 0:
            spark_streaming_driver_writeAheadLog_closeFileAfterWrite = "false"
        configList.append("spark.streaming.driver.writeAheadLog.closeFileAfterWrite    " + str(
            spark_streaming_driver_writeAheadLog_closeFileAfterWrite) + "\n")

        if configVec[44] == 1:
            spark_streaming_receiver_writeAheadLog_closeFileAfterWrite = "true"
        elif configVec[44] == 0:
            spark_streaming_receiver_writeAheadLog_closeFileAfterWrite = "false"
        configList.append("spark.streaming.receiver.writeAheadLog.closeFileAfterWrite    " + str(
            spark_streaming_receiver_writeAheadLog_closeFileAfterWrite) + "\n")

    makeSparkConfigFileCommand = "scp " + str(
        sparkTunerWarehouse) + "/SparkTunerReports/tmp/sparktunerConfigTemplate.conf " + \
                                 str(sparkTunerWarehouse) + "/SparkTunerReports/outputConfigurationFiles/" + str(
        filename) + "sparktunerConfig.conf"
    os.system(makeSparkConfigFileCommand)
    sparkConf = open(str(sparkTunerWarehouse) + "/SparkTunerReports/outputConfigurationFiles/" + str(
        filename) + "sparktunerConfig.conf", "a")
    for line in configList:
        sparkConf.write(line)

    sparkConf.close()
    return


# Convert normalized data to original data
def deCodein(inList, enableSparkStreaming):
    deList = []

    spark_io_compression_zstd_level = inList[0] * (5 - 1) + 1
    deList.append(spark_io_compression_zstd_level)

    spark_io_compression_zstd_bufferSize = inList[1] * (96 - 16) + 16
    deList.append(spark_io_compression_zstd_bufferSize)

    # The num of executors
    spark_executor_instances = inList[2] * (envParser.workerNum * envParser.nodeAvailableCore * 0.75 - 1) + 1
    deList.append(round(spark_executor_instances))

    # Application Propertie
    spark_driver_cores = inList[3] * (envParser.nodeAvailableCore - envParser.minAllocatedCore) + 1
    deList.append(round(spark_driver_cores))

    spark_driver_memory = inList[4] * (
            envParser.maxAllocatedMemory - envParser.minAllocatedMemory) + envParser.minAllocatedMemory
    deList.append(round(spark_driver_memory))

    if inList[5] > 0.5:
        spark_memory_offHeap_enabled = 1
    else:
        spark_memory_offHeap_enabled = 0
    deList.append(spark_memory_offHeap_enabled)

    spark_memory_offHeap_size = inList[6] * (envParser.maxAllocatedMemory * 0.2 - 512) + 512
    deList.append(round(spark_memory_offHeap_size))

    if spark_memory_offHeap_enabled == 1:
        spark_executor_memory = inList[7] * (
                envParser.maxAllocatedMemory - envParser.minAllocatedMemory - spark_memory_offHeap_size) + envParser.minAllocatedMemory
    else:
        spark_executor_memory = inList[7] * (
                envParser.maxAllocatedMemory - envParser.minAllocatedMemory) + envParser.minAllocatedMemory
    deList.append(round(spark_executor_memory))

    # Shuffle Behavior
    spark_reducer_maxSizeInFlight = inList[8] * (128 - 40) + 40
    deList.append(round(spark_reducer_maxSizeInFlight))

    if inList[9] > 0.5:
        spark_shuffle_compress = 1
    else:
        spark_shuffle_compress = 0
    deList.append(spark_shuffle_compress)

    spark_shuffle_file_buffer = inList[10] * (96 - 16) + 16
    deList.append(round(spark_shuffle_file_buffer))

    spark_shuffle_sort_bypassMergeThreshold = inList[11] * (1000 - 100) + 100
    deList.append(round(spark_shuffle_sort_bypassMergeThreshold))

    if inList[12] > 0.5:
        spark_shuffle_spill_compress = 1
    else:
        spark_shuffle_spill_compress = 0
    deList.append(spark_shuffle_spill_compress)

    # Compression and Serialization
    if inList[13] > 0.5:
        spark_broadcast_compress = 1
    else:
        spark_broadcast_compress = 0
    deList.append(spark_broadcast_compress)

    spark_kryoserializer_buffer_max = inList[14] * (128 - 8) + 8
    deList.append(round(spark_kryoserializer_buffer_max))

    spark_kryoserializer_buffer = inList[15] * (128 - 8) + 8
    deList.append(round(spark_kryoserializer_buffer))

    if inList[16] > 0.5:
        spark_rdd_compress = 1
    else:
        spark_rdd_compress = 0
    deList.append(spark_rdd_compress)

    # Memory Management
    spark_memory_fraction = inList[17] * (0.9 - 0.5) + 0.5
    deList.append(spark_memory_fraction)

    spark_memory_storageFraction = inList[18] * (0.9 - 0.1) + 0.1
    deList.append(spark_memory_storageFraction)

    spark_broadcast_blockSize = inList[19] * (5 - 1) + 1
    deList.append(round(spark_broadcast_blockSize))

    spark_executor_cores = inList[20] * (
            envParser.maxAllocatedCore - envParser.minAllocatedCore) + envParser.minAllocatedCore
    deList.append(round(spark_executor_cores))

    spark_storage_memoryMapThreshold = inList[21] * (500 - 50) + 50
    deList.append(round(spark_storage_memoryMapThreshold))

    # Scheduling
    spark_locality_wait = inList[22] * (10 - 1) + 1
    deList.append(round(spark_locality_wait))

    spark_scheduler_revive_interval = inList[23] * (1000 - 100) + 100
    deList.append(round(spark_scheduler_revive_interval))

    if spark_memory_offHeap_enabled == 1:
        spark_executor_memoryOverhead = inList[24] * (envParser.maxAllocatedMemory
                                                      - spark_memory_offHeap_size - spark_executor_memory - 384) + 384
    else:
        spark_executor_memoryOverhead = inList[24] * (envParser.maxAllocatedMemory - spark_executor_memory - 384) + 384
    deList.append(spark_executor_memoryOverhead)

    spark_default_parallelism = inList[25] * (
            envParser.nodeAvailableCore * envParser.workerNum * 3 - envParser.minAllocatedCore) + envParser.minAllocatedCore
    deList.append(round(spark_default_parallelism))

    spark_shuffle_io_numConnectionsPerPeer = inList[26] * (5 - 1) + 1
    deList.append(spark_shuffle_io_numConnectionsPerPeer)

    spark_sql_shuffle_partitions = inList[27] * (
            envParser.nodeAvailableCore * envParser.workerNum * 3 - envParser.minAllocatedCore) + envParser.minAllocatedCore
    deList.append(round(spark_sql_shuffle_partitions))

    spark_sql_inMemoryColumnarStorage_compressed = (inList[28] - 0) / (1 - 0)
    if spark_sql_inMemoryColumnarStorage_compressed > 0.5:
        spark_sql_inMemoryColumnarStorage_compressed = 1
    else:
        spark_sql_inMemoryColumnarStorage_compressed = 0
    deList.append(spark_sql_inMemoryColumnarStorage_compressed)

    spark_sql_inMemoryColumnarStorage_batchSize = inList[29] * (12000 - 8000) + 8000
    deList.append(spark_sql_inMemoryColumnarStorage_batchSize)

    spark_sql_inMemoryColumnarStorage_partitionPruning = (inList[30] - 0) / (1 - 0)
    if spark_sql_inMemoryColumnarStorage_partitionPruning > 0.5:
        spark_sql_inMemoryColumnarStorage_partitionPruning = 1
    else:
        spark_sql_inMemoryColumnarStorage_partitionPruning = 0
    deList.append(spark_sql_inMemoryColumnarStorage_partitionPruning)

    spark_sql_join_preferSortMergeJoin = (inList[31] - 0) / (1 - 0)
    if spark_sql_join_preferSortMergeJoin > 0.5:
        spark_sql_join_preferSortMergeJoin = 1
    else:
        spark_sql_join_preferSortMergeJoin = 0
    deList.append(spark_sql_join_preferSortMergeJoin)

    spark_sql_sort_enableRadixSort = (inList[32] - 0) / (1 - 0)
    if spark_sql_sort_enableRadixSort > 0.5:
        spark_sql_sort_enableRadixSort = 1
    else:
        spark_sql_sort_enableRadixSort = 0
    deList.append(spark_sql_sort_enableRadixSort)

    spark_sql_retainGroupColumns = (inList[33] - 0) / (1 - 0)
    if spark_sql_retainGroupColumns > 0.5:
        spark_sql_retainGroupColumns = 1
    else:
        spark_sql_retainGroupColumns = 0
    deList.append(spark_sql_retainGroupColumns)

    spark_sql_codegen_maxFields = inList[34] * (120 - 80) + 80
    deList.append(spark_sql_codegen_maxFields)

    spark_sql_codegen_aggregate_map_twolevel_enable = (inList[35] - 0) / (1 - 0)
    if spark_sql_codegen_aggregate_map_twolevel_enable > 0.5:
        spark_sql_codegen_aggregate_map_twolevel_enable = 1
    else:
        spark_sql_codegen_aggregate_map_twolevel_enable = 0
    deList.append(spark_sql_codegen_aggregate_map_twolevel_enable)

    spark_sql_cartesianProductExec_buffer_in_memory_threshold = inList[36] * (4096 - 2048) + 1024
    if spark_sql_cartesianProductExec_buffer_in_memory_threshold > 2048:
        spark_sql_cartesianProductExec_buffer_in_memory_threshold = 4096
    elif spark_sql_cartesianProductExec_buffer_in_memory_threshold > 1024:
        spark_sql_cartesianProductExec_buffer_in_memory_threshold = 2048
    else:
        spark_sql_cartesianProductExec_buffer_in_memory_threshold = 1024
    deList.append(spark_sql_cartesianProductExec_buffer_in_memory_threshold)

    spark_sql_autoBroadcastJoinThreshold = (inList[37] * (2 - 0.5) + 0.5) * 10485760
    deList.append(spark_sql_autoBroadcastJoinThreshold)

    if enableSparkStreaming:
        spark_streaming_backpressure_enabled = (inList[38] - 0) / (1 - 0)
        if spark_streaming_backpressure_enabled > 0.5:
            spark_streaming_backpressure_enabled = 1
        else:
            spark_streaming_backpressure_enabled = 0
        deList.append(spark_streaming_backpressure_enabled)

        spark_streaming_backpressure_initialRate = (inList[39] * (100 - 1) + 1) * 10000
        deList.append(spark_streaming_backpressure_initialRate)

        spark_streaming_receiver_writeAheadLog_enable = (inList[40] - 0) / (1 - 0)
        if spark_streaming_receiver_writeAheadLog_enable > 0.5:
            spark_streaming_receiver_writeAheadLog_enable = 1
        else:
            spark_streaming_receiver_writeAheadLog_enable = 0
        deList.append(spark_streaming_receiver_writeAheadLog_enable)

        spark_streaming_unpersist = (inList[41] - 0) / (1 - 0)
        if spark_streaming_unpersist > 0.5:
            spark_streaming_unpersist = 1
        else:
            spark_streaming_unpersist = 0
        deList.append(spark_streaming_unpersist)

        spark_streaming_kafka_maxRatePerPartition = (inList[42] * (100 - 1) + 1) * 200
        deList.append(spark_streaming_kafka_maxRatePerPartition)

        spark_streaming_driver_writeAheadLog_closeFileAfterWrite = (inList[43] - 0) / (1 - 0)
        if spark_streaming_driver_writeAheadLog_closeFileAfterWrite > 0.5:
            spark_streaming_driver_writeAheadLog_closeFileAfterWrite = 1
        else:
            spark_streaming_driver_writeAheadLog_closeFileAfterWrite = 0
        deList.append(spark_streaming_driver_writeAheadLog_closeFileAfterWrite)

        spark_streaming_receiver_writeAheadLog_closeFileAfterWrite = (inList[44] - 0) / (1 - 0)
        if spark_streaming_receiver_writeAheadLog_closeFileAfterWrite > 0.5:
            spark_streaming_receiver_writeAheadLog_closeFileAfterWrite = 1
        else:
            spark_streaming_receiver_writeAheadLog_closeFileAfterWrite = 0
        deList.append(spark_streaming_receiver_writeAheadLog_closeFileAfterWrite)

    return deList


# Convert original data to normalized data
def enCodein(inList, enableStreaming):
    enList = []

    spark_io_compression_zstd_level = (inList[0] - 1) / (5 - 1)
    enList.append(spark_io_compression_zstd_level)

    spark_io_compression_zstd_bufferSize = (inList[1] - 16) / (96 - 16)
    enList.append(spark_io_compression_zstd_bufferSize)

    # The num of executors
    spark_executor_instances = (inList[2] - 1) / (envParser.workerNum * envParser.nodeAvailableCore * 0.75 - 1)
    enList.append(spark_executor_instances)

    # Application Propertie
    spark_driver_cores = (inList[3] - envParser.minAllocatedCore) / (
            envParser.nodeAvailableCore - envParser.minAllocatedCore)
    enList.append(spark_driver_cores)

    spark_driver_memory = (inList[4] - envParser.minAllocatedMemory) / (
            envParser.maxAllocatedMemory - envParser.minAllocatedMemory)
    enList.append(spark_driver_memory)

    spark_memory_offHeap_enabled = (inList[5] - 0) / (1 - 0)
    if spark_memory_offHeap_enabled > 0.5:
        spark_memory_offHeap_enabled = 1
    else:
        spark_memory_offHeap_enabled = 0
    enList.append(spark_memory_offHeap_enabled)

    spark_memory_offHeap_size = (inList[6] - 512) / (envParser.nodeAvailableMemory * 0.5 - 512)
    enList.append(spark_memory_offHeap_size)

    spark_executor_memory = (inList[7] - envParser.minAllocatedMemory) / (
            envParser.maxAllocatedMemory - envParser.minAllocatedMemory)
    enList.append(spark_executor_memory)

    spark_reducer_maxSizeInFlight = (inList[8] - 40) / (128 - 40)
    enList.append(spark_reducer_maxSizeInFlight)

    spark_shuffle_compress = (inList[9] - 0) / (1 - 0)
    if spark_shuffle_compress > 0.5:
        spark_shuffle_compress = 1
    else:
        spark_shuffle_compress = 0
    enList.append(spark_shuffle_compress)

    spark_shuffle_file_buffer = (inList[10] - 16) / (96 - 16)
    enList.append(spark_shuffle_file_buffer)

    spark_shuffle_sort_bypassMergeThreshold = (inList[11] - 100) / (1000 - 100)
    enList.append(spark_shuffle_sort_bypassMergeThreshold)

    spark_shuffle_spill_compress = (inList[12] - 0) / (1 - 0)
    if spark_shuffle_spill_compress > 0.5:
        spark_shuffle_spill_compress = 1
    else:
        spark_shuffle_spill_compress = 0
    enList.append(spark_shuffle_spill_compress)

    # Compression and Serialization

    spark_broadcast_compress = (inList[13] - 0) / (1 - 0)
    if spark_broadcast_compress > 0.5:
        spark_broadcast_compress = 1
    else:
        spark_broadcast_compress = 0
    enList.append(spark_broadcast_compress)

    spark_kryoserializer_buffer_max = (inList[14] - 8) / (128 - 8)
    enList.append(spark_kryoserializer_buffer_max)

    spark_kryoserializer_buffer = (inList[15] - 8) / (128 - 8)
    enList.append(spark_kryoserializer_buffer)

    spark_rdd_compress = (inList[16] - 0) / (1 - 0)
    if spark_rdd_compress > 0.5:
        spark_rdd_compress = 1
    else:
        spark_rdd_compress = 0
    enList.append(spark_rdd_compress)

    spark_memory_fraction = (inList[17] - 0.5) / (0.9 - 0.5)
    enList.append(spark_memory_fraction)

    spark_memory_storageFraction = (inList[18] - 0.1) / (0.9 - 0.1)
    enList.append(spark_memory_storageFraction)

    spark_broadcast_blockSize = (inList[19] - 1) / (5 - 1)
    enList.append(spark_broadcast_blockSize)

    spark_executor_cores = (inList[20] - envParser.minAllocatedCore) / (
            envParser.maxAllocatedCore - envParser.minAllocatedCore)
    enList.append(spark_executor_cores)

    spark_storage_memoryMapThreshold = (inList[21] - 50) / (500 - 50)
    enList.append(spark_storage_memoryMapThreshold)
    # Scheduling

    spark_locality_wait = (inList[22] - 1) / (10 - 1)
    enList.append(spark_locality_wait)

    spark_scheduler_revive_interval = (inList[23] - 100) / (1000 - 100)
    enList.append(spark_scheduler_revive_interval)

    spark_executor_memoryOverhead = (inList[24] - 384) / (envParser.nodeAvailableMemory - 384)
    enList.append(spark_executor_memoryOverhead)

    spark_default_parallelism = (inList[25] - envParser.minAllocatedCore) / (
            envParser.nodeAvailableCore * envParser.workerNum * 3 - envParser.minAllocatedCore)
    enList.append(spark_default_parallelism)

    spark_shuffle_io_numConnectionsPerPeer = (inList[26] - 1) / (5 - 1)
    enList.append(spark_shuffle_io_numConnectionsPerPeer)

    spark_sql_shuffle_partitions = (inList[27] - envParser.minAllocatedCore) / (
            envParser.nodeAvailableCore * envParser.workerNum * 3 - envParser.minAllocatedCore)
    enList.append(spark_sql_shuffle_partitions)

    spark_sql_inMemoryColumnarStorage_compressed = (inList[28] - 0) / (1 - 0)
    if spark_sql_inMemoryColumnarStorage_compressed > 0.5:
        spark_sql_inMemoryColumnarStorage_compressed = 1
    else:
        spark_sql_inMemoryColumnarStorage_compressed = 0
    enList.append(spark_sql_inMemoryColumnarStorage_compressed)

    spark_sql_inMemoryColumnarStorage_batchSize = (inList[29] - 8000) / (12000 - 8000)
    enList.append(spark_sql_inMemoryColumnarStorage_batchSize)

    spark_sql_inMemoryColumnarStorage_partitionPruning = (inList[30] - 0) / (1 - 0)
    if spark_sql_inMemoryColumnarStorage_partitionPruning > 0.5:
        spark_sql_inMemoryColumnarStorage_partitionPruning = 1
    else:
        spark_sql_inMemoryColumnarStorage_partitionPruning = 0
    enList.append(spark_sql_inMemoryColumnarStorage_partitionPruning)

    spark_sql_join_preferSortMergeJoin = (inList[31] - 0) / (1 - 0)
    if spark_sql_join_preferSortMergeJoin > 0.5:
        spark_sql_join_preferSortMergeJoin = 1
    else:
        spark_sql_join_preferSortMergeJoin = 0
    enList.append(spark_sql_join_preferSortMergeJoin)

    spark_sql_sort_enableRadixSort = (inList[32] - 0) / (1 - 0)
    if spark_sql_sort_enableRadixSort > 0.5:
        spark_sql_sort_enableRadixSort = 1
    else:
        spark_sql_sort_enableRadixSort = 0
    enList.append(spark_sql_sort_enableRadixSort)

    spark_sql_retainGroupColumns = (inList[33] - 0) / (1 - 0)
    if spark_sql_retainGroupColumns > 0.5:
        spark_sql_retainGroupColumns = 1
    else:
        spark_sql_retainGroupColumns = 0
    enList.append(spark_sql_retainGroupColumns)

    spark_sql_codegen_maxFields = (inList[34] - 80) / (120 - 80)
    enList.append(spark_sql_codegen_maxFields)

    spark_sql_codegen_aggregate_map_twolevel_enable = (inList[35] - 0) / (1 - 0)
    if spark_sql_codegen_aggregate_map_twolevel_enable > 0.5:
        spark_sql_codegen_aggregate_map_twolevel_enable = 1
    else:
        spark_sql_codegen_aggregate_map_twolevel_enable = 0
    enList.append(spark_sql_codegen_aggregate_map_twolevel_enable)

    spark_sql_cartesianProductExec_buffer_in_memory_threshold = (inList[36] - 1024) / (4096 - 1024)
    if spark_sql_cartesianProductExec_buffer_in_memory_threshold > (2 / 3):
        spark_sql_cartesianProductExec_buffer_in_memory_threshold = 1
    elif spark_sql_cartesianProductExec_buffer_in_memory_threshold >= (1 / 3):
        spark_sql_cartesianProductExec_buffer_in_memory_threshold = 0.5
    else:
        spark_sql_cartesianProductExec_buffer_in_memory_threshold = 0
    enList.append(spark_sql_cartesianProductExec_buffer_in_memory_threshold)

    spark_sql_autoBroadcastJoinThreshold = (inList[37] / 10485760 - 0.5) / (2 - 0.5)
    enList.append(spark_sql_autoBroadcastJoinThreshold)

    if enableStreaming:
        spark_streaming_backpressure_enabled = (inList[38] - 0) / (1 - 0)
        if spark_streaming_backpressure_enabled > 0.5:
            spark_streaming_backpressure_enabled = 1
        else:
            spark_streaming_backpressure_enabled = 0
        enList.append(spark_streaming_backpressure_enabled)

        spark_streaming_backpressure_initialRate = (inList[39] / 10000 - 1) / (100 - 1)
        enList.append(spark_streaming_backpressure_initialRate)

        spark_streaming_receiver_writeAheadLog_enable = (inList[40] - 0) / (1 - 0)
        if spark_streaming_receiver_writeAheadLog_enable > 0.5:
            spark_streaming_receiver_writeAheadLog_enable = 1
        else:
            spark_streaming_receiver_writeAheadLog_enable = 0
        enList.append(spark_streaming_receiver_writeAheadLog_enable)

        spark_streaming_unpersist = (inList[41] - 0) / (1 - 0)
        if spark_streaming_unpersist > 0.5:
            spark_streaming_unpersist = 1
        else:
            spark_streaming_unpersist = 0
        enList.append(spark_streaming_unpersist)

        spark_streaming_kafka_maxRatePerPartition = (inList[42] / 200 - 1) / (100 - 1)
        enList.append(spark_streaming_kafka_maxRatePerPartition)

        spark_streaming_driver_writeAheadLog_closeFileAfterWrite = (inList[43] - 0) / (1 - 0)
        if spark_streaming_driver_writeAheadLog_closeFileAfterWrite > 0.5:
            spark_streaming_driver_writeAheadLog_closeFileAfterWrite = 1
        else:
            spark_streaming_driver_writeAheadLog_closeFileAfterWrite = 0
        enList.append(spark_streaming_driver_writeAheadLog_closeFileAfterWrite)

        spark_streaming_receiver_writeAheadLog_closeFileAfterWrite = (inList[44] - 0) / (1 - 0)
        if spark_streaming_receiver_writeAheadLog_closeFileAfterWrite > 0.5:
            spark_streaming_receiver_writeAheadLog_closeFileAfterWrite = 1
        else:
            spark_streaming_receiver_writeAheadLog_closeFileAfterWrite = 0
        enList.append(spark_streaming_receiver_writeAheadLog_closeFileAfterWrite)

    return enList


# Normalized the sampling data
def normalizeData():
    data = []
    nomalizedData = []

    with open(str(sparkTunerWarehouse) + "/SparkTunerReports/time_conf.csv", 'rt') as csvfile:
        next(csvfile)
        reader = csv.reader(csvfile)
        for row in reader:
            data.append(row)
    columnsWithStreaming = ["V1", "V2", "V3", "V4", "V5", "V6", "V7", "V8"
        , "V9", "V10", "V11", "V12", "V13", "V14", "V15"
        , "V16", "V17", "V18", "V19", "V20", "V21", "V22"
        , "V23", "V24", "V25", "V26", "V27", "V28", "V29"
        , "V30", "V31", "V32", "V33", "V34", "V35", "V36"
        , "V37", "V38", "V39", "V40", "V41", "V42", "V43"
        , "V44", "V45", "V46"]

    columnsWithStreamingTrans = ['V6', 'V10', 'V13', 'V14', 'V17', 'V29'
        , 'V31', 'V32', 'V33', 'V34', 'V36', 'V39', 'V41', 'V42'
        , 'V44', 'V45']

    columnsWithOutStreaming = ["V1", "V2", "V3", "V4", "V5", "V6", "V7", "V8"
        , "V9", "V10", "V11", "V12", "V13", "V14", "V15"
        , "V16", "V17", "V18", "V19", "V20", "V21", "V22"
        , "V23", "V24", "V25", "V26", "V27", "V28", "V29"
        , "V30", "V31", "V32", "V33", "V34", "V35", "V36"
        , "V37", "V38", "V39"]

    columnsWithOutStreamingTrans = ['V6', 'V10', 'V13', 'V14', 'V17', 'V29', 'V31'
        , 'V32', 'V33', 'V34', 'V36']

    columns = columnsWithOutStreaming
    columnsTrans = columnsWithOutStreamingTrans
    if enableStreaming:
        columns = columnsWithStreaming
        columnsTrans = columnsWithStreamingTrans
    df = pd.DataFrame(data[0:], columns=columns)
    transformer = LabelEncoder()
    for col in columnsTrans:
        df[col] = transformer.fit_transform(df[col].values)
    if os.path.exists(str(sparkTunerWarehouse) + "/SparkTunerReports/Normalized_confs.csv"):
        cmdDeleteNormalizedFile = "rm " + str(sparkTunerWarehouse) + "/SparkTunerReports/Normalized_confs.csv"
        os.system(cmdDeleteNormalizedFile)
    NFile = open(str(sparkTunerWarehouse) + "/SparkTunerReports/Normalized_confs.csv", "a")
    NFile_csv = csv.writer(NFile)
    NFile_csv.writerow(columns)
    for i in range(df.shape[0]):
        row = list(map(float, df.iloc[i].values))
        newRow = enCodein(row, enableStreaming)
        ExtralColumns = [row[len(columns) - 1]]
        newRow.extend(ExtralColumns)
        nomalizedData.append(list(map(float, newRow)))
        NFile_csv.writerow(newRow)
    NFile.close()


if __name__ == '__main__':
    normalizeData()
