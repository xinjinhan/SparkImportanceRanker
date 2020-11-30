# -*- coding:utf-8 _*-

import csv
import random
import time
from collections import Sequence
from itertools import repeat

import numpy as np
from deap import base, creator
from deap import tools
from sklearn import ensemble

import EnvironmentParser as envParser
import DataProcessor as dataProcessor
import SGBRTModeler as modeler

sparkTunerWarehouse = envParser.sparkTunerWarehouse
enableStreaming = envParser.enableSparkStreamingTuner


def loadModel():
    regr = ensemble.GradientBoostingRegressor()
    coefficientOfDetermination = 0
    times = 0
    while times < 10 and coefficientOfDetermination < 0.6:
        XTrain, XTest, YTrain, YTest = modeler.divideTrainingSetAndTestSet()
        regr = modeler.initializeGradientBoostingRegressor(XTrain, XTest, YTrain, YTest)
        coefficientOfDetermination = min(regr.score(XTrain, YTrain), regr.score(XTest, YTest))
        times += 1
        print(str(times) + " " + str(coefficientOfDetermination))
    return regr, coefficientOfDetermination


regr, coefficientOfDetermination = loadModel()
LOW = 0.0
UP = 1.0
conflen = envParser.getConfigOrFeatureNum()
importance = []
for i in range(conflen):
    importance.append(10 / conflen)

creator.create("FitnessMin", base.Fitness, weights=(-1.0,))
creator.create("Individual", list, fitness=creator.FitnessMin)

LOW = 0.0
UP = 1.0
IND_SIZE = conflen  # Number of chromosomes
toolbox = base.Toolbox()
toolbox.register("attr_float", random.uniform, 0, 1)
toolbox.register("individual", tools.initRepeat, creator.Individual, toolbox.attr_float, n=IND_SIZE)
toolbox.register("population", tools.initRepeat, list, toolbox.individual)


def evaluate(individual):
    indivList = individual
    # print("indivList == " + str(indivList))
    pre_res = regr.predict(np.array([indivList]))
    # print("predict == " + str(pre_res) + "\n")
    return pre_res,


def mutate(individual, low, up, indpbs):
    """Mutate an individual by replacing attributes, with probability *indpb*,
      by a integer uniformly drawn between *low* and *up* inclusively.

      :param individual: :term:`Sequence <sequence>` individual to be mutated.
      :param low: The lower bound or a :term:`python:sequence` of
                  of lower bounds of the range from wich to draw the new
                  integer.
      :param up: The upper bound or a :term:`python:sequence` of
                 of upper bounds of the range from wich to draw the new
                 integer.
      :param indpbs: Independent probability for each attribute to be mutated.
      :returns: A tuple of one individual.
      """
    size = len(individual)
    if not isinstance(low, Sequence):
        low = repeat(low, size)
    elif len(low) < size:
        raise IndexError("low must be at least the size of individual: %d < %d" % (len(low), size))
    if not isinstance(up, Sequence):
        up = repeat(up, size)
    elif len(up) < size:
        raise IndexError("up must be at least the size of individual: %d < %d" % (len(up), size))
    for i, xl, xu, indpb in zip(range(size), low, up, indpbs):
        if random.random() < indpb:
            tmp = random.uniform(xl, xu) / 3
            if individual[i] + tmp <= 1:
                individual[i] = individual[i] + tmp
    return individual,


# use tools in deap to creat our application
toolbox.register("mate", tools.cxTwoPoint)  # mate: cross
toolbox.register("mutate", mutate, low=LOW, up=UP, indpbs=importance)  # mutate
toolbox.register("select", tools.selTournament, tournsize=3)  # select: select the best individual to keep
toolbox.register("evaluate", evaluate)  # commit our evaluate


def gaSearch(popSize, NGEN):
    # create an initial population of 300 individuals (where
    # each individual == a list of integers)
    randomSeed = int(time.time())
    random.seed(randomSeed)
    pop = toolbox.population(n=popSize)
    CXPB, MUTPB = 0.5, 0.4

    '''
    # CXPB  == the probability with which two individuals
    #       are crossed
    #
    # MUTPB == the probability for mutating an individual
    #
    # NGEN  == the number of generations for which the
    #       evolution runs
    '''
    # Evaluate the entire population
    fitnesses = map(toolbox.evaluate, pop)
    print("---------- Finish Population Initialing ----------")
    print("----------         Searching......      ----------")
    for ind, fit in zip(pop, fitnesses):
        ind.fitness.values = fit
    # print("=================================================")
    # print("        Evaluated %i individuals" % len(pop))  # LEN(POP) should be 300
    # print("        Iterative %i times --" % NGEN)
    # print("=================================================")
    genralConfList = []
    for g in range(NGEN):
        if g % 10 == 0:
            print("========    Generation " + str(g + 10) + "has finished    =========")
            tmp = tools.selBest(pop, 1)[0]
            # tmp == the population information in every generation
            genralConfList.append(tmp)
        # Select the next generation individuals
        offspring = toolbox.select(pop, len(pop))
        # Clone the selected individuals
        offspring = list(map(toolbox.clone, offspring))
        # Change map to list,The documentation on the official website == wrong

        # Apply crossover and mutation on the offspring
        for child1, child2 in zip(offspring[::2], offspring[1::2]):
            if random.random() < CXPB:
                toolbox.mate(child1, child2)
                del child1.fitness.values
                del child2.fitness.values
        for mutant in offspring:
            if random.random() < MUTPB:
                toolbox.mutate(mutant)
                del mutant.fitness.values
        # Evaluate the individuals with an invalid fitness
        invalid_ind = [ind for ind in offspring if not ind.fitness.valid]
        fitnesses = map(toolbox.evaluate, invalid_ind)
        for ind, fit in zip(invalid_ind, fitnesses):
            ind.fitness.values = fit
        # The population == entirely replaced by the offspring
        pop[:] = offspring
    best_ind = tools.selBest(pop, 1)[0]
    genralConfList.append(best_ind)
    # return the result:Last individual,The Return of Evaluate function
    return best_ind, best_ind.fitness.values, genralConfList


def main():
    import time
    decodeArray = []
    if coefficientOfDetermination < 0.6:
        data = []
        time = []
        with open(str(sparkTunerWarehouse) + "/SparkTunerReports/Normalized_confs.csv", 'r') as csvfile:
            next(csvfile)
            reader = csv.reader(csvfile)
            for row in reader:
                data.append(row)
                time.append(row[-1])
        sortedTime = time.copy()
        sortedTime = [float(s) for s in sortedTime]
        sortedTime.sort()
        if enableStreaming:
            sortedTime.reverse()
        flag = 0
        flagTime = str(sortedTime[flag])
        conf = data[time.index(flagTime)]
        conf = [float(c) for c in conf]
        temp = dataProcessor.deCodein(conf, enableStreaming)
        dataProcessor.writeConf(temp, 0, enableStreaming)
    else:
        popSize = 10000  # the size of population
        NGEN = envParser.iterationTime  # the num of generations
        t1 = time.time()
        best_ind, best_ind.fitness.values, generalConfList = gaSearch(popSize, NGEN)
        t2 = time.time()
        sparktunerRecord = open(str(sparkTunerWarehouse) + "/SparkTunerReports/record.txt", "a")
        print("Searching Costs Time    " + str(int(t2 - t1)) + "s", file=sparktunerRecord)
        sparktunerRecord.close()

        a = open("/home/BernardX/tmp/output.csv", "w")
        b = csv.writer(a)

        for i in range(len(generalConfList)):
            temp = dataProcessor.deCodein(generalConfList[i], enableStreaming)

            b.writerow(generalConfList[i])

            decodeArray.append(temp)
            dataProcessor.writeConf(temp, i, enableStreaming)
        a.close()

if __name__ == '__main__':
    print("==================================================" + "\n"
          + "    Model Building & Configurations Searching     " + "\n"
          + "--------------------------------------------------")
    main()
    print("--------------------------------------------------" + "\n"
        + "Model Building & Configurations Searching Finished" + "\n"
        + "==================================================" + "\n")
