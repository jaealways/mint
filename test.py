import random
from copy import deepcopy

import numpy as np
import plotly.graph_objects as go
from dtaidistance import dtw
from plotly.subplots import make_subplots
from scipy import interpolate
import math
import matplotlib.pyplot as plt


NUM_OF_TRAJECTORIES = 100
MIN_LEN_OF_TRAJECTORY = 16
MAX_LEN_OF_TRAJECTORY = 40

THRESHOLD = 1.0

trajectoriesSet = {}

for item in range(NUM_OF_TRAJECTORIES):
    length = MAX_LEN_OF_TRAJECTORY
    tempTrajectory = np.random.randint(low=-100, high=100, size=int(length / 4)).astype(float) / 100

    oldScale = np.arange(0, int(length / 4))
    interpolationFunction = interpolate.interp1d(oldScale, tempTrajectory)

    newScale = np.linspace(0, int(length / 4) - 1, length)
    tempTrajectory = interpolationFunction(newScale)

    trajectoriesSet[(str(item),)] = [tempTrajectory]

trajectories = deepcopy(trajectoriesSet)
distanceMatrixDictionary = {}

iteration = 1
while True:
    distanceMatrix = np.empty((len(trajectories), len(trajectories),))
    distanceMatrix[:] = np.nan

    for index1, (filter1, trajectory1) in enumerate(trajectories.items()):
        tempArray = []

        for index2, (filter2, trajectory2) in enumerate(trajectories.items()):

            if index1 > index2:
                continue

            elif index1 == index2:
                continue

            else:
                unionFilter = filter1 + filter2
                sorted(unionFilter)

                if unionFilter in distanceMatrixDictionary.keys():
                    distanceMatrix[index1][index2] = distanceMatrixDictionary.get(unionFilter)

                    continue

                metric = []
                for subItem1 in trajectory1:

                    for subItem2 in trajectory2:
                        metric.append(dtw.distance(subItem1, subItem2, psi=1))

                metric = max(metric)

                distanceMatrix[index1][index2] = metric
                distanceMatrixDictionary[unionFilter] = metric

    minValue = np.min(list(distanceMatrixDictionary.values()))

    if minValue > THRESHOLD:
        break

    minIndices = np.where(distanceMatrix == minValue)
    minIndices = list(zip(minIndices[0], minIndices[1]))

    minIndex = minIndices[0]

    filter1 = list(trajectories.keys())[minIndex[0]]
    filter2 = list(trajectories.keys())[minIndex[1]]

    trajectory1 = trajectories.get(filter1)
    trajectory2 = trajectories.get(filter2)

    unionFilter = filter1 + filter2
    sorted(unionFilter)

    trajectoryGroup = trajectory1 + trajectory2

    trajectories = {key: value for key, value in trajectories.items()
                    if all(value not in unionFilter for value in key)}

    distanceMatrixDictionary = {key: value for key, value in distanceMatrixDictionary.items()
                                if all(value not in unionFilter for value in key)}

    trajectories[unionFilter] = trajectoryGroup

    print(iteration, 'finished!')
    iteration += 1

    if len(list(trajectories.keys())) == 1:
        break

for key, _ in trajectories.items():
    print(key)

dtw_x = dtw_y = math.ceil(math.sqrt(len(trajectories)))
dtw_n = [(x, y) for x in range(dtw_x) for y in range(dtw_y)][:len(trajectories)]

fig, axs = plt.subplots(dtw_x, dtw_y, figsize=(25, 25))
fig.suptitle('Clusters')

for value, dtw_cluster in zip(trajectories.values(), dtw_n):
    for index, subValue in enumerate(value):
        axs[dtw_cluster].plot(subValue, c="gray", alpha=0.5)
    axs[dtw_cluster].plot(np.average(np.vstack(value), axis=0), c="blue")
    cluster_number = dtw_cluster[0] * dtw_y + dtw_cluster[1] + 1
    axs[dtw_cluster].set_title(f"Cluster {cluster_number}")

plt.show()

