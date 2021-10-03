import time
from dask_interface import Initialization
from  dask_interface import CoupleDask
import numpy as np
import pandas as pd
import yaml
import dask.array as da

with open(r'config.yml') as file:
    data = yaml.load(file, Loader=yaml.FullLoader)
    Ssize = data["parallelism"]["height"]*data["parallelism"]["width"]
    generations = data["MaxtimeSteps"]
    Sworkers = data["workers"]
    timeStep = 1
C = Initialization(Ssize, Sworkers)
C.client.get_versions(check=True)
print("je suis la moi dans le client apres le check versions")

# Results
Results = []

for g in range(0, generations, timeStep):
    #arrays = C.get_data()
    #arrays = da.reshape(arrays, (arrays.shape[1],arrays.shape[2]))
    #s = arrays.sum().compute()
    #Results.append(s)
    #print(arrays)
    print("je suis ds la bcl")
print(Results)
