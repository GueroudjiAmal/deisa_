import sys
import dask
import numpy as np
import pandas as pd
import dask.array as da
from dask.distributed import Client, Event, get_client, comm, Queue, Future, Variable
import time
import asyncio
import json


class metadata:
    index = list()
    data = ""
    shap = None
    typ = ""
    def __init__(self, name):
        self.name = name

def connect(sched_file):
    # reconstruct a string
    sched = ''.join(chr(i) for i in sched_file)
    with open(sched[:-1]) as f:
        s = json.load(f)
    adr = s["address"]
    client  = get_client(adr)
    return client


def init(sched_file, rank, size, arrays, deisa_arrays_dtype):
    client = connect(sched_file)
    return Bridge(client, size, rank, arrays, deisa_arrays_dtype)

class Bridge:
    workers = []
    def __init__(self, Client, Ssize, rank, arrays, deisa_arrays_dtype):
        self.client  = Client
        self.rank = rank
        listw = Variable("workers").get()
        if Ssize > len(listw): #more processes than workers
            self.workers = [listw[rank%len(listw)]]
        else:
            k = len(listw)//Ssize # more workers than processes
            self.workers = listw[rank*k:rank*k+ k]
        self.arrays = arrays

        for ele in self.arrays:
            self.arrays[ele]["dtype"] = str(deisa_arrays_dtype[ele])
            self.arrays[ele]["timedim"] = self.arrays[ele]["timedim"][0]
            self.position = [self.arrays[ele]["starts"][i]//self.arrays[ele]["subsizes"][i] for i in range(len(np.array(self.arrays[ele]["sizes"])))]
            print(self.rank, self.position)
        Variable("Arrays").set(arrays)

    def create_key(self, timestep, name):
        self.position[self.arrays[name]["timedim"]]= timestep
        position = tuple(self.position)
        return (name, position)

    def publish_data(self, data, data_name, timestep):
        key = self.create_key(timestep, data_name)
        self.client.scatter(data, direct = True, workers=self.workers)

class CoupleDask :
    adr = ""
    client = None
    workers = []
    queues = []
    def __init__(self, Ssize, Sworker):
        with open('scheduler.json') as f:
            s = json.load(f)
        self.adr = s["address"]
        self.client  = get_client(self.adr)
        self.workers = [comm.get_address_host_port(i,strict=False) for i in self.client.scheduler_info()["workers"].keys()]
        while (len(self.workers)!= Sworker):
            self.workers = [comm.get_address_host_port(i,strict=False) for i in self.client.scheduler_info()["workers"].keys()]

        Variable("workers").set(self.workers)
        self.Ssize = Ssize
        self.arrays = dict()


    def get_data(self):
        self.arrays = Variable("Arrays").get()
        print(self.arrays)

    def create_dask_array(self, array):
        print("create dask array ")


def Initialization(Ssize, Sworker):
    return CoupleDask(Ssize, Sworker)
