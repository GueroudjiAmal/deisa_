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

def publish_data(data, timestep):
    print("time_step", timestep)

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
            self.position = np.array(self.arrays[ele]["starts"])//np.array(self.arrays[ele]["sizes"])
            print(self.rank, self.position)
        Variable("Arrays").set(arrays)
        print("init done")

    def publish_data(slf, data, timestep):
        print("time_step", timestep)

    def publish_data1(self, g, data) :
        name = "dns" + str(self.rank) + "g" + str(g)
        index = self.position.tolist()
        index.append(g.item())
        ds = metadata(name)
        ds.data = None
        ds.index = index
        ds.shap = data.shape
        ds.typ = str(data.dtype)
        tic = time.perf_counter()
        d_future = self.client.scatter(data, direct = True, workers=self.workers)
        toc = time.perf_counter()
        scatter = toc-tic
        to = time.perf_counter()
        self.queue.put(dict(ds.__dict__.items()))
        self.queue.put( d_future)
        to1 = time.perf_counter()
        q = to1 -to
        toc2 = time.perf_counter()
        publish = toc2-tic2
        return scatter, publish, q

    def Finalize(self):
        self.queue.put(1)


def Init1(Ssize, rank, pos, gmax):
    return Bridge(Ssize,rank, pos, gmax)

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
        self.queues = [Queue("queue"+str(i)) for i in range(Ssize)]
        self.Ssize = Ssize
        self.arrays = dict()


    def get_data(self):
        self.arrays = Variable("Arrays").get()
        print(self.arrays)

    def Finalization(self):
        for q in self.queues:
            q.get()
        #self.client.shutdown()

def Initialization(Ssize, Sworker):
    return CoupleDask(Ssize, Sworker)
