# -*- coding: utf-8 -*-
"""
    Hadoop metrics.
    Created by Rayzh.
"""

import requests
import json

class RManager:
    def __init__(self, rmaddrs='http://localhost:8088/'):
        self.rmaddrs = rmaddrs
    
    def clusterinfo(self):
        res = requests.get(self.rmaddrs + 'ws/v1/cluster/metrics')
        cluster_totalNodes = res.json()['clusterMetrics']['totalNodes']
        cluster_unhealthyNodes = res.json()['clusterMetrics']['unhealthyNodes']
        cluster_availableGB = res.json()['clusterMetrics']['availableMB'] / 1024
        cluster_totalGB = res.json()['clusterMetrics']['totalMB'] / 1024
        cluster_totalVirtualCores = res.json()['clusterMetrics']['totalVirtualCores']
        cluster_availableVirtualCores = res.json()['clusterMetrics']['availableVirtualCores']

        ret = dict(cluster_totalNodes=cluster_totalNodes, cluster_unhealthyNodes=cluster_unhealthyNodes, cluster_availableGB=cluster_availableGB, 
                        cluster_totalGB=cluster_totalGB, cluster_totalVirtualCores=cluster_totalVirtualCores, cluster_availableVirtualCores=cluster_availableVirtualCores)
        return json.dumps(ret)

    def clusternodes(self):
        res = requests.get(self.rmaddrs + 'ws/v1/cluster/nodes')
        nodes = res.json()['nodes']['node']
        ret = dict()
        for each_node in nodes:
            state = each_node['state']
            hostname = each_node['nodeHostName']
            usedMemory = each_node['usedMemoryMB'] / 1024
            availMemory = each_node['availMemoryMB'] / 1024
            node_dict = dict()
            node_dict['hostname'] = hostname
            node_dict['state'] = state
            node_dict['usedMemory'] = usedMemory
            node_dict['availMemory'] = availMemory

            ret.setdefault('nodes',[]).append(node_dict)
        return json.dumps(ret)

class NamenodeJMX:
    def __init__(self, nnaddrs='http://localhost:50070/'):
        self.nnaddrs = nnaddrs
    
    def nnmemory(self):
        res = requests.get(self.nnaddrs + 'jmx?qry=Hadoop:service=NameNode,name=JvmMetrics')
        memheapmaxm = res.json()['beans'][0]['MemHeapMaxM']
        memheapusedm = res.json()['beans'][0]['MemHeapUsedM']
        gctimemillis = res.json()['beans'][0]['GcTimeMillis']

        ret = dict(memheapmax=memheapmaxm, memheapused=memheapusedm, gctimemilli=gctimemillis)
        return json.dumps(ret)

    def nnfssystem(self):
        res = requests.get(self.nnaddrs + 'jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState')
        fstotal = round(res.json()['beans'][0]['CapacityTotal']/1024/1024/1024,2)
        fsused = round(res.json()['beans'][0]['CapacityUsed']/1024/1024/1024,2)
        fsfiletotal = res.json()['beans'][0]['FilesTotal']
        dnlived = res.json()['beans'][0]['NumLiveDataNodes']
        dndead = res.json()['beans'][0]['NumDeadDataNodes']

        ret = dict(fstotal=fstotal, fsused=fsused, fsfiletotal=fsfiletotal, dnlived=dnlived, dndead=dndead)
        return json.dumps(ret)

    



