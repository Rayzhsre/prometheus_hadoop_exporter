# -*- coding: utf-8 -*-
"""
    Prometheus hadoop exporter base hadoop rest api's.
    Created by Rayzh.
"""

from prometheus_client import generate_latest, Gauge, CollectorRegistry
from metrics import RManager, NamenodeJMX
from flask import Response, Flask
import json


app = Flask(__name__)

@app.route("/")
def index():
    return 'prometheus hadoop exporter'

@app.route("/metrics")
def hadoopResponse():
    cluster_nodes.set(json.loads((rm.clusterinfo()))['cluster_totalNodes'])
    cluster_unhealthyNodes.set(json.loads((rm.clusterinfo()))['cluster_unhealthyNodes'])
    cluster_availableGB.set(json.loads((rm.clusterinfo()))['cluster_availableGB'])
    cluster_totalGB.set(json.loads((rm.clusterinfo()))['cluster_totalGB'])
    cluster_totalVirtualCores.set(json.loads((rm.clusterinfo()))['cluster_totalVirtualCores'])
    cluster_availableVirtualCores.set(json.loads((rm.clusterinfo()))['cluster_availableVirtualCores'])

    memheapmaxm.set(json.loads((nnjmx.nnmemory()))['memheapmax'])
    memheapusedm.set(json.loads((nnjmx.nnmemory()))['memheapused'])
    gctimemillis.set(json.loads((nnjmx.nnmemory()))['gctimemilli'])

    fsused.set(json.loads(nnjmx.nnfssystem())['fsused'])
    fsfiletotal.set(json.loads(nnjmx.nnfssystem())['fsfiletotal'])
    dnlived.set(json.loads(nnjmx.nnfssystem())['dnlived'])
    dndead.set(json.loads(nnjmx.nnfssystem())['dndead'])
    fstotal.set(json.loads(nnjmx.nnfssystem())['fstotal'])

    return Response(generate_latest(REGISTRY), mimetype="text/plain")
    

if __name__ == '__main__':
    REGISTRY = CollectorRegistry(auto_describe=False)
    # RM 
    rm = RManager('http://100.33.31.127:8088/')
    cluster_nodes = Gauge('totalNodes', 'cluster total nodes', registry=REGISTRY)
    cluster_unhealthyNodes = Gauge('unhealthyNodes', 'cluster unhealth Nodes', registry=REGISTRY)
    cluster_availableGB = Gauge('availableGB', 'cluster availableGB', registry=REGISTRY)
    cluster_totalGB = Gauge('totalGB', 'cluster totalGB', registry=REGISTRY)
    cluster_totalVirtualCores = Gauge('totalVirtualCores', 'cluster totalVirtualCores', registry=REGISTRY)
    cluster_availableVirtualCores = Gauge('availableVirtualCores', 'cluster availableVirtualCores', registry=REGISTRY)

    # JMX
    nnjmx = NamenodeJMX('http://100.33.31.127:50070/')
    memheapmaxm = Gauge('jvmheapmax', 'cluster jvm heap max', registry=REGISTRY)
    memheapusedm = Gauge('jvmheapused', 'cluster jvm heap used', registry=REGISTRY)
    gctimemillis = Gauge('jvmgctime', 'cluster jvm gc time', registry=REGISTRY)

    # HDFS
    fstotal = Gauge('hdfsstotal', 'hdfs total', registry=REGISTRY)
    fsused = Gauge('hdfsused', 'hdfs used', registry=REGISTRY)
    fsfiletotal = Gauge('hdfsfilestotal', 'hdfs files total', registry=REGISTRY)
    dnlived = Gauge('datanodelived', 'datanode alived', registry=REGISTRY)
    dndead = Gauge('datanodedead', 'datanode dead', registry=REGISTRY)

    app.run(host="0.0.0.0")
