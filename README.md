# Pravega [![Build Status](https://travis-ci.com/emccode/pravega.svg?token=qhH3WLZqyhzViixpn6ZT&branch=master)](https://travis-ci.com/emccode/pravega) [![Gitter chat](https://badges.gitter.im/emccode/pravega/services.png)](https://gitter.im/emccode/pravega)

Pravega is a distributed storage service offering a new storage abstraction called a Stream

### **Durable**
Data is replicated and persisted to disk before being acknowledged.

### **Exactly once delivery**
Producers use transaction to ensure data is written exactly once. 

### **Infinite**
Pravega is designed to store streams for infinite period of time. Size of stream is not bounded by the capacity of a node, but by the capacity of a cluster.

### **Elastic** 
Due to the variable nature of volume, variety and velocity of incoming and outgoing data streams, Pravega dynamically and transparently splits and merges segments of streams based on load and throughout. 

### **Scalable**
Pravega is designed to have no acceptable limitation on number of streams, segments, or even on stream length.

### **Resilient to Failures**
Pravega self-detects failures and self-recovers from these cases, ensuring continuous flow of stream required by business continuity.


### **Global**
Pravega replicates streams globally, enabling allowing producers and consumers access streams across the globe and fail over among sites for high availability in the event of site wide disaster.
