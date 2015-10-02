# legion

- [Purpose](#purpose)
    - [Examples](#examples)
        - [Most of "Big Data":](#most-of-"big-data")
        - [Messaging](#messaging)
        - [General Scalability](#general-scalability)
- [Development Status](#development-status)
    - [TODO](#todo)
        - [Connection Manager](#connection-manager)
        - [Balancing algorithm](#balancing-algorithm)
        - [Routing](#routing)
        - [Missing Functionality](#missing-functionality)


Legion is a haskell framework that abstracts state partitioning, data
replication, request routing, and cluster rebalancing, making it easy
to implement large and robust distributed data applications.


Check out https://github.com/taphu/legion-cache for a simple application
that makes use of the legion framework.


I also recently slapped together a lighting talk, with
[these slides](https://docs.google.com/presentation/d/1XWZp9aPfeIxfgBWoTVUkLOgO5rgS54xZo0F4FgLKu7g/edit?usp=sharing)

## Purpose

Legion's purpose is to make it easy to write statful applications which
are scalable homogeneously across and unbounded number of nodes.

### Examples

To illustrate the purpose of the Legion framework, it may helpful to give some
examples of existing software that could be solved by using Legion.

Examples include:

#### Most of "Big Data":
- Riak
- ElasitcSearch
- Hadoop
- DynamoDB
- Other distributed storage and map/reduce.

#### Messaging
- RabbitMQ
- Jabber
- Other large scale AMQP
- Other distributed queuing.

#### General Scalability

Any sort of software system that scales homogeneously across multiple machines
is going to run hard up against at least one of the general problems that
Legion is designed to solve.

Homogeneously scalable systems generally require:

- Distributed State, which means partitioning of the state data.
- Request routing, that sends the code to the data instead of bringing the data to the code.
- Flexible capacity, meaning that you can add nodes to the cluster, which means cluster rebalancing.
- Durability and Availability, which mean replicated state.


## Development Status

The Legion framework is still under heavy development. Right now I'm
working on the rebalancing functionality, and I haven't started on the
data replication part yet. The plan for data replication is to encode
the partition state into a graph-based CRDT, with nodes representing
individual requests. The signature for the user-implemented requests is
pure, i.e. it is not in the IO monad, so unless the user does something
unsafe, then request application should be fully deterministic, allowing
the graph CRDT to work.

### TODO

#### Connection Manager

- Figure out a way to reliably broadcast, possibly using a gossip protocol
  or something.

#### Balancing algorithm

- Don't treat the entire keyspace as equal. Balance the cluster according to
  partition "weight". The simplest implementation could be that a partition has
  a weight of 1 if it exists and a weight of 0 if it does not exists, but
  probably something based on the size of the raw partition data would be
  better. In the future we might also try to figure out a way to calculate a
  value for "weight" that means "projected CPU load", so we can balance evenly
  across CPU resources as well as space.

- Think about introducing the idea of node capacity, rather than assuming a
  strictly homogeneous cluster.

- Figure out how and when to eject and blacklist a node from the cluster, and
  how to reclaim (or rebuild, once data replication happens) the partitions
  handled by that node.


#### Routing

- Don't use the network stack to route message locally.

- Maybe don't even use the routing mechanism to handle user requests destined
  for the local peer.

#### Missing Functionality

- Recovery Startup Mode

- Data Replication

- Think about how we might allow the user-provided request handler to
  be written in other languages.
