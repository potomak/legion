# legion

- [Purpose](#purpose)
    - [Examples](#examples)
        - [Most of "Big Data":](#most-of-"big-data")
        - [Messaging](#messaging)
        - [General Scalability](#general-scalability)
- [Development Status](#development-status)


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
working on the rebalancing functionality, and I haven't started on
the data replication part yet. The plan for data replication is encode
the partition state into a graph-based CRDT, with nodes representing
individual requests. The signature for the user-implemented requests is
pure, i.e. it is not in the IO monad, so unless the user does something
unsafe, then request application should be fully deterministic, allowing
the graph CRDT to work.


