# legion

- [Purpose](#purpose)
    - [Examples](#examples)
        - [Most of "Big Data":](#most-of-big-data)
        - [Messaging](#messaging)
        - [General Scalability](#general-scalability)
- [Development Status](#development-status)
- [FAQ](#faq)
    - [How do a "partition" in my Legion application and a "partition" as a subset of records in a distributed database relate to one another?](#how-do-a-partition-in-my-legion-application-and-a-partition-as-a-subset-of-records-in-a-distributed-database-relate-to-one-another)


Legion is a mathematically sound framework for writing horizontally
scalable user applications. Historically, horizontal scalability has
been achieved via the property of statelessness. Programmers would
design their applications to be free of any kind of persistent state,
avoiding the problem of distributed state management. This almost never
turns out to really be possible, so programmers achieve "statelessness"
by delegating application state management to some kind of external,
shared database -- which ends up having its own scalability problems.

In addition to scalability problems, which modern databases (especially NoSQL
databases) have done a good job of solving, there is another, more fundamental
problem facing these architectures: The application is not really stateless.

Legion is a Haskell framework that abstracts state partitioning, data
replication, request routing, and cluster rebalancing, making it easy
to implement large and robust distributed data applications.

Examples of services that rely on partitioning include ElasticSearch,
Riak, DynamoDB, and others. In other words, almost all scalable databases.

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

The Legion framework is still experimental.

## FAQ

### How do a "partition" in my Legion application and a "partition" as a subset of records in a distributed database relate to one another?

Some people find the term "partition" confusing because of the way
it is typically used to describe subsets of a table in distributed
relational databases. That's ok. The term "partition" as used here
has a more general meaning, primarily because of the more generalized
nature of Legion as compared to a distributed database.

In Legion, a partition is an abstract unit of state upon which user
requests operate. It is called a "partition" because it "is separate
from every other partition", meaning that an individual request can only
operate upon a single partition, and can never span multiple partitions.
Furthermore, Legion can only guarantee consistency within the partition
boundaries.

Another characteristic of a partition is that Legion cannot subdivide
it.  All of the data on one partition is guaranteed to be located on the
same physical node. Legion treats partitions as the smallest unit of
data that can be rebalanced across the cluster.

In a relational database partition, it is sometimes the case that the
table can be "repartitioned", where rows from one partition move to
the other. This has no analog in Legion. In Legion, a partition is an
atomic unit of data which cannot be subdivided.


