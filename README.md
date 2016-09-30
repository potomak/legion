# legion

- [Motivation](#motivation)
    - [Disadvantages of Offloading State to the DB](#disadvantages-of-offloading-state-to-the-db)
    - [Solutions](#solutions)
- [Development Status](#development-status)
    - [Examples](#examples)
- [FAQ](#faq)
    - [How do a "partition" in my Legion application and a "partition" as a subset of records in a distributed database relate to one another?](#how-do-a-partition-in-my-legion-application-and-a-partition-as-a-subset-of-records-in-a-distributed-database-relate-to-one-another)
    - [Why Haskell?](#why-haskell)


Legion is a framework for writing horizontally scalable stateful
applications, particularly microservices.

## Motivation

Writing stateful microservices is hard. Typically, the way stateful
services are written to make them easy is they are written as stateless
services that offload state to a database, making the database the
stateful service. This approach has several disadvantages, the most
important of which is that it is not always possible in principal to
accomplish what you need.

Why is it hard to write stateful microservices *without* resorting to
the DB?  Well, for the same reason it is hard to write a distributed
database in the first place. If you are storing state, you have to
worry about scaling that state by distributing it across a cluster,
ensuring the durability replicating the state, and routing requests
to the location where the state is stored.  You have to worry about
nodes entering an exiting the cluster, and how the state is repaired
and rebalanced when the cluster topology changes.

Wouldn't it be nice if you could get all that for free and just focus
on logic of your microservice application?

### Disadvantages of Offloading State to the DB

- Data Transfer.

  Transfer costs are only trivial if the size of your state is trivial,
  and probably not even then if you are dealing with frequently accessed
  objects, or hot spots. It is difficult to offload state to the DB in
  this way if the size of your state objects is large.

- Consistency Is Still a Problem.

  Distributed databases have gotten good at providing eventual consistency
  for the semantics of database operations, but not for the semantics of
  your application. Counters are a common example of this. Say a field
  in your DB object represents some kind of counter keeping track of the
  instances of some event or other. Two instances of the event happen
  simultaneously on two different nodes. Node A reads the current value,
  which is 10. Node B reads the current value, which is 10. Node A adds
  1, and stores the new value as 11. Node B adds 1, and stores the new
  value as 11. Two events happened, but the countered only moved from 10
  to 11. Your data is now inconsistent in relation to your application
  semantics.

  It is true that some databases are starting to provide tools to handle
  this specific case, and others which are similar, but those tools are
  not typically generalizable, or else require locking which may lead
  to substandard performance, or break A or P in the CAP Theorem.

  Another approach some people take to solve this problem is to store
  CRDTs in the database layer (in fact, Legion relies heavily on
  CRDTs internally). This approach is limited by the support of your
  database, and in any case using CRDTs this way is problematic because
  the growth of most CRDTs is unbounded over time, causing the size of
  the CRDT to become prohibitively large. It is very difficult to do
  garbage collection on such CRDTs in a hybrid system.  One of the most
  important things Legion does internally is implement asynchronous CRDT
  garbage collection.

### Solutions

The general philosophy that Legion takes to solving the problems of the
application/DB hybrid approach is not new. Instead of moving data to
where a request is being handled, we move the request handling to where
the data lives.  What is interesting is the implementation, which has the
following characteristics:

- Request Routing.

  User's of the Legion framework supply a request handler which is used
  to service application requests. Requests are routed by the Legion
  runtime to a node in cluster where the data actually resides and the
  request is executed by the user-provided request handler.

- CAP Theorem.

  Legion chooses A and P. In other words, Legion focuses on eventual
  consistency while maintaining availability and fault tolerance.

  This is a little bit trickier than it seems at first glance. You are
  probably used to this option being chosen by distributed databases;
  in fact choosing A and P is basically the whole point of why many
  distributed databases exist in the first place. However, distributed
  DBs don't offer eventual consistency over **arbitrary user-defined
  semantics**. See "Consistency Is Still a Problem" above. Being
  eventually consistent with arbitrary semantics is a lot harder than with
  "last write wins".

- Meet Semilattices.

  TODO: fill out this section.

- Pure Haskell Interface.

  TODO: fill out this section.

- Automatic Rebalancing.

  TODO: fill out this section.

- Replication.

  TODO: fill out this section.

## Development Status

The Legion framework is still experimental.

### Examples

Check out the
[legion-discovery](https://github.com/owensmurray/legion-discovery)
project for an example of a stateful web services that advantage of
Legion's ability to define your own operations on your data. Take a look at
[`Network.Legion.Discovery.App`](https://github.com/owensmurray/legion-discovery/blob/master/src/Network/Legion/Discovery/App.hs)
to see where the magic of defining a Legion application happens. The rest
of the code is mostly just standard HTTP-interface-written-in-Haskell,
and requests sent to the Legion runtime.

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


### Why Haskell?

Developing correct distributed systems is hard. One reason it is hard is
because it comes with a large number of very subtle rules and constraints
that are not part of the average development process and require highly
specialized knowledge. Typically this knowledge is entirely unrelated
to the business problem you are trying to solve. Violating any of those
constraints can lead to a nightmare of data corruption, scalability,
or availability problems.

Most languages are unable to enforce distributed constraints in the type
system, forcing the developer to very carefully tread through a proverbial
mine field. Making an error in even one step can have an associated cost
that is wildly disproportionate to the subtlety of the error.

Haskell on the other hand, has a type system that can be used to express
these constraints. In addition to implementing the distributed runtime,
providing a distribution-safe API is a major part of what makes Legion
awesome. It fences off the mines so you can run through the mine field
full tilt. If you hit one, the cost to your organization is a compile
time error, instead of a fundamentally broken and failing project.


