{- |
  Legion is a mathematically sound framework for writing horizontally
  scalable user applications. Historically, horizontal scalability has
  been achieved via the property of statelessness. Programmers would
  design their applications to be free of any kind of persistent state,
  avoiding the problem of distributed state management. This almost never
  turns out to really be possible, so programmers achieve "statelessness"
  by delegating application state management to some kind of external,
  shared database -- which ends up having its own scalability problems.

  In addition to scalability problems, which modern databases (especially
  NoSQL databases) have done a good job of solving, there is another,
  more fundamental problem facing these architectures: The application
  is not really stateless.

  Legion is a Haskell framework that abstracts state partitioning, data
  replication, request routing, and cluster rebalancing, making it easy
  to implement large and robust distributed data applications.

  Examples of services that rely on partitioning include ElasticSearch,
  Riak, DynamoDB, and others. In other words, almost all scalable
  databases.
-}

module Network.Legion (
  -- * Using Legion

  -- ** Starting the Legion Runtime
  -- $startup
  forkLegionary,
  StartupMode(..),
  Runtime,

  -- ** Runtime Configuration
  -- $framework-config
  LegionarySettings(..),

  -- ** Making Runtime Requests
  makeRequest,
  search,

  -- * Implementing a Legion Application
  -- $service-implementaiton

  -- ** Indexing
  -- $indexing
  Legionary(..),
  LegionConstraints,
  Persistence(..),
  ApplyDelta(..),
  Tag(..),

  -- * Other Types
  SearchTag(..),
  IndexRecord(..),
  PartitionKey(..),
  PartitionPowerState,

  -- * Utils
  newMemoryPersistence,
  diskPersistence,
) where

import Prelude hiding (lookup, readFile, writeFile, null)

import Network.Legion.Application (LegionConstraints,
  Persistence(Persistence, getState, saveState, list),
  Legionary(Legionary, persistence, handleRequest, index))
import Network.Legion.Basics (newMemoryPersistence, diskPersistence)
import Network.Legion.Index (Tag(Tag, unTag), IndexRecord(IndexRecord,
  irTag, irKey), SearchTag(SearchTag, stTag, stKey))
import Network.Legion.PartitionKey (PartitionKey(K, unKey))
import Network.Legion.PartitionState (PartitionPowerState)
import Network.Legion.PowerState (ApplyDelta(apply))
import Network.Legion.Runtime (StartupMode(NewCluster, JoinCluster),
  forkLegionary, Runtime, makeRequest, search)
import Network.Legion.Settings (LegionarySettings(LegionarySettings,
  adminHost, adminPort, peerBindAddr, joinBindAddr))

--------------------------------------------------------------------------------

-- $service-implementaiton
-- Whenever you use Legion to develop a distributed application, your
-- application is going to be divided into two major parts, the state/less/
-- part, and the state/ful/ part. The stateless part is going to be the
-- context in which a legion node is running -- probably a web server if you
-- are exposing your application as a web service. Legion itself is focused
-- mainly on the stateful part, and it will do all the heavy lifting on
-- that side of things. However, it is worth mentioning a few things about
-- the stateless part before we move on.
--
-- The unit of state that Legion knows about is called a \"partition\". Each
-- partition is identified by a 'PartitionKey', and it is replicated across
-- the cluster. Each partition acts as the unit of state for handling
-- stateful user requests which are routed to it based on the `PartitionKey`
-- associated with the request. What the stateful part of Legion is
-- /not/ able to do is figure out what partition key is associated with
-- the request in the first place. This is a function of the stateless
-- part of the application. Generally speaking, the stateless part of
-- your application is going to be responsible for
--
--   * Starting up the Legion runtime using 'forkLegionary'.
--   * Identifying the partition key to which a request should be applied
--     (e.g.  maybe this is some component of a URL, or else an identifier
--     stashed in a browser cookie).
--   * Marshalling application requests into requests to the Legion runtime.
--   * Marshalling the Legion runtime response into an application response.
--
-- Legion doesn't really address any of these things, mainly because there
-- are already plenty of great ways to write stateless services. What
-- Legion does provide is a runtime that can be embedded in the stateless
-- part of your application, that transparently handles all of the hard
-- stateful stuff, like replication, rebalancing, request routing, etc.
--
-- The only thing required to implement a legion service is to
-- provide a request handler and a persistence layer by constructing a
-- 'Legionary' value and passing it to 'forkLegionary'. The stateful
-- part of your application will live mostly within the request handler
-- 'handleRequest'. If you look at 'handleRequest', you will see that
-- it is abstract over the type variables @i@, @o@, and @s@.
--
-- > handleRequest :: PartitionKey -> i -> s -> o
--
-- These are the types your application has to fill in. @i@ stands for
-- "input", which is the type of requests your application accepts; @o@
-- stands for "output", which is the type of responses your application
-- will generate in response to those requests, and @s@ stands for "state",
-- which is the application state that each partition can assume.
--
-- Implementing a request handler is pretty straight forward, but
-- there is a little bit more to it than meets the eye. If you look at
-- 'forkLegionary', you will see a constraint named @'LegionConstraints'
-- i o s@, which is short-hand for a long list of typeclasses that
-- your @i@, @o@, and @s@ types are going to have to implement. Of
-- particular interest is the 'ApplyDelta' typeclass. If you look at
-- 'handleRequest', you will see that it is defined in terms of an input,
-- an existing state, and an output, but there is no mention of any /new/
-- state that is generated as a result of handling the request.
--
-- This is where the 'ApplyDelta' typeclass comes in. Where 'handleRequest'
-- takes an input and a state and produces an output, the 'apply' function
-- of the 'ApplyDelta' typeclass takes an input and a state and produces
-- a new state.
--
-- > apply :: i -> s -> s
--
-- The reason that Legion splits the definition of what it means to
-- fully handle an input into two functions like this is because of the
-- approach it takes to solving distributed systems problems. Describing
-- this entirely is beyond the scope of this section of documentation
-- (TODO link to more info) but the TL;DR is that 'handleRequest' will
-- only get called once for each input, but 'apply' has a very good
-- chance of being called more than once for various reasons including
-- re-playing the application of requests to resolve non-determinism.
--
-- Taking yet another look at 'handleRequest', you will see that it
-- makes no provision for a non-existent partition state (i.e., it is
-- written in terms of @s@, not @Maybe s@. Same goes for 'ApplyDelta').
-- This framework takes the somewhat platonic philosophical view that all
-- mathematical values exist somewhere and that there is no such thing as
-- non-existent partition. When you first spin up a Legion application,
-- all of those partitions are going to have a default value, which is
-- 'Data.Default.Class.def' (Because your partition state must be an
-- instance of the 'Data.Default.Class.Default' typeclass). This doesn't
-- take up infinite disk space because 'Data.Default.Class.def' values
-- are cleverly encoded as a zero-length string of bytes. ;-)
--
-- The persistence layer provides the framework with a way to store the
-- various partition states. This allows you to choose any number of
-- persistence strategies, including only in memory, on disk, or in some
-- external database.
--
-- See 'newMemoryPersistence' and 'diskPersistence' if you need to get
-- started quickly with an in-memory persistence layer.

--------------------------------------------------------------------------------

-- $indexing
-- Legion gives you a way to index your partitions so that you can find
-- partitions that have certain characteristics without having to know
-- the partition key a priori. Conceptually, the "index" is a single,
-- global, ordered list of 'IndexRecord's. The 'search' function allows
-- you to scroll forward through this list at will.
-- 
-- Each partition may generate zero or more 'IndexRecord's. This
-- is determined by the 'index' function, which is defined by your
-- specific Legion application. For each 'Tag' returned by 'index', an
-- 'IndexRecord' is generated such that:
-- 
-- > @IndexRecord {irTag = <your tag>, irKey = <partition key>}@
-- 

--------------------------------------------------------------------------------

-- $startup
-- While this section is being worked on, you can check out the
-- [legion-cache](https://github.com/taphu/legion-cache) project for a
-- working example of how to build a basic distributed key-value store
-- using Legion.

--------------------------------------------------------------------------------

-- $framework-config
-- The legion framework has several operational parameters which can
-- be controlled using configuration. These include the address binding
-- used to expose the cluster management service endpoint and what file
-- to use for cluster state journaling.

