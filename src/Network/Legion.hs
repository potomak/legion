{- |
  Legion is a framework designed to help people implement large-scale
  distributed stateful services that scale horizontally using the
  technique of data partitioning, sometimes known as "sharding". Examples
  of services that rely on partitioning include ElasticSearch, Riak,
  DynamoDB, and others.

  In other words, this framework is an abstraction over partitioning,
  replication, cluster-rebalancing, node discovery, and request routing;
  allowing the user to focus on request logic and storage strategies.
-}

module Network.Legion (
  -- * Service Implementation
  -- $service-implementaiton
  Legionary(..),
  LegionConstraints,
  Persistence(..),
  ApplyDelta(..),
  RequestMsg,
  -- * Invoking Legion
  -- $invocation
  forkLegionary,
  runLegionary,
  StartupMode(..),
  -- * Fundamental Types
  PartitionKey(..),
  PartitionPowerState,
  -- * Framework Configuration
  -- $framework-config
  LegionarySettings(..),
  -- * Utils
  newMemoryPersistence,
  diskPersistence,
) where

import Prelude hiding (lookup, readFile, writeFile, null)

import Network.Legion.Application (LegionConstraints,
  Persistence(Persistence, getState, saveState, list),
  Legionary(Legionary, persistence, handleRequest), RequestMsg)
import Network.Legion.Basics (newMemoryPersistence, diskPersistence)
import Network.Legion.PartitionKey (PartitionKey(K, unkey))
import Network.Legion.PartitionState (PartitionPowerState)
import Network.Legion.PowerState (ApplyDelta(apply))
import Network.Legion.Runtime (runLegionary, StartupMode(NewCluster,
  JoinCluster), forkLegionary)
import Network.Legion.Settings (LegionarySettings(LegionarySettings,
  adminHost, adminPort, peerBindAddr, joinBindAddr))

--------------------------------------------------------------------------------

-- $service-implementaiton
-- Whenever you use Legion to develop a distributed application, your
-- application is going to be divided into two major parts, the state/less/
-- part, and the state/ful/ part. The state/less/ part is going to be the
-- context in which a legion node is running -- probably a web server if you
-- are exposing your application as a web service. Legion itself is focused
-- mainly on the state/ful/ part, and it will do all the heavy lifting on
-- that side of things. However, it is worth mentioning a few things about
-- the state/less/ part before we move on.
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
--   * Marshalling user requests into their equivalent stateful application
--     request (e.g. gathering data from a web requests).
--   * Identifying the partition key to which a request should be applied
--     (e.g.  maybe this is some component of a URL, or else an identifier
--     stashed in a browser cookie).
--   * Submitting the stateful application request to the Legion runtime.
--   * Marshalling the Legion runtime response into something the user can
--     understand (e.g. constructing some kind of HTTP response).
-- 
-- Legion doesn't really address any of these things, mainly because there
-- are already plenty of great ways to write stateless services. What Legion
-- does provide is a runtime that can be embedded in the stateless part of
-- your application, that transparently handles all of the hard stateful
-- stuff, like replication, rebalancing, request routing, etc.
-- 
-- The only thing required to implement a legion service is to
-- provide a request handler and a persistence layer by constructing a
-- 'Legionary' value and passing it to 'forkLegionary'. The stateful
-- part of your application will live mostly within the request handler
-- 'handleRequest'. If you look at 'handleRequest', you will see that it
-- is abstract over the type variables @i@, @o@, and @s@.
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
-- there is a little bit more to it that meets the eye. If you look at
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
-- re-ordering the application of requests that were initially handled
-- "simultaneously" by two different nodes.
-- 
-- Taking yet another look at 'handleRequest', you will see that it
-- makes no provision for a non-existent partition state (i.e., it is
-- written in terms of @s@, not @Maybe s@. Same goes for 'ApplyDelta').
-- This framework takes the somewhat platonic philosophical view that all
-- mathematical values exist somewhere and that there is no such thing as
-- non-existence partition. When you first spin up a Legion application,
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

-- $invocation
-- Notes on invocation.

--------------------------------------------------------------------------------

-- $framework-config
-- The legion framework has several operational parameters which can
-- be controlled using configuration. These include the address binding
-- used to expose the cluster management service endpoint and what file
-- to use for cluster state journaling.

