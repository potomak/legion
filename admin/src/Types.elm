module Types exposing
  ( Model
  , ClusterState
  , Msg(LoadClusterState, FetchSucceed, FetchFail)
  , decodeClusterState
  )

import Html exposing (Html, h1, div, text)
import Json.Decode exposing (Decoder, succeed, string, (:=))
import Json.Decode.Extra exposing ((|:))
import Http


type alias Model =
  { clusterState : Maybe ClusterState
  }

type alias ClusterState =
  { migration : String
  --, cluster : Cluster
  --, partitions : Partitions
  , self : String
  }

type Msg
  = LoadClusterState
  | FetchSucceed ClusterState
  | FetchFail Http.Error

--type alias Cluster =
--  { powerState : PowerState
--  , now : String
--  , self : String
--  , peerStates : PeerStates
--  }


decodeClusterState : Decoder ClusterState
decodeClusterState =
  succeed ClusterState
    |: ("migration" := string)
    |: ("self" := string)
