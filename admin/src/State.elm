module State exposing (init, update, subscriptions)

import Debug
import Http
import Task

import Types exposing (Model, ClusterState, Msg(LoadClusterState, FetchSucceed,
  FetchFail), decodeClusterState)


init : (Model, Cmd Msg)
init = (Model Nothing, fetchClusterState)


update : Msg -> Model -> (Model, Cmd Msg)
update msg model =
  case msg of
    LoadClusterState ->
      (model, fetchClusterState)
    FetchSucceed clusterState ->
      (Model (Just clusterState), Cmd.none)
    FetchFail e ->
      Debug.log (toString e)
      (model, Cmd.none)


subscriptions : Model -> Sub Msg
subscriptions model =
  Sub.none


-- Private functions


fetchClusterState : Cmd Msg
fetchClusterState =
  Task.perform FetchFail FetchSucceed getClusterState


getClusterState : Platform.Task Http.Error ClusterState
getClusterState =
  Http.get decodeClusterState "http://localhost:8000/clusterstate_example.js"
