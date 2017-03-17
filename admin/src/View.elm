module View exposing (root)

import Html exposing (Html, h1, div, text)

import Types exposing (ClusterState, Model, Msg)


root : Model -> Html Msg
root model =
  div []
    [
      h1 [] [text "Legion admin"],
      clusterState model.clusterState
    ]


-- Private functions


clusterState : Maybe ClusterState -> Html Msg
clusterState mClusterState =
  case mClusterState of
    Just _ -> text "THERE'S A CLUSTER STATE..."
    Nothing -> text "No cluster state"
