module App exposing (main)

import Html.App exposing (program)

import State
import View


main : Program Never
main =
  program
    { init = State.init
    , view = View.root
    , update = State.update
    , subscriptions = State.subscriptions
    }
