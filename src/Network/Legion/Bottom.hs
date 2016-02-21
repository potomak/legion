{- |
  This module contains the class of types that have a bottom value. This
  is similar in spirit to `minBound` of the `Bounded` type class but
  does not require that the type also have an upper bound.
-}
module Network.Legion.Bottom (
  Bottom(..)
) where

{- |
  The class of types that have a bottom value. This is similar in spirit
  to `minBound` of the `Bounded` type class but does not require that
  the type also have an upper bound.
-}
class Bottom a where
  {- |
    The bottom value of the type @a@
  -}
  bottom :: a

instance Bottom (Maybe a) where
  bottom = Nothing

