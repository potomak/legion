{- |
  This module contains the BSockAddr data type.
-}
module Network.Legion.BSockAddr (
  BSockAddr(..)
) where

import Data.Binary (Binary(put, get))
import Data.Word (Word8)
import Network.Socket (SockAddr(SockAddrInet, SockAddrInet6, SockAddrUnix,
  SockAddrCan))


{- |
  A type useful only for creating a `Binary` instance of `SockAddr`.
-}
newtype BSockAddr = BSockAddr {getAddr :: SockAddr} deriving (Show, Eq)

instance Binary BSockAddr where
  put (BSockAddr addr) =
    case addr of
      SockAddrInet p h -> do
        put (0 :: Word8)
        put (fromEnum p, h)
      SockAddrInet6 p f h s -> do
        put (1 :: Word8)
        put (fromEnum p, f, h, s)
      SockAddrUnix s -> do
        put (2 :: Word8)
        put s
      SockAddrCan a -> do
        put (3 :: Word8)
        put a

  get = BSockAddr <$> do
    c <- get
    case (c :: Word8) of
      0 -> do
        (p, h) <- get
        return (SockAddrInet (toEnum p) h)
      1 -> do
        (p, f, h, s) <- get
        return (SockAddrInet6 (toEnum p) f h s)
      2 -> SockAddrUnix <$> get
      3 -> SockAddrCan <$> get
      _ ->
        fail
          $ "Can't decode BSockAddr because the constructor tag "
          ++ "was not understood. Probably this data is representing "
          ++ "something else."



