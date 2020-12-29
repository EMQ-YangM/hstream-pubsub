{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Main where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Monad
import Data.IORef
import Data.IntMap (IntMap)
import qualified Data.IntMap as IM
import Data.IntSet (IntSet)
import qualified Data.IntSet as IS
import Data.Maybe
import Data.String
import Data.Word (Word8)
import Network.MQTT.Topic
import Network.MQTT.Trie
import Z.Data.Builder (Builder, build)
import qualified Z.Data.Builder as B
import Z.Data.Parser as P
import Z.Data.Text (Text)
import qualified Z.Data.Text as T
import Z.Data.Vector (w2c)
import qualified Z.Data.Vector as V
import qualified Z.Data.Vector.FlatIntMap as M
import Z.IO
import Z.IO.Network.TCP
import Prelude hiding (lookup)

main :: IO ()
main = client

type Message = V.Bytes

data Command
  = Sub Filter
  | Pub Topic Message
  deriving (Show, Eq)

encode :: Command -> Builder ()
encode (Sub f) = do
  B.char7 's'
  let len = filterLength f
  B.int len
  encodeFilter f
encode (Pub t m) = do
  B.char7 'p'
  let tlen = topicLength t
      mlen = V.length m
  B.int tlen
  encodeTopic t
  B.int mlen
  B.bytes m

cParser :: Parser Command
cParser = do
  (decodePrim @Word8)
    >>= ( \case
            's' -> do
              i <- int @Int
              v <- P.take i
              case parse filterParser v of
                (_, Left e) -> fail (show e)
                (_, Right a) -> return $ Sub a
            'p' -> do
              i <- int @Int
              v <- P.take i
              case parse topicParser v of
                (_, Left e) -> error (show e)
                (_, Right a) -> do
                  j <- int @Int
                  v1 <- P.take j
                  return $ Pub a v1
            e -> fail $ show e
        )
      . w2c

t1 = Pub "a/a/c/b" "adjfoasd"

-- >>> show 1
t2 = encode t1

-- >>> t3
t3 = parse cParser (B.build $ t2 <> t2)

data State = State
  { chanMap :: IntMap (TQueue Message),
    trie :: Trie IntSet,
    index :: Int
  }

initState = State IM.empty empty 0

start = do
  i <- newIORef initState
  server i

server :: IORef State -> IO ()
server stref = startTCPServer defaultTCPServerConfig $ \tcp -> do
  i <- newBufferedInput tcp
  o <- newBufferedOutput tcp
  print "tcp connect"
  modifyIORef stref $ \s -> s {index = index s + 1}
  tq <- newTQueueIO
  print "fork sendMessage thread"
  forkIO (sendMessage o tq)
  print "start receive message"
  index <- index <$> readIORef stref
  receiveMessage index i tq
  where
    loop i o = do
      readParser cParser i >>= print
      loop i o
    receiveMessage index i tq = do
      readParser cParser i >>= \case
        Left e -> print "finish"
        Right c -> do
          print c
          state <- readIORef stref
          case c of
            Sub f -> do
              let ncm = IM.insert index tq (chanMap state) -- new chanMap
                  nt = insertWith IS.union f (IS.singleton index) (trie state) -- new trie
              writeIORef stref (state {chanMap = ncm, trie = nt}) -- update state
              receiveMessage index i tq -- loop
            Pub t m -> do
              let v = lookup t (trie state)
              unless (IS.null v) $ do
                let v' = Data.Maybe.mapMaybe (`IM.lookup` chanMap state) (IS.toList v)
                print $ show v
                forM_ v' $ \c -> atomically $ writeTQueue c m
              receiveMessage index i tq -- loop
              -- receiveMessage i tq -- loop
    sendMessage o tq = do
      m <- atomically $ readTQueue tq
      print m
      writeBuffer o m >> flushBuffer o
      print "send finish"
      sendMessage o tq

client :: IO ()
client = withResource (initTCPClient defaultTCPClientConfig) $ \tcp -> do
  i <- newBufferedInput tcp
  o <- newBufferedOutput tcp
  forkIO $
    forever $
      readBuffer i >>= print

  forever $ do
    print "input line"
    l <- getLine
    print "finish"
    case words l of
      "s" : x : _ -> writeBuffer o $ B.build $ encode $ Sub (fromString x)
      "p" : x : y : _ -> writeBuffer o $ B.build $ encode $ Pub (fromString x) (B.build $ B.string7 y)
      e -> print "invalid input"
    flushBuffer o
