{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}

module Network.MQTT.Topic where

import Control.Applicative (Alternative (many, (<|>)))
import Data.List (intercalate)
import Data.List.NonEmpty (NonEmpty (..))
import Data.String (IsString (..))
import Data.Word (Word8)
import Z.Data.Builder
import qualified Z.Data.Parser as P
import qualified Z.Data.Text as T
import Z.Data.Vector (Bytes, indexMaybe, null, pack, packASCII)
import qualified Z.Data.Vector as V
import Prelude hiding (null)

-- | According to the MQTT specification a topic
--
--  * must not be empty
--  * must not contain @+@, @#@ or @\\NUL@ characters
newtype Topic = Topic (NonEmpty Level) deriving (Eq, Ord)

-- | According to the MQTT specification a filter
--
--  * must not be empty
--  * must not contain a @\\NUL@ character
newtype Filter = Filter (NonEmpty Level) deriving (Eq, Ord)

-- | A `Level` represents a single path element of a `Topic` or a `Filter`.
newtype Level = Level Bytes deriving (Eq, Ord)

filterLength :: Filter -> Int
filterLength (Filter (a :| as)) = levelLength a + sum (map ((+ 1) . levelLength) as)

topicLength :: Topic -> Int
topicLength (Topic (a :| as)) = levelLength a + sum (map ((+ 1) . levelLength) as)

levelLength :: Level -> Int
levelLength (Level bs) = V.length bs

encodeTopic :: Topic -> Builder ()
encodeTopic (Topic (a :| as)) =
  case as of
    [] -> encodeLevel a
    xs -> encodeLevel a <> loop xs
  where
    loop [] = return ()
    loop (x : xs) = do
      char7 '/'
      encodeLevel x
      loop xs

encodeFilter :: Filter -> Builder ()
encodeFilter (Filter f) = encodeTopic (Topic f)

encodeLevel :: Level -> Builder ()
encodeLevel (Level bs) = bytes bs

instance Show Topic where
  show (Topic xs) = show (Filter xs)

instance Show Filter where
  show (Filter (x :| xs)) = intercalate "/" $ f x : map f xs
    where
      f (Level l) = show (T.validate l)

instance Show Level where
  show (Level l) = show (T.validate l)

instance IsString Topic where
  fromString s = case P.parse topicParser (packASCII s) of
    (_, Left e) -> error $ show e
    (_, Right v) -> v

instance IsString Filter where
  fromString s = case P.parse filterParser (packASCII s) of
    (_, Left e) -> error $ show e
    (_, Right v) -> v

instance IsString Level where
  fromString s = case P.parse levelParser (packASCII s) of
    (_, Left e) -> error $ show e
    (_, Right v) -> v

topicLevels :: Topic -> NonEmpty Level
topicLevels (Topic x) = x
{-# INLINE topicLevels #-}

filterLevels :: Filter -> NonEmpty Level
filterLevels (Filter x) = x
{-# INLINE filterLevels #-}

zero, plus, hash, slash, dollar :: Word8
zero = 0x00
plus = 0x2b
hash = 0x23
slash = 0x2f
dollar = 0x24

topicParser :: P.Parser Topic
topicParser = (<|> fail "invalid topic") $ do
  _ <- P.peek
  level <- pLevel
  levels <- many (pSlash >> pLevel)
  P.endOfInput
  return (Topic $ level :| levels)
  where
    pSlash = P.word8 slash
    pLevel = Level <$> P.takeWhile (\w8 -> w8 /= slash && w8 /= zero && w8 /= hash && w8 /= plus)

filterParser :: P.Parser Filter
filterParser =
  (<|> fail "invalid filter") $
    Filter <$> do
      _ <- P.peek
      (x : xs) <- pLevels
      return (x :| xs)
  where
    pSlash = P.word8 slash

    pLevel = Level <$> P.takeWhile (\w8 -> w8 /= slash && w8 /= zero && w8 /= hash && w8 /= plus)

    pLevels = pl1 <|> pl2 <|> pl3

    pl1 :: P.Parser [Level]
    pl1 = do
      P.word8 hash
      P.endOfInput
      return [multiLevelWildcard]

    pl2 :: P.Parser [Level]
    pl2 = do
      P.word8 plus
      (P.endOfInput >> return [singleLevelWildcard])
        <|> (pSlash >> (:) singleLevelWildcard <$> pLevels)

    pl3 :: P.Parser [Level]
    pl3 = do
      x <- pLevel
      xs <- (P.endOfInput >> return []) <|> (pSlash >> pLevels)
      return $ x : xs

levelParser :: P.Parser Level
levelParser = do
  x <- P.takeWhile (\w8 -> w8 /= slash && w8 /= zero)
  P.endOfInput
  return (Level x)

-- | The @#@ path element. It must only appear at the end of a `Filter`.
multiLevelWildcard :: Level
multiLevelWildcard = Level $ pack [hash]

-- | The @+@ path element. It may appear anywhere within a `Filter`.
singleLevelWildcard :: Level
singleLevelWildcard = Level $ pack [plus]

-- | Returns `True` iff the `Level` starts with @$@.
startsWithDollar :: Level -> Bool
startsWithDollar (Level bs) =
  not (null bs) && indexMaybe bs 0 == Just dollar
