-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Convert between HL Ledger.Types and the LL types generated from .proto files
module DA.Ledger.Convert (
    lowerTimestamp,
    Perhaps, perhaps,
    runRaise,
    raiseList,
    raiseParty,
    raiseTimestamp,
    raisePackageId,
    RaiseFailureReason(..),
    ) where

import Control.Exception (evaluate,try,SomeException)
import Data.Text.Lazy (Text)
import Data.Vector as Vector (Vector,toList)

import qualified Google.Protobuf.Timestamp as LL

import DA.Ledger.Types

-- lower

lowerTimestamp :: Timestamp -> LL.Timestamp
lowerTimestamp = \case
    Timestamp{..} ->
        LL.Timestamp {
        timestampSeconds = fromIntegral seconds,
        timestampNanos = fromIntegral nanos
        }

-- raise

runRaise :: (a -> Perhaps b) -> a -> IO (Perhaps b)
runRaise raise a = fmap collapseErrors $ try $ evaluate $ raise a
    where
        collapseErrors :: Either SomeException (Perhaps a) -> Perhaps a
        collapseErrors = either (Left . ThrewException) id


data RaiseFailureReason = Missing String | Unexpected String | ThrewException SomeException deriving Show

type Perhaps a = Either RaiseFailureReason a

missing :: String -> Perhaps a
missing = Left . Missing

perhaps :: String -> Maybe a -> Perhaps a
perhaps tag = \case
    Nothing -> missing tag
    Just a -> Right a

raiseTimestamp :: LL.Timestamp -> Perhaps Timestamp
raiseTimestamp = \case
    LL.Timestamp{timestampSeconds,timestampNanos} ->
        return $ Timestamp {seconds = fromIntegral timestampSeconds,
                            nanos = fromIntegral timestampNanos}

raiseList :: (a -> Perhaps b) -> Vector a -> Perhaps [b]
raiseList f v = loop (Vector.toList v)
    where loop = \case
              [] -> return []
              x:xs -> do y <- f x; ys <- loop xs; return $ y:ys

raiseParty :: Text -> Perhaps Party
raiseParty = fmap Party . raiseText "Party"

raisePackageId :: Text -> Perhaps PackageId
raisePackageId = fmap PackageId . raiseText "PackageId"

raiseText :: String -> Text -> Perhaps Text
raiseText tag = perhaps tag . \case "" -> Nothing; x -> Just x
