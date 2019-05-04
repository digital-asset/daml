-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE OverloadedStrings #-}

module DA.Service.Logger.Impl.Pure
    ( makeHandle
    , makeNopHandle
    , LogEntry(..)
    , LogState(..)
    , emptyState
    , showState
    ) where

import           Control.Lens                 (Lens', use, (%=))
import           Control.Monad.State.Class    (MonadState)

import qualified Data.Aeson                   as Aeson
import           Data.Aeson.Encode.Pretty     (encodePretty)
import qualified Data.ByteString.Lazy         as BSL
import qualified Data.Map.Strict              as MS
import qualified Data.Text.Extended           as T
import qualified Data.Text.Encoding           as E

import qualified DA.Service.Logger            as Logger

-- | Source locations, we can't use Parsec's built-in source location,
-- because they implement a silly (non pretty-print compatible) Show
-- instance.
data Loc
    = Loc FilePath Int Int
    | NoLoc String -- reason for no location information
    deriving (Show)


callStackToLoc :: Loc
callStackToLoc = NoLoc "N/A"


------------------------------------------------------------------------------
-- Types
------------------------------------------------------------------------------

data LogEntry = LogEntry
    { _leMessage   :: !Aeson.Value
    , _lePriority  :: !Logger.Priority
    , _leLoc       :: !Loc
    , _leContext   :: ![T.Text]
    , _leTags      :: ![T.Text]
    }

data LogState = LogState
    { _lsMessages      :: ![LogEntry]
    , _lsCounters      :: !(MS.Map T.Text Int)
    , _lsGauges        :: !(MS.Map T.Text Int)
    , _lsTags          :: ![T.Text]
    }

lsMessages :: Lens' LogState [LogEntry]
lsMessages f logState = fmap (\msgs -> logState { _lsMessages = msgs }) (f (_lsMessages logState))

lsTags :: Lens' LogState [T.Text]
lsTags f logState = fmap (\tags -> logState { _lsTags = tags }) (f (_lsTags logState))

emptyState :: LogState
emptyState = LogState
    { _lsMessages      = []
    , _lsCounters      = MS.empty
    , _lsGauges        = MS.empty
    , _lsTags          = []
    }

--------------------------------------------------------------------------------
-- Functions
--------------------------------------------------------------------------------

-- | Create a pure no-op logger
makeNopHandle :: Monad m => Logger.Handle m
makeNopHandle = Logger.Handle
    { Logger.logJson = \_prio _msg -> return ()
    , Logger.tagAction = \_tag action -> action
    , Logger.tagHandle = const makeNopHandle
    }

-- | Create a pure Logger handle within an arbitrary state monad.
makeHandle
    :: forall s m. MonadState s m
    => Lens' s LogState
    -- ^ Lens to LogState from 's'.
    -> T.Text
    -- ^ The handle tag
    -> Logger.Handle m
makeHandle logState handleTag = makeHandle' [handleTag]
  where
    makeHandle' :: [T.Text] -> Logger.Handle m
    makeHandle' context =
      Logger.Handle
        { Logger.logJson = \prio msg -> do
            tags <- use (logState . lsTags)
            let !loc = callStackToLoc
            let entry = LogEntry {
                _leMessage  = Aeson.toJSON msg
              , _lePriority = prio
              , _leLoc      = loc
              , _leContext  = context
              , _leTags     = tags
              }
            logState . lsMessages %= (:) entry

        , Logger.tagAction = \tag action -> do
            logState . lsTags %= (:) tag
            result <- action
            logState . lsTags %= tail
            return result

        , Logger.tagHandle = \tag -> makeHandle' (tag : context)
        }

-- | Show the log state in printable format
showState :: LogState -> T.Text
showState state =
    "Messages:\n\n" <> tmap prettyMessage (reverse $ _lsMessages state) <>
    "Counters:\n\n" <> tmap prettyMetric (MS.toList (_lsCounters state)) <>
    "\nGauges:\n\n" <> tmap prettyMetric (MS.toList (_lsGauges state))

  where
    tmap f = T.concat . map f

    prettyMessage :: LogEntry -> T.Text
    prettyMessage entry =
        (T.show $ _lePriority entry) <>
        " [" <> (T.unwords $ reverse $ _leContext entry) <> "]" <>
        " [" <> (T.unwords $ reverse $ _leTags entry) <> "]" <>
        " [" <> (T.show $ _leLoc entry) <> "]\n" <>
        (E.decodeUtf8 $ BSL.toStrict $ encodePretty $ _leMessage entry) <>
        "\n\n"

    prettyMetric (metric, value) =
       "   " <> metric <> " = " <> T.show value <> "\n"
