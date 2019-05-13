-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DA.Service.Logger.Impl.IO
    ( newStdoutLogger
    , newStderrLogger
    , newIOLogger
    ) where

import DA.Service.Logger

import           Control.Concurrent           (ThreadId, myThreadId)
import           Control.Concurrent.STM       (atomically)
import           Control.Concurrent.MVar      (MVar, newMVar, withMVar)
import           Control.Concurrent.STM.TVar  (TVar, newTVarIO, readTVarIO, modifyTVar')
import           Control.Exception            (bracket_)

import Control.Monad
import Data.Maybe

import qualified Data.Aeson                   as Aeson
import           Data.Aeson.Encode.Pretty     (encodePretty)
import qualified Data.ByteString.Lazy.Char8   as BSL8
import           Data.Time                    ()
import           Data.Time.Clock              (getCurrentTime)
import qualified Data.HashMap.Strict          as HMS
import qualified Data.Text.Extended           as T
import qualified Data.Text.Encoding           as TE
import           GHC.Stack

import qualified System.IO

------------------------------------------------------------------------------
-- IO Logger implementation
------------------------------------------------------------------------------

-- | Create a simple logger that outputs messages to 'stdout'.
newStdoutLogger :: T.Text -> IO (Handle IO)
newStdoutLogger = newIOLogger System.IO.stdout Nothing Debug

-- | Create a simple logger that outputs messages to 'stderr'.
newStderrLogger :: Priority -> T.Text -> IO (Handle IO)
newStderrLogger = newIOLogger System.IO.stderr Nothing

-- | Create a simple logger that outputs messages to given File handle.
newIOLogger
  :: System.IO.Handle
  -> Maybe Int
  -> Priority
  -> T.Text
  -> IO (Handle IO)
newIOLogger ioHandle mbMaxMessageLength priorityThreshold handleLabel
  = do
    tagsTVar     <- newTVarIO HMS.empty
    lockMVar     <- newMVar ()
    return $ makeHandle
           $ IHandle [handleLabel] tagsTVar ioHandle lockMVar mbMaxMessageLength
  where
    makeHandle ih =
      Handle
        {
          logJson = ioLogJson ih priorityThreshold

        , tagAction = \label action -> do
            tid <- myThreadId
            bracket_
              (addTag ih tid label)
              (dropTag ih tid)
              action

        , tagHandle = \label ->
            makeHandle $ ih { ihContext = label : ihContext ih }
        }

-- | Internal handle for IO based logger
data IHandle = IHandle
    { ihContext      :: ![T.Text]
      -- ^ The handle context (tagHandle)
    , ihTags         :: !(TVar (HMS.HashMap ThreadId [T.Text]))
      -- ^ Per thread tag stack
    , ihOutputH      :: !System.IO.Handle
      -- ^ Output file handle
    , ihOutputLock   :: !(MVar ())
      -- ^ Output lock to serialize messages to avoid interleaving
    , ihMaxMessageLength :: !(Maybe Int)
      -- ^ Maximum length of log message body
    }

addTag :: IHandle -> ThreadId -> T.Text -> IO ()
addTag ih tid tag =
    atomically . modifyTVar' (ihTags ih) $
      HMS.insertWith (++) tid [tag]

dropTag :: IHandle -> ThreadId -> IO ()
dropTag ih tid =
    atomically . modifyTVar' (ihTags ih) $
      HMS.update
        (\case
           [_tag]      -> Nothing
           (_tag:tags) -> Just tags
           _other      -> error "IMPOSSIBLE"
        )
        tid

ioLogJson
  :: (HasCallStack, Aeson.ToJSON a)
  => IHandle
  -> Priority
  -> Priority
  -> a
  -> IO ()
ioLogJson ih threshold prio msg =
    when (prio >= threshold) $
    withMVar (ihOutputLock ih) $
    \_ -> do
        tid <- myThreadId
        tags <- fromMaybe [] . HMS.lookup tid <$> readTVarIO (ihTags ih)
        now <- getCurrentTime
        let outH = ihOutputH ih
        System.IO.hPutStrLn outH
           $ "\n"
          <> take 22 (show now)
          <> prioToString prio
          <> showTags (ihContext ih) <> showTags tags
        BSL8.hPutStrLn outH $ truncateBSL8 $ case Aeson.toJSON msg of
          -- Print strings without quoting
          Aeson.String txt -> BSL8.fromStrict $ TE.encodeUtf8 txt
          _otherwise       -> encodePretty msg

        -- flush the out handle to ensure the message can be seen
        System.IO.hFlush outH
  where
    truncateBSL8 str = case ihMaxMessageLength ih of
        Just maxLength ->
            case BSL8.splitAt (fromIntegral $ maxLength - 3) str of
              (h, t)
                  | BSL8.null t -> h
                  | otherwise   -> h <> "..."
        Nothing ->
            str

showTags :: [T.Text] -> String
showTags tags = " [" <> unwords (reverse $ map T.unpack tags) <> "] "

prioToString :: Priority -> String
prioToString = \case
      Error   -> " [ERROR] "
      Info    -> " [INFO]  "
      Debug   -> " [DEBUG] "
      Warning -> " [WARN]  "
