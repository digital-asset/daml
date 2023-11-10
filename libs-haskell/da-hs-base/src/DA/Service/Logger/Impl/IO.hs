-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module DA.Service.Logger.Impl.IO
    ( newStderrLogger
    , newIOLogger
    ) where

import DA.Service.Logger

import           Control.Concurrent.MVar      (MVar, newMVar, withMVar)

import Control.Monad

import Data.Aeson qualified                   as Aeson
import           Data.Aeson.Encode.Pretty     (encodePretty)
import Data.ByteString.Lazy.Char8 qualified   as BSL8
import           Data.Char                    (toUpper)
import           Data.Time                    ()
import           Data.Time.Clock              (getCurrentTime)
import Data.Text.Extended qualified           as T
import Data.Text.Encoding qualified           as TE
import           GHC.Stack

import System.IO qualified

------------------------------------------------------------------------------
-- IO Logger implementation
------------------------------------------------------------------------------

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
    lockMVar     <- newMVar ()
    return $ makeHandle
           $ IHandle [handleLabel] ioHandle lockMVar mbMaxMessageLength
  where
    makeHandle ih =
      Handle
        {
          logJson = ioLogJson ih priorityThreshold

        , tagHandle = \label ->
            makeHandle $ ih { ihContext = label : ihContext ih }
        }

-- | Internal handle for IO based logger
data IHandle = IHandle
    { ihContext      :: ![T.Text]
      -- ^ The handle context (tagHandle)
    , ihOutputH      :: !System.IO.Handle
      -- ^ Output file handle
    , ihOutputLock   :: !(MVar ())
      -- ^ Output lock to serialize messages to avoid interleaving
    , ihMaxMessageLength :: !(Maybe Int)
      -- ^ Maximum length of log message body
    }

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
        now <- getCurrentTime
        let outH = ihOutputH ih
        System.IO.hPutStrLn outH
           $ "\n"
          <> take 22 (show now)
          <> prioToString prio
          <> showTags (ihContext ih)
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
prioToString prio = " [" ++ prio' ++ "] "
  where prio' = map toUpper (show prio)
