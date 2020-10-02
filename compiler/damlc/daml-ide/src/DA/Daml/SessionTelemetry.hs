-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.SessionTelemetry
    ( withSessionPings
    ) where

import Control.Concurrent.Async
import Control.Concurrent.Extra
import Control.Monad
import qualified Data.HashMap.Strict as HM
import Data.Int
import qualified Data.Text as T
import Development.IDE.LSP.Server
import qualified Language.Haskell.LSP.Core as LSP
import System.Clock
import System.Time.Extra

import qualified DA.Service.Logger as Lgr

data SessionState = SessionState
  { lastActive :: !(Var TimeSpec)
  -- ^ Monotonic timespec to check if users were active within the last session.
  , gcpLogger :: Lgr.Handle IO
  -- ^ The logger we use to send session pings.
  }

initSessionState :: Lgr.Handle IO -> IO SessionState
initSessionState gcpLogger = do
    lastActive <- newVar =<< getTime Monotonic
    pure SessionState{..}

setSessionHandlers :: SessionState -> PartialHandlers a
setSessionHandlers SessionState{..} = PartialHandlers  $ \WithMessage{..} handlers -> pure handlers
    { LSP.didOpenTextDocumentNotificationHandler = withNotification (LSP.didOpenTextDocumentNotificationHandler handlers) $
        \_ _ _ -> touch
    , LSP.didCloseTextDocumentNotificationHandler = withNotification (LSP.didCloseTextDocumentNotificationHandler handlers) $
        \_ _ _ -> touch
    , LSP.didChangeTextDocumentNotificationHandler = withNotification (LSP.didChangeTextDocumentNotificationHandler handlers) $
        \_ _ _ -> touch
    }
    where
        touch = writeVar lastActive =<< getTime Monotonic

withSessionPings :: Lgr.Handle IO -> (PartialHandlers b -> IO a) -> IO a
withSessionPings lgr f = do
    sessionState <- initSessionState lgr
    withAsync (pingThread sessionState) $ const (f $ setSessionHandlers sessionState)
  where pingThread SessionState{..} = forever $ do
            currentTime <- getTime Monotonic
            lastActive <- readVar lastActive
            when (currentTime - lastActive <= TimeSpec (activeMinutesInterval * 60) 0) $ do
              Lgr.logDebug gcpLogger "Sending session ping"
              -- We were active in the last 5 minutes so send a ping.
              sendSessionPing gcpLogger
            -- sleep for 5 minutes and then check again
            sleep (fromIntegral activeMinutesInterval * 60)

-- | We consider a user to be active if they’ve done an action in the last 5 minutes.
activeMinutesInterval :: Int64
activeMinutesInterval = 5

sendSessionPing :: Lgr.Handle IO -> IO ()
sendSessionPing lgr = Lgr.logJson lgr Lgr.Telemetry $ HM.fromList @T.Text @T.Text
  [ ("type", "session_ping")
  ]

