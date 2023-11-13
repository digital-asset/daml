-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.SessionTelemetry
    ( withPlugin
    ) where

import Control.Concurrent.Async
import Control.Concurrent.Extra
import Control.Monad
import Control.Monad.IO.Class
import DA.Service.Logger qualified as Lgr
import Data.HashMap.Strict qualified as HM
import Data.Int
import Data.Text qualified as T
import Development.IDE.Plugin
import Language.LSP.Types
import System.Clock
import System.Time.Extra

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

setSessionHandlers :: SessionState -> Plugin c
setSessionHandlers SessionState{..} = Plugin
    { pluginCommands = mempty
    , pluginHandlers = mempty
    , pluginRules = mempty
    , pluginNotificationHandlers = mconcat
        [ pluginNotificationHandler STextDocumentDidOpen $ \_ _ -> liftIO touch
        , pluginNotificationHandler STextDocumentDidClose $ \_ _ -> liftIO touch
        , pluginNotificationHandler STextDocumentDidChange $ \_ _ -> liftIO touch
        ]
    }
    where
        touch = writeVar lastActive =<< getTime Monotonic

withPlugin :: Lgr.Handle IO -> (Plugin c -> IO a) -> IO a
withPlugin lgr f = do
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

