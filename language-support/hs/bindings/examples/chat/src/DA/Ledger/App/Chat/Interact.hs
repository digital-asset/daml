-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Interact with a ChatLedger, submitting commands and tracking extern transitions.
-- This is basically unchanged from the Nim example
module DA.Ledger.App.Chat.Interact (InteractState(..), makeInteractState, runSubmit) where

import Control.Concurrent (forkIO)
import Control.Concurrent.MVar
import Data.List
import DA.Ledger.App.Chat.ChatLedger (Handle,sendCommand,getTrans)
import DA.Ledger.App.Chat.Contracts (ChatContract)
import DA.Ledger.App.Chat.Domain (Party)
import DA.Ledger.App.Chat.Local (State,initState,UserCommand,externalizeCommand,applyTrans,applyTransQuiet,introduceEveryone)
import DA.Ledger.App.Chat.Logging (Logger,colourLog)
import DA.Ledger.PastAndFuture (PastAndFuture(..))
import DA.Ledger.Stream (Stream,Closed(EOS,Abnormal,reason),takeStream)
import System.Console.ANSI (Color(..))

data InteractState = InteractState {
    whoami :: Party,
    talking :: Maybe Party,
    sv :: MVar State,
    stream :: Stream ChatContract
    }

makeInteractState :: Handle -> Logger -> Party -> IO InteractState
makeInteractState h xlog whoami = do
    sv <- newMVar initState
    let partyLog = colourLog Blue xlog
    stream <- manageUpdates h whoami partyLog sv
    return InteractState{whoami,talking=Nothing,sv,stream}

sendShowingRejection :: Party -> Handle -> Logger -> ChatContract -> IO ()
sendShowingRejection whoami h log cc =
    sendCommand whoami h cc >>= \case
    Nothing -> return ()
    Just rej -> log $ "command rejected by ledger: " <> rej

runSubmit :: Handle -> Logger -> InteractState -> UserCommand -> IO ()
runSubmit h log is uc = do
    --log $ "uc: " <> show uc
    let InteractState{whoami,talking,sv} = is
    s <- readMVar sv
    case externalizeCommand whoami talking s uc of
        Left reason -> log reason
        Right cc -> sendShowingRejection whoami h log cc

-- Manage updates in response to ChatContract from the ledger

manageUpdates :: Handle -> Party -> Logger -> MVar State -> IO (Stream ChatContract)
manageUpdates h whoami log sv = do
    PastAndFuture{past,future} <- getTrans whoami h
    log $ "replaying " <> show (length past) <> " transactions"
    modifyMVar_ sv (\s -> return $ foldl' (applyTransQuiet whoami) s past)
    withMVar sv $ \s -> sendShowingRejection whoami h log (introduceEveryone whoami s)
    _ <- forkIO (updateX h whoami log sv future)
    return future

updateX :: Handle -> Party -> Logger -> MVar State -> Stream ChatContract -> IO ()
updateX h whoami log sv stream = loop
  where
    loop = do
        takeStream stream >>= \case
            Left EOS -> do
                log "transaction stream has reached EOS"
            Left Abnormal{reason} -> do
                log $ "transaction stream is closed: " <> reason
            Right cc -> do
                applyX h whoami log sv cc
                loop

applyX :: Handle -> Party -> Logger -> MVar State -> ChatContract -> IO ()
applyX h whoami log sv cc = do
    s <- takeMVar sv
    let (s',ans,replies) = applyTrans whoami s cc
    mapM_ (sendShowingRejection whoami h log) replies
    mapM_ (log . show) ans
    putMVar sv s'
