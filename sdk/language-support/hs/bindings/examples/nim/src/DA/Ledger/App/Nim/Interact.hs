-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Interact with a NimLedger, submitting commands and tracking extern transitions.
-- Used by Robot and UI
module DA.Ledger.App.Nim.Interact(PlayerState(..), makePlayerState, runSubmit) where

import Control.Concurrent
import System.Console.ANSI(Color(..))

import DA.Ledger.PastAndFuture
import DA.Ledger.Stream
import DA.Ledger.App.Nim.Domain
import DA.Ledger.App.Nim.Local(State,UserCommand,initState,externalizeCommand,applyManyTrans,applyTrans)
import DA.Ledger.App.Nim.Logging
import DA.Ledger.App.Nim.NimLedger(Handle,sendCommand,getTrans)
import DA.Ledger.App.Nim.NimTrans

data PlayerState = PlayerState {
    player :: Player,
    sv :: MVar State,
    stream :: Stream NimTrans
    }

makePlayerState :: Handle -> Logger -> Player -> IO PlayerState
makePlayerState h xlog player = do
    let knownPlayers = [alice,bob,charles] -- TODO: remove when automated
    let s = initState player knownPlayers
    sv <- newMVar s
    let playerLog = colourLog Blue xlog
    stream <- manageUpdates h player playerLog sv
    return PlayerState{player,sv,stream}
    where
    alice = Player "Alice"
    bob = Player "Bob"
    charles = Player "Charles"

runSubmit :: Handle -> Logger -> PlayerState -> UserCommand -> IO ()
runSubmit h log ps lc = do
    --log $ "lc: " <> show lc
    let PlayerState{player,sv} = ps
    let party = partyOfPlayer player
    s <- readMVar sv
    case externalizeCommand player s lc of
        Nothing -> do
            log $ "bad local command: " <> show lc
            return ()
        Just xc -> do
            sendCommand party h xc >>= \case
                Nothing -> return ()
                Just rej -> do
                    log $ "command rejected by ledger: " <> rej

-- Manage updates in response to NimTrans from the ledger

manageUpdates :: Handle -> Player -> Logger -> MVar State -> IO (Stream NimTrans)
manageUpdates h player log sv = do
    PastAndFuture{past,future} <- getTrans player h
    log $ "replaying " <> show (length past) <> " transactions"
    modifyMVar_ sv (\s -> applyManyTrans log s past)
    _ <- forkIO (updateX log sv future)
    return future

updateX :: Logger -> MVar State -> Stream NimTrans -> IO ()
updateX log sv stream = loop
  where
    loop = do
        takeStream stream >>= \case
            Left EOS -> do
                log "transaction stream has reached EOS"
            Left Abnormal{reason} -> do
                log $ "transaction stream is closed: " <> reason
            Right xt -> do
                applyX log sv xt
                loop

applyX :: Logger -> MVar State -> NimTrans -> IO ()
applyX log sv xt = do
    s <- takeMVar sv
    --log $ "xt: " <> show xt
    (lts,s') <- either fail return (applyTrans s xt)
    mapM_ (\lt -> log $ show lt) lts
    putMVar sv s'
