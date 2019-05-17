-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


-- Simulate a ledger
-- Accepting commands, and feeding back the resultant transitions

module SimLedger(Handle, connect, sendCommand, getTrans) where

import Control.Concurrent
import Control.Monad(when,filterM)
import Data.List as List(filter)
import qualified Data.Map.Strict as Map(lookup,empty,adjust,insert)
import Data.Map.Strict (Map)
import System.Time.Extra(sleep)

import Domain
import External
import DA.Ledger.Stream as Stream
import DA.Ledger.PastAndFuture

data Client = Client { player :: Player, stream :: Stream XTrans }

newClient :: Player -> IO Client
newClient player = do
    stream <- newStream
    return Client{player,stream}

sendToClient :: XTrans -> Client -> IO ()
sendToClient xt Client{player,stream} = do
    when (canSeeTrans player xt) $ writeStream stream (Right xt)

isClientClosed :: Client -> IO Bool
isClientClosed Client{stream} = do
    Stream.isClosed stream >>= \case
        Just _ -> return True
        Nothing -> return False

sendListToClients :: [XTrans] -> [Client] -> IO ()
sendListToClients xts clients =
    mapM_ (\xt -> mapM_ (sendToClient xt) clients) xts

after :: Double -> IO () -> IO ()
after n action = do
    _ <- forkIO (do sleep n; action)
    return ()

data Handle = LH {
    state :: MVar Ledger,
    history :: MVar [XTrans],
    watching :: MVar [Client]
    }

connect :: IO Handle
connect = do
    state <- newMVar emptyLedger
    history <- newMVar []
    watching <- newMVar []
    return LH {state,history,watching}

sendCommand :: Handle -> XCommand -> IO (Maybe Rejection)
sendCommand LH{state,watching,history} com = do
    ledger <- takeMVar state
    (ledger',rejOpt,xts) <- recordOnLedger ledger com
    putMVar state ledger'
    modifyMVar_ history (return . (reverse xts ++))
    clients <- takeMVar watching
    -- TODO: would be better to setup onClose handler
    clients' <- filterM (fmap not . isClientClosed) clients
    putMVar watching clients'
    -- delay to simulate the lag of a real ledger
    after 1.5 (sendListToClients xts clients')
    return rejOpt

getTrans :: Player -> Handle -> IO (PastAndFuture XTrans)
getTrans player LH{watching,history} = do
    client@Client{stream=future} <- newClient player
    modifyMVar_ watching (return . (client:))
    past <- fmap (reverse . List.filter (canSeeTrans player)) $ readMVar history
    return PF{past, future}

canSeeTrans :: Player -> XTrans -> Bool
canSeeTrans player xt = player `elem` playersOfTrans xt

playersOfTrans :: XTrans -> [Player]
playersOfTrans = \case
    NewOffer {offer} -> playersOfOffer offer
    OfferWithdrawn{offer} -> playersOfOffer offer
    NewGame {game} -> playersOfGame game
    GameMove {game} -> playersOfGame game

data Ledger = Ledger (Map Xoid (Offer,Bool)) (Map Xgid (Game,Bool))

emptyLedger :: Ledger
emptyLedger = Ledger Map.empty Map.empty

type Rejection = String

-- TODO: Better return type --  IO (Either Rejection (Ledger,[XTrans]))
-- which makes clear than on rejection we chage nothing & generate no xtrans
recordOnLedger :: Ledger -> XCommand -> IO (Ledger,Maybe Rejection,[XTrans])
recordOnLedger ledger@(Ledger os gs) = \case

    OfferGame offer -> do
        oid <- genXoid
        let os' = Map.insert oid (offer,True) os
        accept (Ledger os' gs) [NewOffer oid offer]

    AcceptOffer acceptor oid ->
        case Map.lookup oid os of
            Nothing -> reject "no such oid"
            Just (_,False) -> reject "double accept"
            Just (offer,True) -> do
                let os' = Map.adjust archive oid os
                if acceptor `notElem` to offer then reject "not in offer to-list" else do
                    xgid <- genXgid
                    let game = initGame acceptor (from offer)
                    let gs' = Map.insert xgid (game,True) gs
                    accept (Ledger os' gs') [OfferWithdrawn oid offer, NewGame {xgid, game}]

    MakeMove player gid move ->
        case Map.lookup gid gs of
            Nothing -> reject "no such gid"
            Just (_,False) -> reject "double play"
            Just (game,True) -> do
                if player /= p1 game then reject "not player1" else do
                    gid' <- genXgid
                    case playMove move game of
                        Left reason -> reject reason
                        Right game' -> do
                            let gs' = Map.insert gid' (game',True) (Map.adjust archive gid gs)
                            accept (Ledger os gs') [GameMove gid gid' game']

  where
      accept l ts = return (l,Nothing,ts)
      reject reason = return (ledger,Just reason,[])
      archive (x,_) = (x,False)
