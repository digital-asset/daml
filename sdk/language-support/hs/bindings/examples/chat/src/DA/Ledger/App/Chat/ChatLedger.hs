-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


-- Abstraction for a Ledger which is hosting the Chat domain model.
-- This is basically unchanged from the Nim example
module DA.Ledger.App.Chat.ChatLedger (Handle, connect, sendCommand, getTrans) where

import Control.Monad(forM)
import DA.Ledger as Ledger
import DA.Ledger.App.Chat.Contracts (ChatContract,extractTransaction,makeLedgerCommand)
import DA.Ledger.App.Chat.Logging (Logger)
import Data.List as List
import Data.Maybe (maybeToList)
import System.Random (randomIO)
import qualified Data.Text.Lazy as Text (pack)
import qualified Data.UUID as UUID

data Handle = Handle {
    log :: Logger,
    lid :: LedgerId,
    pid :: PackageId
    }

type Rejection = String

port :: Port
port = 6865 -- port on which we expect to find a ledger. should be a command line option

run :: TimeoutSeconds -> LedgerService a -> IO a
run timeout ls  = runLedgerService ls timeout (configOfPort port)

connect :: Logger -> IO Handle
connect log = do
    lid <- run 5 getLedgerIdentity

    discovery <- run 5 $ do
        pids <- listPackages lid
        forM pids $ \pid -> do
            getPackage lid pid >>= \case
                Nothing -> return (pid, False)
                Just package -> do
                    return (pid, containsChat package)

    case List.filter snd discovery of
        [] -> fail "cant find package containing Chat"
        xs@(_:_:_) -> fail $ "found multiple packages containing Chat: " <> show (map fst xs)
        [(pid,_)] -> return Handle{log,lid,pid}

containsChat :: Package -> Bool
containsChat package = "Chat" `isInfixOf` show package -- TODO: be more principled

sendCommand :: Party -> Handle -> ChatContract -> IO (Maybe Rejection)
sendCommand asParty h@Handle{pid} cc = do
    let com = makeLedgerCommand pid cc
    submitCommand h asParty com >>= \case
        Left rejection -> return $ Just rejection
        Right () -> return Nothing

getTrans :: Party -> Handle -> IO (PastAndFuture ChatContract)
getTrans party Handle{log,lid} = do
    pf <- run 6000 $ getTransactionsPF lid party
    mapListPF (fmap concat . mapM (fmap maybeToList . extractTransaction log)) pf

submitCommand :: Handle -> Party -> Command -> IO (Either String ())
submitCommand Handle{lid} party com = do
    cid <- randomCid
    run 5 $ Ledger.submit (Commands {lid,wid,aid=myAid,cid,actAs=[party],readAs=[],dedupPeriod=Nothing,coms=[com],minLeTimeAbs=Nothing,minLeTimeRel=Nothing,sid})
    where
        wid = Nothing
        myAid = ApplicationId "chat-console"
        sid = Nothing
randomCid :: IO CommandId
randomCid = do fmap (CommandId . Text.pack . UUID.toString) randomIO
