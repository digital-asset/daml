-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Abstraction for a Ledger which is hosting the Nim domain model.
module DA.Ledger.App.Nim.NimLedger(Handle, connect, sendCommand, getTrans) where

import Control.Monad(forM)
import DA.Ledger as Ledger
import DA.Ledger.App.Nim.Domain
import DA.Ledger.App.Nim.Logging
import DA.Ledger.App.Nim.NimCommand
import DA.Ledger.App.Nim.NimTrans
import Data.List as List
import System.Random(randomIO)
import Data.Text.Lazy qualified as Text (pack)
import Data.UUID qualified as UUID

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
                    return (pid, containsNim package)

    case List.filter snd discovery of
        [] -> fail "cant find package containing Nim"
        xs@(_:_:_) -> fail $ "found multiple packages containing Nim: " <> show (map fst xs)
        [(pid,_)] -> return Handle{log,lid,pid}

containsNim :: Package -> Bool
containsNim package = "Nim" `isInfixOf` show package -- TODO: be more principled

sendCommand :: Party -> Handle -> NimCommand -> IO (Maybe Rejection)
sendCommand asParty h@Handle{pid} xcom = do
    let com = makeLedgerCommands pid xcom
    submitCommand h asParty com >>= \case
        Left rejection -> return $ Just rejection
        Right () -> return Nothing

getTrans :: Player -> Handle -> IO (PastAndFuture NimTrans)
getTrans player Handle{log,lid} = do
    let party = partyOfPlayer player
    pf <- run 6000 $ getTransactionsPF lid party
    mapListPF (fmap concat . mapM (extractTransaction log)) pf

submitCommand :: Handle -> Party -> Command -> IO (Either String ())
submitCommand Handle{lid} party com = do
    cid <- randomCid
    run 5 (Ledger.submit (Commands {lid,wid,aid=myAid,cid,actAs=[party],readAs=[],dedupPeriod=Nothing,coms=[com],minLeTimeAbs=Nothing,minLeTimeRel=Nothing,sid}))
    where
        wid = Nothing
        myAid = ApplicationId "nim"
        sid = Nothing

randomCid :: IO CommandId
randomCid = do fmap (CommandId . Text.pack . UUID.toString) randomIO
