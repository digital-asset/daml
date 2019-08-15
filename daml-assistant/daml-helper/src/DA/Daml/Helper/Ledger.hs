-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DA.Daml.Helper.Ledger (
    HostAndPort(..),
    listParties, PartyDetails(..), Party(..),
    lookupParty,
    allocateParty,
    uploadDarFile,
    ) where

import DA.Ledger(LedgerService,PartyDetails(..),Party(..))
import Data.List.Extra as List
import Data.String(fromString)
import qualified DA.Ledger as L
import qualified Data.ByteString as BS
import qualified Data.Text.Lazy as Text(pack)

data HostAndPort = HostAndPort { host :: String, port :: Int }

instance Show HostAndPort where
    show HostAndPort{host,port} = host <> ":" <> show port

listParties :: HostAndPort -> IO [PartyDetails]
listParties hp = run hp L.listKnownParties

lookupParty :: HostAndPort -> String -> IO (Maybe Party)
lookupParty hp name = do
    xs <- listParties hp
    let text = Text.pack name
    let pred PartyDetails{displayName,party} = if text == displayName then Just party else Nothing
    return $ List.firstJust pred xs

allocateParty :: HostAndPort -> String -> IO Party
allocateParty hp name = run hp $ do
    let text = Text.pack name
    let request = L.AllocatePartyRequest
            { partyIdHint = text
            , displayName = text }
    PartyDetails{party} <- L.allocateParty request
    return party

uploadDarFile :: HostAndPort -> BS.ByteString -> IO ()
uploadDarFile hp bytes = run hp $ do
    L.uploadDarFile bytes >>= either fail return

run :: HostAndPort -> LedgerService a -> IO a
run hp ls = do
    let HostAndPort{host,port} = hp
    let timeout = 30 :: L.TimeoutSeconds
    let ledgerClientConfig = L.configOfHostAndPort (L.Host $ fromString host) (L.Port port)
    L.runLedgerService ls timeout ledgerClientConfig
