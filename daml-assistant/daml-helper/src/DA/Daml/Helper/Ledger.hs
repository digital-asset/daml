-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DA.Daml.Helper.Ledger (
    LedgerArgs(..), Token(..), L.ClientSSLConfig(..),
    L.ClientSSLKeyCertPair(..),
    listParties, PartyDetails(..), Party(..),
    lookupParty,
    allocateParty,
    uploadDarFile,
    ) where

import DA.Ledger(LedgerService,PartyDetails(..),Party(..),Token)
import Data.List.Extra as List
import Data.String(fromString)
import qualified DA.Ledger as L
import qualified Data.ByteString as BS
import qualified Data.Text.Lazy as Text(pack)

data LedgerArgs = LedgerArgs
  { host :: String
  , port :: Int
  , tokM :: Maybe Token
  , sslConfigM :: Maybe L.ClientSSLConfig
  }

instance Show LedgerArgs where
    show LedgerArgs{host,port} = host <> ":" <> show port

listParties :: LedgerArgs -> IO [PartyDetails]
listParties hp = run hp L.listKnownParties

lookupParty :: LedgerArgs -> String -> IO (Maybe Party)
lookupParty hp name = do
    xs <- listParties hp
    let text = Text.pack name
    let pred PartyDetails{displayName,party} = if text == displayName then Just party else Nothing
    return $ List.firstJust pred xs

allocateParty :: LedgerArgs -> String -> IO Party
allocateParty hp name = run hp $ do
    let text = Text.pack name
    let request = L.AllocatePartyRequest
            { partyIdHint = text
            , displayName = text }
    PartyDetails{party} <- L.allocateParty request
    return party

uploadDarFile :: LedgerArgs -> BS.ByteString -> IO ()
uploadDarFile hp bytes = run hp $ do
    L.uploadDarFile bytes >>= either fail return

run :: LedgerArgs -> LedgerService a -> IO a
run hp ls = do
    let LedgerArgs{host,port,tokM} = hp
    let ls' = case tokM of Nothing -> ls; Just tok -> L.setToken tok ls
    let timeout = 30 :: L.TimeoutSeconds
    let ledgerClientConfig =
            L.configOfHostAndPort
                (L.Host $ fromString host)
                (L.Port port)
                (sslConfigM hp)
    L.runLedgerService ls' timeout ledgerClientConfig
