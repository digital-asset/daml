-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Nim commands, to be sent to the external ledger.
module DA.Ledger.App.Nim.NimCommand(NimCommand(..), makeLedgerCommands,) where

import DA.Ledger as Ledger
import DA.Ledger.IsLedgerValue
import DA.Ledger.App.Nim.Domain

type Oid = ContractId
type Gid = ContractId

data NimCommand
    = OfferGame Offer
    | AcceptOffer Player Oid
    | MakeMove Gid Move
    deriving Show

makeLedgerCommands :: PackageId -> NimCommand -> Command
makeLedgerCommands pid = \case
    OfferGame offer -> do
        let mod = ModuleName "Nim"
        let ent = EntityName "GameOffer"
        let tid = TemplateId (Identifier pid mod ent)
        let args = toRecord offer
        CreateCommand {tid,args}

    AcceptOffer party oid -> do
        let mod = ModuleName "Nim"
        let ent = EntityName "GameOffer"
        let tid = TemplateId (Identifier pid mod ent)
        let choice = Choice "GameOffer_Accept"
        let arg = VRecord (Record Nothing [RecordField "" (toValue party)])
        ExerciseCommand {tid,cid=oid,choice,arg}

    MakeMove gid move -> do
        let mod = ModuleName "Nim"
        let ent = EntityName "GameInProgress"
        let tid = TemplateId (Identifier pid mod ent)
        let choice = Choice "Game_Take"
        let arg = VRecord (Record Nothing [RecordField "" (toValue move)])
        ExerciseCommand {tid,cid=gid,choice,arg}
