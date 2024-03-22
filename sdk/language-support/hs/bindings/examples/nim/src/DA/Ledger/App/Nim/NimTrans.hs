-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Nim transitions, coming from the external ledger.
module DA.Ledger.App.Nim.NimTrans(NimTrans(..), extractTransaction,) where

import DA.Ledger as Ledger
import DA.Ledger.IsLedgerValue
import DA.Ledger.App.Nim.Domain
import DA.Ledger.App.Nim.Logging

-- order id & game id

type Oid = ContractId
type Gid = ContractId

data NimTrans
    = NewOffer { oid :: Oid, offer :: Offer }
    | OfferWithdrawn { oid :: Oid }
    | NewGame { oid :: Oid, gid :: Gid, game :: Game }
    | GameMove { oldGid :: Gid, newGid :: Gid, game :: Game }
    deriving Show

-- This is not very nice, matching on the entire event-list of a transaction
-- But it is necessary to link the flow GameInProgress contracts which make up a single game
extractEvents :: [Event] -> Maybe [NimTrans]
extractEvents = \case
    [CreatedEvent{cid=oid, tid=TemplateId Identifier{ent=EntityName"GameOffer"}, createArgs}] -> do
        offer <- fromRecord createArgs
        return [NewOffer{oid,offer}]

    [ArchivedEvent{cid=oid, tid=TemplateId Identifier{ent=EntityName"GameOffer"}}] -> do
        return [OfferWithdrawn{oid}]

    [ArchivedEvent{cid=oid, tid=TemplateId Identifier{ent=EntityName"GameOffer"}},
     CreatedEvent{cid=gid, tid=TemplateId Identifier{ent=EntityName"GameInProgress"}, createArgs}] -> do
        game <- fromRecord createArgs
        return [OfferWithdrawn{oid}, NewGame{gid,oid,game}]

    [ArchivedEvent{cid=oldGid, tid=TemplateId Identifier{ent=EntityName"GameInProgress"}},
     CreatedEvent{cid=newGid, tid=TemplateId Identifier{ent=EntityName"GameInProgress"}, createArgs}] -> do
        game <- fromRecord createArgs
        return [GameMove {oldGid,newGid,game}]

    _ ->
        Nothing

extractTransaction :: Logger -> Transaction -> IO [NimTrans]
extractTransaction log = \case
  Transaction{events} -> do
    case extractEvents events of
        Just xs -> return xs
        Nothing -> do
            log "Surprising ledger transaction events: "
            mapM_ (\e -> log $ "- " <> show e) events
            return []
