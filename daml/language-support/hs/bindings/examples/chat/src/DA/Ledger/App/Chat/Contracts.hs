-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Chat Contracts, both sent-to, and coming-from the external ledger.
module DA.Ledger.App.Chat.Contracts (
    ChatContract(..),
    makeLedgerCommand,extractTransaction,
    ) where

import DA.Ledger (
    PackageId,Command,ModuleName(..),EntityName(..),TemplateId(..),Identifier(..),
    Command(..), Event(..), Transaction(..)
    )

import DA.Ledger.App.Chat.Domain (Introduce,Message,Broadcast)
import DA.Ledger.App.Chat.Logging (Logger)
import DA.Ledger.IsLedgerValue (toRecord,fromRecord)

data ChatContract
    = Introduce Introduce
    | Message Message
    | Broadcast Broadcast

makeLedgerCommand :: PackageId -> ChatContract -> Command
makeLedgerCommand pid = \case
    Introduce x -> do
        let mod = ModuleName "Chat"
        let ent = EntityName "Introduce"
        let tid = TemplateId (Identifier pid mod ent)
        let args = toRecord x
        CreateCommand {tid,args}
    Message x -> do
        let mod = ModuleName "Chat"
        let ent = EntityName "Message"
        let tid = TemplateId (Identifier pid mod ent)
        let args = toRecord x
        CreateCommand {tid,args}
    Broadcast x -> do
        let mod = ModuleName "Chat"
        let ent = EntityName "Broadcast"
        let tid = TemplateId (Identifier pid mod ent)
        let args = toRecord x
        CreateCommand {tid,args}

extractEvents :: [Event] -> Maybe ChatContract
extractEvents = \case
    [CreatedEvent{tid=TemplateId Identifier{ent=EntityName"Introduce"}, createArgs}] -> do
        x <- fromRecord createArgs
        return $ Introduce x
    [CreatedEvent{tid=TemplateId Identifier{ent=EntityName"Message"}, createArgs}] -> do
        x <- fromRecord createArgs
        return $ Message x
    [CreatedEvent{tid=TemplateId Identifier{ent=EntityName"Broadcast"}, createArgs}] -> do
        x <- fromRecord createArgs
        return $ Broadcast x
    _ ->
        Nothing

extractTransaction :: Logger -> Transaction -> IO (Maybe ChatContract)
extractTransaction log Transaction{events} = do
    case extractEvents events of
        x@(Just _) -> return x
        Nothing -> do
            log "Surprising ledger transaction events: "
            mapM_ (\e -> log $ "- " <> show e) events
            return Nothing
