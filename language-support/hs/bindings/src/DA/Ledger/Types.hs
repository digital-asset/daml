-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DuplicateRecordFields #-}

module DA.Ledger.Types( -- High Level types for communication over Ledger API

    Commands(..),
    Command(..),
    Completion(..),
    WIP_Transaction(..),
    Event(..),
    Value(..),
    Record(..),
    RecordField(..),
    Variant(..),
    Identifier(..),
    Timestamp(..),
    Status, --(..), -- TODO

    MicroSecondsSinceEpoch(..),
    DaysSinceEpoch(..),
    TemplateId(..),
    LedgerId(..),
    TransactionId(..),
    EventId(..),
    ContractId(..),
    WorkflowId(..),
    ApplicationId(..),
    CommandId(..),
    PackageId(..),
    ConstructorId(..),
    VariantId(..),
    Choice(..),
    Party(..),
    ModuleName(..),
    EntityName(..),
    AbsOffset(..),

    ) where

import Data.Map (Map)
import Data.Text.Lazy (Text)
import qualified Data.Text.Lazy as Text

-- TODO: deriving Show everywhere?

-- commands.proto

data Commands = Commands {
    lid    :: LedgerId,
    wid    :: Maybe WorkflowId,
    aid    :: ApplicationId,
    cid    :: CommandId,
    party  :: Party,
    leTime :: Timestamp,
    mrTime :: Timestamp,
    coms   :: [Command] }

data Command
    = CreateCommand {
        tid  :: TemplateId,
        args :: Record }

    | ExerciseCommand {
        tid    :: TemplateId,
        cid    :: ContractId,
        choice :: Choice,
        arg    :: Value }

    | CreateAndExerciseCommand {
        tid        :: TemplateId,
        createArgs :: Record,
        choice     :: Choice,
        choiceArg  :: Value }

-- completion.proto

data Completion
    = Completion {
        cid    :: CommandId,
        status :: Status }

-- transaction.proto

data WIP_Transaction
    = WIP_Transaction {
        trid   :: TransactionId,
        cid    :: Maybe CommandId,
        wid    :: Maybe WorkflowId,
        leTime :: Timestamp,
        events :: [Event],
        ofset  :: AbsOffset }

-- event.proto

data Event
    = CreatedEvent {
        eid        :: EventId,
        cid        :: ContractId,
        tid        :: TemplateId,
        createArgs :: Record,
        witness    :: [Party] }

    | ArchivedEvent {
        eid     :: EventId,
        cid     :: ContractId,
        tid     :: TemplateId,
        witness :: [Party] }

    | ExercisedEvent {
        eid       :: EventId,
        cid       :: ContractId,
        tid       :: TemplateId,
        ccEid     :: EventId,
        choice    :: Choice,
        choiceArg :: Value,
        acting    :: [Party],
        consuming :: Bool,
        witness   :: [Party],
        childEids :: [EventId] }

-- value.proto

data Value
    = VRecord Record
    | VVariant Variant
    | VContract ContractId
    | VList [Value]
    | VInt Int
    | VDecimal Text -- TODO: why not Int?
    | VString Text
    | VTimestamp MicroSecondsSinceEpoch
    | VParty Party
    | VBool Bool
    | VUnit
    | VDate DaysSinceEpoch
    | VOpt (Maybe Value)
    | VMap (Map Text Value)

data Record
    = Record {
        rid    :: Maybe Identifier,
        fields :: [RecordField] }

data RecordField
    = RecordField {
        label :: Text,
        value :: Value }

data Variant
    = Variant {
        vid         :: VariantId,
        constructor :: ConstructorId,
        value       :: Value }

data Identifier
    = Identifier {
        pid :: PackageId,
        mod :: ModuleName,
        ent :: EntityName }

newtype MicroSecondsSinceEpoch = MicroSecondsSinceEpoch Int
newtype DaysSinceEpoch = DaysSinceEpoch Int

data Timestamp
    = Timestamp {
        seconds :: Integer,
        nanos   :: Integer }

data Status -- TODO: from standard google proto, determining success/failure

newtype TemplateId = TemplateId Identifier

newtype LedgerId = LedgerId { unLedgerId :: Text } deriving Show

-- Text wrappers
newtype TransactionId = TransactionId { unTransactionId :: Text }
newtype EventId = EventId { unEventId :: Text }
newtype ContractId = ContractId { unContractId :: Text }
newtype WorkflowId = WorkflowId { unWorkflowId :: Text }
newtype ApplicationId = ApplicationId { unApplicationId :: Text } deriving Show
newtype CommandId = CommandId { unCommandId :: Text }
newtype PackageId = PackageId { unPackageId :: Text }
newtype ConstructorId = ConstructorId { unConstructorId :: Text }
newtype VariantId = VariantId { unVariantId :: Text }

newtype Choice = Choice { unChoice :: Text }

newtype Party = Party { unParty :: Text }
instance Show Party where show = Text.unpack . unParty

newtype ModuleName = ModuleName { unModuleName :: Text }
newtype EntityName = EntityName { unEntityName :: Text }

newtype AbsOffset = AbsOffset { unAbsOffset :: Text } -- TODO: why not an int?

-- TODO: .proto message types now yet handled
{-
message Checkpoint {
message LedgerConfiguration {
message LedgerOffset {
message TraceContext {
message TransactionFilter {
message Filters {
message InclusiveFilters {
message TransactionTree {
message TreeEvent {
-}
