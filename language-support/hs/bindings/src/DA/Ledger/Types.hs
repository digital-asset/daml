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
    deriving Show

-- completion.proto

data Completion
    = Completion {
        cid    :: CommandId,
        status :: Status }
    deriving Show

-- transaction.proto

data WIP_Transaction
    = WIP_Transaction {
        trid   :: TransactionId,
        cid    :: Maybe CommandId,
        wid    :: Maybe WorkflowId,
        leTime :: Timestamp,
        events :: [Event],
        ofset  :: AbsOffset } deriving Show

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
        childEids :: [EventId] } deriving Show

-- value.proto

data Value
    = VRecord Record
    | VVariant Variant
    | VContract ContractId
    | VList [Value]
    | VInt Int
    | VDecimal Text -- TODO: Maybe use Haskell Decimal type
    | VString Text
    | VTimestamp MicroSecondsSinceEpoch
    | VParty Party
    | VBool Bool
    | VUnit
    | VDate DaysSinceEpoch
    | VOpt (Maybe Value)
    | VMap (Map Text Value)
    deriving Show

data Record
    = Record {
        rid    :: Maybe Identifier,
        fields :: [RecordField] } deriving Show

data RecordField
    = RecordField {
        label :: Text,
        value :: Value } deriving Show

data Variant
    = Variant {
        vid         :: VariantId,
        constructor :: ConstructorId,
        value       :: Value } deriving Show

data Identifier
    = Identifier {
        pid :: PackageId,
        mod :: ModuleName,
        ent :: EntityName } deriving Show

newtype MicroSecondsSinceEpoch = MicroSecondsSinceEpoch Int deriving Show-- TODO: Int64?
newtype DaysSinceEpoch = DaysSinceEpoch Int  deriving Show

data Timestamp
    = Timestamp {
        seconds :: Integer, -- TODO: Int64?
        nanos   :: Integer }  deriving Show

data Status = Status-- TODO: from standard google proto, determining success/failure
 deriving Show

newtype TemplateId = TemplateId Identifier
    deriving Show

newtype LedgerId = LedgerId { unLedgerId :: Text } deriving Show

-- Text wrappers
newtype TransactionId = TransactionId { unTransactionId :: Text } deriving Show
newtype EventId = EventId { unEventId :: Text } deriving Show
newtype ContractId = ContractId { unContractId :: Text } deriving Show
newtype WorkflowId = WorkflowId { unWorkflowId :: Text } deriving Show
newtype ApplicationId = ApplicationId { unApplicationId :: Text } deriving Show
newtype CommandId = CommandId { unCommandId :: Text } deriving (Show,Eq)
newtype PackageId = PackageId { unPackageId :: Text } deriving Show
newtype ConstructorId = ConstructorId { unConstructorId :: Text } deriving Show
newtype VariantId = VariantId { unVariantId :: Text } deriving Show

newtype Choice = Choice { unChoice :: Text } deriving Show

newtype Party = Party { unParty :: Text }
instance Show Party where show = Text.unpack . unParty

newtype ModuleName = ModuleName { unModuleName :: Text } deriving Show
newtype EntityName = EntityName { unEntityName :: Text } deriving Show

newtype AbsOffset = AbsOffset { unAbsOffset :: Text }  deriving Show -- TODO: why not an int?

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
