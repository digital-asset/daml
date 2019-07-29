-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


-- Convert between HL Ledger.Types and the LL types generated from .proto files
module DA.Ledger.Convert (
    lowerCommands, lowerLedgerOffset, lowerTimestamp,
    Perhaps, perhaps,
    raiseList,
    raiseParty,
    raiseTransaction,
    raiseTransactionTree,
    raiseCompletionStreamResponse,
    raiseGetActiveContractsResponse,
    raiseAbsLedgerOffset,
    raiseGetLedgerConfigurationResponse,
    raiseGetTimeResponse,
    raiseTimestamp,
    raisePackageId,
    RaiseFailureReason,
    ) where

import Data.Map(Map)
import Data.Maybe (fromMaybe)
import Data.Text.Lazy (Text)
import Data.Vector as Vector (Vector,fromList,toList)

import qualified Google.Protobuf.Empty as LL
import qualified Google.Protobuf.Timestamp as LL
import qualified Com.Digitalasset.Ledger.Api.V1.ActiveContractsService as LL
import qualified Com.Digitalasset.Ledger.Api.V1.CommandCompletionService as LL
import qualified Com.Digitalasset.Ledger.Api.V1.LedgerConfigurationService as LL
import qualified Com.Digitalasset.Ledger.Api.V1.Testing.TimeService as LL
import qualified Com.Digitalasset.Ledger.Api.V1.Commands as LL
import qualified Com.Digitalasset.Ledger.Api.V1.Completion as LL
import qualified Com.Digitalasset.Ledger.Api.V1.Event as LL
import qualified Com.Digitalasset.Ledger.Api.V1.Transaction as LL
import qualified Com.Digitalasset.Ledger.Api.V1.Value as LL
import qualified Com.Digitalasset.Ledger.Api.V1.LedgerOffset as LL
import qualified Data.Map as Map
import qualified Proto3.Suite.Types as LL

import DA.Ledger.Types

-- lower

lowerLedgerOffset :: LedgerOffset -> LL.LedgerOffset
lowerLedgerOffset = \case
    LedgerBegin ->
        LL.LedgerOffset {
        ledgerOffsetValue = Just (LL.LedgerOffsetValueBoundary (LL.Enumerated (Right LL.LedgerOffset_LedgerBoundaryLEDGER_BEGIN)))
        }
    LedgerEnd ->
        LL.LedgerOffset {
        ledgerOffsetValue = Just (LL.LedgerOffsetValueBoundary (LL.Enumerated (Right LL.LedgerOffset_LedgerBoundaryLEDGER_END)))
        }
    LedgerAbsOffset abs ->
        LL.LedgerOffset {
        ledgerOffsetValue = Just (LL.LedgerOffsetValueAbsolute (unAbsOffset abs))
        }

lowerCommands :: Commands -> LL.Commands
lowerCommands = \case
    Commands{..} ->
        LL.Commands {
        commandsLedgerId = unLedgerId lid,
        commandsWorkflowId = unWorkflowId (fromMaybe (WorkflowId "") wid),
        commandsApplicationId = unApplicationId aid,
        commandsCommandId = unCommandId cid,
        commandsParty = unParty party,
        commandsLedgerEffectiveTime = Just (lowerTimestamp leTime),
        commandsMaximumRecordTime = Just (lowerTimestamp mrTime),
        commandsCommands = Vector.fromList $ map lowerCommand coms }

lowerCommand :: Command -> LL.Command
lowerCommand = \case
    CreateCommand{..} ->
        LL.Command $ Just $ LL.CommandCommandCreate $ LL.CreateCommand {
        createCommandTemplateId = Just (lowerTemplateId tid),
        createCommandCreateArguments = Just (lowerRecord args)}

    ExerciseCommand{..} ->
        LL.Command $ Just $ LL.CommandCommandExercise $ LL.ExerciseCommand {
        exerciseCommandTemplateId = Just (lowerTemplateId tid),
        exerciseCommandContractId = unContractId cid,
        exerciseCommandChoice = unChoice choice,
        exerciseCommandChoiceArgument = Just (lowerValue arg) }

    CreateAndExerciseCommand{..} ->
        LL.Command $ Just $ LL.CommandCommandCreateAndExercise $ LL.CreateAndExerciseCommand {
        createAndExerciseCommandTemplateId = Just (lowerTemplateId tid),
        createAndExerciseCommandCreateArguments = Just (lowerRecord createArgs),
        createAndExerciseCommandChoice = unChoice choice,
        createAndExerciseCommandChoiceArgument = Just (lowerValue choiceArg) }

lowerTemplateId :: TemplateId -> LL.Identifier
lowerTemplateId (TemplateId x) = lowerIdentifier x

lowerIdentifier :: Identifier -> LL.Identifier
lowerIdentifier = \case
    Identifier{..} ->
        LL.Identifier {
        identifierPackageId = unPackageId pid,
        identifierModuleName = unModuleName mod,
        identifierEntityName = unEntityName ent }

lowerTimestamp :: Timestamp -> LL.Timestamp
lowerTimestamp = \case
    Timestamp{..} ->
        LL.Timestamp {
        timestampSeconds = fromIntegral seconds,
        timestampNanos = fromIntegral nanos
        }

lowerValue :: Value -> LL.Value
lowerValue = LL.Value . Just . \case -- TODO: more cases here
    VRecord r -> (LL.ValueSumRecord . lowerRecord) r
    VVariant _ -> undefined
    VContract c -> (LL.ValueSumContractId . unContractId) c
    VList vs -> (LL.ValueSumList . LL.List . Vector.fromList . map lowerValue) vs
    VInt i -> (LL.ValueSumInt64 . fromIntegral) i
    VDecimal t -> LL.ValueSumDecimal t
    VString t -> LL.ValueSumText t
    VTimestamp _ -> undefined
    VParty p -> (LL.ValueSumParty . unParty) p
    VBool b -> LL.ValueSumBool b
    VUnit -> LL.ValueSumUnit LL.Empty{}
    VDate _ -> undefined
    VOpt o -> (LL.ValueSumOptional . LL.Optional . fmap lowerValue) o
    VMap _ -> undefined

lowerRecord :: Record -> LL.Record
lowerRecord = \case
    Record{..} ->
        LL.Record {
        recordRecordId = fmap lowerIdentifier rid,
        recordFields = Vector.fromList $ map lowerRecordField fields }

lowerRecordField :: RecordField -> LL.RecordField
lowerRecordField = \case
    RecordField{..} ->
        LL.RecordField {
        recordFieldLabel = label,
        recordFieldValue = Just (lowerValue fieldValue) }


-- raise

data RaiseFailureReason = Missing String | Unexpected String deriving Show

type Perhaps a = Either RaiseFailureReason a

unexpected :: String -> Perhaps a
unexpected = Left . Unexpected

missing :: String -> Perhaps a
missing = Left . Missing

perhaps :: String -> Maybe a -> Perhaps a
perhaps tag = \case
    Nothing -> missing tag
    Just a -> Right a

optional :: Perhaps a -> Maybe a
optional = \case
    Left _ -> Nothing
    Right a -> Just a

raiseGetTimeResponse :: LL.GetTimeResponse -> Perhaps Timestamp
raiseGetTimeResponse = \case
    LL.GetTimeResponse{..} -> do
        perhaps "current_time" getTimeResponseCurrentTime >>= raiseTimestamp

raiseGetLedgerConfigurationResponse :: LL.GetLedgerConfigurationResponse -> Perhaps LedgerConfiguration
raiseGetLedgerConfigurationResponse x =
    perhaps "ledgerConfiguration" (LL.getLedgerConfigurationResponseLedgerConfiguration x)
    >>= raiseLedgerConfiguration


raiseLedgerConfiguration :: LL.LedgerConfiguration -> Perhaps LedgerConfiguration
raiseLedgerConfiguration = \case
    LL.LedgerConfiguration{..} -> do
        minTtl <- perhaps "min_ttl" ledgerConfigurationMinTtl
        maxTtl <- perhaps "max_ttl" ledgerConfigurationMaxTtl
        return $ LedgerConfiguration {..}

raiseGetActiveContractsResponse :: LL.GetActiveContractsResponse -> Perhaps (AbsOffset,Maybe WorkflowId,[Event])
raiseGetActiveContractsResponse = \case
    LL.GetActiveContractsResponse{..} -> do
        offset <- raiseAbsOffset getActiveContractsResponseOffset
        let wid = optional (raiseWorkflowId getActiveContractsResponseWorkflowId)
        events <- raiseList (raiseEvent . mkEventFromCreatedEvent) getActiveContractsResponseActiveContracts
        return (offset,wid,events)
    where
        mkEventFromCreatedEvent :: LL.CreatedEvent -> LL.Event
        mkEventFromCreatedEvent = LL.Event . Just . LL.EventEventCreated

raiseCompletionStreamResponse :: LL.CompletionStreamResponse -> Perhaps (Maybe Checkpoint,[Completion])
raiseCompletionStreamResponse = \case
    LL.CompletionStreamResponse{..} -> do
        let checkpoint = completionStreamResponseCheckpoint >>= optional . raiseCheckpoint
        completions <- raiseCompletions completionStreamResponseCompletions
        return (checkpoint,completions)

raiseCompletions :: Vector LL.Completion -> Perhaps [Completion]
raiseCompletions = raiseList raiseCompletion

raiseCompletion :: LL.Completion -> Perhaps Completion
raiseCompletion = \case
    LL.Completion{..} -> do
        cid <- raiseCommandId completionCommandId
        let status = Status --TODO: stop loosing info
        return Completion{..}

raiseCheckpoint :: LL.Checkpoint -> Perhaps Checkpoint
raiseCheckpoint = \case
    LL.Checkpoint{..} -> do
        let _ = checkpointRecordTime -- TODO: dont ignore!
        offset <- perhaps "checkpointOffset" checkpointOffset >>= raiseAbsLedgerOffset
        return Checkpoint{..}

raiseAbsLedgerOffset :: LL.LedgerOffset -> Perhaps AbsOffset
raiseAbsLedgerOffset = \case
    LL.LedgerOffset Nothing -> missing "LedgerOffset"
    LL.LedgerOffset (Just sum) -> case sum of
       LL.LedgerOffsetValueAbsolute text -> raiseAbsOffset text
       LL.LedgerOffsetValueBoundary _ -> unexpected "non-Absolute LedgerOffset"

raiseTransactionTree :: LL.TransactionTree -> Perhaps TransactionTree
raiseTransactionTree = \case
    LL.TransactionTree{..} -> do
    trid <- raiseTransactionId transactionTreeTransactionId
    let cid = optional (raiseCommandId transactionTreeCommandId)
    let wid = optional (raiseWorkflowId transactionTreeWorkflowId)
    leTime <- perhaps "transactionTreeEffectiveAt" transactionTreeEffectiveAt >>= raiseTimestamp
    offset <- raiseAbsOffset transactionTreeOffset
    events <- raiseMap raiseEventId raiseTreeEvent transactionTreeEventsById
    roots <- raiseList raiseEventId transactionTreeRootEventIds
    return TransactionTree {..}

raiseTreeEvent :: LL.TreeEvent -> Perhaps TreeEvent
raiseTreeEvent = \case
    LL.TreeEvent{treeEventKind = Nothing} -> missing "TreeEvent"
    LL.TreeEvent(Just (LL.TreeEventKindExercised LL.ExercisedEvent{..})) -> do
        eid <- raiseEventId exercisedEventEventId
        cid <- raiseContractId exercisedEventContractId
        tid <- perhaps "exercisedEventTemplateId" exercisedEventTemplateId >>= raiseTemplateId
        choice <- raiseChoice exercisedEventChoice
        choiceArg <- perhaps "exercisedEventChoiceArgument" exercisedEventChoiceArgument >>= raiseValue
        acting <- raiseList raiseParty exercisedEventActingParties
        let consuming = exercisedEventConsuming -- no conversion needed for Bool
        witness <- raiseList raiseParty exercisedEventWitnessParties
        childEids <- raiseList raiseEventId exercisedEventChildEventIds
        result <- perhaps "exercisedEventExerciseResult" exercisedEventExerciseResult >>= raiseValue
        return ExercisedTreeEvent{..}

    LL.TreeEvent(Just (LL.TreeEventKindCreated LL.CreatedEvent{..})) -> do
        eid <- raiseEventId createdEventEventId
        cid <- raiseContractId createdEventContractId
        tid <- perhaps "createdEventTemplateId" createdEventTemplateId >>= raiseTemplateId
        let key = createdEventContractKey >>= optional . raiseValue
        createArgs <- perhaps "createdEventCreateArguments" createdEventCreateArguments >>= raiseRecord
        witness <- raiseList raiseParty createdEventWitnessParties
        signatories <- raiseList raiseParty createdEventSignatories
        observers <- raiseList raiseParty createdEventObservers
        return CreatedTreeEvent{..}

raiseTransaction :: LL.Transaction -> Perhaps Transaction
raiseTransaction = \case
    LL.Transaction{..} -> do
    -- NOTE: "<-" is used when a field is required, "let" when a field is optional
    trid <- raiseTransactionId transactionTransactionId
    let cid = optional (raiseCommandId transactionCommandId)
    let wid = optional (raiseWorkflowId transactionWorkflowId)
    leTime <- perhaps "transactionEffectiveAt" transactionEffectiveAt >>= raiseTimestamp
    events <- raiseList raiseEvent transactionEvents
    offset <- raiseAbsOffset transactionOffset
    return Transaction {..}

raiseEvent :: LL.Event -> Perhaps Event
raiseEvent = \case
    LL.Event{eventEvent = Nothing} -> missing "Event"
    LL.Event(Just (LL.EventEventArchived LL.ArchivedEvent{..})) -> do
        eid <- raiseEventId archivedEventEventId
        cid <- raiseContractId archivedEventContractId
        tid <- perhaps "archivedEventTemplateId" archivedEventTemplateId >>= raiseTemplateId
        witness <- raiseList raiseParty archivedEventWitnessParties
        return ArchivedEvent{..}
    LL.Event(Just (LL.EventEventCreated LL.CreatedEvent{..})) -> do
        eid <- raiseEventId createdEventEventId
        cid <- raiseContractId createdEventContractId
        tid <- perhaps "createdEventTemplateId" createdEventTemplateId >>= raiseTemplateId
        let key = createdEventContractKey >>= optional . raiseValue
        createArgs <- perhaps "createdEventCreateArguments" createdEventCreateArguments >>= raiseRecord
        witness <- raiseList raiseParty createdEventWitnessParties
        signatories <- raiseList raiseParty createdEventSignatories
        observers <- raiseList raiseParty createdEventObservers
        return CreatedEvent{..}

raiseRecord :: LL.Record -> Perhaps Record
raiseRecord = \case
    LL.Record{..} -> do
        let rid = recordRecordId >>= optional . raiseIdentifier
        fields <- raiseList raiseRecordField recordFields
        return Record{..}

raiseRecordField :: LL.RecordField -> Perhaps RecordField
raiseRecordField = \case
    LL.RecordField{..} -> do
        let label = recordFieldLabel
        fieldValue <- perhaps "recordFieldValue" recordFieldValue >>= raiseValue
        return RecordField{..}

-- TODO: more cases here
raiseValue :: LL.Value -> Perhaps Value
raiseValue = \case
    LL.Value Nothing -> missing "Value"
    LL.Value (Just sum) -> case sum of
        LL.ValueSumRecord r -> (fmap VRecord . raiseRecord) r
        LL.ValueSumVariant _ -> undefined
        LL.ValueSumEnum _ -> undefined
        LL.ValueSumContractId c -> (return . VContract . ContractId) c
        LL.ValueSumList vs -> (fmap VList . raiseList raiseValue . LL.listElements) vs
        LL.ValueSumInt64 i -> (return . VInt . fromIntegral) i
        LL.ValueSumDecimal t -> (return . VDecimal) t
        LL.ValueSumText t -> (return . VString) t
        LL.ValueSumTimestamp _ -> undefined
        LL.ValueSumParty p -> (return . VParty . Party) p
        LL.ValueSumBool b -> (return . VBool) b
        LL.ValueSumUnit _ -> return VUnit
        LL.ValueSumDate _ -> undefined
        LL.ValueSumOptional o -> (fmap VOpt . raiseOptional) o
        LL.ValueSumMap _ -> undefined

raiseOptional :: LL.Optional -> Perhaps (Maybe Value)
raiseOptional = \case
    LL.Optional Nothing -> return Nothing
    LL.Optional (Just v) -> fmap Just (raiseValue v)

raiseTimestamp :: LL.Timestamp -> Perhaps Timestamp
raiseTimestamp = \case
    LL.Timestamp{timestampSeconds,timestampNanos} ->
        return $ Timestamp {seconds = fromIntegral timestampSeconds,
                            nanos = fromIntegral timestampNanos}

-- TODO: check that the text matches the spec in ledger_offset.proto
raiseAbsOffset :: Text -> Perhaps AbsOffset
raiseAbsOffset = Right . AbsOffset

raiseTemplateId :: LL.Identifier -> Perhaps TemplateId
raiseTemplateId = fmap TemplateId . raiseIdentifier

raiseIdentifier :: LL.Identifier -> Perhaps Identifier
raiseIdentifier = \case
    LL.Identifier{..} -> do
        pid <- raisePackageId identifierPackageId
        mod <- raiseModuleName identifierModuleName
        ent <- raiseEntityName identifierEntityName
        return Identifier{..}

raiseList :: (a -> Perhaps b) -> Vector a -> Perhaps [b]
raiseList f v = loop (Vector.toList v)
    where loop = \case
              [] -> return []
              x:xs -> do y <- f x; ys <- loop xs; return $ y:ys

raiseMap :: forall k k' v v'. Ord k'
         => (k -> Perhaps k')
         -> (v -> Perhaps v')
         -> Map k (Maybe v) -- The Maybe is an artifact of grpc's encoding of maps, as expressed by grpc-haskell
         -> Perhaps (Map k' v')
raiseMap raiseK raiseV = fmap Map.fromList . mapM raiseKV . Map.toList
    where raiseKV :: (k, Maybe v) -> Perhaps (k', v')
          raiseKV (kLow,vLowOpt) = do
              k <- raiseK kLow
              case vLowOpt of
                  Nothing -> missing "mapElem"
                  Just vLow -> do
                      v <- raiseV vLow
                      return (k,v)

raiseTransactionId :: Text -> Perhaps TransactionId
raiseTransactionId = fmap TransactionId . raiseText "TransactionId"

raiseWorkflowId :: Text -> Perhaps WorkflowId
raiseWorkflowId = fmap WorkflowId . raiseText "WorkflowId"

raiseCommandId :: Text -> Perhaps CommandId
raiseCommandId = fmap CommandId . raiseText "CommandId"

raiseEventId :: Text -> Perhaps EventId
raiseEventId = fmap EventId . raiseText "EventId"

raiseContractId :: Text -> Perhaps ContractId
raiseContractId = fmap ContractId . raiseText "ContractId"

raiseChoice :: Text -> Perhaps Choice
raiseChoice = fmap Choice . raiseText "Choice"

raiseParty :: Text -> Perhaps Party
raiseParty = fmap Party . raiseText "Party"

raisePackageId :: Text -> Perhaps PackageId
raisePackageId = fmap PackageId . raiseText "PackageId"

raiseModuleName :: Text -> Perhaps ModuleName
raiseModuleName = fmap ModuleName . raiseText "ModuleName"

raiseEntityName :: Text -> Perhaps EntityName
raiseEntityName = fmap EntityName . raiseText "EntityName"

raiseText :: String -> Text -> Perhaps Text
raiseText tag = perhaps tag . \case "" -> Nothing; x -> Just x
