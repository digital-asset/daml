-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

-- Convert between HL Ledger.Types and the LL types generated from .proto files
module DA.Ledger.Convert (lowerCommands,raiseTransaction) where

import Data.Maybe (fromMaybe)
import Data.Text.Lazy (Text)
import Data.Vector as Vector (Vector,fromList,toList)

import qualified DA.Ledger.LowLevel as LL
import DA.Ledger.Types

-- lower

lowerCommands :: Commands -> LL.Commands
lowerCommands = \case
    Commands{lid,wid,aid,cid,party,leTime,mrTime,coms} ->
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
    CreateCommand{tid,args} ->
        LL.Command $ Just $ LL.CommandCommandCreate $ LL.CreateCommand {
        createCommandTemplateId = Just (lowerTemplateId tid),
        createCommandCreateArguments = Just (lowerRecord args)}

    ExerciseCommand{tid,cid,choice,arg} ->
        LL.Command $ Just $ LL.CommandCommandExercise $ LL.ExerciseCommand {
        exerciseCommandTemplateId = Just (lowerTemplateId tid),
        exerciseCommandContractId = unContractId cid,
        exerciseCommandChoice = unChoice choice,
        exerciseCommandChoiceArgument = Just (lowerValue arg) }

    CreateAndExerciseCommand{tid,createArgs,choice,choiceArg} ->
        LL.Command $ Just $ LL.CommandCommandCreateAndExercise $ LL.CreateAndExerciseCommand {
        createAndExerciseCommandTemplateId = Just (lowerTemplateId tid),
        createAndExerciseCommandCreateArguments = Just (lowerRecord createArgs),
        createAndExerciseCommandChoice = unChoice choice,
        createAndExerciseCommandChoiceArgument = Just (lowerValue choiceArg) }

lowerTemplateId :: TemplateId -> LL.Identifier
lowerTemplateId (TemplateId x) = lowerIdentifier x

lowerIdentifier :: Identifier -> LL.Identifier
lowerIdentifier = \case
    Identifier{pid,mod,ent} ->
        LL.Identifier {
        identifierPackageId = unPackageId pid,
        identifierName = "", -- marked as deprecated in .proto
        identifierModuleName = unModuleName mod,
        identifierEntityName = unEntityName ent }

lowerTimestamp :: Timestamp -> LL.Timestamp
lowerTimestamp = \case
    Timestamp{seconds,nanos} ->
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
    Record{rid,fields} ->
        LL.Record {
        recordRecordId = fmap lowerIdentifier rid,
        recordFields = Vector.fromList $ map lowerRecordField fields }

lowerRecordField :: RecordField -> LL.RecordField
lowerRecordField = \case
    RecordField{label,fieldValue} ->
        LL.RecordField {
        recordFieldLabel = label,
        recordFieldValue = Just (lowerValue fieldValue) }


-- raise

data RaiseFailureReason = Missing String deriving Show

type Perhaps a = Either RaiseFailureReason a

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

raiseTransaction :: LL.Transaction -> Perhaps Transaction
raiseTransaction = \case
    LL.Transaction{transactionTransactionId,
                   transactionCommandId,
                   transactionWorkflowId,
                   transactionEffectiveAt,
                   transactionEvents,
                   transactionOffset} -> do
    -- NOTE: "<-" is used when a field is required, "let" when a field is optional
    trid <- raiseTransactionId transactionTransactionId
    let cid = optional (raiseCommandId transactionCommandId)
    let wid = optional (raiseWorkflowId transactionWorkflowId)
    leTime <- perhaps "transactionEffectiveAt" transactionEffectiveAt >>= raiseTimestamp
    events <- raiseList raiseEvent transactionEvents
    offset <- raiseAbsOffset transactionOffset
    return Transaction {trid, cid, wid, leTime, events, offset}

raiseEvent :: LL.Event -> Perhaps Event
raiseEvent = \case
    LL.Event{eventEvent = Nothing} -> missing "Event"
    LL.Event(Just (LL.EventEventArchived
                   LL.ArchivedEvent{archivedEventEventId,
                                    archivedEventContractId,
                                    archivedEventTemplateId,
                                    archivedEventWitnessParties})) -> do
        eid <- raiseEventId archivedEventEventId
        cid <- raiseContractId archivedEventContractId
        tid <- perhaps "archivedEventTemplateId" archivedEventTemplateId >>= raiseTemplateId
        witness <- raiseList raiseParty archivedEventWitnessParties
        return ArchivedEvent{eid,cid,tid,witness}
    LL.Event(Just (LL.EventEventCreated
                   LL.CreatedEvent{createdEventEventId,
                                   createdEventContractId,
                                   createdEventTemplateId,
                                   createdEventContractKey,
                                   createdEventCreateArguments,
                                   createdEventWitnessParties})) -> do
        eid <- raiseEventId createdEventEventId
        cid <- raiseContractId createdEventContractId
        tid <- perhaps "createdEventTemplateId" createdEventTemplateId >>= raiseTemplateId
        let key = createdEventContractKey >>= optional . raiseValue
        createArgs <- perhaps "createdEventCreateArguments" createdEventCreateArguments >>= raiseRecord
        witness <- raiseList raiseParty createdEventWitnessParties
        return CreatedEvent{eid,cid,tid,key,createArgs,witness}

raiseRecord :: LL.Record -> Perhaps Record
raiseRecord = \case
    LL.Record{recordRecordId,recordFields} -> do
        let rid = recordRecordId >>= optional . raiseIdentifier
        fields <- raiseList raiseRecordField recordFields
        return Record{rid,fields}

raiseRecordField :: LL.RecordField -> Perhaps RecordField
raiseRecordField = \case
    LL.RecordField{recordFieldLabel,recordFieldValue} -> do
        let label = recordFieldLabel
        fieldValue <- perhaps "recordFieldValue" recordFieldValue >>= raiseValue
        return RecordField{label,fieldValue}

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
    LL.Identifier{identifierPackageId,
                  --identifierName, --marked as deprecated in value.proto
                  identifierModuleName,
                  identifierEntityName} -> do
        pid <- raisePackageId identifierPackageId
        mod <- raiseModuleName identifierModuleName
        ent <- raiseEntityName identifierEntityName
        return Identifier{pid,mod,ent}

raiseList :: (a -> Perhaps b) -> Vector a -> Perhaps [b]
raiseList f v = loop (Vector.toList v)
    where loop = \case
              [] -> return []
              x:xs -> do y <- f x; ys <- loop xs; return $ y:ys

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
