-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

-- Convert between HL Ledger.Types and the LL types generated from .proto files
module DA.Ledger.Convert (lowerCommands) where

import Data.Maybe (fromMaybe)
import Data.Vector as Vector (fromList)

import qualified DA.Ledger.LowLevel as LL
import DA.Ledger.Types

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
lowerValue = \case -- TODO: more cases here
    VRecord r -> LL.Value $ Just $ LL.ValueSumRecord $ lowerRecord r
    VVariant _ -> undefined
    VContract _ -> undefined
    VList vs -> LL.Value $ Just $ LL.ValueSumList $ LL.List $ Vector.fromList $ map lowerValue vs
    VInt i -> LL.Value $ Just $ LL.ValueSumInt64 $ fromIntegral i
    VDecimal s -> LL.Value $ Just $ LL.ValueSumDecimal s
    VString s -> LL.Value $ Just $ LL.ValueSumText s
    VTimestamp _ -> undefined
    VParty p -> LL.Value $ Just $ LL.ValueSumParty $ unParty p
    VBool _ -> undefined
    VUnit -> undefined
    VDate _ -> undefined
    VOpt _ -> undefined
    VMap _ -> undefined

lowerRecord :: Record -> LL.Record
lowerRecord = \case
    Record{rid,fields} ->
        LL.Record {
        recordRecordId = fmap lowerIdentifier rid,
        recordFields = Vector.fromList $ map lowerRecordField fields }

lowerRecordField :: RecordField -> LL.RecordField
lowerRecordField = \case
    RecordField{label,value} ->
        LL.RecordField {
        recordFieldLabel = label,
        recordFieldValue = Just (lowerValue value) }
