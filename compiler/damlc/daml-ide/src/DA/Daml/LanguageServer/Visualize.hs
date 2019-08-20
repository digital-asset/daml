-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Execute Daml commands

module DA.Daml.LanguageServer.Visualize
    ( setCommandHandler
    ) where

import qualified Data.Aeson as Aeson
import           Language.Haskell.LSP.Types
import           Development.IDE.LSP.Server

import Development.IDE.Types.Logger

import qualified Data.Text as T

import Development.IDE.Core.Rules
import Development.IDE.Core.Service.Daml

import Development.IDE.Core.Shake
import Development.IDE.Core.RuleTypes.Daml

import Language.Haskell.LSP.Messages
import qualified Language.Haskell.LSP.Core as LSP
import Development.IDE.Types.Location

collectTexts :: List Aeson.Value -> Maybe NormalizedFilePath
collectTexts (List [Aeson.String file])  = Just (toNormalizedFilePath (T.unpack file))
collectTexts _= Nothing

onCommand
    :: IdeState
    -> ExecuteCommandParams
    -> IO Aeson.Value
onCommand ide execParsms = case execParsms of
    ExecuteCommandParams "daml/damlVisualize" (Just _arguments) -> do
        case collectTexts _arguments of
            Just mod -> do
                    logInfo (ideLogger ide) "Generating visualization for current daml project"
                    Just dots <- runAction ide (use GenerateVisualization mod)
                    return $ Aeson.String dots
            Nothing     -> do
                logError (ideLogger ide) "Expected a single module to visualize, got multiple module"
                return $ Aeson.String "Expected a single module to visualize, got multiple module"
    ExecuteCommandParams  _ (Just _arguments) -> do
        logError (ideLogger ide) "Command is not supported"
        return Aeson.Null
    ExecuteCommandParams  _ Nothing -> do
        logError (ideLogger ide) "Missing DAML module to visualize"
        return Aeson.Null

setCommandHandler ::PartialHandlers
setCommandHandler = PartialHandlers $ \WithMessage{..} x -> return x {
    LSP.executeCommandHandler = withResponse RspExecuteCommand $ const onCommand
}