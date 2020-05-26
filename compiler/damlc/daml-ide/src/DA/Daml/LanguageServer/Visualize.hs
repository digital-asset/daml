-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LanguageServer.Visualize
    ( setCommandHandler
    ) where

import qualified Data.Aeson as Aeson
import Language.Haskell.LSP.Types
import Development.IDE.LSP.Server
import Development.IDE.Types.Logger
import qualified Data.Text as T
import Development.IDE.Core.Rules
import Development.IDE.Core.Rules.Daml
import Development.IDE.Core.Service.Daml
import Language.Haskell.LSP.Messages
import qualified Language.Haskell.LSP.Core as LSP
import Development.IDE.Types.Location
import qualified DA.Daml.Visual as Visual

collectTexts :: List Aeson.Value -> Maybe NormalizedFilePath
collectTexts (List [Aeson.String file])  = Just (toNormalizedFilePath' (T.unpack file))
collectTexts _= Nothing

onCommand
    :: IdeState
    -> ExecuteCommandParams
    -> IO (Either ResponseError Aeson.Value)
onCommand ide execParsms = case execParsms of
    ExecuteCommandParams "daml/damlVisualize" (Just _arguments) _ -> do
        case collectTexts _arguments of
            Just mod -> do
                    logInfo (ideLogger ide) "Generating visualization for current daml project"
                    world <- runAction ide (worldForFile mod)
                    let dots = T.pack $ Visual.dotFileGen world
                    return $ Right $ Aeson.String dots
            Nothing     -> do
                logError (ideLogger ide) "Expected a single module to visualize, got multiple module"
                return $ Right $ Aeson.String "Expected a single module to visualize, got multiple module"
    ExecuteCommandParams command args _ -> do
        let err = T.pack ("Unsupported command " ++ show command ++ "with args " ++ show args)
        logError (ideLogger ide) err
        return $ Left (ResponseError InvalidParams err Nothing)

setCommandHandler ::PartialHandlers a
setCommandHandler = PartialHandlers $ \WithMessage{..} x -> return x {
    LSP.executeCommandHandler = withResponse RspExecuteCommand $ const onCommand
}
