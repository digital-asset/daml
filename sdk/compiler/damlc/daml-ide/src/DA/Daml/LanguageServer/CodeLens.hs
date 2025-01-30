-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


-- | Gather code lenses like scenario execution for a Daml file.
module DA.Daml.LanguageServer.CodeLens
    ( plugin
    ) where

import Control.Monad.IO.Class
import qualified DA.Daml.LF.Ast as LF
import DA.Daml.UtilLF (sourceLocToRange)
import qualified Data.Aeson as Aeson
import Development.IDE.Core.Service.Daml
import Data.Foldable
import qualified Data.Text as T
import Development.IDE.Core.PositionMapping
import Development.IDE.Core.Rules.Daml
import Development.IDE.Core.RuleTypes.Daml
import Development.IDE.Core.Shake
import Development.IDE.Plugin
import Development.IDE.Types.Logger
import Development.IDE.Types.Location
import Language.LSP.Types
import qualified Language.LSP.Server as LSP

-- | Gather code lenses like scenario execution for a Daml file.
handle
    :: IdeState
    -> CodeLensParams
    -> LSP.LspM c (Either e (List CodeLens))
handle ide CodeLensParams{_textDocument=TextDocumentIdentifier uri} = liftIO $ Right <$> do
    mbResult <- case uriToFilePath' uri of
        Just (toNormalizedFilePath' -> filePath) -> do
          logInfo (ideLogger ide) $ "CodeLens request for file: " <> T.pack (fromNormalizedFilePath filePath)
          mbModMapping <- runAction ide $ useWithStale GenerateRawDalf filePath
          case mbModMapping of
              Nothing -> pure []
              Just (mod, mapping) ->
                  pure
                      [ virtualResourceToCodeLens (range, name, vr)
                      | (valRef, Just loc) <- scriptsInModule mod
                      , let name = LF.unExprValName (LF.qualObject valRef)
                      , let vr = VRScenario filePath name
                      , Just range <- [toCurrentRange mapping $ sourceLocToRange loc]
                      ]
        Nothing -> pure []

    pure $ List $ toList mbResult

-- | Convert a compiler virtual resource into a code lens.
virtualResourceToCodeLens
    :: (Range, T.Text, VirtualResource)
    -> CodeLens
virtualResourceToCodeLens (range, title, vr) =
 CodeLens
    { _range = range
    , _command = Just $ Command
        (T.pack ("Script results")
        "daml.showResource"
        (Just $ List
              [ Aeson.String $ "Script: " <> title
              , Aeson.String $ virtualResourceToUri vr])
    , _xdata = Nothing
    }

plugin :: Plugin c
plugin = Plugin
  { pluginRules = mempty
  , pluginCommands = mempty
  , pluginHandlers = pluginHandler STextDocumentCodeLens handle
  , pluginNotificationHandlers = mempty
  }
