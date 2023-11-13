-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


-- | Gather code lenses like scenario execution for a Daml file.
module DA.Daml.LanguageServer.CodeLens
    ( plugin
    ) where

import Control.Monad.IO.Class
import DA.Daml.LF.Ast qualified as LF
import DA.Daml.UtilLF (sourceLocToRange)
import Data.Aeson qualified as Aeson
import Data.Foldable
import Data.Text qualified as T
import Development.IDE.Core.PositionMapping
import Development.IDE.Core.RuleTypes.Daml
import Development.IDE.Core.Rules.Daml
import Development.IDE.Core.Service.Daml
import Development.IDE.Core.Shake
import Development.IDE.Plugin
import Development.IDE.Types.Location
import Development.IDE.Types.Logger
import Language.LSP.Server qualified as LSP
import Language.LSP.Types

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
                      [ virtualResourceToCodeLens (range, kind, name, vr)
                      | (kind, (valRef, Just loc)) <- map (Scenario,) (scenariosInModule mod) ++ map (Script,) (scriptsInModule mod)
                      , let name = LF.unExprValName (LF.qualObject valRef)
                      , let vr = VRScenario filePath name
                      , Just range <- [toCurrentRange mapping $ sourceLocToRange loc]
                      ]
        Nothing -> pure []

    pure $ List $ toList mbResult

data Kind = Scenario | Script
  deriving Show

-- | Convert a compiler virtual resource into a code lens.
virtualResourceToCodeLens
    :: (Range, Kind, T.Text, VirtualResource)
    -> CodeLens
virtualResourceToCodeLens (range, kind, title, vr) =
 CodeLens
    { _range = range
    , _command = Just $ Command
        (T.pack (show kind) <> " results")
        "daml.showResource"
        (Just $ List
              [ Aeson.String $ T.pack (show kind) <> ": " <> title
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
