-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LanguageServer.Visualize
    ( plugin
    ) where

import Control.Monad.IO.Class
import qualified Data.Aeson as Aeson
import Language.LSP.Types
import Development.IDE.Types.Logger
import qualified Data.Text as T
import Development.IDE.Plugin
import Development.IDE.Core.Rules
import Development.IDE.Core.Rules.Daml
import Development.IDE.Core.Service.Daml
import qualified Language.LSP.Server as LSP
import Development.IDE.Types.Location
import qualified DA.Daml.Visual as Visual

onCommand
    :: IdeState
    -> FilePath
    -> LSP.LspM c (Either ResponseError Aeson.Value)
onCommand ide (toNormalizedFilePath' -> mod) = do
    liftIO $ logInfo (ideLogger ide) "Generating visualization for current daml project"
    world <- liftIO $ runAction ide (worldForFile mod)
    let dots = T.pack $ Visual.dotFileGen world
    return $ Right $ Aeson.String dots

plugin :: Plugin c
plugin = Plugin
  { pluginRules = mempty
  , pluginCommands = [PluginCommand "daml/damlVisualize" onCommand]
  , pluginNotificationHandlers = mempty
  , pluginHandlers = mempty
  }
