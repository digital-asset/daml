-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module Development.IDE.Core.IdeState.Daml
    ( IdeState
    , getDamlIdeState
    , withDamlIdeState
    , enabledPlugins
    ) where

import Control.Exception
import DA.Daml.Options
import DA.Daml.Options.Types
import qualified DA.Service.Logger as Logger
import qualified DA.Daml.LF.ScenarioServiceClient as Scenario
import Development.IDE.Core.Debouncer
import Development.IDE.Core.API
import Development.IDE.Core.Rules.Daml
import Development.IDE.Core.Shake
import Development.IDE.Plugin
import Development.IDE.Plugin.Completions as Completions
import Development.IDE.Plugin.CodeAction as CodeAction
import qualified Development.IDE.Types.Logger as IdeLogger
import qualified Language.LSP.Types as LSP

getDamlIdeState
    :: Options
    -> Maybe Scenario.Handle
    -> Logger.Handle IO
    -> Debouncer LSP.NormalizedUri
    -> ShakeLspEnv
    -> VFSHandle
    -> IO IdeState
getDamlIdeState compilerOpts mbScenarioService loggerH debouncer lspEnv vfs = do
    let rule = mainRule compilerOpts <> pluginRules enabledPlugins
    damlEnv <- mkDamlEnv compilerOpts mbScenarioService
    initialise rule lspEnv (toIdeLogger loggerH) debouncer damlEnv (toCompileOpts compilerOpts) vfs

enabledPlugins :: Plugin a
enabledPlugins = Completions.plugin <> CodeAction.plugin

-- Wrapper for the common case where the scenario service
-- will be started automatically (if enabled)
-- and we use the builtin VFSHandle. We always disable
-- the debouncer here since this is not used in the IDE.
withDamlIdeState
    :: Options
    -> Logger.Handle IO
    -> NotificationHandler
    -> (IdeState -> IO a)
    -> IO a
withDamlIdeState opts@Options{..} loggerH eventHandler f = do
    scenarioServiceConfig <- Scenario.readScenarioServiceConfig
    Scenario.withScenarioService' optScenarioService optEnableScenarios optDamlLfVersion loggerH scenarioServiceConfig $ \mbScenarioService -> do
        vfs <- makeVFSHandle
        bracket
            (getDamlIdeState opts mbScenarioService loggerH noopDebouncer (DummyLspEnv eventHandler) vfs)
            shutdown
            f

-- | Adapter to the IDE logger module.
toIdeLogger :: Logger.Handle IO -> IdeLogger.Logger
toIdeLogger h = IdeLogger.Logger $ \case
    IdeLogger.Error -> Logger.logError h
    IdeLogger.Warning -> Logger.logWarning h
    IdeLogger.Info -> Logger.logInfo h
    IdeLogger.Debug -> Logger.logDebug h
    IdeLogger.Telemetry -> Logger.logTelemetry h
