-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module Development.IDE.Core.IdeState.Daml
    ( IdeState
    , getDamlIdeState
    , withDamlIdeState
    , enabledPlugins
    , toIdeLogger
    ) where

import Control.Exception
import DA.Daml.LF.ScenarioServiceClient qualified as Scenario
import DA.Daml.Options
import DA.Daml.Options.Types
import DA.Service.Logger qualified as Logger
import Development.IDE.Core.API
import Development.IDE.Core.Debouncer
import Development.IDE.Core.Rules.Daml
import Development.IDE.Core.Shake
import Development.IDE.Plugin
import Development.IDE.Plugin.CodeAction as CodeAction
import Development.IDE.Plugin.Completions as Completions
import Development.IDE.Types.Logger qualified as IdeLogger
import Language.LSP.Types qualified as LSP

getDamlIdeState
    :: Options
    -> StudioAutorunAllScenarios
    -> Maybe Scenario.Handle
    -> Logger.Handle IO
    -> Debouncer LSP.NormalizedUri
    -> ShakeLspEnv
    -> VFSHandle
    -> IO IdeState
getDamlIdeState compilerOpts autorunAllScenarios mbScenarioService loggerH debouncer lspEnv vfs = do
    let rule = mainRule compilerOpts <> pluginRules enabledPlugins
    damlEnv <- mkDamlEnv compilerOpts autorunAllScenarios mbScenarioService
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
            (getDamlIdeState opts (StudioAutorunAllScenarios True) mbScenarioService loggerH noopDebouncer (DummyLspEnv eventHandler) vfs)
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
