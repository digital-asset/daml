-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Development.IDE.Core.IdeState.Daml
    ( IdeState
    , getDamlIdeState
    , withDamlIdeState
    , enabledPlugins
    ) where

import Data.Default
import qualified Language.Haskell.LSP.Messages as LSP

import Control.Exception
import DA.Daml.Options
import DA.Daml.Options.Types
import qualified DA.Service.Logger as Logger
import qualified DA.Daml.LF.ScenarioServiceClient as Scenario
import Development.IDE.Core.Debouncer
import Development.IDE.Core.API
import Development.IDE.Core.Rules.Daml
import Development.IDE.Plugin
import Development.IDE.Plugin.Completions as Completions
import Development.IDE.Plugin.CodeAction as CodeAction
import qualified Development.IDE.Types.Logger as IdeLogger
import Development.IDE.Types.Options
import qualified Language.Haskell.LSP.Types as LSP
import qualified Language.Haskell.LSP.Types.Capabilities as LSP

getDamlIdeState
    :: Options
    -> Maybe Scenario.Handle
    -> Logger.Handle IO
    -> Debouncer LSP.NormalizedUri
    -> LSP.ClientCapabilities
    -> IO LSP.LspId
    -> (LSP.FromServerMessage -> IO ())
    -> VFSHandle
    -> IdeReportProgress
    -> IO IdeState
getDamlIdeState compilerOpts mbScenarioService loggerH debouncer caps getLspId eventHandler vfs reportProgress = do
    let rule = mainRule compilerOpts <> pluginRules enabledPlugins
    damlEnv <- mkDamlEnv compilerOpts mbScenarioService
    initialise caps rule getLspId eventHandler (toIdeLogger loggerH) debouncer damlEnv (toCompileOpts compilerOpts reportProgress) vfs

enabledPlugins :: Plugin a
enabledPlugins = Completions.plugin <> CodeAction.plugin

-- Wrapper for the common case where the scenario service
-- will be started automatically (if enabled)
-- and we use the builtin VFSHandle. We always disable
-- the debouncer here since this is not used in the IDE.
withDamlIdeState
    :: Options
    -> Logger.Handle IO
    -> (LSP.FromServerMessage -> IO ())
    -> (IdeState -> IO a)
    -> IO a
withDamlIdeState opts@Options{..} loggerH eventHandler f = do
    scenarioServiceConfig <- Scenario.readScenarioServiceConfig
    Scenario.withScenarioService' optScenarioService loggerH scenarioServiceConfig $ \mbScenarioService -> do
        vfs <- makeVFSHandle
        -- We only use withDamlIdeState outside of the IDE where we do not care about
        -- progress reporting.
        bracket
            (getDamlIdeState opts mbScenarioService loggerH noopDebouncer def (pure $ LSP.IdInt 0) eventHandler vfs (IdeReportProgress False))
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
