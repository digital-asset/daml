-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Cli.Damlc.IdeState
    ( IdeState
    , getDamlIdeState
    , withDamlIdeState
    ) where

import qualified Language.Haskell.LSP.Messages as LSP

import Control.Exception
import DA.Daml.Options
import DA.Daml.Options.Types
import qualified DA.Service.Logger as Logger
import qualified DA.Daml.Compiler.Scenario as Scenario
import Development.IDE.Core.Rules.Daml
import Development.IDE.Core.API
import qualified Development.IDE.Types.Logger as IdeLogger
import Development.IDE.Types.Options

getDamlIdeState
    :: Options
    -> Maybe Scenario.Handle
    -> Logger.Handle IO
    -> (LSP.FromServerMessage -> IO ())
    -> VFSHandle
    -> IdeReportProgress
    -> IO IdeState
getDamlIdeState compilerOpts mbScenarioService loggerH eventHandler vfs reportProgress = do
    let rule = mainRule compilerOpts
    damlEnv <- mkDamlEnv compilerOpts mbScenarioService
    initialise rule eventHandler (toIdeLogger loggerH) damlEnv (toCompileOpts compilerOpts reportProgress) vfs

-- Wrapper for the common case where the scenario service will be started automatically (if enabled)
-- and we use the builtin VFSHandle.
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
            (getDamlIdeState opts mbScenarioService loggerH eventHandler vfs (IdeReportProgress False))
            shutdown
            f

-- | Adapter to the IDE logger module.
toIdeLogger :: Logger.Handle IO -> IdeLogger.Logger
toIdeLogger h = IdeLogger.Logger $ \case
    IdeLogger.Error -> Logger.logError h
    IdeLogger.Warning -> Logger.logWarning h
    IdeLogger.Info -> Logger.logInfo h
    IdeLogger.Debug -> Logger.logDebug h
