-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Cli.Damlc.IdeState
    ( IdeState
    , getDamlIdeState
    , withDamlIdeState
    ) where

import qualified Language.Haskell.LSP.Messages as LSP

import DA.Daml.GHC.Compiler.Options
import DA.Daml.GHC.Compiler.Options.Types
import qualified DA.Service.Logger as Logger
import qualified DA.Service.Daml.Compiler.Impl.Scenario as Scenario
import Development.IDE.Core.Rules.Daml
import Development.IDE.Core.API
import qualified Development.IDE.Types.Logger as IdeLogger

getDamlIdeState
    :: Options
    -> Maybe Scenario.Handle
    -> Logger.Handle IO
    -> (LSP.FromServerMessage -> IO ())
    -> VFSHandle
    -> IO IdeState
getDamlIdeState compilerOpts mbScenarioService loggerH eventHandler vfs = do
    let rule = mainRule compilerOpts
    damlEnv <- mkDamlEnv compilerOpts mbScenarioService
    initialise rule eventHandler (toIdeLogger loggerH) damlEnv (toCompileOpts compilerOpts) vfs

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
        ideState <- getDamlIdeState opts mbScenarioService loggerH eventHandler vfs
        f ideState

-- | Adapter to the IDE logger module.
toIdeLogger :: Logger.Handle IO -> IdeLogger.Logger
toIdeLogger h = IdeLogger.Logger $ \case
    IdeLogger.Error -> Logger.logError h
    IdeLogger.Warning -> Logger.logWarning h
    IdeLogger.Info -> Logger.logInfo h
    IdeLogger.Debug -> Logger.logDebug h
