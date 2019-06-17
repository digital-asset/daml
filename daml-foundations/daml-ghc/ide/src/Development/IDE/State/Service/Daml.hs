-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE OverloadedStrings #-}
module Development.IDE.State.Service.Daml(
    Env(..),
    getServiceEnv,
    DamlEnv(..),
    getDamlServiceEnv,
    IdeState, initialise, shutdown,
    runAction, runActions,
    runActionSynchronous, runActionsSynchronous,
    setFilesOfInterest, modifyFilesOfInterest, setOpenVirtualResources, modifyOpenVirtualResources,
    writeProfile,
    getDiagnostics, unsafeClearDiagnostics,
    logDebug, logSeriousError
    ) where

import Control.Concurrent.Extra
import Control.Monad
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Set (Set)
import qualified Data.Set as Set
import qualified Data.Text as T
import Data.Tuple.Extra
import Development.Shake

import qualified Development.IDE.Logger as Logger
import Development.IDE.State.Service hiding (initialise)
import Development.IDE.State.FileStore
import qualified Development.IDE.State.Service as IDE
import Development.IDE.State.Shake
import Development.IDE.Types.Diagnostics
import Development.IDE.Types.LSP
import qualified Language.Haskell.LSP.Messages as LSP

import DA.Daml.GHC.Compiler.Options
import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.ScenarioServiceClient as SS

data DamlEnv = DamlEnv
  { envScenarioService :: Maybe SS.Handle
  , envOpenVirtualResources :: Var (Set VirtualResource)
  , envScenarioContexts :: Var (Map NormalizedFilePath SS.ContextId)
  -- ^ This is a map from the file for which the context was created to
  -- the context id. We use this to track which scenario contexts
  -- are active so that we can GC inactive scenarios.
  -- This should eventually go away and we should track scenario contexts
  -- in the same way that we track diagnostics.
  , envPreviousScenarioContexts :: Var [SS.ContextId]
  -- ^ The scenario contexts we used as GC roots in the last iteration.
  -- This is used to avoid unnecessary GC calls.
  , envDamlLfVersion :: LF.Version
  , envScenarioValidation :: ScenarioValidation
  }

instance IsIdeGlobal DamlEnv

mkDamlEnv :: Options -> Maybe SS.Handle -> IO DamlEnv
mkDamlEnv opts scenarioService = do
    openVRsVar <- newVar Set.empty
    scenarioContextsVar <- newVar Map.empty
    previousScenarioContextsVar <- newVar []
    pure DamlEnv
        { envScenarioService = scenarioService
        , envOpenVirtualResources = openVRsVar
        , envScenarioContexts = scenarioContextsVar
        , envPreviousScenarioContexts = previousScenarioContextsVar
        , envDamlLfVersion = optDamlLfVersion opts
        , envScenarioValidation = optScenarioValidation opts
        }

getDamlServiceEnv :: Action DamlEnv
getDamlServiceEnv = getIdeGlobalAction

setOpenVirtualResources :: IdeState -> Set VirtualResource -> IO ()
setOpenVirtualResources state resources = modifyOpenVirtualResources state (const resources)

modifyOpenVirtualResources :: IdeState -> (Set VirtualResource -> Set VirtualResource) -> IO ()
modifyOpenVirtualResources state f = do
    DamlEnv{..} <- getIdeGlobalState state
    vrs <- modifyVar envOpenVirtualResources $ pure . dupe . f
    logDebug state $ "Set vrs of interest to: " <> T.pack (show $ Set.toList vrs)
    void $ shakeRun state [] (const $ pure ())

initialise
    :: Rules ()
    -> (LSP.FromServerMessage -> IO ())
    -> Logger.Handle
    -> Options
    -> VFSHandle
    -> Maybe SS.Handle
    -> IO IdeState
initialise mainRule toDiags logger options vfs scenarioService =
    IDE.initialise
        (do addIdeGlobal =<< liftIO (mkDamlEnv options scenarioService)
            mainRule)
        toDiags
        logger
        (toCompileOpts options)
        vfs
