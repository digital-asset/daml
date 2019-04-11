-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module Development.IDE.State.Service.Daml(
    Env(..),
    getServiceEnv,
    DamlEnv(..),
    getDamlServiceEnv,
    IdeState, initialise, shutdown,
    runAction, runActions,
    setFilesOfInterest, setOpenVirtualResources,
    writeProfile,
    getDiagnostics, unsafeClearDiagnostics,
    logDebug, logInfo, logWarning, logError
    ) where

import Control.Concurrent.Extra
import Control.Concurrent.STM
import Control.Monad
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Set (Set)
import qualified Data.Set as Set
import Development.Shake

import qualified Development.IDE.Logger as Logger
import Development.IDE.State.Service hiding (initialise)
import qualified Development.IDE.State.Service as IDE
import Development.IDE.State.Shake
import Development.IDE.Types.LSP

import DA.Daml.GHC.Compiler.Options
import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.ScenarioServiceClient as SS

data DamlEnv = DamlEnv
  { envScenarioService :: Maybe SS.Handle
  , envOpenVirtualResources :: Var (Set VirtualResource)
  , envScenarioContexts :: Var (Map FilePath SS.ContextId)
  -- ^ This is a map from the file for which the context was created to
  -- the context id. We use this to track which scenario contexts
  -- are active so that we can GC inactive scenarios.
  -- This should eventually go away and we should track scenario contexts
  -- in the same way that we track diagnostics.
  , envScenarioContextRoots :: Var (Map FilePath FilePath)
  -- ^ This is a map from a file A to a file B whose scenario context should be
  -- used for executing scenarios in A. We use this when running the scenarios
  -- in transitive dependencies of the files of interest so that we only need
  -- one scenario context per file of interest.
  , envPreviousScenarioContexts :: Var [SS.ContextId]
  -- ^ The scenario contexts we used as GC roots in the last iteration.
  -- This is used to avoid unnecessary GC calls.
  , envDamlLfVersion :: LF.Version
  }

instance IsIdeGlobal DamlEnv

mkDamlEnv :: Options -> Maybe SS.Handle -> IO DamlEnv
mkDamlEnv opts scenarioService = do
    openVRsVar <- newVar Set.empty
    scenarioContextsVar <- newVar Map.empty
    scenarioContextRootsVar <- newVar Map.empty
    previousScenarioContextsVar <- newVar []
    pure DamlEnv
        { envScenarioService = scenarioService
        , envOpenVirtualResources = openVRsVar
        , envScenarioContexts = scenarioContextsVar
        , envScenarioContextRoots = scenarioContextRootsVar
        , envPreviousScenarioContexts = previousScenarioContextsVar
        , envDamlLfVersion = optDamlLfVersion opts
        }

getDamlServiceEnv :: Action DamlEnv
getDamlServiceEnv = getIdeGlobalAction

setOpenVirtualResources :: IdeState -> Set VirtualResource -> IO ()
setOpenVirtualResources state resources = do
    DamlEnv{..} <- getIdeGlobalState state
    modifyVar_ envOpenVirtualResources $ const $ return resources
    void $ shakeRun state []

initialise :: Rules ()
           -> Maybe (Event -> STM ())
           -> Logger.Handle IO
           -> Options
           -> Maybe SS.Handle
           -> IO IdeState
initialise mainRule toDiags logger options scenarioService =
    IDE.initialise
        (do addIdeGlobal =<< liftIO (mkDamlEnv options scenarioService)
            mainRule)
        toDiags
        logger
        (toCompileOpts options)
