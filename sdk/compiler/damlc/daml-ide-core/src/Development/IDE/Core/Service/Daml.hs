-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Development.IDE.Core.Service.Daml(
    VirtualResource(..),
    DamlEnv(..),
    mkDamlEnv,
    getDamlServiceEnv,
    IdeState, initialise, shutdown,
    runAction,
    runActionSync,
    setFilesOfInterest, modifyFilesOfInterest, setOpenVirtualResources, modifyOpenVirtualResources,
    writeProfile,
    getDiagnostics, unsafeClearDiagnostics,
    ideLogger
    ) where

import Control.Concurrent.Extra
import Control.Monad
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HashMap
import Data.HashSet (HashSet)
import qualified Data.HashSet as HashSet
import qualified Data.Text as T
import Data.Tuple.Extra
import Development.Shake

import Development.IDE.Types.Logger
import Development.IDE.Core.Debouncer
import Development.IDE.Core.Service hiding (initialise)
import Development.IDE.Core.FileStore
import qualified Development.IDE.Core.Service as IDE
import Development.IDE.Core.OfInterest
import Development.IDE.Core.RuleTypes.Daml
import Development.IDE.Core.Shake
import Development.IDE.Types.Location
import Development.IDE.Types.Options

import DA.Daml.Options.Types
import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.ScenarioServiceClient as SS
import DA.Pretty (PrettyLevel)

data DamlEnv = DamlEnv
  { envScenarioService :: Maybe SS.Handle
  , envOpenVirtualResources :: Var (HashSet VirtualResource)
  , envScenarioContexts :: MVar (HashMap NormalizedFilePath SS.ContextId)
  -- ^ This is a map from the file for which the context was created to
  -- the context id. We use this to track which scenario contexts
  -- are active so that we can GC inactive scenarios.
  -- This should eventually go away and we should track scenario contexts
  -- in the same way that we track diagnostics.
  , envPreviousScenarioContexts :: MVar [SS.ContextId]
  -- ^ The scenario contexts we used as GC roots in the last iteration.
  -- This is used to avoid unnecessary GC calls.
  , envDamlLfVersion :: LF.Version
  , envSkipScenarioValidation :: SkipScenarioValidation
  , envEnableScenarios :: EnableScenarios
  , envAllowLargeTuples :: AllowLargeTuples
  , envStudioAutorunAllScenarios :: StudioAutorunAllScenarios
  , envTestFilter :: T.Text -> Bool
  , envDetailLevel :: PrettyLevel
  }

instance IsIdeGlobal DamlEnv

mkDamlEnv :: Options -> StudioAutorunAllScenarios -> Maybe SS.Handle -> IO DamlEnv
mkDamlEnv opts autorunAllScenarios scenarioService = do
    openVRsVar <- newVar HashSet.empty
    scenarioContextsVar <- newMVar HashMap.empty
    previousScenarioContextsVar <- newMVar []
    pure DamlEnv
        { envScenarioService = scenarioService
        , envOpenVirtualResources = openVRsVar
        , envScenarioContexts = scenarioContextsVar
        , envPreviousScenarioContexts = previousScenarioContextsVar
        , envDamlLfVersion = optDamlLfVersion opts
        , envSkipScenarioValidation = optSkipScenarioValidation opts
        , envEnableScenarios = optEnableScenarios opts
        , envAllowLargeTuples = optAllowLargeTuples opts
        , envStudioAutorunAllScenarios = autorunAllScenarios
        , envTestFilter = optTestFilter opts
        , envDetailLevel = optDetailLevel opts
        }

getDamlServiceEnv :: Action DamlEnv
getDamlServiceEnv = getIdeGlobalAction

setOpenVirtualResources :: IdeState -> HashSet VirtualResource -> IO ()
setOpenVirtualResources state resources = modifyOpenVirtualResources state (const resources)

modifyOpenVirtualResources :: IdeState -> (HashSet VirtualResource -> HashSet VirtualResource) -> IO ()
modifyOpenVirtualResources state f = do
    DamlEnv{..} <- getIdeGlobalState state
    vrs <- modifyVar envOpenVirtualResources $ pure . dupe . f
    logDebug (ideLogger state) $ "Set vrs of interest to: " <> T.pack (show $ HashSet.toList vrs)
    void $ shakeRun state []

initialise
    :: Rules ()
    -> ShakeLspEnv
    -> Logger
    -> Debouncer NormalizedUri
    -> DamlEnv
    -> IdeOptions
    -> VFSHandle
    -> IO IdeState
initialise mainRule lspEnv logger debouncer damlEnv options vfs =
    IDE.initialise
        (do addIdeGlobal damlEnv
            mainRule)
        lspEnv
        logger
        debouncer
        options
        vfs
