-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import qualified DA.Daml.LF.ScriptServiceClient as SS
import DA.Pretty (PrettyLevel)

data DamlEnv = DamlEnv
  { envScriptService :: Maybe SS.Handle
  , envOpenVirtualResources :: Var (HashSet VirtualResource)
  , envScriptContexts :: MVar (HashMap NormalizedFilePath SS.ContextId)
  -- ^ This is a map from the file for which the context was created to
  -- the context id. We use this to track which script contexts
  -- are active so that we can GC inactive scripts.
  -- This should eventually go away and we should track script contexts
  -- in the same way that we track diagnostics.
  , envPreviousScriptContexts :: MVar [SS.ContextId]
  -- ^ The script contexts we used as GC roots in the last iteration.
  -- This is used to avoid unnecessary GC calls.
  , envDamlLfVersion :: LF.Version
  , envSkipScriptValidation :: SkipScriptValidation
  , envEnableScripts :: EnableScripts
  , envEnableInterfaces :: EnableInterfaces
  , envStudioAutorunAllScripts :: StudioAutorunAllScripts
  , envTestFilter :: T.Text -> Bool
  , envDetailLevel :: PrettyLevel
  }

instance IsIdeGlobal DamlEnv

mkDamlEnv :: Options -> StudioAutorunAllScripts -> Maybe SS.Handle -> IO DamlEnv
mkDamlEnv opts autorunAllScripts scriptService = do
    openVRsVar <- newVar HashSet.empty
    scriptContextsVar <- newMVar HashMap.empty
    previousScriptContextsVar <- newMVar []
    pure DamlEnv
        { envScriptService = scriptService
        , envOpenVirtualResources = openVRsVar
        , envScriptContexts = scriptContextsVar
        , envPreviousScriptContexts = previousScriptContextsVar
        , envDamlLfVersion = optDamlLfVersion opts
        , envSkipScriptValidation = optSkipScriptValidation opts
        , envEnableScripts = optEnableScripts opts
        , envEnableInterfaces = optEnableInterfaces opts
        , envStudioAutorunAllScripts = autorunAllScripts
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
