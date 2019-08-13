-- Copyright (c) 2019 The DAML Authors. All rights reserved.
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
import Control.DeepSeq
import GHC.Generics
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Set (Set)
import qualified Data.Set as Set
import qualified Data.Text as T
import Data.Tuple.Extra
import Development.Shake

import Development.IDE.Types.Logger
import Development.IDE.Core.Service hiding (initialise)
import Development.IDE.Core.FileStore
import qualified Development.IDE.Core.Service as IDE
import Development.IDE.Core.OfInterest
import Development.IDE.Core.Shake
import Development.IDE.Types.Location
import Development.IDE.Types.Options
import qualified Language.Haskell.LSP.Messages as LSP

import DA.Daml.Options.Types
import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.ScenarioServiceClient as SS

-- | Virtual resources
data VirtualResource = VRScenario
    { vrScenarioFile :: !NormalizedFilePath
    , vrScenarioName :: !T.Text
    } deriving (Eq, Ord, Show, Generic)
    -- ^ VRScenario identifies a scenario in a given file.
    -- This virtual resource is associated with the HTML result of
    -- interpreting the corresponding scenario.

instance NFData VirtualResource


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
    logDebug (ideLogger state) $ "Set vrs of interest to: " <> T.pack (show $ Set.toList vrs)
    void $ shakeRun state []

initialise
    :: Rules ()
    -> (LSP.FromServerMessage -> IO ())
    -> Logger
    -> DamlEnv
    -> IdeOptions
    -> VFSHandle
    -> IO IdeState
initialise mainRule toDiags logger damlEnv options vfs =
    IDE.initialise
        (do addIdeGlobal damlEnv
            mainRule)
        toDiags
        logger
        options
        vfs
