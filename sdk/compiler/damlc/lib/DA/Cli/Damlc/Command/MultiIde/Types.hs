-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE GADTs #-}

module DA.Cli.Damlc.Command.MultiIde.Types (
  module DA.Cli.Damlc.Command.MultiIde.Types
) where

import Control.Concurrent.Async (Async)
import Control.Concurrent.STM.TChan
import Control.Concurrent.STM.TVar
import Control.Concurrent.STM.TMVar
import Control.Concurrent.MVar
import Control.Exception (SomeException)
import Control.Monad (void)
import Control.Monad.STM
import DA.Daml.Project.Types (PackagePath (..), UnresolvedReleaseVersion, unresolvedReleaseVersionToString)
import DA.Daml.Resolution.Config (PackageResolutionData (..), ResolutionData (..), getResolutionData)
import Data.Aeson
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BSL
import Data.Function (on)
import qualified Data.IxMap as IM
import qualified Data.Map as Map
import Data.Maybe (fromMaybe, isNothing, listToMaybe)
import qualified Data.Set as Set
import qualified Data.Text as T
import Data.Time.Clock (NominalDiffTime, UTCTime, diffUTCTime)
import qualified Language.LSP.Types as LSP
import System.IO.Extra
import System.Process.Typed (Process)
import qualified DA.Service.Logger as Logger
import qualified DA.Service.Logger.Impl.IO as Logger

newtype PackageHome = PackageHome {unPackageHome :: FilePath} deriving (Show, Eq, Ord)

toPackagePath :: PackageHome -> PackagePath
toPackagePath (PackageHome path) = PackagePath path

newtype DarFile = DarFile {unDarFile :: FilePath} deriving (Show, Eq, Ord)
newtype DamlFile = DamlFile {unDamlFile :: FilePath} deriving (Show, Eq, Ord)

newtype UnitId = UnitId {unUnitId :: String} deriving (Show, Eq, Ord)

data TrackedMethod (m :: LSP.Method from 'LSP.Request) where
  TrackedSingleMethodFromClient
    :: forall (m :: LSP.Method 'LSP.FromClient 'LSP.Request)
    .  LSP.SMethod m
    -> LSP.FromClientMessage -- | Store the whole message for re-transmission on subIde restart
    -> PackageHome -- | Store the recipient subIde for this message
    -> TrackedMethod m
  TrackedSingleMethodFromServer
    :: forall (m :: LSP.Method 'LSP.FromServer 'LSP.Request)
    .  LSP.SMethod m
    -> Maybe PackageHome -- | Store the IDE that sent the request (or don't, for requests sent by the coordinator)
    -> TrackedMethod m
  TrackedAllMethod :: forall (m :: LSP.Method 'LSP.FromClient 'LSP.Request).
    { tamMethod :: LSP.SMethod m
        -- ^ The method of the initial request
    , tamLspId :: LSP.LspId m
    , tamClientMessage :: LSP.FromClientMessage
        -- ^ Store the whole message for re-transmission on subIde restart
    , tamCombiner :: ResponseCombiner m
        -- ^ How to combine the results from each IDE
    , tamRemainingResponsePackageHomes :: [PackageHome]
        -- ^ The IDES that have not yet replied to this message
    , tamResponses :: [(PackageHome, Either LSP.ResponseError (LSP.ResponseResult m))]
    } -> TrackedMethod m

tmMethod
  :: forall (from :: LSP.From) (m :: LSP.Method from 'LSP.Request)
  .  TrackedMethod m
  -> LSP.SMethod m
tmMethod (TrackedSingleMethodFromClient m _ _) = m
tmMethod (TrackedSingleMethodFromServer m _) = m
tmMethod (TrackedAllMethod {tamMethod}) = tamMethod

tmClientMessage
  :: forall (m :: LSP.Method 'LSP.FromClient 'LSP.Request)
  .  TrackedMethod m
  -> LSP.FromClientMessage
tmClientMessage (TrackedSingleMethodFromClient _ msg _) = msg
tmClientMessage (TrackedAllMethod {tamClientMessage}) = tamClientMessage

type MethodTracker (from :: LSP.From) = IM.IxMap @(LSP.Method from 'LSP.Request) LSP.LspId TrackedMethod
type MethodTrackerVar (from :: LSP.From) = TVar (MethodTracker from)

data SubIdeInstance = SubIdeInstance
  { ideInhandleAsync :: Async ()
  , ideInHandle :: Handle
  , ideInHandleChannel :: TChan BSL.ByteString
  , ideOutHandle :: Handle
  , ideOutHandleAsync :: Async ()
    -- ^ For sending messages to that SubIde
  , ideErrHandle :: Handle
  , ideErrText :: TVar T.Text
  , ideErrTextAsync :: Async ()
  , ideProcess :: Process Handle Handle Handle
  , ideHome :: PackageHome
  , ideMessageIdPrefix :: T.Text
    -- ^ Some unique string used to prefix message ids created by the SubIde, to avoid collisions with other SubIdes
    -- We use the stringified process ID
    -- TODO[SW]: This isn't strictly safe since this data exists for a short time after subIde shutdown, duplicates could be created.
  , ideUnitId :: UnitId
    -- ^ Unit ID of the package this SubIde handles
    -- Of the form "daml-script-0.0.1"
  , ideMResolutionPath :: Maybe FilePath
    -- ^ When using DPM, a resolution file is created for each process
    -- this points to the file so it can be removed in cleanup
  }

instance Eq SubIdeInstance where
  -- ideMessageIdPrefix is derived from process id, so this equality is of the process.
  (==) = (==) `on` ideMessageIdPrefix

instance Ord SubIdeInstance where
  -- ideMessageIdPrefix is derived from process id, so this ordering is of the process.
  compare = compare `on` ideMessageIdPrefix

-- When the IDE is disabled, it must have a diagnostic saying why
data IdeDataDisabled
  = IdeDataNotDisabled
  | IdeDataDisabled
      { iddSeverity :: LSP.DiagnosticSeverity
      , iddMessage :: T.Text
      }
  deriving (Show, Eq)

-- We store an optional main ide, the currently closing ides (kept only so they can reply to their shutdowns), and open files
-- open files must outlive the main subide so we can re-send the TextDocumentDidOpen messages on new ide startup
data SubIdeData = SubIdeData
  { ideDataHome :: PackageHome
  , ideDataMain :: Maybe SubIdeInstance
  , ideDataClosing :: Set.Set SubIdeInstance
  , ideDataOpenFiles :: Set.Set DamlFile
  , ideDataOpenDamlYaml :: Bool
  , ideDataIsFullDamlYaml :: Bool
    -- ^ We support daml.yaml files with only an sdk version. These should not be considered as packages
    -- however, we still want to track data about these, as file changes can promote/demote them to/from packages
    -- Thus we store this flag, rather than omitting from subIdes
  , ideDataFailures :: [(UTCTime, T.Text)]
  , ideDataDisabled :: IdeDataDisabled
  , ideDataUsingLocalComponents :: Bool
    -- ^ See psUsingLocalComponents
  }

defaultSubIdeData :: PackageHome -> SubIdeData
defaultSubIdeData home = SubIdeData home Nothing Set.empty Set.empty False False [] IdeDataNotDisabled False

lookupSubIde :: PackageHome -> SubIdes -> SubIdeData
lookupSubIde home ides = fromMaybe (defaultSubIdeData home) $ Map.lookup home ides

ideShouldDisableTimeout :: NominalDiffTime
ideShouldDisableTimeout = 5

ideShouldDisable :: SubIdeData -> Bool
ideShouldDisable (ideDataFailures -> ((t1, _):(t2, _):_)) = t1 `diffUTCTime` t2 < ideShouldDisableTimeout
ideShouldDisable _ = False

ideIsDisabled :: SubIdeData -> Bool
ideIsDisabled (ideDataDisabled -> IdeDataDisabled {}) = True
ideIsDisabled _ = False

ideGetLastFailureMessage :: SubIdeData -> Maybe T.Text
ideGetLastFailureMessage = fmap snd . listToMaybe . ideDataFailures

-- SubIdes placed in a TMVar. The emptyness representents a modification lock.
-- The lock unsures the following properties:
--   If multiple messages are sent to a new IDE at the same time, the first will create and hold a lock, while the rest wait on that lock (avoid multiple create)
--   We never attempt to send messages on a stale IDE. If we ever read SubIdesVar with the intent to send a message on a SubIde, we must hold the so a shutdown
--     cannot be sent on that IDE until we are done. This ensures that when a shutdown does occur, it is impossible for non-shutdown messages to be added to the
--     queue after the shutdown.
type SubIdes = Map.Map PackageHome SubIdeData
type SubIdesVar = TMVar SubIdes

-- Helper functions for holding the subIdes var
withIDEsAtomic :: MultiIdeState -> (SubIdes -> STM (SubIdes, a)) -> IO a
withIDEsAtomic miState f = atomically $ do
  ides <- takeTMVar $ misSubIdesVar miState
  (ides', res) <- f ides
  putTMVar (misSubIdesVar miState) ides'
  pure res

holdingIDEsAtomic :: MultiIdeState -> (SubIdes -> STM a) -> IO a
holdingIDEsAtomic miState f = withIDEsAtomic miState $ \ides -> (ides,) <$> f ides

withIDEsAtomic_ :: MultiIdeState -> (SubIdes -> STM SubIdes) -> IO ()
withIDEsAtomic_ miState f = void $ withIDEsAtomic miState $ fmap (, ()) . f

withIDEs :: MultiIdeState -> (SubIdes -> IO (SubIdes, a)) -> IO a
withIDEs miState f = do
  ides <- atomically $ takeTMVar $ misSubIdesVar miState
  (ides', res) <- f ides
  atomically $ putTMVar (misSubIdesVar miState) ides'
  pure res

holdingIDEs :: MultiIdeState -> (SubIdes -> IO a) -> IO a
holdingIDEs miState f = withIDEs miState $ \ides -> (ides,) <$> f ides

withIDEs_ :: MultiIdeState -> (SubIdes -> IO SubIdes) -> IO ()
withIDEs_ miState f = void $ withIDEs miState $ fmap (, ()) . f

-- Stores the initialize messages sent by the client to be forwarded to SubIdes when they are created.
type InitParams = LSP.InitializeParams
type InitParamsVar = MVar InitParams

-- Maps a packages unit id to its source location, using PackageOnDisk for all packages in multi-package.yaml
-- and PackageInDar for all known dars (currently extracted from data-dependencies)
data PackageSourceLocation = PackageOnDisk PackageHome | PackageInDar DarFile deriving Show
type MultiPackageYamlMapping = Map.Map UnitId PackageSourceLocation
type MultiPackageYamlMappingVar = TMVar MultiPackageYamlMapping

-- Maps a dar path to the list of packages that directly depend on it
type DarDependentPackages = Map.Map DarFile (Set.Set PackageHome)
type DarDependentPackagesVar = TMVar DarDependentPackages

-- "Cache" for the home path of files/directories
-- Cleared on daml.yaml modification and file deletion
type SourceFileHomes = Map.Map FilePath PackageHome
type SourceFileHomesVar = TMVar SourceFileHomes

-- Takes unblock messages IO, subIde itself and message bytestring
-- Extracted to types to resolve cycles in dependencies
type SubIdeMessageHandler = IO () -> SubIdeInstance -> B.ByteString -> IO ()

-- Used to extract the unsafeAddNewSubIdeAndSend function to resolve dependency cycles
type UnsafeAddNewSubIdeAndSend = SubIdes -> PackageHome -> Maybe LSP.FromClientMessage -> IO SubIdes

-- Used to extract the rebootIdeByHome function to resolve dependency cycles
type RebootIdeByHome = PackageHome -> IO ()

type GlobalErrorsVar = MVar GlobalErrors

-- Handles errors that prevent environments from starting
-- See ClientCommunication.hs for more details
data GlobalErrors = GlobalErrors
  { geUpdatePackageError :: Maybe String
  , geResolutionError :: Maybe String
  , gePendingHomes :: Set.Set PackageHome
  }
  deriving Eq

instance Show GlobalErrors where
  show (GlobalErrors pkgError resError _) = maybe "" renderPkgError pkgError <> maybe "" renderResError resError
    where
      renderPkgError err = "Failed to read multi-package.yaml or DARs:\n  " <> err <> "\n"
      renderResError err = "Failed to resolve SDK versions via DPM:\n  " <> err <> "\n"

emptyGlobalErrors :: GlobalErrors
emptyGlobalErrors = GlobalErrors Nothing Nothing Set.empty

data SdkVersionData = SdkVersionData
  { svdVersion :: UnresolvedReleaseVersion
  , svdOverrides :: Map.Map T.Text (Maybe T.Text)
    -- ^ Overrides for checking equality with DPM
    -- Map from component name to maybe component version (Nothing -> local path)
  }
  deriving (Ord, Eq, Show)

-- Shows SDK version with the number of overrides, rather than overrides themselves, to reduce clutter
renderSdkVersionData :: SdkVersionData -> T.Text
renderSdkVersionData (SdkVersionData ver overrides) | Map.null overrides = T.pack $ unresolvedReleaseVersionToString ver
renderSdkVersionData (SdkVersionData ver overrides) = T.pack $ unresolvedReleaseVersionToString ver <> " (with " <> show (Map.size overrides) <> " override(s))"

-- Whether an SDK version uses any local-path overrides, which would mean such an environment should be restarted
-- more often
sdkVersionDataUsingLocalOverrides :: SdkVersionData -> Bool
sdkVersionDataUsingLocalOverrides (SdkVersionData _ overrides) = any isNothing overrides

data SdkInstallData = SdkInstallData
  { sidVersionData :: SdkVersionData
  , sidPendingHomes :: Set.Set PackageHome
  , sidStatus :: SdkInstallStatus
  }
  deriving (Show, Eq)

data SdkInstallStatus
  = SISCanAsk
  | SISAsking
  | SISInstalling (Async ())
  | SISDenied
  | SISFailed T.Text (Maybe SomeException)

instance Eq SdkInstallStatus where
  SISCanAsk == SISCanAsk = True
  SISAsking == SISAsking = True
  (SISInstalling thread1) == (SISInstalling thread2) = thread1 == thread2
  SISDenied == SISDenied = True
  (SISFailed _ _) == (SISFailed _ _) = True
  _ == _ = False

instance Show SdkInstallStatus where
  show SISCanAsk = "SISCanAsk"
  show SISAsking = "SISAsking"
  show (SISInstalling _) = "SISInstalling"
  show SISDenied = "SISDenied"
  show (SISFailed log err) = "SISFailed (" <> show log <> ") (" <> show err <> ")"

type SdkInstallDatas = Map.Map SdkVersionData SdkInstallData
type SdkInstallDatasVar = TMVar SdkInstallDatas

getSdkInstallData :: SdkVersionData -> SdkInstallDatas -> SdkInstallData
getSdkInstallData ver = fromMaybe (SdkInstallData ver mempty SISCanAsk) . Map.lookup ver

data DamlSdkInstallProgressNotificationKind
  = InstallProgressBegin
  | InstallProgressReport
  | InstallProgressEnd

data DamlSdkInstallProgressNotification = DamlSdkInstallProgressNotification
  { -- Identifier like `3.2.0-<overrides-hash>`
    sipSdkVersionIdentifier :: T.Text
  , -- Rendered version like `3.2.0 (with 3 overrides)`
    sipSdkVersionRendered :: T.Text
  , sipKind :: DamlSdkInstallProgressNotificationKind
  , sipProgress :: Int
  }

instance ToJSON DamlSdkInstallProgressNotification where
  toJSON (DamlSdkInstallProgressNotification {..}) = object
    [ "sdkVersionIdentifier" .= sipSdkVersionIdentifier
    , "sdkVersionRendered" .= sipSdkVersionRendered
    , "kind" .= case sipKind of
        InstallProgressBegin -> "begin" :: T.Text
        InstallProgressReport -> "report"
        InstallProgressEnd -> "end"
    , "progress" .= sipProgress
    ]

damlSdkInstallProgressMethod :: T.Text
damlSdkInstallProgressMethod = "daml/sdkInstallProgress"

newtype DamlSdkInstallCancelNotification = DamlSdkInstallCancelNotification
  { sicSdkVersionIdentifier :: T.Text
  }

instance FromJSON DamlSdkInstallCancelNotification where
  parseJSON = withObject "DamlSdkInstallCancelNotification" $ \v ->
    DamlSdkInstallCancelNotification <$> v .: "sdkVersionidentifier"

damlSdkInstallCancelMethod :: T.Text
damlSdkInstallCancelMethod = "daml/sdkInstallCancel"

-- Alternate form of ResolutionData that uses Multi-Ide newtypes, and separates main from orphan package resolutions
data MultiIdeResolutionData = MultiIdeResolutionData
  { mainPackages :: Map.Map PackageHome PackageResolutionData
  , orphanPackages :: Map.Map PackageHome PackageResolutionData
  }
  deriving (Show, Eq)
type MultiIdeResolutionDataVar = MVar MultiIdeResolutionData

toMultiIdeResolutionData :: ResolutionData -> MultiIdeResolutionData
toMultiIdeResolutionData (ResolutionData mapping) = MultiIdeResolutionData (Map.mapKeys PackageHome mapping) mempty

data MultiIdeState = MultiIdeState
  { misFromClientMethodTrackerVar :: MethodTrackerVar 'LSP.FromClient
    -- ^ The client will track its own IDs to ensure they're unique, so no worries about collisions
  , misFromServerMethodTrackerVar :: MethodTrackerVar 'LSP.FromServer
    -- ^ We will prefix LspIds before they get here based on their SubIde messageIdPrefix, to avoid collisions
  , misSubIdesVar :: SubIdesVar
  , misInitParamsVar :: InitParamsVar
  , misToClientChan :: TChan BSL.ByteString
  , misMultiPackageMappingVar :: MultiPackageYamlMappingVar
  , misDarDependentPackagesVar :: DarDependentPackagesVar
  , misLogger :: Logger.Handle IO
  , misMultiPackageHome :: FilePath
  , misDefaultPackagePath :: PackageHome
  , misSourceFileHomesVar :: SourceFileHomesVar
  , misSubIdeArgs :: [String]
  , misSubIdeMessageHandler :: SubIdeMessageHandler
  , misUnsafeAddNewSubIdeAndSend :: UnsafeAddNewSubIdeAndSend
  , misRebootIdeByHome :: RebootIdeByHome
  , misSdkInstallDatasVar :: SdkInstallDatasVar
  , misIdentifier :: Maybe T.Text
  , misResolutionData :: MultiIdeResolutionDataVar
  , misGlobalErrors :: GlobalErrorsVar
  }

logError :: MultiIdeState -> String -> IO ()
logError miState msg = Logger.logError (misLogger miState) (T.pack msg)

logWarning :: MultiIdeState -> String -> IO ()
logWarning miState msg = Logger.logWarning (misLogger miState) (T.pack msg)

logInfo :: MultiIdeState -> String -> IO ()
logInfo miState msg = Logger.logInfo (misLogger miState) (T.pack msg)

logDebug :: MultiIdeState -> String -> IO ()
logDebug miState msg = Logger.logDebug (misLogger miState) (T.pack msg)

newMultiIdeState
  :: FilePath
  -> PackageHome
  -> Logger.Priority
  -> Maybe T.Text
  -> [String]
  -> (MultiIdeState -> SubIdeMessageHandler)
  -> (MultiIdeState -> UnsafeAddNewSubIdeAndSend)
  -> (MultiIdeState -> RebootIdeByHome)
  -> IO MultiIdeState
newMultiIdeState misMultiPackageHome misDefaultPackagePath logThreshold misIdentifier misSubIdeArgs subIdeMessageHandler unsafeAddNewSubIdeAndSend rebootIdeByHome = do
  (misFromClientMethodTrackerVar :: MethodTrackerVar 'LSP.FromClient) <- newTVarIO IM.emptyIxMap
  (misFromServerMethodTrackerVar :: MethodTrackerVar 'LSP.FromServer) <- newTVarIO IM.emptyIxMap
  misSubIdesVar <- newTMVarIO @SubIdes mempty
  misInitParamsVar <- newEmptyMVar @InitParams
  misToClientChan <- atomically newTChan
  misMultiPackageMappingVar <- newTMVarIO @MultiPackageYamlMapping mempty
  misDarDependentPackagesVar <- newTMVarIO @DarDependentPackages mempty
  misSourceFileHomesVar <- newTMVarIO @SourceFileHomes mempty
  misLogger <- Logger.newStderrLogger logThreshold "Multi-IDE"
  misSdkInstallDatasVar <- newTMVarIO @SdkInstallDatas mempty
  mResolutionData <- getResolutionData
  misResolutionData <- maybe newEmptyMVar (newMVar . toMultiIdeResolutionData) mResolutionData
  misGlobalErrors <- newMVar emptyGlobalErrors
  let miState =
        MultiIdeState 
          { misSubIdeMessageHandler = subIdeMessageHandler miState
          , misUnsafeAddNewSubIdeAndSend = unsafeAddNewSubIdeAndSend miState
          , misRebootIdeByHome = rebootIdeByHome miState
          , ..
          }
  pure miState

-- Forwarding

{-
Types of behaviour we want:

Regularly handling by a single IDE - works for requests and notifications
  e.g. TextDocumentDidOpen
Ignore it
  e.g. Initialize
Forward a notification to all IDEs
  e.g. workspace folders changed, exit
Forward a request to all IDEs and somehow combine the result
  e.g.
    symbol lookup -> combine monoidically
    shutdown -> response is empty, so identity after all responses
  This is the hard one as we need some way to define the combination logic
    which will ideally wait for all IDEs to reply to the request and apply this function over the (possibly failing) result
  This mostly covers FromClient requests that we can't pull a filepath from

  Previously thought we would need this more, now we only really use it for shutdown - ensuring all SubIdes shutdown before replying.
  We'll keep it in though since we'll likely get more capabilities supported when we upgrade ghc/move to HLS
-}

-- TODO: Consider splitting this into one data type for request and one for notification
-- rather than reusing the Single constructor over both and restricting via types
data ForwardingBehaviour (m :: LSP.Method 'LSP.FromClient t) where
  Single
    :: forall t (m :: LSP.Method 'LSP.FromClient t)
    .  FilePath
    -> ForwardingBehaviour m
  AllRequest
    :: forall (m :: LSP.Method 'LSP.FromClient 'LSP.Request)
    .  ResponseCombiner m
    -> ForwardingBehaviour m
  AllNotification
    :: ForwardingBehaviour (m :: LSP.Method 'LSP.FromClient 'LSP.Notification)
  -- For the case where a File Path cannot be deduced because it is unrecognised (i.e. unknown schema)
  CannotForwardRequest
    :: forall t (m :: LSP.Method 'LSP.FromClient t)
    .  ForwardingBehaviour m

-- Akin to ClientNotOrReq tagged with ForwardingBehaviour, and CustomMethod realised to req/not
data Forwarding (m :: LSP.Method 'LSP.FromClient t) where
  ForwardRequest
    :: forall (m :: LSP.Method 'LSP.FromClient 'LSP.Request)
    .  LSP.RequestMessage m
    -> ForwardingBehaviour m
    -> Forwarding m
  ForwardNotification
    :: forall (m :: LSP.Method 'LSP.FromClient 'LSP.Notification)
    .  LSP.NotificationMessage m
    -> ForwardingBehaviour m
    -> Forwarding m
  ExplicitHandler
    :: (  (LSP.FromServerMessage -> IO ())
       -> (FilePath -> LSP.FromClientMessage -> IO ())
       -> IO ()
       )
    -> Forwarding (m :: LSP.Method 'LSP.FromClient t)

type ResponseCombiner (m :: LSP.Method 'LSP.FromClient 'LSP.Request) =
  [(PackageHome, Either LSP.ResponseError (LSP.ResponseResult m))] -> Either LSP.ResponseError (LSP.ResponseResult m)

data SMethodWithSender (m :: LSP.Method 'LSP.FromServer t) = SMethodWithSender
  { smsMethod :: LSP.SMethod m
  , smsSender :: Maybe PackageHome
  }

data PackageSummary = PackageSummary
  { psUnitId :: UnitId
  , psDeps :: [DarFile]
  , -- Includes overrides, necessary for install and knowing when an environment should be rebooted
    psSdkVersionData :: SdkVersionData
  }
