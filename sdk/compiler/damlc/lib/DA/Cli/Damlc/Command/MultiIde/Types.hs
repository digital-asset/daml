-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import Control.Monad (void)
import Control.Monad.STM
import DA.Daml.Project.Types (ProjectPath (..), UnresolvedReleaseVersion)
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BSL
import Data.Function (on)
import qualified Data.IxMap as IM
import qualified Data.Map as Map
import Data.Maybe (fromMaybe)
import qualified Data.Set as Set
import qualified Data.Text as T
import Data.Time.Clock (NominalDiffTime, UTCTime, diffUTCTime)
import qualified Language.LSP.Types as LSP
import System.IO.Extra
import System.Process.Typed (Process)
import qualified DA.Service.Logger as Logger
import qualified DA.Service.Logger.Impl.IO as Logger

newtype PackageHome = PackageHome {unPackageHome :: FilePath} deriving (Show, Eq, Ord)

toProjectPath :: PackageHome -> ProjectPath
toProjectPath (PackageHome path) = ProjectPath path

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
  }

instance Eq SubIdeInstance where
  -- ideMessageIdPrefix is derived from process id, so this equality is of the process.
  (==) = (==) `on` ideMessageIdPrefix

instance Ord SubIdeInstance where
  -- ideMessageIdPrefix is derived from process id, so this ordering is of the process.
  compare = compare `on` ideMessageIdPrefix

-- We store an optional main ide, the currently closing ides (kept only so they can reply to their shutdowns), and open files
-- open files must outlive the main subide so we can re-send the TextDocumentDidOpen messages on new ide startup
data SubIdeData = SubIdeData
  { ideDataHome :: PackageHome
  , ideDataMain :: Maybe SubIdeInstance
  , ideDataClosing :: Set.Set SubIdeInstance
  , ideDataOpenFiles :: Set.Set DamlFile
  , ideDataFailTimes :: [UTCTime]
  , ideDataDisabled :: Bool
  , ideDataLastError :: Maybe String
  }

defaultSubIdeData :: PackageHome -> SubIdeData
defaultSubIdeData home = SubIdeData home Nothing Set.empty Set.empty [] False Nothing

lookupSubIde :: PackageHome -> SubIdes -> SubIdeData
lookupSubIde home ides = fromMaybe (defaultSubIdeData home) $ Map.lookup home ides

ideShouldDisableTimeout :: NominalDiffTime
ideShouldDisableTimeout = 5

ideShouldDisable :: SubIdeData -> Bool
ideShouldDisable (ideDataFailTimes -> (t1:t2:_)) = t1 `diffUTCTime` t2 < ideShouldDisableTimeout
ideShouldDisable _ = False

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
  }

logError :: MultiIdeState -> String -> IO ()
logError miState msg = Logger.logError (misLogger miState) (T.pack msg)

logWarning :: MultiIdeState -> String -> IO ()
logWarning miState msg = Logger.logWarning (misLogger miState) (T.pack msg)

logInfo :: MultiIdeState -> String -> IO ()
logInfo miState msg = Logger.logInfo (misLogger miState) (T.pack msg)

logDebug :: MultiIdeState -> String -> IO ()
logDebug miState msg = Logger.logDebug (misLogger miState) (T.pack msg)

newMultiIdeState :: FilePath -> PackageHome -> Logger.Priority -> [String] -> (MultiIdeState -> SubIdeMessageHandler) -> IO MultiIdeState
newMultiIdeState misMultiPackageHome misDefaultPackagePath logThreshold misSubIdeArgs subIdeMessageHandler = do
  (misFromClientMethodTrackerVar :: MethodTrackerVar 'LSP.FromClient) <- newTVarIO IM.emptyIxMap
  (misFromServerMethodTrackerVar :: MethodTrackerVar 'LSP.FromServer) <- newTVarIO IM.emptyIxMap
  misSubIdesVar <- newTMVarIO @SubIdes mempty
  misInitParamsVar <- newEmptyMVar @InitParams
  misToClientChan <- atomically newTChan
  misMultiPackageMappingVar <- newTMVarIO @MultiPackageYamlMapping mempty
  misDarDependentPackagesVar <- newTMVarIO @DarDependentPackages mempty
  misSourceFileHomesVar <- newTMVarIO @SourceFileHomes mempty
  misLogger <- Logger.newStderrLogger logThreshold "Multi-IDE"
  let miState = MultiIdeState {misSubIdeMessageHandler = subIdeMessageHandler miState, ..}
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
  , psReleaseVersion :: UnresolvedReleaseVersion
  }
