-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE PolyKinds           #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE ApplicativeDo       #-}
{-# LANGUAGE RankNTypes       #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DisambiguateRecordFields #-}
{-# LANGUAGE GADTs #-}

module DA.Cli.Damlc.Command.MultiIde.Types (
  module DA.Cli.Damlc.Command.MultiIde.Types
) where

import Control.Concurrent.Async (Async)
import Control.Concurrent.STM.TChan
import Control.Concurrent.STM.TVar
import Control.Concurrent.STM.TMVar
import Control.Concurrent.MVar
import Control.Monad.STM
import qualified Data.ByteString.Lazy as BSL
import qualified Data.IxMap as IM
import qualified Data.Map as Map
import qualified Data.Text as T
import qualified Language.LSP.Types as LSP
import System.IO.Extra
import System.Process.Typed (Process)

data TrackedMethod (m :: LSP.Method from 'LSP.Request) where
  TrackedSingleMethodFromClient
    :: forall (m :: LSP.Method 'LSP.FromClient 'LSP.Request)
    .  LSP.SMethod m
    -> TrackedMethod m
  TrackedSingleMethodFromServer
    :: forall (m :: LSP.Method 'LSP.FromServer 'LSP.Request)
    .  LSP.SMethod m
    -> FilePath -- Also store the IDE that sent the request
    -> TrackedMethod m
  TrackedAllMethod :: forall (m :: LSP.Method 'LSP.FromClient 'LSP.Request).
    { tamMethod :: LSP.SMethod m
        -- ^ The method of the initial request
    , tamLspId :: LSP.LspId m
    , tamCombiner :: ResponseCombiner m
        -- ^ How to combine the results from each IDE
    , tamRemainingResponseIDERoots :: [FilePath]
        -- ^ The IDES that have not yet replied to this message
    , tamResponses :: [(FilePath, Either LSP.ResponseError (LSP.ResponseResult m))]
    } -> TrackedMethod m

tmMethod
  :: forall (from :: LSP.From) (m :: LSP.Method from 'LSP.Request)
  .  TrackedMethod m
  -> LSP.SMethod m
tmMethod (TrackedSingleMethodFromClient m) = m
tmMethod (TrackedSingleMethodFromServer m _) = m
tmMethod (TrackedAllMethod {tamMethod}) = tamMethod

type MethodTracker (from :: LSP.From) = IM.IxMap @(LSP.Method from 'LSP.Request) LSP.LspId TrackedMethod
type MethodTrackerVar (from :: LSP.From) = TVar (MethodTracker from)

data SubIDE = SubIDE
  { ideInhandleAsync :: Async ()
  , ideInHandle :: Handle
  , ideInHandleChannel :: TChan BSL.ByteString
  , ideOutHandleAsync :: Async ()
    -- ^ For sending messages to that SubIDE
  , ideProcess :: Process Handle Handle ()
  , ideHomeDirectory :: FilePath
  , ideMessageIdPrefix :: T.Text
    -- ^ Some unique string used to prefix message ids created by the SubIDE, to avoid collisions with other SubIDEs
    -- We use the stringified process ID
  , ideActive :: Bool
  , ideUnitId :: String
    -- ^ Unit ID of the package this SubIDE handles
    -- Of the form "daml-script-0.0.1"
  }

-- SubIDEs placed in a TMVar. The emptyness representents a modification lock.
-- The lock unsures the following properties:
--   If multiple messages are sent to a new IDE at the same time, the first will create and hold a lock, while the rest wait on that lock (avoid multiple create)
--   We never attempt to send messages on a stale IDE. If we ever read SubIDEsVar with the intent to send a message on a SubIDE, we must hold the so a shutdown
--     cannot be sent on that IDE until we are done. This ensures that when a shutdown does occur, it is impossible for non-shutdown messages to be added to the
--     queue after the shutdown.
type SubIDEs = Map.Map FilePath SubIDE
type SubIDEsVar = TMVar SubIDEs

onlyActiveSubIdes :: SubIDEs -> SubIDEs
onlyActiveSubIdes = Map.filter ideActive

-- Stores the initialize messages sent by the client to be forwarded to SubIDEs when they are created.
type InitParams = LSP.InitializeParams
type InitParamsVar = MVar InitParams

-- Maps a packages unit id to its source file path, for all packages listed in a multi-package.yaml
type MultiPackageYamlMapping = Map.Map String FilePath

data MultiIdeState = MultiIdeState
  { fromClientMethodTrackerVar :: MethodTrackerVar 'LSP.FromClient
    -- ^ The client will track its own IDs to ensure they're unique, so no worries about collisions
  , fromServerMethodTrackerVar :: MethodTrackerVar 'LSP.FromServer
    -- ^ We will prefix LspIds before they get here based on their SubIDE messageIdPrefix, to avoid collisions
  , subIDEsVar :: SubIDEsVar
  , initParamsVar :: InitParamsVar
  , toClientChan :: TChan BSL.ByteString
  , multiPackageMapping :: MultiPackageYamlMapping
  }

newMultiIdeState :: MultiPackageYamlMapping -> IO MultiIdeState
newMultiIdeState multiPackageMapping = do
  (fromClientMethodTrackerVar :: MethodTrackerVar 'LSP.FromClient) <- newTVarIO IM.emptyIxMap
  (fromServerMethodTrackerVar :: MethodTrackerVar 'LSP.FromServer) <- newTVarIO IM.emptyIxMap
  subIDEsVar <- newTMVarIO @SubIDEs mempty
  initParamsVar <- newEmptyMVar @InitParams
  toClientChan <- atomically newTChan
  pure MultiIdeState {..}

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
  [(FilePath, Either LSP.ResponseError (LSP.ResponseResult m))] -> Either LSP.ResponseError (LSP.ResponseResult m)
