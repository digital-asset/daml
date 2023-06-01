-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE RankNTypes #-}

module DA.Daml.LanguageServer
    ( runLanguageServer
    ) where

import           Language.LSP.Types
import qualified Development.IDE.LSP.LanguageServer as LS
import Control.Monad.IO.Class
import qualified Data.Aeson as Aeson
import Data.Default

import qualified DA.Daml.LanguageServer.CodeLens as VirtualResource
import Development.IDE.Types.Logger

import qualified Data.HashSet as HS
import qualified Data.Text as T

import Development.IDE.Core.FileStore
import Development.IDE.Core.Rules
import Development.IDE.Core.Rules.Daml
import Development.IDE.Core.Service.Daml
import Development.IDE.Plugin

import qualified DA.Daml.SessionTelemetry as SessionTelemetry
import qualified DA.Service.Logger as Lgr
import qualified Network.URI                               as URI

import qualified Language.LSP.Server as LSP

textShow :: Show a => a -> T.Text
textShow = T.pack . show

------------------------------------------------------------------------
-- Request handlers
------------------------------------------------------------------------

setHandlersKeepAlive :: Plugin c
setHandlersKeepAlive = Plugin
    { pluginCommands = mempty
    , pluginRules = mempty
    , pluginHandlers = pluginHandler (SCustomMethod "daml/keepAlive")  $ \_ _ -> pure (Right Aeson.Null)
    , pluginNotificationHandlers = mempty
    }

setHandlersVirtualResource :: Plugin c
setHandlersVirtualResource = Plugin
    { pluginRules = mempty
    , pluginHandlers = mempty
    , pluginCommands = mempty
    , pluginNotificationHandlers = mconcat
          [ pluginNotificationHandler STextDocumentDidOpen $ \ide (DidOpenTextDocumentParams TextDocumentItem{_uri}) ->
            liftIO $ withUriDaml _uri $ \vr -> do
                logInfo (ideLogger ide) $ "Opened virtual resource: " <> textShow vr
                logTelemetry (ideLogger ide) "Viewed scenario results"
                modifyOpenVirtualResources ide (HS.insert vr)

          , pluginNotificationHandler STextDocumentDidClose $ \ide (DidCloseTextDocumentParams TextDocumentIdentifier{_uri}) ->
            liftIO $ withUriDaml _uri $ \vr -> do
                logInfo (ideLogger ide) $ "Closed virtual resource: " <> textShow vr
                modifyOpenVirtualResources ide (HS.delete vr)
          ]
    }

withUriDaml :: Uri -> (VirtualResource -> IO ()) -> IO ()
withUriDaml x f
    | Just uri <- URI.parseURI $ T.unpack $ getUri x
    , URI.uriScheme uri == "daml:"
    , Just vr <- uriToVirtualResource uri
    = f vr
withUriDaml _ _ = return ()


------------------------------------------------------------------------
-- Server execution
------------------------------------------------------------------------

runLanguageServer
    :: Show c
    => Lgr.Handle IO
    -> Plugin c
    -> c
    -> (LSP.LanguageContextEnv c -> VFSHandle -> Maybe FilePath -> IO IdeState)
    -> IO ()
runLanguageServer lgr plugins conf getIdeState = SessionTelemetry.withPlugin lgr $ \sessionHandlerPlugin -> do
    let allPlugins = plugins <> setHandlersKeepAlive <> setHandlersVirtualResource <> VirtualResource.plugin <> sessionHandlerPlugin
    let onConfigurationChange c _ = Right c
    let options = def { LSP.executeCommandCommands = Just (commandIds allPlugins) }
    LS.runLanguageServer options conf onConfigurationChange allPlugins getIdeState
