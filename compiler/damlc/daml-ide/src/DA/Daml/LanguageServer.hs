-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE RankNTypes #-}

module DA.Daml.LanguageServer
    ( runLanguageServer
    ) where

import           Language.Haskell.LSP.Types
import           Language.Haskell.LSP.Types.Capabilities
import           Development.IDE.LSP.Server
import qualified Development.IDE.LSP.LanguageServer as LS
import Control.Monad.Extra
import Data.Default

import DA.Daml.LanguageServer.CodeLens
import Development.IDE.Types.Logger

import qualified Data.Aeson                                as Aeson
import qualified Data.HashSet as HS
import qualified Data.Text as T

import Development.IDE.Core.FileStore
import Development.IDE.Core.Rules
import Development.IDE.Core.Rules.Daml
import Development.IDE.Core.Service.Daml
import Development.IDE.Plugin

import DA.Daml.SessionTelemetry
import DA.Daml.LanguageServer.Visualize
import qualified DA.Service.Logger as Lgr
import qualified Network.URI                               as URI

import Language.Haskell.LSP.Messages
import qualified Language.Haskell.LSP.Core as LSP
import qualified Language.Haskell.LSP.Types as LSP


textShow :: Show a => a -> T.Text
textShow = T.pack . show

------------------------------------------------------------------------
-- Request handlers
------------------------------------------------------------------------

setHandlersKeepAlive :: PartialHandlers a
setHandlersKeepAlive = PartialHandlers $ \WithMessage{..} x -> return x
    {LSP.customRequestHandler = Just $ \msg@RequestMessage{_method} ->
        case _method of
            CustomClientMethod "daml/keepAlive" ->
                whenJust (withResponse RspCustomServer (\_ _ _ -> pure (Right Aeson.Null))) ($ msg)
            _ -> whenJust (LSP.customRequestHandler x) ($ msg)
    }

setIgnoreOptionalHandlers :: PartialHandlers a
setIgnoreOptionalHandlers = PartialHandlers $ \WithMessage{..} x -> return x
    {LSP.customRequestHandler = Just $ \msg@RequestMessage{_method} ->
         case _method of
             CustomClientMethod s
               | optionalPrefix `T.isPrefixOf` s -> pure ()
             _ -> whenJust (LSP.customRequestHandler x) ($ msg)
    ,LSP.customNotificationHandler = Just $ \msg@NotificationMessage{_method} ->
         case _method of
             CustomClientMethod s
               | optionalPrefix `T.isPrefixOf` s -> pure ()
             _ -> whenJust (LSP.customNotificationHandler x) ($ msg)
    }
    -- | According to the LSP spec methods starting with $/ are optional
    -- and can be ignored. In particular, VSCode sometimes seems to send
    -- $/setTraceNotification which we want to ignore.
    where optionalPrefix = "$/"

setHandlersVirtualResource :: PartialHandlers a
setHandlersVirtualResource = PartialHandlers $ \WithMessage{..} x -> return x
    {LSP.didOpenTextDocumentNotificationHandler = withNotification (LSP.didOpenTextDocumentNotificationHandler x) $
        \_ ide (DidOpenTextDocumentParams TextDocumentItem{_uri}) ->
            withUriDaml _uri $ \vr -> do
                logInfo (ideLogger ide) $ "Opened virtual resource: " <> textShow vr
                logTelemetry (ideLogger ide) "Viewed scenario results"
                modifyOpenVirtualResources ide (HS.insert vr)

    ,LSP.didCloseTextDocumentNotificationHandler = withNotification (LSP.didCloseTextDocumentNotificationHandler x) $
        \_ ide (DidCloseTextDocumentParams TextDocumentIdentifier{_uri}) -> do
            withUriDaml _uri $ \vr -> do
                logInfo (ideLogger ide) $ "Closed virtual resource: " <> textShow vr
                modifyOpenVirtualResources ide (HS.delete vr)
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
    :: Lgr.Handle IO
    -> Plugin ()
    -> (IO LSP.LspId -> (FromServerMessage -> IO ()) -> VFSHandle -> ClientCapabilities -> IO IdeState)
    -> IO ()
runLanguageServer lgr plugins getIdeState = withSessionPings lgr $ \setSessionHandlers -> do
    let handlers = setHandlersKeepAlive <> setHandlersVirtualResource <> setHandlersCodeLens <> setIgnoreOptionalHandlers <> setCommandHandler <> setSessionHandlers
    let onInitialConfiguration = const $ Right ()
    let onConfigurationChange = const $ Right ()
    LS.runLanguageServer options (pluginHandler plugins <> handlers) onInitialConfiguration onConfigurationChange getIdeState


options :: LSP.Options
options = def { LSP.executeCommandCommands = Just ["daml/damlVisualize"] }
