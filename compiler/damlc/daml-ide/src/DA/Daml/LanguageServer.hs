-- Copyright (c) 2019 The DAML Authors. All rights reserved.
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
import qualified Data.Set                                  as S
import qualified Data.Text as T

import Development.IDE.Core.FileStore
import Development.IDE.Core.Rules
import Development.IDE.Core.Rules.Daml
import Development.IDE.Core.Service.Daml

import Development.IDE.Core.Shake
import Development.IDE.Core.RuleTypes.Daml

import qualified Network.URI                               as URI

import Language.Haskell.LSP.Messages
import qualified Language.Haskell.LSP.Core as LSP
import Development.IDE.Types.Location



textShow :: Show a => a -> T.Text
textShow = T.pack . show

------------------------------------------------------------------------
-- Request handlers
------------------------------------------------------------------------

setHandlersKeepAlive :: PartialHandlers
setHandlersKeepAlive = PartialHandlers $ \WithMessage{..} x -> return x
    {LSP.customRequestHandler = Just $ \msg@RequestMessage{_method} ->
        case _method of
            CustomClientMethod "daml/keepAlive" ->
                maybe (return ()) ($ msg) $
                withResponse RspCustomServer (\_ _ _ -> return Aeson.Null)
            _ -> whenJust (LSP.customRequestHandler x) ($ msg)
    }

setIgnoreOptionalHandlers :: PartialHandlers
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

tt :: Aeson.Value -> T.Text
tt  (Aeson.String x ) = x
tt _ = error "this has to be reported in ide"


filesFromExecParams :: List Aeson.Value -> NormalizedFilePath
filesFromExecParams (List files) =  case map tt files of
    [] -> error "this has to be reported in ide"
    (h : _) -> toNormalizedFilePath $ T.unpack h

onCommand
    :: IdeState
    -> ExecuteCommandParams
    -> IO Aeson.Value
onCommand ide ExecuteCommandParams{..} = do
    case _arguments of
        Nothing -> return $ Aeson.String "Generate Dalf then call visualize file path not set"
        Just path -> do
            mbModMapping <- runAction ide (useWithStale GenerateVisualization (filesFromExecParams path))
            case mbModMapping of
                Nothing -> logInfo (ideLogger ide) "Generating dalf failed"
                Just (mod, _mapping) -> logInfo (ideLogger ide) (textShow mod)
            return $ Aeson.String "Generate Dalf then call visualize"

setCommandHandler ::PartialHandlers
setCommandHandler = PartialHandlers $ \WithMessage{..} x -> return x {
    LSP.executeCommandHandler = withResponse RspExecuteCommand $ const onCommand
}


-- setHandlerDamlVisualize :: PartialHandlers
-- setHandlerDamlVisualize = PartialHandlers $ \WithMessage{..} x -> return x
--     {LSP.customRequestHandler = Just $ \msg@RequestMessage{_method} ->
--         case _method of
--             CustomClientMethod "daml/damlVisualize" -> maybe (return ()) ($ msg) $
--                 withResponse RspCustomServer (\_ ide _ -> do
--                     mbModMapping <- runAction ide (useWithStale GenerateDalf "filePath")
--                     case mbModMapping of
--                         Nothing ->logInfo (ideLogger ide) "nothing to see"
--                         Just (mod, _mapping) -> logInfo (ideLogger ide) (textShow mod)
--                     return $ Aeson.String "Generate Dalf then call visualize")
--             _ -> whenJust (LSP.customRequestHandler x) ($ msg)
--     }

setHandlersVirtualResource :: PartialHandlers
setHandlersVirtualResource = PartialHandlers $ \WithMessage{..} x -> return x
    {LSP.didOpenTextDocumentNotificationHandler = withNotification (LSP.didOpenTextDocumentNotificationHandler x) $
        \_ ide (DidOpenTextDocumentParams TextDocumentItem{_uri}) ->
            withUriDaml _uri $ \vr -> do
                logInfo (ideLogger ide) $ "Opened virtual resource: " <> textShow vr
                modifyOpenVirtualResources ide (S.insert vr)

    ,LSP.didCloseTextDocumentNotificationHandler = withNotification (LSP.didCloseTextDocumentNotificationHandler x) $
        \_ ide (DidCloseTextDocumentParams TextDocumentIdentifier{_uri}) -> do
            withUriDaml _uri $ \vr -> do
                logInfo (ideLogger ide) $ "Closed virtual resource: " <> textShow vr
                modifyOpenVirtualResources ide (S.delete vr)
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
    :: ((FromServerMessage -> IO ()) -> VFSHandle -> ClientCapabilities -> IO IdeState)
    -> IO ()
runLanguageServer getIdeState = do
    let handlers = setHandlersKeepAlive <> setHandlersVirtualResource <> setHandlersCodeLens <> setIgnoreOptionalHandlers <> setCommandHandler
    LS.runLanguageServer options handlers getIdeState


options :: LSP.Options
options = def
    { LSP.codeLensProvider = Just $ CodeLensOptions $ Just False
    }
