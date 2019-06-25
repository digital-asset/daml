-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE RankNTypes #-}

-- WARNING: A copy of DA.Service.Daml.LanguageServer, try to keep them in sync
-- This version removes the daml: handling
module Development.IDE.LSP.LanguageServer
    ( runLanguageServer
    ) where

import           Development.IDE.LSP.Protocol
import           Development.IDE.LSP.Server hiding (runServer)
import qualified Language.Haskell.LSP.Control as LSP
import qualified Language.Haskell.LSP.Core as LSP
import qualified Language.Haskell.LSP.Messages as LSP
import qualified Language.Haskell.LSP.Types as LSP
import Control.Concurrent.STM
import Control.Concurrent.Extra
import Control.Concurrent.Async
import Data.Default
import qualified Data.Aeson.Text as Aeson
import qualified Data.Text.Lazy as TL
import           GHC.IO.Handle                    (hDuplicate, hDuplicateTo)
import System.IO
import Control.Monad

import Control.Monad.IO.Class
import qualified Development.IDE.LSP.Definition as LS.Definition
import qualified Development.IDE.LSP.Hover      as LS.Hover
import Development.IDE.Types.Logger
import Development.IDE.Core.Service
import Development.IDE.Types.Location

import qualified Data.Aeson                                as Aeson
import qualified Data.Rope.UTF16 as Rope
import qualified Data.Set                                  as S
import qualified Data.Text as T

import Development.IDE.Core.FileStore
import Development.IDE.Core.OfInterest

import qualified Network.URI                               as URI

import qualified System.Exit

import Language.Haskell.LSP.Core (LspFuncs(..))
import Language.Haskell.LSP.Messages
import Language.Haskell.LSP.VFS

textShow :: Show a => a -> T.Text
textShow = T.pack . show

------------------------------------------------------------------------
-- Request handlers
------------------------------------------------------------------------

handleRequest
    :: Logger
    -> IdeState
    -> (forall resp. resp -> ResponseMessage resp)
    -> (ErrorCode -> ResponseMessage ())
    -> ServerRequest
    -> IO FromServerMessage
handleRequest logger compilerH makeResponse makeErrorResponse = \case
    Shutdown -> do
      logInfo logger "Shutdown request received, terminating."
      System.Exit.exitSuccess

    KeepAlive -> pure $ RspCustomServer $ makeResponse Aeson.Null

    Definition params -> RspDefinition . makeResponse <$> LS.Definition.handle logger compilerH params
    Hover params -> RspHover . makeResponse <$> LS.Hover.handle logger compilerH params
    CodeLens _params -> pure $ RspCodeLens $ makeResponse mempty

    req -> do
        logWarning logger ("Method not found" <> T.pack (show req))
        pure $ RspError $ makeErrorResponse MethodNotFound


handleNotification :: LspFuncs () -> Logger -> IdeState -> ServerNotification -> IO ()
handleNotification lspFuncs logger compilerH = \case

    DidOpenTextDocument (DidOpenTextDocumentParams item) -> do
        case URI.parseURI $ T.unpack $ getUri $ _uri (item :: TextDocumentItem) of
          Just uri
              | URI.uriScheme uri == "file:"
              -> handleDidOpenFile item

              | otherwise
              -> logWarning logger $ "Unknown scheme in URI: "
                    <> textShow uri

          _ -> logSeriousError logger $ "Invalid URI in DidOpenTextDocument: "
                    <> textShow (_uri (item :: TextDocumentItem))

    DidChangeTextDocument (DidChangeTextDocumentParams docId _) -> do
        let uri = _uri (docId :: VersionedTextDocumentIdentifier)

        case uriToFilePath' uri of
          Just (toNormalizedFilePath -> filePath) -> do
            mbVirtual <- getVirtualFileFunc lspFuncs $ toNormalizedUri uri
            let contents = maybe "" (Rope.toText . (_text :: VirtualFile -> Rope.Rope)) mbVirtual
            onFileModified compilerH filePath (Just contents)
            logInfo logger
              $ "Updated text document: " <> textShow (fromNormalizedFilePath filePath)

          Nothing ->
            logSeriousError logger
              $ "Invalid file path: " <> textShow (_uri (docId :: VersionedTextDocumentIdentifier))

    DidCloseTextDocument (DidCloseTextDocumentParams (TextDocumentIdentifier uri)) ->
        case URI.parseURI $ T.unpack $ getUri uri of
          Just uri'
              | URI.uriScheme uri' == "file:" -> do
                    Just fp <- pure $ toNormalizedFilePath <$> uriToFilePath' uri
                    handleDidCloseFile fp
              | otherwise -> logWarning logger $ "Unknown scheme in URI: " <> textShow uri

          _ -> logSeriousError logger
                 $    "Invalid URI in DidCloseTextDocument: "
                   <> textShow uri

    DidSaveTextDocument _params ->
      pure ()

    UnknownNotification _method _params -> return ()
  where
    -- Note that the state changes here are not atomic.
    -- When we have parallel compilation we could manage the state
    -- changes in STM so that we can atomically change the state.
    -- Internally it should be done via the IO oracle. See PROD-2808.
    handleDidOpenFile (TextDocumentItem uri _ _ contents) = do
        Just filePath <- pure $ toNormalizedFilePath <$> uriToFilePath' uri
        onFileModified compilerH filePath (Just contents)
        modifyFilesOfInterest compilerH (S.insert filePath)
        logInfo logger $ "Opened text document: " <> textShow filePath

    handleDidCloseFile filePath = do
         logInfo logger $ "Closed text document: " <> textShow (fromNormalizedFilePath filePath)
         onFileModified compilerH filePath Nothing
         modifyFilesOfInterest compilerH (S.delete filePath)

-- | Manages the file store (caching compilation results and unsaved content).
onFileModified
    :: IdeState
    -> NormalizedFilePath
    -> Maybe T.Text
    -> IO ()
onFileModified service fp mbContents = do
    logDebug (ideLogger service) $ "File modified " <> T.pack (show fp)
    setBufferModified service fp mbContents

------------------------------------------------------------------------
-- Server execution
------------------------------------------------------------------------

runLanguageServer
    :: Logger
    -> ((FromServerMessage -> IO ()) -> VFSHandle -> IO IdeState)
    -> IO ()
runLanguageServer loggerH getIdeState = do
    let getHandlers lspFuncs = do
            compilerH <- getIdeState (sendFunc lspFuncs) (makeLSPVFSHandle lspFuncs)
            pure $ Handlers (handleRequest loggerH compilerH) (handleNotification lspFuncs loggerH compilerH)
    liftIO $ runServer loggerH getHandlers

runServer
    :: Logger
    -> (LSP.LspFuncs () -> IO Handlers)
    -- ^ Notification handler for language server notifications
    -> IO ()
runServer loggerH getHandlers = do
    -- DEL-6257: Move stdout to another file descriptor and duplicate stderr
    -- to stdout. This guards against stray prints from corrupting the JSON-RPC
    -- message stream.
    newStdout <- hDuplicate stdout
    stderr `hDuplicateTo` stdout

    -- Print out a single space to assert that the above redirection works.
    -- This is interleaved with the logger, hence we just print a space here in
    -- order not to mess up the output too much. Verified that this breaks
    -- the language server tests without the redirection.
    putStr " " >> hFlush stdout
    clientMsgChan <- newTChanIO
    -- These barriers are signaled when the threads reading from these chans exit.
    -- This should not happen but if it does, we will make sure that the whole server
    -- dies and can be restarted instead of losing threads silently.
    clientMsgBarrier <- newBarrier
    void $ waitAnyCancel =<< traverse async
        [ void $ LSP.runWithHandles
            stdin
            newStdout
            ( const $ Right ()
            , handleInit (signalBarrier clientMsgBarrier ()) clientMsgChan
            )
            (handlers clientMsgChan)
            options
            Nothing
        , void $ waitBarrier clientMsgBarrier
        ]
    where
        handleInit :: IO () -> TChan LSP.FromClientMessage -> LSP.LspFuncs () -> IO (Maybe LSP.ResponseError)
        handleInit exitClientMsg clientMsgChan lspFuncs@LSP.LspFuncs{..} = do
            Handlers{..} <- getHandlers lspFuncs
            let requestHandler' (req, reqId) = requestHandler
                    (\res -> ResponseMessage "2.0" (responseId reqId) (Just res) Nothing)
                    (\err -> ResponseMessage "2.0" (responseId reqId) Nothing (Just $ ResponseError err "" Nothing))
                    req
            _ <- flip forkFinally (const exitClientMsg) $ forever $ do
                msg <- atomically $ readTChan clientMsgChan
                case convClientMsg msg of
                    Nothing -> logSeriousError loggerH $ "Unknown client msg: " <> T.pack (show msg)
                    Just (Left notif) -> notificationHandler notif
                    Just (Right req) -> sendFunc =<< requestHandler' req
            pure Nothing

convClientMsg :: LSP.FromClientMessage -> Maybe (Either ServerNotification (ServerRequest, LspId))
convClientMsg msg = case msg of
    LSP.ReqInitialize m -> unknownReq m
    LSP.ReqShutdown m -> Just $ Right (Shutdown, reqId m)

    LSP.ReqHover m -> toReq Hover m

    LSP.ReqCompletion m -> toReq Completion m
    LSP.ReqCompletionItemResolve m -> unknownReq m

    LSP.ReqSignatureHelp m -> toReq SignatureHelp m

    LSP.ReqDefinition m -> toReq Definition m
    LSP.ReqTypeDefinition m -> toReq Definition m
    LSP.ReqImplementation m -> toReq Definition m

    LSP.ReqFindReferences m -> toReq References m
    LSP.ReqDocumentHighlights m -> unknownReq m
    LSP.ReqDocumentSymbols m -> toReq DocumentSymbol m
    LSP.ReqWorkspaceSymbols m -> toReq WorkspaceSymbol m
    LSP.ReqCodeAction m -> unknownReq m

    LSP.ReqCodeLens m -> toReq CodeLens m
    LSP.ReqCodeLensResolve m -> unknownReq m

    LSP.ReqDocumentLink m -> unknownReq m
    LSP.ReqDocumentLinkResolve m -> unknownReq m
    LSP.ReqDocumentColor m -> unknownReq m
    LSP.ReqColorPresentation m -> unknownReq m

    LSP.ReqDocumentFormatting m -> toReq Formatting m
    LSP.ReqDocumentRangeFormatting m -> unknownReq m
    LSP.ReqDocumentOnTypeFormatting m -> unknownReq m

    LSP.ReqRename m -> toReq Rename m

    LSP.ReqFoldingRange m -> unknownReq m
    LSP.ReqExecuteCommand m -> unknownReq m
    LSP.ReqWillSaveWaitUntil m -> unknownReq m
    LSP.ReqCustomClient m -> case reqMethod m of
        CustomClientMethod "daml/keepAlive" -> Just $ Right (KeepAlive, reqId m)
        _ -> unknownReq m

    LSP.NotInitialized m -> unknownNot m
    LSP.NotExit m -> unknownNot m
    LSP.NotCancelRequestFromClient m -> unknownNot m
    LSP.NotDidChangeConfiguration m -> unknownNot m
    LSP.NotDidOpenTextDocument m -> toNot DidOpenTextDocument m
    LSP.NotDidChangeTextDocument m -> toNot DidChangeTextDocument m
    LSP.NotDidCloseTextDocument m -> toNot DidCloseTextDocument m
    LSP.NotWillSaveTextDocument m -> unknownNot m
    LSP.NotDidSaveTextDocument m -> toNot DidSaveTextDocument m
    LSP.NotDidChangeWatchedFiles m -> unknownNot m
    LSP.NotDidChangeWorkspaceFolders m -> unknownNot m
    LSP.NotProgressCancel m -> unknownNot m
    LSP.NotCustomClient m -> unknownNot m

    LSP.RspApplyWorkspaceEdit _ -> Nothing
    LSP.RspFromClient _ -> Nothing
  where toReq constr msg = Just $ Right (constr $ reqParams msg, reqId msg)
        toNot constr msg = Just $ Left $ constr $ notParams msg
        unknownReq (LSP.RequestMessage _ id method params) =
            Just $ Right (UnknownRequest (TL.toStrict $ Aeson.encodeToLazyText method) (Aeson.toJSON params), id)
        unknownNot (LSP.NotificationMessage _ method params) =
            Just $ Left $ UnknownNotification (TL.toStrict $ Aeson.encodeToLazyText method) (Aeson.toJSON params)
        -- Type-restricted wrappers to make DuplicateRecordFields less annoying.
        reqParams :: RequestMessage m req resp -> req
        reqParams = _params
        reqId :: RequestMessage m req resp -> LspId
        reqId = _id
        reqMethod :: RequestMessage m req resp -> m
        reqMethod = _method
        notParams :: NotificationMessage m a -> a
        notParams = _params

handlers :: TChan LSP.FromClientMessage -> LSP.Handlers
handlers chan = def
    { LSP.hoverHandler = emit LSP.ReqHover
    , LSP.definitionHandler = emit LSP.ReqDefinition
    , LSP.codeLensHandler = emit LSP.ReqCodeLens
    , LSP.didOpenTextDocumentNotificationHandler = emit LSP.NotDidOpenTextDocument
    , LSP.didChangeTextDocumentNotificationHandler = emit LSP.NotDidChangeTextDocument
    , LSP.didCloseTextDocumentNotificationHandler = emit LSP.NotDidCloseTextDocument
    , LSP.didSaveTextDocumentNotificationHandler = emit LSP.NotDidSaveTextDocument
    , LSP.initializedHandler = emit LSP.NotInitialized
    , LSP.exitNotificationHandler = Nothing
    -- If the exit notification handler is set to `Nothing`
    -- haskell-lsp will take care of shutting down the server for us.
    , LSP.customRequestHandler = emit LSP.ReqCustomClient
    , LSP.cancelNotificationHandler = Just $ const $ pure ()
    -- ^ We just ignore cancel requests which is allowed according to
    -- the spec. Installing a handler avoids errors about the missing handler.
    }
    where
        emit :: (a -> LSP.FromClientMessage) -> Maybe (LSP.Handler a)
        emit f = Just $ atomically . writeTChan chan . f

options :: LSP.Options
options = def
    { LSP.textDocumentSync = Just TextDocumentSyncOptions
          { _openClose = Just True
          , _change = Just TdSyncIncremental
          , _willSave = Nothing
          , _willSaveWaitUntil = Nothing
          , _save = Just $ SaveOptions $ Just False
          }
    , LSP.codeLensProvider = Just $ CodeLensOptions $ Just False
    }
