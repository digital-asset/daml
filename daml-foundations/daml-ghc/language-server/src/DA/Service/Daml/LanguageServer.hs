-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE RankNTypes #-}

-- WARNING: A copy of Development.IDE.LSP.LanguageServer, try to keep them in sync
module DA.Service.Daml.LanguageServer
    ( runLanguageServer
    ) where

import           Development.IDE.LSP.Protocol
import           Development.IDE.LSP.Server

import Control.Monad.IO.Class
import qualified DA.Service.Daml.LanguageServer.CodeLens   as LS.CodeLens
import qualified Development.IDE.LSP.Definition as LS.Definition
import qualified Development.IDE.LSP.Hover      as LS.Hover
import Development.IDE.Types.Logger

import qualified Data.Aeson                                as Aeson
import qualified Data.Rope.UTF16 as Rope
import qualified Data.Set                                  as S
import qualified Data.Text as T

import Development.IDE.Core.FileStore
import Development.IDE.Core.Rules
import Development.IDE.Core.Rules.Daml
import Development.IDE.Core.Service.Daml
import Development.IDE.Types.Location

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
    CodeLens params -> RspCodeLens . makeResponse <$> LS.CodeLens.handle logger compilerH params

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

              | URI.uriScheme uri == "daml:"
              -> handleDidOpenVirtualResource uri

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
              | URI.uriScheme uri' == "daml:" -> handleDidCloseVirtualResource uri'
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

    handleDidOpenVirtualResource uri = do
         case uriToVirtualResource uri of
           Nothing -> logWarning logger $ "Failed to parse virtual resource URI: " <> textShow uri
           Just vr -> do
               logInfo logger $ "Opened virtual resource: " <> textShow vr
               modifyOpenVirtualResources compilerH (S.insert vr)

    handleDidCloseFile filePath = do
         logInfo logger $ "Closed text document: " <> textShow (fromNormalizedFilePath filePath)
         onFileModified compilerH filePath Nothing
         modifyFilesOfInterest compilerH (S.delete filePath)

    handleDidCloseVirtualResource uri = do
        logInfo logger $ "Closed virtual resource: " <> textShow uri
        case uriToVirtualResource uri of
           Nothing -> logWarning logger "Failed to parse virtual resource URI!"
           Just vr -> modifyOpenVirtualResources compilerH (S.delete vr)

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
runLanguageServer logger getIdeState = do
    let getHandlers lspFuncs = do
            compilerH <- getIdeState (sendFunc lspFuncs) (makeLSPVFSHandle lspFuncs)
            pure $ Handlers (handleRequest logger compilerH) (handleNotification lspFuncs logger compilerH)
    liftIO $ runServer logger getHandlers
