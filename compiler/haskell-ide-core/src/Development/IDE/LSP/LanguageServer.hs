-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE RankNTypes #-}

module Development.IDE.LSP.LanguageServer
    ( runLanguageServer
    ) where

import           Development.IDE.LSP.Protocol
import           Development.IDE.LSP.Server
import Development.IDE.State.Service as Compiler

import Control.Monad.IO.Class
import qualified Development.IDE.LSP.Definition as LS.Definition
import qualified Development.IDE.LSP.Hover      as LS.Hover
import qualified Development.IDE.Logger as Logger

import qualified Data.Aeson                                as Aeson
import           Data.IORef                                (IORef, atomicModifyIORef', newIORef)
import qualified Data.Rope.UTF16 as Rope
import qualified Data.Set                                  as S
import qualified Data.Text as T

import Development.IDE.State.FileStore
import qualified Development.IDE.Types.Diagnostics as Compiler

import qualified Network.URI                               as URI

import qualified System.Exit

import Language.Haskell.LSP.Core (LspFuncs(..))
import Language.Haskell.LSP.Messages
import Language.Haskell.LSP.VFS

textShow :: Show a => a -> T.Text
textShow = T.pack . show

------------------------------------------------------------------------
-- Types
------------------------------------------------------------------------

-- | Language server state
data State = State
    { sOpenDocuments        :: !(S.Set Compiler.NormalizedFilePath)
    }

-- | Implementation handle
data IHandle p t = IHandle
    { ihState     :: !(IORef State)
    , ihLoggerH   :: !Logger.Handle
    , ihCompilerH :: !Compiler.IdeState
    }

------------------------------------------------------------------------
-- Request handlers
------------------------------------------------------------------------

handleRequest
    :: IHandle () ()
    -> (forall resp. resp -> ResponseMessage resp)
    -> (ErrorCode -> ResponseMessage ())
    -> ServerRequest
    -> IO FromServerMessage
handleRequest (IHandle _stateRef loggerH compilerH) makeResponse makeErrorResponse = \case
    Shutdown -> do
      Logger.logInfo loggerH "Shutdown request received, terminating."
      System.Exit.exitSuccess

    KeepAlive -> pure $ RspCustomServer $ makeResponse Aeson.Null

    Definition params -> RspDefinition . makeResponse <$> LS.Definition.handle loggerH compilerH params
    Hover params -> RspHover . makeResponse <$> LS.Hover.handle loggerH compilerH params

    req -> do
        Logger.logWarning loggerH ("Method not found" <> T.pack (show req))
        pure $ RspError $ makeErrorResponse MethodNotFound


handleNotification :: LspFuncs () -> IHandle () () -> ServerNotification -> IO ()
handleNotification lspFuncs (IHandle stateRef loggerH compilerH) = \case

    DidOpenTextDocument (DidOpenTextDocumentParams item) -> do
        case URI.parseURI $ T.unpack $ getUri $ _uri (item :: TextDocumentItem) of
          Just uri
              | URI.uriScheme uri == "file:"
              -> handleDidOpenFile item

              | otherwise
              -> Logger.logWarning loggerH $ "Unknown scheme in URI: "
                    <> textShow uri

          _ -> Logger.logSeriousError loggerH $ "Invalid URI in DidOpenTextDocument: "
                    <> textShow (_uri (item :: TextDocumentItem))

    DidChangeTextDocument (DidChangeTextDocumentParams docId _) -> do
        let uri = _uri (docId :: VersionedTextDocumentIdentifier)

        case Compiler.uriToFilePath' uri of
          Just (Compiler.toNormalizedFilePath -> filePath) -> do
            mbVirtual <- getVirtualFileFunc lspFuncs $ toNormalizedUri uri
            let contents = maybe "" (Rope.toText . (_text :: VirtualFile -> Rope.Rope)) mbVirtual
            setBufferModified compilerH filePath (Just contents)
            Logger.logInfo loggerH
              $ "Updated text document: " <> textShow (Compiler.fromNormalizedFilePath filePath)

          Nothing ->
            Logger.logSeriousError loggerH
              $ "Invalid file path: " <> textShow (_uri (docId :: VersionedTextDocumentIdentifier))

    DidCloseTextDocument (DidCloseTextDocumentParams (TextDocumentIdentifier uri)) ->
        case URI.parseURI $ T.unpack $ getUri uri of
          Just uri'
              | URI.uriScheme uri' == "file:" -> do
                    Just fp <- pure $ Compiler.toNormalizedFilePath <$> Compiler.uriToFilePath' uri
                    handleDidCloseFile fp
              | otherwise -> Logger.logWarning loggerH $ "Unknown scheme in URI: " <> textShow uri

          _ -> Logger.logSeriousError loggerH
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
        Just filePath <- pure $ Compiler.toNormalizedFilePath <$> Compiler.uriToFilePath' uri
        documents <- atomicModifyIORef' stateRef $
          \state -> let documents = S.insert filePath $ sOpenDocuments state
                    in ( state { sOpenDocuments = documents }
                       , documents
                       )



        -- Update the file contents
        setBufferModified compilerH filePath (Just contents)

        -- Update the list of open files
        Compiler.setFilesOfInterest compilerH documents

        Logger.logInfo loggerH $ "Opened text document: " <> textShow filePath

    handleDidCloseFile filePath = do
         Logger.logInfo loggerH $ "Closed text document: " <> textShow (Compiler.fromNormalizedFilePath filePath)
         documents <- atomicModifyIORef' stateRef $
           \state -> let documents = S.delete filePath $ sOpenDocuments state
                     in ( state { sOpenDocuments = documents }
                        , documents
                        )
         Compiler.setFilesOfInterest compilerH documents
         setBufferModified compilerH filePath Nothing


------------------------------------------------------------------------
-- Server execution
------------------------------------------------------------------------

runLanguageServer
    :: Logger.Handle
    -> ((FromServerMessage -> IO ()) -> VFSHandle -> IO Compiler.IdeState)
    -> IO ()
runLanguageServer loggerH getIdeState = do
    state     <- liftIO $ newIORef $ State S.empty
    let getHandlers lspFuncs = do
            compilerH <- getIdeState (sendFunc lspFuncs) (makeLSPVFSHandle lspFuncs)
            let ihandle = IHandle
                    { ihState = state
                    , ihLoggerH = loggerH
                    , ihCompilerH = compilerH
                    }
            pure $ Handlers (handleRequest ihandle) (handleNotification lspFuncs ihandle)
    liftIO $ runServer loggerH getHandlers
