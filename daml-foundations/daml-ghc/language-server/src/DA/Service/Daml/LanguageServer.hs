-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE RankNTypes #-}

module DA.Service.Daml.LanguageServer
    ( runLanguageServer
    ) where

import Control.Exception.Safe

import           DA.LanguageServer.Protocol
import           DA.LanguageServer.Server

import Control.Monad.IO.Class
import qualified DA.Daml.LF.Ast as LF
import qualified DA.Service.Daml.Compiler.Impl.Handle as Compiler
import qualified DA.Service.Daml.LanguageServer.CodeLens   as LS.CodeLens
import qualified DA.Service.Daml.LanguageServer.Definition as LS.Definition
import qualified DA.Service.Daml.LanguageServer.Hover      as LS.Hover
import qualified DA.Service.Logger                         as Logger
import DAML.Project.Consts

import qualified Data.Aeson                                as Aeson
import           Data.IORef                                (IORef, atomicModifyIORef', newIORef)
import qualified Data.Rope.UTF16 as Rope
import qualified Data.Set                                  as S
import qualified Data.Text.Extended                        as T

import Development.IDE.State.FileStore
import qualified Development.IDE.Types.Diagnostics as Compiler
import           Development.IDE.Types.LSP as Compiler

import qualified Network.URI                               as URI

import qualified System.Exit

import Language.Haskell.LSP.Core (LspFuncs(..))
import Language.Haskell.LSP.Messages
import Language.Haskell.LSP.VFS

------------------------------------------------------------------------
-- Types
------------------------------------------------------------------------

-- | Language server state
data State = State
    { sOpenDocuments        :: !(S.Set Compiler.NormalizedFilePath)
    , sOpenVirtualResources :: !(S.Set Compiler.VirtualResource)
    }

-- | Implementation handle
data IHandle p t = IHandle
    { ihState     :: !(IORef State)
    , ihLoggerH   :: !(Logger.Handle IO)
    , ihCompilerH :: !Compiler.IdeState
    }

------------------------------------------------------------------------
-- Request handlers
------------------------------------------------------------------------

handleRequest
    :: IHandle () LF.Package
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
    CodeLens params -> RspCodeLens . makeResponse <$> LS.CodeLens.handle loggerH compilerH params

    req -> do
        Logger.logWarning loggerH ("Method not found" <> T.pack (show req))
        pure $ RspError $ makeErrorResponse MethodNotFound


handleNotification :: LspFuncs () -> IHandle () LF.Package -> ServerNotification -> IO ()
handleNotification lspFuncs (IHandle stateRef loggerH compilerH) = \case

    DidOpenTextDocument (DidOpenTextDocumentParams item) -> do
        case URI.parseURI $ T.unpack $ getUri $ _uri (item :: TextDocumentItem) of
          Just uri
              | URI.uriScheme uri == "file:"
              -> handleDidOpenFile item

              | URI.uriScheme uri == "daml:"
              -> handleDidOpenVirtualResource uri

              | otherwise
              -> Logger.logWarning loggerH $ "Unknown scheme in URI: "
                    <> T.show uri

          _ -> Logger.logError loggerH $ "Invalid URI in DidOpenTextDocument: "
                    <> T.show (_uri (item :: TextDocumentItem))

    DidChangeTextDocument (DidChangeTextDocumentParams docId _) -> do
        let uri = _uri (docId :: VersionedTextDocumentIdentifier)

        case Compiler.uriToFilePath' uri of
          Just (Compiler.toNormalizedFilePath -> filePath) -> do
            mbVirtual <- getVirtualFileFunc lspFuncs $ toNormalizedUri uri
            let contents = maybe "" (Rope.toText . (_text :: VirtualFile -> Rope.Rope)) mbVirtual
            Compiler.onFileModified compilerH filePath (Just contents)
            Logger.logInfo loggerH
              $ "Updated text document: " <> T.show (Compiler.fromNormalizedFilePath filePath)

          Nothing ->
            Logger.logError loggerH
              $ "Invalid file path: " <> T.show (_uri (docId :: VersionedTextDocumentIdentifier))

    DidCloseTextDocument (DidCloseTextDocumentParams (TextDocumentIdentifier uri)) ->
        case URI.parseURI $ T.unpack $ getUri uri of
          Just uri'
              | URI.uriScheme uri' == "file:" -> do
                    Just fp <- pure $ Compiler.toNormalizedFilePath <$> Compiler.uriToFilePath' uri
                    handleDidCloseFile fp
              | URI.uriScheme uri' == "daml:" -> handleDidCloseVirtualResource uri'
              | otherwise -> Logger.logWarning loggerH $ "Unknown scheme in URI: " <> T.show uri

          _ -> Logger.logError loggerH
                 $    "Invalid URI in DidCloseTextDocument: "
                   <> T.show uri

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
        Compiler.onFileModified compilerH filePath (Just contents)

        -- Update the list of open files
        Compiler.setFilesOfInterest compilerH (S.toList documents)

        Logger.logInfo loggerH $ "Opened text document: " <> T.show filePath

    handleDidOpenVirtualResource uri = do
         case Compiler.uriToVirtualResource uri of
           Nothing -> do
               Logger.logWarning loggerH $ "Failed to parse virtual resource URI: " <> T.show uri
               pure ()
           Just vr -> do
               Logger.logInfo loggerH $ "Opened virtual resource: " <> T.show vr
               resources <- atomicModifyIORef' stateRef $
                 \state -> let resources = S.insert vr $ sOpenVirtualResources state
                           in ( state { sOpenVirtualResources = resources }
                              , resources
                              )
               Compiler.setOpenVirtualResources compilerH $ S.toList resources

    handleDidCloseFile filePath = do
         Logger.logInfo loggerH $ "Closed text document: " <> T.show (Compiler.fromNormalizedFilePath filePath)
         documents <- atomicModifyIORef' stateRef $
           \state -> let documents = S.delete filePath $ sOpenDocuments state
                     in ( state { sOpenDocuments = documents }
                        , documents
                        )
         Compiler.setFilesOfInterest compilerH (S.toList documents)
         Compiler.onFileModified compilerH filePath Nothing

    handleDidCloseVirtualResource uri = do
        Logger.logInfo loggerH $ "Closed virtual resource: " <> T.show uri
        case Compiler.uriToVirtualResource uri of
           Nothing -> do
               Logger.logWarning loggerH "Failed to parse virtual resource URI!"
               pure ()
           Just vr -> do
               resources <- atomicModifyIORef' stateRef $
                 \state -> let resources = S.delete vr $ sOpenVirtualResources state
                           in (state { sOpenVirtualResources = resources }
                              , resources
                              )
               Compiler.setOpenVirtualResources compilerH $ S.toList resources

------------------------------------------------------------------------
-- Server execution
------------------------------------------------------------------------

runLanguageServer
    :: Logger.Handle IO
    -> ((FromServerMessage -> IO ()) -> VFSHandle -> IO Compiler.IdeState)
    -> IO ()
runLanguageServer loggerH getIdeState = do
    sdkVersion <- liftIO (getSdkVersion `catchIO` const (pure "Unknown (not started via the assistant)"))
    liftIO $ Logger.logInfo loggerH (T.pack $ "SDK version: " <> sdkVersion)
    state     <- liftIO $ newIORef $ State S.empty S.empty
    let getHandlers lspFuncs = do
            compilerH <- getIdeState (sendFunc lspFuncs) (makeLSPVFSHandle lspFuncs)
            let ihandle = IHandle
                    { ihState = state
                    , ihLoggerH = loggerH
                    , ihCompilerH = compilerH
                    }
            pure $ Handlers (handleRequest ihandle) (handleNotification lspFuncs ihandle)
    liftIO $ runServer loggerH getHandlers
