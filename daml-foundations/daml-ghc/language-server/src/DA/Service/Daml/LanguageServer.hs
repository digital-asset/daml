-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell    #-}

module DA.Service.Daml.LanguageServer
    ( runLanguageServer
    , VirtualResourceChangedParams(..)
    ) where

import qualified Control.Concurrent.Async                  as Async
import           Control.Concurrent.STM                    (TChan, atomically, newTChanIO,
                                                            readTChan, writeTChan)
import Control.Exception.Safe

import           DA.LanguageServer.Protocol
import           DA.LanguageServer.Server

import Control.Monad
import Data.List.Extra
import Control.Monad.IO.Class
import qualified DA.Daml.LF.Ast as LF
import qualified DA.Service.Daml.Compiler.Impl.Handle as Compiler
import qualified DA.Service.Daml.LanguageServer.CodeLens   as LS.CodeLens
import qualified DA.Service.Daml.LanguageServer.Definition as LS.Definition
import qualified DA.Service.Daml.LanguageServer.Hover      as LS.Hover
import qualified DA.Service.Logger                         as Logger
import DAML.Project.Consts

import qualified Data.Aeson                                as Aeson
import           Data.Aeson.TH.Extended                    (deriveDAToJSON, deriveDAFromJSON)
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
    { sOpenDocuments        :: !(S.Set FilePath)
    , sOpenVirtualResources :: !(S.Set Compiler.VirtualResource)
    }

-- | Implementation handle
data IHandle p t = IHandle
    { ihState     :: !(IORef State)
    , ihLoggerH   :: !(Logger.Handle IO)
    , ihCompilerH :: !Compiler.IdeState
    , ihNotifChan :: !(TChan ClientNotification)
    -- ^ Channel to send notifications to the client.
    }

 ------------------------------------------------------------------------------
-- Defaults
------------------------------------------------------------------------------

-- | Virtual resource changed notification
-- This notification is sent by the server to the client when
-- an open virtual resource changes.
virtualResourceChangedNotification :: T.Text
virtualResourceChangedNotification = "daml/virtualResource/didChange"

-- | Parameters for the virtual resource changed notification
data VirtualResourceChangedParams = VirtualResourceChangedParams
    { _vrcpUri      :: !T.Text
      -- ^ The uri of the virtual resource.
    , _vrcpContents :: !T.Text
      -- ^ The new contents of the virtual resource.
    } deriving Show

deriveDAToJSON "_vrcp" ''VirtualResourceChangedParams
deriveDAFromJSON "_vrcp" ''VirtualResourceChangedParams

-- | Information regarding validations done for a DAML workspace.
workspaceValidationsNotification :: T.Text
workspaceValidationsNotification = "daml/workspace/validations"

-- | Parameters to update the client about the number of files that have been updated.
data WorkspaceValidationsParams = WorkspaceValidationsParams
    { _wvpFinishedValidations :: !Int
      -- ^ Tracks the number of validations we have already finished.
    , _wvpTotalValidations    :: !Int
      -- ^ Tracks the number of total validation steps we need to perform.
    }

deriveDAToJSON "_wvp" ''WorkspaceValidationsParams

------------------------------------------------------------------------
-- Request handlers
------------------------------------------------------------------------

handleRequest
    :: IHandle () LF.Package
    -> (forall resp. resp -> ResponseMessage resp)
    -> (ErrorCode -> ResponseMessage ())
    -> ServerRequest
    -> IO FromServerMessage
handleRequest (IHandle _stateRef loggerH compilerH _notifChan) makeResponse makeErrorResponse = \case
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
handleNotification lspFuncs (IHandle stateRef loggerH compilerH _notifChan) = \case

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
          Just filePath -> do
            mbVirtual <- getVirtualFileFunc lspFuncs uri
            let contents = maybe "" (Rope.toText . (_text :: VirtualFile -> Rope.Rope)) mbVirtual
            Compiler.onFileModified compilerH filePath (Just contents)
            Logger.logInfo loggerH
              $ "Updated text document: " <> T.show filePath

          Nothing ->
            Logger.logError loggerH
              $ "Invalid file path: " <> T.show (_uri (docId :: VersionedTextDocumentIdentifier))

    DidCloseTextDocument (DidCloseTextDocumentParams (TextDocumentIdentifier uri)) ->
        case URI.parseURI $ T.unpack $ getUri uri of
          Just uri'
              | URI.uriScheme uri' == "file:" -> do
                    Just fp <- pure $ Compiler.uriToFilePath' uri
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
        Just filePath <- pure $ Compiler.uriToFilePath' uri
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
         Logger.logInfo loggerH $ "Closed text document: " <> T.show filePath
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
    -> ((Compiler.Event -> IO ()) -> VFSHandle -> IO Compiler.IdeState)
    -> IO ()
runLanguageServer loggerH getIdeState = do
    sdkVersion <- liftIO (getSdkVersion `catchIO` const (pure "Unknown (not started via the assistant)"))
    liftIO $ Logger.logInfo loggerH (T.pack $ "SDK version: " <> sdkVersion)
    notifChan <- liftIO newTChanIO
    eventChan <- liftIO newTChanIO
    state     <- liftIO $ newIORef $ State S.empty S.empty
    let getHandlers lspFuncs = do
            compilerH <- getIdeState (atomically . writeTChan eventChan) (makeLSPVFSHandle lspFuncs)
            let ihandle = IHandle
                    { ihState = state
                    , ihLoggerH = loggerH
                    , ihCompilerH = compilerH
                    , ihNotifChan = notifChan
                    }
            pure $ Handlers (handleRequest ihandle) (handleNotification lspFuncs ihandle)
    liftIO $ Async.race_
      (eventSlinger eventChan notifChan)
      (runServer loggerH getHandlers notifChan)

-- | Event slinger slings compiler events to the client as notifications.
eventSlinger
    :: TChan Compiler.Event
    -> TChan ClientNotification
    -> IO ()
eventSlinger eventChan notifChan =
    forever $
        atomically $ readTChan eventChan >>= \case
            Compiler.EventFileDiagnostics (fp, diags) -> do
                writeTChan notifChan
                    $ PublishDiagnostics
                    $ PublishDiagnosticsParams
                    (Compiler.filePathToUri' fp)
                    (List $ nubOrd diags)

            Compiler.EventVirtualResourceChanged vr content -> do
                writeTChan notifChan
                    $ CustomNotification virtualResourceChangedNotification
                    $ Aeson.toJSON
                    $ VirtualResourceChangedParams (Compiler.virtualResourceToUri vr) content

            Compiler.EventFileValidation finishedValidations totalValidations -> do
                writeTChan notifChan
                    $ CustomNotification workspaceValidationsNotification
                    $ Aeson.toJSON
                    $ WorkspaceValidationsParams finishedValidations totalValidations
