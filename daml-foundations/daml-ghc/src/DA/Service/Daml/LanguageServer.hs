-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude  #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE TemplateHaskell    #-}

module DA.Service.Daml.LanguageServer
    ( runLanguageServer
    ) where

import           Control.Concurrent                        (threadDelay)
import qualified Control.Concurrent.Async                  as Async
import           Control.Concurrent.STM                    (STM, TChan, atomically, newTChanIO,
                                                            readTChan, writeTChan)
import qualified Control.Monad.Managed                     as Managed

import           DA.LanguageServer.Protocol
import           DA.LanguageServer.Server

import           DA.Prelude
import qualified DA.Daml.LF.Ast as LF
import qualified DA.Service.Daml.Compiler.Impl.Handle as Compiler
import qualified DA.Service.Daml.LanguageServer.CodeLens   as LS.CodeLens
import           DA.Service.Daml.LanguageServer.Common
import qualified DA.Service.Daml.LanguageServer.Definition as LS.Definition
import qualified DA.Service.Daml.LanguageServer.Hover      as LS.Hover
import qualified DA.Service.Logger                         as Logger

import qualified Data.Aeson                                as Aeson
import           Data.Aeson.TH.Extended                    (deriveDAToJSON)
import           Data.IORef                                (IORef, atomicModifyIORef', newIORef)
import qualified Data.Set                                  as S
import qualified Data.Text.Extended                        as T

import qualified Development.IDE.Types.Diagnostics as Compiler
import           Development.IDE.Types.LSP as Compiler

import qualified Network.URI                               as URI

import qualified System.Exit

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
    }

deriveDAToJSON "_vrcp" ''VirtualResourceChangedParams

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
-- Server capabilities
------------------------------------------------------------------------

serverCapabilities :: ServerCapabilities
serverCapabilities = ServerCapabilities
    { scTextDocumentSync                 = Just SyncFull
    , scHoverProvider                    = True
    , scCompletionProvider               = Nothing
    , scSignatureHelpProvider            = Nothing
    , scDefinitionProvider               = True
    , scReferencesProvider               = False
    , scDocumentHighlightProvider        = False
    , scDocumentSymbolProvider           = False
    , scWorkspaceSymbolProvider          = False
    , scCodeActionProvider               = False
    , scCodeLensProvider                 = True
    , scDocumentFormattingProvider       = False
    , scDocumentRangeFormattingProvider  = False
    , scDocumentOnTypeFormattingProvider = False
    , scRenameProvider                   = False
    }

------------------------------------------------------------------------
-- Request handlers
------------------------------------------------------------------------

handleRequest :: IHandle () LF.Package -> ServerRequest -> IO (Either ServerError Aeson.Value)
handleRequest (IHandle _stateRef loggerH compilerH _notifChan) = \case
    Initialize _params -> do
        pure $ Right $ Aeson.toJSON $ InitializeResult serverCapabilities

    Shutdown -> do
      Logger.logInfo loggerH "Shutdown request received, terminating."
      System.Exit.exitSuccess

    KeepAlive ->
      pure $ Right Aeson.Null

    Definition      params -> LS.Definition.handle         loggerH compilerH params
    Hover           params -> LS.Hover.handle              loggerH compilerH params
    CodeLens        params -> LS.CodeLens.handle           loggerH compilerH params

    req -> do
        Logger.logJson loggerH Logger.Warning ("Method not found" :: T.Text, req)
        pure $ Left MethodNotFound


handleNotification :: IHandle () LF.Package -> ServerNotification -> IO ()
handleNotification (IHandle stateRef loggerH compilerH _notifChan) = \case

    DidOpenTextDocument (DidOpenTextDocumentParams item) ->
        case URI.parseURI $ T.unpack $ unTagged $ tdiUri item of
          Just uri
              | URI.uriScheme uri == "file:"
              -> handleDidOpenFile (URI.unEscapeString (URI.uriPath uri)) (tdiText item)

              | URI.uriScheme uri == "daml:"
              -> handleDidOpenVirtualResource uri

              | otherwise
              -> Logger.logWarning loggerH $ "Unknown scheme in URI: "
                    <> T.show uri

          _ -> Logger.logError loggerH $ "Invalid URI in DidOpenTextDocument: "
                    <> T.show (tdiUri item)

    DidChangeTextDocument (DidChangeTextDocumentParams docId changes) ->
        case documentUriToFilePath $ vtdiUri docId of
          Just filePath -> do
            -- ISSUE DEL-3281: Add support for incremental synchronisation
            -- to language server.
            let newContents = tdcceText <$> lastMay changes
            Compiler.onFileModified compilerH filePath newContents

            Logger.logInfo loggerH
              $ "Updated text document: " <> T.show filePath

          Nothing ->
            Logger.logError loggerH
              $ "Invalid file path: " <> T.show (vtdiUri docId)

    DidCloseTextDocument (DidCloseTextDocumentParams docId) ->
        case URI.parseURI $ T.unpack $ unTagged $ tdidUri docId of
          Just uri
              | URI.uriScheme uri == "file:" -> handleDidCloseFile (URI.unEscapeString $ URI.uriPath uri)
              | URI.uriScheme uri == "daml:" -> handleDidCloseVirtualResource uri
              | otherwise -> Logger.logWarning loggerH $ "Unknown scheme in URI: " <> T.show uri

          _ -> Logger.logError loggerH
                 $    "Invalid URI in DidCloseTextDocument: "
                   <> T.show (tdidUri docId)

    DidSaveTextDocument _params ->
      pure ()

    UnknownNotification _method _params -> return ()
  where
    -- Note that the state changes here are not atomic.
    -- When we have parallel compilation we could manage the state
    -- changes in STM so that we can atomically change the state.
    -- Internally it should be done via the IO oracle. See PROD-2808.
    handleDidOpenFile filePath contents = do
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

runLanguageServer  :: (  Maybe (Compiler.Event -> STM ())
                      -> Logger.Handle IO
                      -> Managed.Managed Compiler.IdeState
                      )
                   -> Logger.Handle IO
                   -> IO ()
runLanguageServer handleBuild loggerH = Managed.runManaged $ do
    notifChan <- liftIO newTChanIO
    eventChan <- liftIO newTChanIO
    state     <- liftIO $ newIORef $ State S.empty S.empty
    compilerH <- handleBuild (Just (writeTChan eventChan)) loggerH

    let ihandle = IHandle {
        ihState = state
      , ihLoggerH = loggerH
      , ihCompilerH = compilerH
      , ihNotifChan = notifChan
      }
    liftIO $ Async.race_
      (eventSlinger loggerH eventChan notifChan)
      (Async.race_
        (forever $ do
            -- Send keep-alive notifications once a second in order to detect
            -- when the parent has exited.
            threadDelay (1000*1000)
            atomically $ writeTChan notifChan
              $ CustomNotification "daml/keepAlive"
              $ Aeson.object []
        )
        (runServer loggerH (handleRequest ihandle) (handleNotification ihandle) notifChan)
      )

-- | Event slinger slings compiler events to the client as notifications.
eventSlinger
    :: Logger.Handle IO
    -> TChan Compiler.Event
    -> TChan ClientNotification
    -> IO ()
eventSlinger loggerH eventChan notifChan =
    forever $ do
        mbFatalErr <- atomically $ readTChan eventChan >>= \case
            Compiler.EventFileDiagnostics (uri, diags) -> do
                writeTChan notifChan
                    $ PublishDiagnostics
                    $ PublishDiagnosticsParams
                    (Tagged $ Compiler.getUri uri)
                    $ map convertDiagnostic
                    $ Compiler.getDiagnosticsFromStore diags
                pure Nothing

            Compiler.EventVirtualResourceChanged vr content -> do
                writeTChan notifChan
                    $ CustomNotification virtualResourceChangedNotification
                    $ Aeson.toJSON
                    $ VirtualResourceChangedParams (Compiler.virtualResourceToUri vr) content
                pure Nothing

            Compiler.EventFileValidation finishedValidations totalValidations -> do
                writeTChan notifChan
                    $ CustomNotification workspaceValidationsNotification
                    $ Aeson.toJSON
                    $ WorkspaceValidationsParams finishedValidations totalValidations
                pure Nothing

            Compiler.EventFatalError err ->
                pure (Just err)

        case mbFatalErr of
          Just err -> do
            Logger.logError loggerH $ "Fatal error in compiler: " <> err
            System.Exit.exitFailure
          Nothing ->
            pure ()
