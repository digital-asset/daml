-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeFamilies #-}

module DA.Cli.Damlc.Command.MultiIde.Handlers (subIdeMessageHandler, clientMessageHandler) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.STM.TMVar
import Control.Concurrent.MVar
import Control.Lens
import Control.Monad
import Control.Monad.STM
import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Types as Aeson
import qualified Data.ByteString as B
import DA.Cli.Damlc.Command.MultiIde.ClientCommunication
import DA.Cli.Damlc.Command.MultiIde.Forwarding
import DA.Cli.Damlc.Command.MultiIde.OpenFiles
import DA.Cli.Damlc.Command.MultiIde.PackageData
import DA.Cli.Damlc.Command.MultiIde.Parsing
import DA.Cli.Damlc.Command.MultiIde.Prefixing
import DA.Cli.Damlc.Command.MultiIde.SdkInstall
import DA.Cli.Damlc.Command.MultiIde.SubIdeManagement
import DA.Cli.Damlc.Command.MultiIde.Types
import DA.Cli.Damlc.Command.MultiIde.Util
import DA.Cli.Damlc.Command.MultiIde.DarDependencies (resolveSourceLocation, unpackDar, unpackedDarsLocation)
import DA.Daml.LanguageServer.SplitGotoDefinition
import Data.Foldable (traverse_)
import Data.List (find, isInfixOf)
import qualified Data.Map as Map
import Data.Maybe (fromMaybe, mapMaybe)
import qualified Language.LSP.Types as LSP
import qualified Language.LSP.Types.Lens as LSP
import System.Exit (exitSuccess)
import System.FilePath.Posix (takeDirectory, takeExtension, takeFileName)

parseCustomResult :: Aeson.FromJSON a => String -> Either LSP.ResponseError Aeson.Value -> Either LSP.ResponseError a
parseCustomResult name =
  fmap $ either (\err -> error $ "Failed to parse response of " <> name <> ": " <> err) id 
    . Aeson.parseEither Aeson.parseJSON

resolveAndUnpackSourceLocation :: MultiIdeState -> PackageSourceLocation -> IO PackageHome
resolveAndUnpackSourceLocation miState pkgSource = do
  (pkgPath, mDarPath) <- resolveSourceLocation miState pkgSource
  forM_ mDarPath $ \darPath -> do
    -- Must shutdown existing IDE first, since folder could be deleted
    -- If no IDE exists, shutdown is a no-op
    logDebug miState $ "Shutting down existing unpacked dar at " <> unPackageHome pkgPath
    shutdownIdeByHome miState pkgPath
    unpackDar miState darPath
  pure pkgPath

-- Handlers

subIdeMessageHandler :: MultiIdeState -> IO () -> SubIdeInstance -> B.ByteString -> IO ()
subIdeMessageHandler miState unblock ide bs = do
  logInfo miState $ "Got new message from " <> unPackageHome (ideHome ide)

  -- Decode a value, parse
  let val :: Aeson.Value
      val = er "eitherDecode" $ Aeson.eitherDecodeStrict bs
  mMsg <- either error id <$> parseServerMessageWithTracker (misFromClientMethodTrackerVar miState) (ideHome ide) val

  -- Adds the various prefixes needed for from server messages to not clash with those from other IDEs
  let prefixer :: LSP.FromServerMessage -> LSP.FromServerMessage
      prefixer = 
        addProgressTokenPrefixToServerMessage (ideMessageIdPrefix ide)
          . addLspPrefixToServerMessage ide
      mPrefixedMsg :: Maybe LSP.FromServerMessage
      mPrefixedMsg = prefixer <$> mMsg

  forM_ mPrefixedMsg $ \msg -> do
    -- If its a request (builtin or custom), save it for response handling.
    putFromServerMessage miState (ideHome ide) msg

    logDebug miState "Message successfully parsed and prefixed."
    case msg of
      LSP.FromServerRsp LSP.SInitialize LSP.ResponseMessage {_result} -> do
        logDebug miState "Got initialization reply, sending initialized and unblocking"
        holdingIDEsAtomic miState $ \ides -> do
          let ideData = lookupSubIde (ideHome ide) ides
          sendPackageDiagnostic miState ideData
          unsafeSendSubIdeSTM ide $ LSP.FromClientMess LSP.SInitialized $ LSP.NotificationMessage "2.0" LSP.SInitialized (Just LSP.InitializedParams)
        unblock
      LSP.FromServerRsp LSP.SShutdown (LSP.ResponseMessage {_id}) | maybe False isCoordinatorShutdownLspId _id -> handleExit miState ide

      -- See STextDocumentDefinition in client handle for description of this path
      LSP.FromServerRsp (LSP.SCustomMethod "daml/tryGetDefinition") LSP.ResponseMessage {_id, _result} -> do
        logInfo miState "Got tryGetDefinition response, handling..."
        let parsedResult = parseCustomResult @(Maybe TryGetDefinitionResult) "daml/tryGetDefinition" _result
            reply :: Either LSP.ResponseError (LSP.ResponseResult 'LSP.TextDocumentDefinition) -> IO ()
            reply rsp = do
              logDebug miState $ "Replying directly to client with " <> show rsp
              sendClient miState $ LSP.FromServerRsp LSP.STextDocumentDefinition $ LSP.ResponseMessage "2.0" (castLspId <$> _id) rsp
            replyLocations :: [LSP.Location] -> IO ()
            replyLocations = reply . Right . LSP.InR . LSP.InL . LSP.List
        case parsedResult of
          -- Request failed, forward error
          Left err -> reply $ Left err
          -- Request didn't find any location information, forward "nothing"
          Right Nothing -> replyLocations []
          -- SubIde containing the reference also contained the definition, so returned no name to lookup
          -- Simply forward this location
          Right (Just (TryGetDefinitionResult loc Nothing)) -> replyLocations [loc]
          -- SubIde containing the reference did not contain the definition, it returns a fake location in .daml and the name
          -- Send a new request to a new SubIde to find the source of this name
          Right (Just (TryGetDefinitionResult loc (Just name))) -> do
            logDebug miState $ "Got name in result! Backup location is " <> show loc
            mSourceLocation <- Map.lookup (UnitId $ tgdnPackageUnitId name) <$> atomically (readTMVar $ misMultiPackageMappingVar miState)
            case mSourceLocation of
              -- Didn't find a home for this name, we do not know where this is defined, so give back the (known to be wrong)
              -- .daml data-dependency path
              -- This is the worst case, we'll later add logic here to unpack and spinup an SubIde for the read-only dependency
              Nothing -> replyLocations [loc]
              -- We found a daml.yaml for this definition, send the getDefinitionByName request to its SubIde
              Just sourceLocation -> do
                home <- resolveAndUnpackSourceLocation miState sourceLocation
                logDebug miState $ "Found unit ID in multi-package mapping, forwarding to " <> unPackageHome home
                let method = LSP.SCustomMethod "daml/gotoDefinitionByName"
                    lspId = maybe (error "No LspId provided back from tryGetDefinition") castLspId _id
                    msg = LSP.FromClientMess method $ LSP.ReqMess $
                      LSP.RequestMessage "2.0" lspId method $ Aeson.toJSON $
                        GotoDefinitionByNameParams loc name
                sendSubIdeByPath miState (unPackageHome home) msg
      
      -- See STextDocumentDefinition in client handle for description of this path
      LSP.FromServerRsp (LSP.SCustomMethod "daml/gotoDefinitionByName") LSP.ResponseMessage {_id, _result} -> do
        logDebug miState "Got gotoDefinitionByName response, handling..."
        let parsedResult = parseCustomResult @GotoDefinitionByNameResult "daml/gotoDefinitionByName" _result
            reply :: Either LSP.ResponseError (LSP.ResponseResult 'LSP.TextDocumentDefinition) -> IO ()
            reply rsp = do
              logDebug miState $ "Replying directly to client with " <> show rsp
              sendClient miState $ LSP.FromServerRsp LSP.STextDocumentDefinition $ LSP.ResponseMessage "2.0" (castLspId <$> _id) rsp
        case parsedResult of
          Left err -> reply $ Left err
          Right loc -> reply $ Right $ LSP.InR $ LSP.InL $ LSP.List [loc]

      LSP.FromServerMess method _ -> do
        logDebug miState $ "Backwarding request " <> show method <> ":\n" <> show msg
        sendClient miState msg
      LSP.FromServerRsp method _ -> do
        logDebug miState $ "Backwarding response to " <> show method <> ":\n" <> show msg
        sendClient miState msg

handleOpenFilesNotification 
  :: forall (m :: LSP.Method 'LSP.FromClient 'LSP.Notification)
  .  MultiIdeState
  -> LSP.NotificationMessage m
  -> FilePath
  -> IO ()
handleOpenFilesNotification miState mess path = atomically $ case (mess ^. LSP.method, takeExtension path) of
  (LSP.STextDocumentDidOpen, ".daml") -> do
    home <- getSourceFileHome miState path
    addOpenFile miState home $ DamlFile path
  (LSP.STextDocumentDidClose, ".daml") -> do
    home <- getSourceFileHome miState path
    removeOpenFile miState home $ DamlFile path
    -- Also remove from the source mapping, in case project structure changes while we're not tracking the file
    sourceFileHomeHandleDamlFileDeleted miState path
  _ -> pure ()

clientMessageHandler :: MultiIdeState -> IO () -> B.ByteString -> IO ()
clientMessageHandler miState unblock bs = do
  logInfo miState "Got new message from client"

  -- Decode a value, parse
  let castFromClientMessage :: LSP.FromClientMessage' SMethodWithSender -> LSP.FromClientMessage
      castFromClientMessage = \case
        LSP.FromClientMess method params -> LSP.FromClientMess method params
        LSP.FromClientRsp (SMethodWithSender method _) params -> LSP.FromClientRsp method params

      val :: Aeson.Value
      val = er "eitherDecode" $ Aeson.eitherDecodeStrict bs

  unPrefixedMsg <- either error id <$> parseClientMessageWithTracker (misFromServerMethodTrackerVar miState) val
  let msg = addProgressTokenPrefixToClientMessage unPrefixedMsg

  case msg of
    -- Store the initialize params for starting subIdes, respond statically with what ghc-ide usually sends.
    LSP.FromClientMess LSP.SInitialize LSP.RequestMessage {_id, _method, _params} -> do
      putMVar (misInitParamsVar miState) _params
      -- Send initialized out first (skipping the queue), then unblock for other messages
      sendClientFirst miState $ LSP.FromServerRsp _method $ LSP.ResponseMessage "2.0" (Just _id) (Right initializeResult)
      unblock

      -- Register watchers for daml.yaml, multi-package.yaml and *.dar files
      putFromServerCoordinatorMessage miState registerFileWatchersMessage
      sendClient miState registerFileWatchersMessage

    LSP.FromClientMess LSP.SWindowWorkDoneProgressCancel notif -> do
      let (newNotif, mPrefix) = stripWorkDoneProgressCancelTokenPrefix notif
          newMsg = LSP.FromClientMess LSP.SWindowWorkDoneProgressCancel newNotif
      -- Find IDE with the correct prefix, send to it if it exists. If it doesn't, the message can be thrown away.
      case mPrefix of
        Nothing -> void $ sendAllSubIdes miState newMsg
        Just prefix -> holdingIDEsAtomic miState $ \ides ->
          let mIde = find (\ideData -> (ideMessageIdPrefix <$> ideDataMain ideData) == Just prefix) ides
           in traverse_ (`unsafeSendSubIdeSTM` newMsg) $ mIde >>= ideDataMain

    LSP.FromClientMess (LSP.SCustomMethod t) (LSP.NotMess notif) | t == damlSdkInstallCancelMethod ->
      handleSdkInstallClientCancelled miState notif

    -- Special handing for STextDocumentDefinition to ask multiple IDEs (the W approach)
    -- When a getDefinition is requested, we cast this request into a tryGetDefinition
    -- This is a method that will take the same logic path as getDefinition, but will also return an
    -- identifier in the cases where it knows the identifier wasn't defined in the package that referenced it
    -- When we receive this name, we lookup against the multi-package.yaml for a package that matches where the identifier
    -- came from. If we find one, we ask (and create if needed) the SubIde that contains the identifier where its defined.
    -- (this is via the getDefinitionByName message)
    -- We also send the backup known incorrect location from the tryGetDefinition, such that if the subIde containing the identifier
    -- can't find the definition, it'll fall back to the known incorrect location.
    -- Once we have this, we return it as a response to the original STextDocumentDefinition request.
    LSP.FromClientMess LSP.STextDocumentDefinition req@LSP.RequestMessage {_id, _method, _params} -> do
      let path = filePathFromParamsWithTextDocument miState req
          lspId = castLspId _id
          method = LSP.SCustomMethod "daml/tryGetDefinition"
          msg = LSP.FromClientMess method $ LSP.ReqMess $
            LSP.RequestMessage "2.0" lspId method $ Aeson.toJSON $
              TryGetDefinitionParams (_params ^. LSP.textDocument) (_params ^. LSP.position)
      logDebug miState "forwarding STextDocumentDefinition as daml/tryGetDefinition"
      sendSubIdeByPath miState path msg

    -- Watched file changes, used for restarting subIdes and changing coordinator state
    LSP.FromClientMess LSP.SWorkspaceDidChangeWatchedFiles msg@LSP.NotificationMessage {_params = LSP.DidChangeWatchedFilesParams (LSP.List changes)} -> do
      let changedPaths =
            mapMaybe (\event -> do
              path <- LSP.uriToFilePath $ event ^. LSP.uri
              -- Filter out any changes to unpacked dars, no reloading logic should happen there
              guard $ not $ unpackedDarsLocation miState `isInfixOf` path
              pure (path ,event ^. LSP.xtype)
            ) changes
      forM_ changedPaths $ \(changedPath, changeType) ->
        case takeFileName changedPath of
          "daml.yaml" -> do
            let home = PackageHome $ takeDirectory changedPath
            logInfo miState $ "daml.yaml change in " <> unPackageHome home <> ". Shutting down IDE"
            atomically $ sourceFileHomeHandleDamlYamlChanged miState home
            allowIdeSdkInstall miState home
            case changeType of
              LSP.FcDeleted -> do
                shutdownIdeByHome miState home
                handleRemovedPackageOpenFiles miState home
              LSP.FcCreated -> do
                handleCreatedPackageOpenFiles miState home
                rebootIdeByHome miState home    
              LSP.FcChanged -> rebootIdeByHome miState home
            void $ updatePackageData miState

          "multi-package.yaml" -> do
            logInfo miState "multi-package.yaml change."
            void $ updatePackageData miState
          _ | takeExtension changedPath == ".dar" -> do
            let darFile = DarFile changedPath
            logInfo miState $ ".dar file changed: " <> changedPath
            idesToShutdown <- fromMaybe mempty . Map.lookup darFile <$> atomically (readTMVar $ misDarDependentPackagesVar miState)
            logDebug miState $ "Shutting down following ides: " <> show idesToShutdown
            traverse_ (lenientRebootIdeByHome miState) idesToShutdown

            void $ updatePackageData miState
          -- for .daml, we remove entry from the sourceFileHome cache if the file is deleted (note that renames/moves are sent as delete then create)
          _ | takeExtension changedPath == ".daml" && changeType == LSP.FcDeleted -> atomically $ sourceFileHomeHandleDamlFileDeleted miState changedPath
          _ -> pure ()
      logDebug miState "all not on filtered DidChangeWatchedFilesParams"
      -- Filter down to only daml files and send those
      let damlOnlyChanges = filter (maybe False (\path -> takeExtension path == ".daml") . LSP.uriToFilePath . view LSP.uri) changes
      sendAllSubIdes_ miState $ LSP.FromClientMess LSP.SWorkspaceDidChangeWatchedFiles $ LSP.params .~ LSP.DidChangeWatchedFilesParams (LSP.List damlOnlyChanges) $ msg

    LSP.FromClientMess LSP.SExit _ -> do
      ides <- atomically $ readTMVar $ misSubIdesVar miState
      traverse_ (handleExit miState) $ Map.mapMaybe ideDataMain ides
      -- Wait half a second for all the exit messages to be sent
      threadDelay 500_000
      exitSuccess

    LSP.FromClientMess meth params ->
      case getMessageForwardingBehaviour miState meth params of
        ForwardRequest mess (Single path) -> do
          logDebug miState $ "single req on method " <> show meth <> " over path " <> path
          let LSP.RequestMessage {_id, _method} = mess
              msg' = castFromClientMessage msg
          sendSubIdeByPath miState path msg'

        ForwardRequest mess (AllRequest combine) -> do
          logDebug miState $ "all req on method " <> show meth
          let LSP.RequestMessage {_id, _method} = mess
              msg' = castFromClientMessage msg
          ides <- sendAllSubIdes miState msg'
          if null ides 
            then sendClient miState $ LSP.FromServerRsp _method $ LSP.ResponseMessage "2.0" (Just _id) $ combine []
            else putReqMethodAll (misFromClientMethodTrackerVar miState) _id _method msg' ides combine

        ForwardNotification mess (Single path) -> do
          logDebug miState $ "single not on method " <> show meth <> " over path " <> path
          handleOpenFilesNotification miState mess path
          -- Notifications aren't stored, so failure to send can be ignored
          sendSubIdeByPath miState path (castFromClientMessage msg)

        ForwardNotification _ AllNotification -> do
          logDebug miState $ "all not on method " <> show meth
          sendAllSubIdes_ miState (castFromClientMessage msg)

        ExplicitHandler handler -> do
          logDebug miState "calling explicit handler"
          handler (sendClient miState) (sendSubIdeByPath miState)
    -- Responses to subIdes
    LSP.FromClientRsp (SMethodWithSender method (Just home)) rMsg -> 
      -- If a response fails, failure is acceptable as the subIde can't be expecting a response if its dead
      sendSubIdeByPath miState (unPackageHome home) $ LSP.FromClientRsp method $ 
        rMsg & LSP.id %~ fmap stripLspPrefix
    -- Responses to coordinator
    LSP.FromClientRsp (SMethodWithSender method Nothing) LSP.ResponseMessage {_id, _result} ->
      case (method, _id) of
        (LSP.SClientRegisterCapability, Just (LSP.IdString "MultiIdeWatchedFiles")) ->
          either (\err -> logError miState $ "Watched file registration failed with " <> show err) (const $ logDebug miState "Successfully registered watched files") _result
        (LSP.SWindowShowMessageRequest, Just lspId) -> handleSdkInstallPromptResponse miState lspId _result
        _ -> pure ()
