-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ApplicativeDo #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DisambiguateRecordFields #-}
{-# LANGUAGE GADTs #-}

module DA.Cli.Damlc.Command.MultiIde (runMultiIde) where

import qualified "zip-archive" Codec.Archive.Zip as Zip
import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (async, cancel, pollSTM)
import Control.Concurrent.STM.TChan
import Control.Concurrent.STM.TMVar
import Control.Concurrent.STM.TVar
import Control.Concurrent.MVar
import Control.Exception(SomeException, displayException, fromException, try)
import Control.Lens
import Control.Monad
import Control.Monad.STM
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.State.Strict (StateT, runStateT, gets, modify')
import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Types as Aeson
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as BSLC
import DA.Cli.Damlc.Command.MultiIde.Forwarding
import DA.Cli.Damlc.Command.MultiIde.Prefixing
import DA.Cli.Damlc.Command.MultiIde.Util
import DA.Cli.Damlc.Command.MultiIde.Parsing
import DA.Cli.Damlc.Command.MultiIde.Types
import DA.Cli.Damlc.Command.MultiIde.DarDependencies (resolveSourceLocation, unpackDar, unpackedDarsLocation)
import DA.Daml.LanguageServer.SplitGotoDefinition
import DA.Daml.LF.Reader (DalfManifest(..), readDalfManifest)
import DA.Daml.Package.Config (MultiPackageConfigFields(..), findMultiPackageConfig, withMultiPackageConfig)
import DA.Daml.Project.Consts (projectConfigName)
import DA.Daml.Project.Types (ProjectPath (..))
import qualified DA.Service.Logger as Logger
import Data.Either (lefts)
import Data.Either.Extra (eitherToMaybe)
import Data.Foldable (traverse_)
import Data.Functor.Product
import Data.List (find, isInfixOf)
import qualified Data.Map as Map
import Data.Maybe (catMaybes, fromMaybe, isJust, mapMaybe, maybeToList)
import qualified Data.Set as Set
import qualified Data.Text as T
import qualified Data.Text.Extended as TE
import qualified Data.Text.IO as T
import Data.Time.Clock (getCurrentTime)
import GHC.Conc (unsafeIOToSTM)
import qualified Language.LSP.Types as LSP
import qualified Language.LSP.Types.Lens as LSP
import qualified SdkVersion.Class
import System.Directory (doesFileExist, getCurrentDirectory)
import System.Environment (getEnv, getEnvironment)
import System.Exit (exitSuccess)
import System.FilePath (takeDirectory, takeExtension, takeFileName, (</>))
import System.IO.Extra
import System.Process (getPid)
import System.Process.Typed (
  ExitCode (..),
  Process,
  createPipe,
  getExitCodeSTM,
  getStderr,
  getStdin,
  getStdout,
  proc,
  setEnv,
  setStderr,
  setStdin,
  setStdout,
  setWorkingDir,
  startProcess,
  unsafeProcessHandle,
 )

-- Spin-up logic

-- add IDE, send initialize, do not send further messages until we get the initialize response and have sent initialized
-- we can do this by locking the sending thread, but still allowing the channel to be pushed
-- we also atomically send a message to the channel, without dropping the lock on the subIDEs var
-- Note that messages sent here should _already_ be in the fromClientMessage tracker
addNewSubIDEAndSend
  :: MultiIdeState
  -> FilePath
  -> Maybe LSP.FromClientMessage
  -> IO ()
addNewSubIDEAndSend miState home mMsg = do
  logDebug miState "Trying to make a SubIDE"
  ides <- atomically $ takeTMVar $ subIDEsVar miState

  let ideData = lookupSubIde home ides
  case ideDataMain ideData of
    Just ide -> do
      logDebug miState "SubIDE already exists"
      forM_ mMsg $ unsafeSendSubIDE ide
      atomically $ putTMVar (subIDEsVar miState) ides
    Nothing | ideShouldDisable ideData || ideDataDisabled ideData -> do
      when (ideDataDisabled ideData) $ logDebug miState "SubIDE failed twice within 5 seconds, disabling SubIDE"

      responses <- getUnrespondedRequestsFallbackResponses miState home
      logDebug miState $ "Found " <> show (length responses) <> " unresponded messages, sending empty replies."
      
      -- Doesn't include mMsg, as if it was request, it'll already be in the tracker, so a reply for it will be in `responses`
      -- As such, we cannot send this on every failed message, 
      let ideData' = ideData {ideDataDisabled = True, ideDataFailTimes = []}
          -- Only add diagnostic messages for first fail to start.
          -- Diagnostic messages trigger the client to send a codeAction request, which would create an infinite loop if we sent
          -- diagnostics with its reply
          messages = responses <> if ideShouldDisable ideData then disableIdeDiagnosticMessages ideData home else []

      atomically $ do
        traverse_ (sendClientSTM miState) messages
        putTMVar (subIDEsVar miState) $ Map.insert home ideData' ides
    Nothing -> do
      logInfo miState $ "Creating new SubIDE for " <> home
      sendClient miState $ clearDiagnostics $ home </> "daml.yaml"

      unitId <- either (\cErr -> error $ "Failed to get unit ID from daml.yaml: " <> show cErr) fst <$> unitIdAndDepsFromDamlYaml home

      subIdeProcess <- runSubProc miState home
      let inHandle = getStdin subIdeProcess
          outHandle = getStdout subIdeProcess
          errHandle = getStderr subIdeProcess

      ideErrText <- newTVarIO @T.Text ""

      -- Handles blocking the sender thread until the IDE is initialized.
      (onceUnblocked, unblock) <- makeIOBlocker

      --           ***** -> SubIDE
      toSubIDEChan <- atomically newTChan
      toSubIDE <- async $ onceUnblocked $ forever $ do
        msg <- atomically $ readTChan toSubIDEChan
        logDebug miState "Pushing message to subIDE"
        putChunk inHandle msg

      --           Coord <- SubIDE
      subIDEToCoord <- async $ do
        -- Wait until our own IDE exists then pass it forward
        ide <- atomically $ fromMaybe (error "Failed to get own IDE") . ideDataMain . lookupSubIde home <$> readTMVar (subIDEsVar miState)
        onChunks outHandle $ subIDEMessageHandler miState unblock ide

      pid <- fromMaybe (error "SubIDE has no PID") <$> getPid (unsafeProcessHandle subIdeProcess)

      ideErrTextAsync <- async $
        let go = do
              text <- T.hGetChunk errHandle
              unless (text == "") $ do
                atomically $ modifyTVar' ideErrText (<> text)
                logDebug miState $ "[SubIDE " <> show pid <> "] " <> T.unpack text
                go
         in go

      mInitParams <- tryReadMVar (initParamsVar miState)
      let !initParams = fromMaybe (error "Attempted to create a SubIDE before initialization!") mInitParams
          initId = LSP.IdString $ T.pack $ show pid
          initMsg :: LSP.FromClientMessage
          initMsg = LSP.FromClientMess LSP.SInitialize LSP.RequestMessage 
            { _id = initId
            , _method = LSP.SInitialize
            , _params = initParams
                { LSP._rootPath = Just $ T.pack home
                , LSP._rootUri = Just $ LSP.filePathToUri home
                }
            , _jsonrpc = "2.0"
            }
          openFileMessage :: FilePath -> T.Text -> LSP.FromClientMessage
          openFileMessage path content = LSP.FromClientMess LSP.STextDocumentDidOpen LSP.NotificationMessage
            { _method = LSP.STextDocumentDidOpen
            , _params = LSP.DidOpenTextDocumentParams
              { _textDocument = LSP.TextDocumentItem
                { _uri = LSP.filePathToUri path
                , _languageId = "daml"
                , _version = 1
                , _text = content
                }
              }
            , _jsonrpc = "2.0"
            } 
          ide = 
            SubIDEInstance
              { ideInhandleAsync = toSubIDE
              , ideInHandle = inHandle
              , ideInHandleChannel = toSubIDEChan
              , ideOutHandleAsync = subIDEToCoord
              , ideErrText = ideErrText
              , ideErrTextAsync = ideErrTextAsync
              , ideProcess = subIdeProcess
              , ideHome = home
              , ideMessageIdPrefix = T.pack $ show pid
              , ideUnitId = unitId
              }
          ideData' = ideData {ideDataMain = Just ide}

      -- Must happen before the initialize message is added, else it'll delete that
      unrespondedRequests <- getUnrespondedRequestsToResend miState home

      logDebug miState "Sending init message to SubIDE"
      putSingleFromClientMessage miState home initMsg
      putChunk inHandle $ Aeson.encode initMsg

      -- Dangerous calls are okay here because we're already holding the subIDEsVar lock
      -- Send the open file notifications
      logDebug miState "Sending open files messages to SubIDE"
      forM_ (ideDataOpenFiles ideData') $ \path -> do
        content <- TE.readFileUtf8 path
        unsafeSendSubIDE ide $ openFileMessage path content
      

      -- Resend all pending requests
      -- No need for re-prefixing or anything like that, messages are stored with the prefixes they need
      -- Note that we want to remove the message we're sending from this list, to not send it twice
      let mMsgLspId = mMsg >>= fromClientRequestLspId
          requestsToResend = filter (\req -> fromClientRequestLspId req /= mMsgLspId) unrespondedRequests
      logDebug miState $ "Found " <> show (length requestsToResend) <> " unresponded messages, resending:\n"
        <> show (fmap (\r -> (fromClientRequestMethod r, fromClientRequestLspId r)) requestsToResend)

      traverse_ (unsafeSendSubIDE ide) requestsToResend

      logDebug miState $ "Sending intended message to SubIDE: " <> show ((\r -> (fromClientRequestMethod r, fromClientRequestLspId r)) <$> mMsg)
      -- Send the intended message
      forM_ mMsg $ unsafeSendSubIDE ide

      atomically $ putTMVar (subIDEsVar miState) $ Map.insert home ideData' ides

disableIdeDiagnosticMessages :: SubIDEData -> FilePath -> [LSP.FromServerMessage]
disableIdeDiagnosticMessages ideData home =
  fullFileDiagnostic 
    ( "GHCIDE refuses to start with the follow error:\n"
    <> fromMaybe "No information" (ideDataLastError ideData)
    )
    <$> ((home </> "daml.yaml") : Set.toList (ideDataOpenFiles ideData))

runSubProc :: MultiIdeState -> FilePath -> IO (Process Handle Handle Handle)
runSubProc miState home = do
  assistantPath <- getEnv "DAML_ASSISTANT"
  -- Need to remove some variables so the sub-assistant will pick them up from the working dir/daml.yaml
  assistantEnv <- filter (flip notElem ["DAML_PROJECT", "DAML_SDK_VERSION", "DAML_SDK"] . fst) <$> getEnvironment

  startProcess $
    proc assistantPath ("ide" : subIdeArgs miState) &
      setStdin createPipe &
      setStdout createPipe &
      setStderr createPipe &
      setWorkingDir home &
      setEnv assistantEnv

-- Spin-down logic

shutdownIdeByPath :: MultiIdeState -> FilePath -> IO ()
shutdownIdeByPath miState home = do
  ides <- atomically $ takeTMVar (subIDEsVar miState)
  ides' <- shutdownIdeWithLock miState ides (lookupSubIde home ides)
  atomically $ putTMVar (subIDEsVar miState) ides'

rebootIdeByPath :: MultiIdeState -> FilePath -> IO ()
rebootIdeByPath miState home = do
  shutdownIdeByPath miState home
  addNewSubIDEAndSend miState home Nothing

-- Version of rebootIdeByPath that only spins up IDEs that were either active, or disabled.
-- Does not spin up IDEs that were naturally shutdown/never started
lenientRebootIdeByPath :: MultiIdeState -> FilePath -> IO ()
lenientRebootIdeByPath miState home = do
  ides <- atomically $ takeTMVar (subIDEsVar miState)
  let ideData = lookupSubIde home ides
      shouldBoot = isJust (ideDataMain ideData) || ideDataDisabled ideData
  ides' <- shutdownIdeWithLock miState ides ideData
  atomically $ putTMVar (subIDEsVar miState) ides'
  when shouldBoot $ addNewSubIDEAndSend miState home Nothing

-- Sends a shutdown message and moves SubIDEInstance to `ideDataClosing`, disallowing any further client messages to be sent to the subIDE
-- given queue nature of TChan, all other pending messages will be sent first before handling shutdown
shutdownIde :: MultiIdeState -> SubIDEData -> IO ()
shutdownIde miState ideData = do
  ides <- atomically $ takeTMVar (subIDEsVar miState)
  ides' <- shutdownIdeWithLock miState ides ideData
  atomically $ putTMVar (subIDEsVar miState) ides'

-- Checks if a shutdown message LspId originated from the multi-ide coordinator
isCoordinatorShutdownLspId :: LSP.LspId 'LSP.Shutdown -> Bool
isCoordinatorShutdownLspId (LSP.IdString str) = "-shutdown" `T.isSuffixOf` str
isCoordinatorShutdownLspId _ = False

-- Core logic of shutdownIdeByPath and shutdownIde.
-- Sends the shutdown message, disables the SubIDEInstance, enables the SubIDEData (for future instances)
shutdownIdeWithLock :: MultiIdeState -> SubIDEs -> SubIDEData -> IO SubIDEs
shutdownIdeWithLock miState ides ideData = do
  case ideDataMain ideData of
    Just ide -> do
      let shutdownId = LSP.IdString $ ideMessageIdPrefix ide <> "-shutdown"
          shutdownMsg :: LSP.FromClientMessage
          shutdownMsg = LSP.FromClientMess LSP.SShutdown LSP.RequestMessage 
            { _id = shutdownId
            , _method = LSP.SShutdown
            , _params = LSP.Empty
            , _jsonrpc = "2.0"
            }
      
      logDebug miState $ "Sending shutdown message to " <> ideDataHome ideData

      putSingleFromClientMessage miState (ideDataHome ideData) shutdownMsg
      unsafeSendSubIDE ide shutdownMsg
      pure $ Map.adjust (\ideData' -> ideData' 
        { ideDataMain = Nothing
        , ideDataClosing = Set.insert ide $ ideDataClosing ideData
        , ideDataFailTimes = []
        , ideDataDisabled = False
        }) (ideDataHome ideData) ides
    Nothing ->
      pure $ Map.adjust (\ideData -> ideData {ideDataFailTimes = [], ideDataDisabled = False}) (ideDataHome ideData) ides

-- To be called once we receive the Shutdown response
-- Safe to assume that the sending channel is empty, so we can end the thread and send the final notification directly on the handle
handleExit :: MultiIdeState -> SubIDEInstance -> IO ()
handleExit miState ide = do
  let (exitMsg :: LSP.FromClientMessage) = LSP.FromClientMess LSP.SExit LSP.NotificationMessage
        { _method = LSP.SExit
        , _params = LSP.Empty
        , _jsonrpc = "2.0"
        }
  logDebug miState $ "Sending exit message to " <> ideHome ide
  -- This will cause the subIDE process to exit
  putChunk (ideInHandle ide) $ Aeson.encode exitMsg

-- Communication logic

-- Dangerous as does not hold the subIDEsVar lock. If a shutdown is called whiled this is running, the message may not be sent.
unsafeSendSubIDE :: SubIDEInstance -> LSP.FromClientMessage -> IO ()
unsafeSendSubIDE ide = atomically . unsafeSendSubIDESTM ide

unsafeSendSubIDESTM :: SubIDEInstance -> LSP.FromClientMessage -> STM ()
unsafeSendSubIDESTM ide = writeTChan (ideInHandleChannel ide) . Aeson.encode

sendClientSTM :: MultiIdeState -> LSP.FromServerMessage -> STM ()
sendClientSTM miState = writeTChan (toClientChan miState) . Aeson.encode

sendClient :: MultiIdeState -> LSP.FromServerMessage -> IO ()
sendClient miState = atomically . sendClientSTM miState

sendAllSubIDEs :: MultiIdeState -> LSP.FromClientMessage -> IO [FilePath]
sendAllSubIDEs miState msg = atomically $ do
  ides <- takeTMVar (subIDEsVar miState)
  let ideInstances = mapMaybe ideDataMain $ Map.elems ides
  homes <- forM ideInstances $ \ide -> ideHome ide <$ writeTChan (ideInHandleChannel ide) (Aeson.encode msg)
  putTMVar (subIDEsVar miState) ides
  pure homes

sendAllSubIDEs_ :: MultiIdeState -> LSP.FromClientMessage -> IO ()
sendAllSubIDEs_ miState = void . sendAllSubIDEs miState

getSourceFileHome :: MultiIdeState -> FilePath -> STM (Maybe FilePath)
getSourceFileHome miState path = do
  sourceFileHomes <- takeTMVar (sourceFileHomesVar miState)
  case Map.lookup path sourceFileHomes of
    Just home -> do
      putTMVar (sourceFileHomesVar miState) sourceFileHomes
      unsafeIOToSTM $ logDebug miState $ "Found cached home for " <> path
      pure $ Just home
    Nothing -> do
      -- Safe as repeat prints are acceptable
      unsafeIOToSTM $ logDebug miState $ "No cached home for " <> path
      -- Read only operation, so safe within STM
      mHome <- unsafeIOToSTM $ findHome path
      unsafeIOToSTM $ logDebug miState $ "File system yielded " <> show mHome
      putTMVar (sourceFileHomesVar miState) $ maybe sourceFileHomes (\home -> Map.insert path home sourceFileHomes) mHome
      pure mHome

sourceFileHomeDeleted :: MultiIdeState -> FilePath -> IO ()
sourceFileHomeDeleted miState path = atomically $ modifyTMVar (sourceFileHomesVar miState) $ Map.delete path

-- When a daml.yaml changes, all files pointing to it are invalidated in the cache
sourceFileHomeDamlYamlChanged :: MultiIdeState -> FilePath -> IO ()
sourceFileHomeDamlYamlChanged miState packagePath = atomically $ modifyTMVar (sourceFileHomesVar miState) $ Map.filter (/=packagePath)

sendSubIDEByPath :: MultiIdeState -> FilePath -> LSP.FromClientMessage -> IO ()
sendSubIDEByPath miState path msg = do
  (mHome, shouldAdd) <- sendSubIDEByPathAux path msg
  -- Lock is dropped then regained here for new IDE. This is acceptable as it's impossible for a shutdown
  -- of the new ide to be sent before its created.
  -- Note that if sendSubIDEByPath is called multiple times concurrently for a new IDE, addNewSubIDEAndSend may be called twice for the same home
  --   addNewSubIDEAndSend handles this internally with its own checks, so this is acceptable.
  forM_ mHome $ \home -> do
    putSingleFromClientMessage miState home msg
    when shouldAdd $ addNewSubIDEAndSend miState home $ Just msg
  where
    -- Returns the path of the subIDE if one exists/can be created, as well as a flag on whether it should be created
    sendSubIDEByPathAux :: FilePath -> LSP.FromClientMessage -> IO (Maybe FilePath, Bool)
    sendSubIDEByPathAux path msg = atomically $ do
      mHome <- getSourceFileHome miState path

      case mHome of
        Just home -> do
          ides <- takeTMVar (subIDEsVar miState)
          let ideData = lookupSubIde home ides
          case ideDataMain ideData of
            -- Here we already have a subIDE, so we forward our message to it before dropping the lock
            Just ide -> do
              writeTChan (ideInHandleChannel ide) (Aeson.encode msg)
              -- Safe as repeat prints are acceptable
              unsafeIOToSTM $ logDebug miState $ "Found relevant SubIDE: " <> ideDataHome ideData
              putTMVar (subIDEsVar miState) ides
              pure (Just home, False)
            -- This path will create a new subIDE at the given home
            Nothing -> do
              putTMVar (subIDEsVar miState) ides
              pure (Just home, True)
        Nothing -> do
          -- We get here if we cannot find a daml.yaml file for a file mentioned in a request
          -- if we're sending a response, ignore it, as this means the server that sent the request has been killed already.
          -- if we're sending a request, respond to the client with an error.
          -- if we're sending a notification, ignore it - theres nothing the protocol allows us to do to signify notification failures.
          let replyError :: forall (m :: LSP.Method 'LSP.FromClient 'LSP.Request). LSP.SMethod m -> LSP.LspId m -> STM ()
              replyError method id =
                sendClientSTM miState $ LSP.FromServerRsp method $ LSP.ResponseMessage "2.0" (Just id) $ Left 
                  $ LSP.ResponseError LSP.InvalidParams ("Could not find daml.yaml for package containing " <> T.pack path) Nothing
          (Nothing, False) <$ case msg of
            LSP.FromClientMess method params ->
              case (LSP.splitClientMethod method, params) of
                (LSP.IsClientReq, LSP.RequestMessage {_id}) -> replyError method _id
                (LSP.IsClientEither, LSP.ReqMess (LSP.RequestMessage {_id})) -> replyError method _id
                _ -> pure ()
            _ -> pure ()

parseCustomResult :: Aeson.FromJSON a => String -> Either LSP.ResponseError Aeson.Value -> Either LSP.ResponseError a
parseCustomResult name =
  fmap $ either (\err -> error $ "Failed to parse response of " <> name <> ": " <> err) id 
    . Aeson.parseEither Aeson.parseJSON

onOpenFiles :: MultiIdeState -> FilePath -> (Set.Set FilePath -> Set.Set FilePath) -> STM ()
onOpenFiles miState home f = modifyTMVarM (subIDEsVar miState) $ \subIdes -> do
  let ideData = lookupSubIde home subIdes
      ideData' = ideData {ideDataOpenFiles = f $ ideDataOpenFiles ideData}
  when (ideDataDisabled ideData') $ traverse_ (sendClientSTM miState) $ disableIdeDiagnosticMessages ideData' home
  pure $ Map.insert home ideData' subIdes

addOpenFile :: MultiIdeState -> FilePath -> FilePath -> STM ()
addOpenFile miState home file = do
  unsafeIOToSTM $ logInfo miState $ "Added open file " <> file <> " to " <> home
  onOpenFiles miState home $ Set.insert file

removeOpenFile :: MultiIdeState -> FilePath -> FilePath -> STM ()
removeOpenFile miState home file = do
  unsafeIOToSTM $ logInfo miState $ "Removed open file " <> file <> " from " <> home
  onOpenFiles miState home $ Set.delete file

resolveAndUnpackSourceLocation :: MultiIdeState -> PackageSourceLocation -> IO FilePath
resolveAndUnpackSourceLocation miState pkgSource = do
  (pkgPath, mDarPath) <- resolveSourceLocation miState pkgSource
  forM_ mDarPath $ \darPath -> do
    -- Must shutdown existing IDE first, since folder could be deleted
    -- If no IDE exists, shutdown is a no-op
    logDebug miState $ "Shutting down existing unpacked dar at " <> pkgPath
    shutdownIdeByPath miState pkgPath
    unpackDar miState darPath
  pure pkgPath

-- Handlers

subIDEMessageHandler :: MultiIdeState -> IO () -> SubIDEInstance -> B.ByteString -> IO ()
subIDEMessageHandler miState unblock ide bs = do
  logInfo miState $ "Got new message from " <> ideHome ide

  -- Decode a value, parse
  let val :: Aeson.Value
      val = er "eitherDecode" $ Aeson.eitherDecodeStrict bs
  mMsg <- either error id <$> parseServerMessageWithTracker (fromClientMethodTrackerVar miState) (ideHome ide) val

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
        -- Dangerous call here is acceptable as this only happens while the ide is booting, before unblocking
        unsafeSendSubIDE ide $ LSP.FromClientMess LSP.SInitialized $ LSP.NotificationMessage "2.0" LSP.SInitialized (Just LSP.InitializedParams)
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
          -- SubIDE containing the reference also contained the definition, so returned no name to lookup
          -- Simply forward this location
          Right (Just (TryGetDefinitionResult loc Nothing)) -> replyLocations [loc]
          -- SubIDE containing the reference did not contain the definition, it returns a fake location in .daml and the name
          -- Send a new request to a new SubIDE to find the source of this name
          Right (Just (TryGetDefinitionResult loc (Just name))) -> do
            logDebug miState $ "Got name in result! Backup location is " <> show loc
            mSourceLocation <- Map.lookup (tgdnPackageUnitId name) <$> atomically (readTMVar $ multiPackageMappingVar miState)
            case mSourceLocation of
              -- Didn't find a home for this name, we do not know where this is defined, so give back the (known to be wrong)
              -- .daml data-dependency path
              -- This is the worst case, we'll later add logic here to unpack and spinup an SubIDE for the read-only dependency
              Nothing -> replyLocations [loc]
              -- We found a daml.yaml for this definition, send the getDefinitionByName request to its SubIDE
              Just sourceLocation -> do
                home <- resolveAndUnpackSourceLocation miState sourceLocation
                logDebug miState $ "Found unit ID in multi-package mapping, forwarding to " <> home
                let method = LSP.SCustomMethod "daml/gotoDefinitionByName"
                    lspId = maybe (error "No LspId provided back from tryGetDefinition") castLspId _id
                    msg = LSP.FromClientMess method $ LSP.ReqMess $
                      LSP.RequestMessage "2.0" lspId method $ Aeson.toJSON $
                        GotoDefinitionByNameParams loc name
                sendSubIDEByPath miState home msg
      
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
  (LSP.STextDocumentDidOpen, ".daml") -> getSourceFileHome miState path >>= traverse_ (\home -> addOpenFile miState home path)
  (LSP.STextDocumentDidClose, ".daml") -> getSourceFileHome miState path >>= traverse_ (\home -> removeOpenFile miState home path)
  _ -> pure ()

clientMessageHandler :: MultiIdeState -> IO () -> B.ByteString -> IO ()
clientMessageHandler miState unblock bs = do
  logInfo miState "Got new message from client"

  -- Decode a value, parse
  let castFromClientMessage :: LSP.FromClientMessage' (Product LSP.SMethod (Const (Maybe FilePath))) -> LSP.FromClientMessage
      castFromClientMessage = \case
        LSP.FromClientMess method params -> LSP.FromClientMess method params
        LSP.FromClientRsp (Pair method _) params -> LSP.FromClientRsp method params

      val :: Aeson.Value
      val = er "eitherDecode" $ Aeson.eitherDecodeStrict bs

  unPrefixedMsg <- either error id <$> parseClientMessageWithTracker (fromServerMethodTrackerVar miState) val
  let msg = addProgressTokenPrefixToClientMessage unPrefixedMsg

  case msg of
    -- Store the initialize params for starting subIDEs, respond statically with what ghc-ide usually sends.
    LSP.FromClientMess LSP.SInitialize LSP.RequestMessage {_id, _method, _params} -> do
      putMVar (initParamsVar miState) _params
      -- Send initialized out directly (skipping the queue), then unblock for other messages
      putChunk stdout $ Aeson.encode $ LSP.FromServerRsp _method $ LSP.ResponseMessage "2.0" (Just _id) (Right initializeResult)
      unblock

      -- Register watchers for daml.yaml, multi-package.yaml and *.dar files
      let LSP.RequestMessage {_id, _method} = registerFileWatchersMessage
      putReqMethodSingleFromServerCoordinator (fromServerMethodTrackerVar miState) _id _method

      sendClient miState $ LSP.FromServerMess _method registerFileWatchersMessage

    LSP.FromClientMess LSP.SWindowWorkDoneProgressCancel notif -> do
      let (newNotif, mPrefix) = stripWorkDoneProgressCancelTokenPrefix notif
          newMsg = LSP.FromClientMess LSP.SWindowWorkDoneProgressCancel newNotif
      -- Find IDE with the correct prefix, send to it if it exists. If it doesn't, the message can be thrown away.
      case mPrefix of
        Nothing -> void $ sendAllSubIDEs miState newMsg
        Just prefix -> atomically $ do
          ides <- takeTMVar $ subIDEsVar miState
          let mIde = find (\ideData -> (ideMessageIdPrefix <$> ideDataMain ideData) == Just prefix) ides
          traverse_ (`unsafeSendSubIDESTM` newMsg) $ mIde >>= ideDataMain
          putTMVar (subIDEsVar miState) ides

    -- Special handing for STextDocumentDefinition to ask multiple IDEs (the W approach)
    -- When a getDefinition is requested, we cast this request into a tryGetDefinition
    -- This is a method that will take the same logic path as getDefinition, but will also return an
    -- identifier in the cases where it knows the identifier wasn't defined in the package that referenced it
    -- When we receive this name, we lookup against the multi-package.yaml for a package that matches where the identifier
    -- came from. If we find one, we ask (and create if needed) the SubIDE that contains the identifier where its defined.
    -- (this is via the getDefinitionByName message)
    -- We also send the backup known incorrect location from the tryGetDefinition, such that if the subIDE containing the identifier
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
      sendSubIDEByPath miState path msg

    -- Watched file changes, used for restarting subIDEs and changing coordinator state
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
            let packagePath = takeDirectory changedPath
            logInfo miState $ "daml.yaml change in " <> packagePath <> ". Shutting down IDE"
            sourceFileHomeDamlYamlChanged miState packagePath
            rebootIdeByPath miState packagePath
            void $ updatePackageData miState
          "multi-package.yaml" -> do
            logInfo miState "multi-package.yaml change."
            void $ updatePackageData miState
          _ | takeExtension changedPath == ".dar" -> do
            logInfo miState $ ".dar file changed: " <> changedPath
            idesToShutdown <- fromMaybe mempty . Map.lookup changedPath <$> atomically (readTMVar $ darDependentPackagesVar miState)
            logDebug miState $ "Shutting down following ides: " <> show idesToShutdown
            traverse_ (lenientRebootIdeByPath miState) idesToShutdown

            void $ updatePackageData miState
          -- for .daml, we remove entry from the sourceFileHome cache if the file is deleted (note that renames/moves are sent as delete then create)
          _ | takeExtension changedPath == ".daml" && changeType == LSP.FcDeleted -> sourceFileHomeDeleted miState changedPath
          _ -> pure ()
      logDebug miState "all not on filtered DidChangeWatchedFilesParams"
      -- Filter down to only daml files and send those
      let damlOnlyChanges = filter (maybe False (\path -> takeExtension path == ".daml") . LSP.uriToFilePath . view LSP.uri) changes
      sendAllSubIDEs_ miState $ LSP.FromClientMess LSP.SWorkspaceDidChangeWatchedFiles $ LSP.params .~ LSP.DidChangeWatchedFilesParams (LSP.List damlOnlyChanges) $ msg

    LSP.FromClientMess meth params ->
      case getMessageForwardingBehaviour miState meth params of
        ForwardRequest mess (Single path) -> do
          logDebug miState $ "single req on method " <> show meth <> " over path " <> path
          let LSP.RequestMessage {_id, _method} = mess
              msg' = castFromClientMessage msg
          sendSubIDEByPath miState path msg'

        ForwardRequest mess (AllRequest combine) -> do
          logDebug miState $ "all req on method " <> show meth
          let LSP.RequestMessage {_id, _method} = mess
              msg' = castFromClientMessage msg
          ides <- sendAllSubIDEs miState msg'
          if null ides 
            then sendClient miState $ LSP.FromServerRsp _method $ LSP.ResponseMessage "2.0" (Just _id) $ combine []
            else putReqMethodAll (fromClientMethodTrackerVar miState) _id _method msg' ides combine

        ForwardNotification mess (Single path) -> do
          logDebug miState $ "single not on method " <> show meth <> " over path " <> path
          handleOpenFilesNotification miState mess path
          -- Notifications aren't stored, so failure to send can be ignored
          sendSubIDEByPath miState path (castFromClientMessage msg)

        ForwardNotification mess AllNotification -> do
          logDebug miState $ "all not on method " <> show meth
          sendAllSubIDEs_ miState (castFromClientMessage msg)
          case mess ^. LSP.method of
            LSP.SExit -> do
              -- Wait half a second for all the exit messages to be sent
              threadDelay 500_000
              exitSuccess
            _ -> pure ()

        ExplicitHandler handler -> do
          logDebug miState "calling explicit handler"
          handler (sendClient miState) (sendSubIDEByPath miState)
    -- Responses to subIDEs
    LSP.FromClientRsp (Pair method (Const (Just home))) rMsg -> 
      -- If a response fails, failure is acceptable as the subIDE can't be expecting a response if its dead
      sendSubIDEByPath miState home $ LSP.FromClientRsp method $ 
        rMsg & LSP.id %~ fmap stripLspPrefix
    -- Responses to coordinator
    LSP.FromClientRsp (Pair method (Const Nothing)) LSP.ResponseMessage {_id, _result} ->
      case (method, _id) of
        (LSP.SClientRegisterCapability, Just (LSP.IdString "MultiIdeWatchedFiles")) ->
          either (\err -> logError miState $ "Watched file registration failed with " <> show err) (const $ logDebug miState "Successfully registered watched files") _result
        _ -> pure ()

{-
TODO: refactor multi-package.yaml discovery logic
Expect a multi-package.yaml at the workspace root
If we do not get one, we continue as normal (no popups) until the user attempts to open/use files in a different package to the first one
  When this occurs, this send a popup:
    Make a multi-package.yaml at the root and reload the editor please :)
    OR tell me where the multi-package.yaml(s) is
      if the user provides multiple, we union that lookup, allowing "cross project boundary" jumps
-}
-- Updates the unit-id to package/dar mapping, as well as the dar to dependent packages mapping
-- for any daml.yamls or dars that are invalid, the ide home paths are returned, and their data is not added to the mapping
updatePackageData :: MultiIdeState -> IO [FilePath]
updatePackageData miState = do
  logInfo miState "Updating package data"
  let ideRoot = multiPackageHome miState

  -- Take locks, throw away current data
  atomically $ do
    void $ takeTMVar (multiPackageMappingVar miState)
    void $ takeTMVar (darDependentPackagesVar miState)
  
  mPkgConfig <- findMultiPackageConfig $ ProjectPath ideRoot
  case mPkgConfig of
    Nothing -> do
      logDebug miState "No multi-package.yaml found"
      damlYamlExists <- doesFileExist $ ideRoot </> projectConfigName
      if damlYamlExists
        then do
          logDebug miState "Found daml.yaml"
          deriveAndWriteMappings [ideRoot] []
        else do
          logDebug miState "No daml.yaml found either"
          deriveAndWriteMappings [] []
    Just path -> do
      logDebug miState "Found multi-package.yaml"
      eRes <- try @SomeException $ withMultiPackageConfig path $ \multiPackage ->
        deriveAndWriteMappings (mpPackagePaths multiPackage) (mpDars multiPackage)
      let multiPackagePath = unwrapProjectPath path </> "multi-package.yaml"
      case eRes of
        Right paths -> do
          sendClient miState $ clearDiagnostics multiPackagePath
          pure paths
        Left err -> do
          -- Ensure the variables are populated before moving on
          atomically $ do
            void $ tryPutTMVar (multiPackageMappingVar miState) Map.empty
            void $ tryPutTMVar (darDependentPackagesVar miState) Map.empty
          sendClient miState $ fullFileDiagnostic ("Error reading multi-package.yaml:\n" <> displayException err) multiPackagePath
          pure []
  where
    -- Gets the unit id of a dar if it can, caches result in stateT
    -- Returns Nothing if anything goes wrong (dar doesn't exist, dar isn't archive, dar manifest malformed, etc.)
    getDarUnitId :: FilePath -> StateT (Map.Map FilePath (Maybe String)) IO (Maybe String)
    getDarUnitId dep = do
      cachedResult <- gets (Map.lookup dep)
      case cachedResult of
        Just res -> pure res
        Nothing -> do
          mUnitId <- lift $ fmap eitherToMaybe $ try @SomeException $ do
            archive <- Zip.toArchive <$> BSL.readFile dep
            manifest <- either fail pure $ readDalfManifest archive
            maybe (fail $ "data-dependency " <> dep <> " missing a package name") pure $ packageName manifest
          modify' $ Map.insert dep mUnitId
          pure mUnitId

    deriveAndWriteMappings :: [FilePath] -> [FilePath] -> IO [FilePath]
    deriveAndWriteMappings packagePaths darPaths = do
      ((invalidHomes, validPackageDatas), darUnitIds) <- flip runStateT mempty $ do
        -- load cache with all multi-package dars
        traverse_ getDarUnitId darPaths
        fmap (bimap catMaybes catMaybes . unzip) $ forM packagePaths $ \packagePath -> do
          mUnitIdAndDeps <- lift $ fmap eitherToMaybe $ unitIdAndDepsFromDamlYaml packagePath
          case mUnitIdAndDeps of
            Just (unitId, deps) -> do
              allDepsValid <- isJust . sequence <$> traverse getDarUnitId deps
              pure (if allDepsValid then Nothing else Just packagePath, Just (packagePath, unitId, deps))
            _ -> pure (Just packagePath, Nothing)

      let packagesOnDisk :: Map.Map String PackageSourceLocation
          packagesOnDisk =
            Map.fromList $ (\(packagePath, unitId, _) -> (unitId, PackageOnDisk packagePath)) <$> validPackageDatas
          darMapping :: Map.Map String PackageSourceLocation
          darMapping =
            Map.fromList $ fmap (\(packagePath, unitId) -> (unitId, PackageInDar packagePath)) $ Map.toList $ Map.mapMaybe id darUnitIds
          multiPackageMapping :: Map.Map String PackageSourceLocation
          multiPackageMapping = packagesOnDisk <> darMapping
          darDependentPackages :: Map.Map FilePath (Set.Set FilePath)
          darDependentPackages = foldr
            (\(packagePath, _, deps) -> Map.unionWith (<>) $ Map.fromList $ zip deps $ repeat $ Set.singleton packagePath
            ) Map.empty validPackageDatas

      logDebug miState $ "Setting multi package mapping to:\n" <> show multiPackageMapping
      logDebug miState $ "Setting dar dependent packages to:\n" <> show darDependentPackages
      atomically $ do
        putTMVar (multiPackageMappingVar miState) multiPackageMapping
        putTMVar (darDependentPackagesVar miState) darDependentPackages

      pure invalidHomes

-- Main loop logic

createDefaultPackage :: SdkVersion.Class.SdkVersioned => IO (FilePath, IO ())
createDefaultPackage = do
  (defaultPackagePath, cleanup) <- newTempDir
  writeFile (defaultPackagePath </> "daml.yaml") $ unlines
    [ "sdk-version: " <> SdkVersion.Class.sdkVersion
    , "name: daml-ide-default-environment"
    , "version: 1.0.0"
    , "source: ."
    , "dependencies:"
    , "  - daml-prim"
    , "  - daml-stdlib"
    ]
  pure (defaultPackagePath, cleanup)

runMultiIde :: SdkVersion.Class.SdkVersioned => Logger.Priority -> [String] -> IO ()
runMultiIde loggingThreshold args = do
  homePath <- getCurrentDirectory
  (defaultPackagePath, cleanupDefaultPackage) <- createDefaultPackage
  let subIdeArgs = if loggingThreshold <= Logger.Debug then "--debug" : args else args
  miState <- newMultiIdeState homePath defaultPackagePath loggingThreshold subIdeArgs
  invalidPackageHomes <- updatePackageData miState

  -- Ensure we don't send messages to the client until it finishes initializing
  (onceUnblocked, unblock) <- makeIOBlocker

  logInfo miState $ "Running with logging threshold of " <> show loggingThreshold
  -- Client <- *****
  toClientThread <- async $ onceUnblocked $ forever $ do
    msg <- atomically $ readTChan $ toClientChan miState
    logDebug miState $ "Pushing message to client:\n" <> BSLC.unpack msg 
    putChunk stdout msg

  -- Client -> Coord
  clientToCoordThread <- async $
    onChunks stdin $ clientMessageHandler miState unblock

  -- All invalid packages get spun up, so their errors are shown
  traverse_ (\home -> addNewSubIDEAndSend miState home Nothing) invalidPackageHomes

  let killAll :: IO ()
      killAll = do
        logDebug miState "Killing subIDEs"
        subIDEs <- atomically $ readTMVar $ subIDEsVar miState
        forM_ subIDEs (shutdownIde miState)
        logInfo miState "MultiIde shutdown"

      -- Get all outcomes from a SubIDEInstance (process and async failures/completions)
      subIdeInstanceOutcomes :: FilePath -> SubIDEInstance -> STM [(FilePath, SubIDEInstance, Either ExitCode SomeException)]
      subIdeInstanceOutcomes home ide = do
        mExitCode <- getExitCodeSTM (ideProcess ide)
        errs <- lefts . catMaybes <$> traverse pollSTM [ideInhandleAsync ide, ideOutHandleAsync ide, ideErrTextAsync ide]
        let mExitOutcome = (home, ide, ) . Left <$> mExitCode
            errorOutcomes = (home, ide, ) . Right <$> errs
        pure $ errorOutcomes <> maybeToList mExitOutcome

      -- Function folded over outcomes to update SubIDEs, keep error list and list subIDEs to reboot
      handleOutcome
        :: ([(FilePath, SomeException)], SubIDEs, [FilePath])
        -> (FilePath, SubIDEInstance, Either ExitCode SomeException)
        -> IO ([(FilePath, SomeException)], SubIDEs, [FilePath])
      handleOutcome (errs, subIDEs, toRestart) (home, ide, outcomeType) =
        case outcomeType of
          -- subIDE process exits
          Left exitCode -> do
            logDebug miState $ "SubIDE at " <> home <> " exited, cleaning up."
            cancel $ ideInhandleAsync ide
            cancel $ ideOutHandleAsync ide
            cancel $ ideErrTextAsync ide
            stderrContent <- T.unpack <$> readTVarIO (ideErrText ide)
            currentTime <- getCurrentTime
            let ideData = lookupSubIde home subIDEs
                isMainIde = ideDataMain ideData == Just ide
                isCrash = exitCode /= ExitSuccess
                ideData' = ideData
                  { ideDataClosing = Set.delete ide $ ideDataClosing ideData
                  , ideDataMain = if isMainIde then Nothing else ideDataMain ideData
                  , ideDataFailTimes = 
                      if isCrash && isMainIde
                        then take 2 $ currentTime : ideDataFailTimes ideData
                        else ideDataFailTimes ideData
                  , ideDataLastError = if isCrash && isMainIde then Just stderrContent else Nothing
                  }
                toRestart' = if isCrash && isMainIde then home : toRestart else toRestart
            when isCrash $
              logWarning miState $ "Proccess failed, stderr content:\n" <> stderrContent
              
            pure (errs, Map.insert home ideData' subIDEs, toRestart')
          -- handler thread errors
          Right exception -> pure ((home, exception) : errs, subIDEs, toRestart)

  forever $ do
    (outcomes, clientThreadExceptions) <- atomically $ do
      subIDEs <- readTMVar $ subIDEsVar miState
      
      outcomes <- fmap concat $ forM (Map.toList subIDEs) $ \(home, subIdeData) -> do
        mainSubIdeOutcomes <- maybe (pure []) (subIdeInstanceOutcomes home) $ ideDataMain subIdeData
        closingSubIdesOutcomes <- concat <$> traverse (subIdeInstanceOutcomes home) (Set.toList $ ideDataClosing subIdeData)
        pure $ mainSubIdeOutcomes <> closingSubIdesOutcomes
      
      clientThreadExceptions <- lefts . catMaybes <$> traverse pollSTM [toClientThread, clientToCoordThread]
      
      when (null outcomes && null clientThreadExceptions) retry

      pure (outcomes, clientThreadExceptions)

    unless (null clientThreadExceptions) $
      if any (\e -> fromException @ExitCode e == Just ExitSuccess) clientThreadExceptions
        then do
          logWarning miState "Exiting!"
          cleanupDefaultPackage
          exitSuccess
        else error $ "1 or more client thread handlers failed: " <> show clientThreadExceptions
    
    unless (null outcomes) $ do
      subIDEs <- atomically $ takeTMVar $ subIDEsVar miState

      (errs, subIDEs', subIDEsToRestart) <- foldM handleOutcome ([], subIDEs, []) outcomes

      atomically $ putTMVar (subIDEsVar miState) subIDEs'

      traverse_ (\home -> addNewSubIDEAndSend miState home Nothing) subIDEsToRestart

      when (not $ null errs) $ do
        cleanupDefaultPackage
        killAll
        error $ "SubIDE handlers failed with following errors:\n" <> unlines ((\(home, err) -> home <> " => " <> show err) <$> errs)
