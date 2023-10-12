-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE PolyKinds           #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE ApplicativeDo       #-}
{-# LANGUAGE RankNTypes       #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DisambiguateRecordFields #-}
{-# LANGUAGE GADTs #-}

module DA.Cli.Damlc.Command.MultiIde (runMultiIde) where

import Control.Concurrent.Async (async, cancel, pollSTM)
import Control.Concurrent.STM.TChan
import Control.Concurrent.STM.TMVar
import Control.Concurrent.MVar
import Control.Exception(AsyncException, handle, throwIO)
import Control.Lens
import Control.Monad
import Control.Monad.STM
import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Types as Aeson
import qualified Data.ByteString as B
import DA.Cli.Damlc.Command.MultiIde.Forwarding
import DA.Cli.Damlc.Command.MultiIde.Prefixing
import DA.Cli.Damlc.Command.MultiIde.Util
import DA.Cli.Damlc.Command.MultiIde.Parsing
import DA.Cli.Damlc.Command.MultiIde.Types
import DA.Daml.LanguageServer.SplitGotoDefinition
import DA.Daml.Package.Config (MultiPackageConfigFields(..), findMultiPackageConfig, withMultiPackageConfig)
import DA.Daml.Project.Types (ProjectPath (..))
import Data.Either (lefts)
import Data.Functor.Product
import Data.List (find, isPrefixOf)
import qualified Data.Map as Map
import Data.Maybe (catMaybes, fromMaybe)
import qualified Data.Text as T
import GHC.Conc (unsafeIOToSTM)
import qualified Language.LSP.Types as LSP
import qualified Language.LSP.Types.Lens as LSP
import System.Environment (getEnv)
import System.IO.Extra
import System.Process (getPid)
import System.Process.Typed (
  Process,
  createPipe,
  getExitCodeSTM,
  getStdin,
  getStdout,
  nullStream,
  proc,
  setStderr,
  setStdin,
  setStdout,
  setWorkingDir,
  startProcess,
  unsafeProcessHandle,
  -- useHandleOpen,
 )

{-
TODO:
  data deps support
    likely recreate the daml.yaml in the package database and spin an IDE there
    need to ensure the ide path lookup doesn't allow crossing the .daml boundary
    also don't want it to install another .daml folder in the .daml folder, so might need more ghcide work?
  generic ide for temp files
    when you simply do ctrl+n, and have an unsaved file, we still want IDE support, but ofc that file has no daml.yaml, so we can only assume bare minimum deps
    we can create an IDE for a project with just those deps in a temp location and forward requests like this to it.
  might need some daml.revealLocation logic
  seems sometimes we get relative paths, so duplicate subIDEs are made
-}

{-
the W approach for STextDocumentDeclaration/STextDocumentDefinition:
TODO: at start, find a multi-package.yaml (discuss how later), create a mapping from package-name and package-version to home directory
  Currently, looking up in the existing open SubIDEs. But ideally, we'll create a new SubIDE if we get a request for something within
  multi-package but without a SubIDE yet (callSubIDE by path will do this for us :))

below is complete:
client -> coord: STextDocumentDefinition (including a path (pathA) and a position(posA))
coord -> subIde(pathA): whatIsIt(pathA, posA)
subIde(pathA) <- coord: identifier(including a name, packageid, might be able to get package name and version)
(somehow get name + version from packageid if we don't have it)
(lookup name and version in the mapping from multi-package.yaml)
if we find a home directory (call it pathB)
  coord -> subIde(pathB): where is this identifier (consider implementing SWorkspaceSymbol to achieve this)
  subIde(pathB) -> coord: location(path (pathC) and position (posC))
  coord -> client: STextDocumentDefinition response(pathC, posC)
else
  -- Note using pathA here, we ask the initial IDE where it is, and it'll likely give some location in .daml
  coord -> subIde(pathA): where is this identifier (consider implementing SWorkspaceSymbol to achieve this)
  subIde(pathA) -> coord: location(path (pathC) and position (posC))
  coord -> client: STextDocumentDefinition response(pathC, posC)


alternatively, "whatIsIt" can return either a location (if its a local def) or a identifier (if its external), and we only continue the lookup if its an identifier
-}

-- Spin-up logic

-- add IDE, send initialize, do not send further messages until we get the initialize response and have sent initialized
-- we can do this by locking the sending thread, but still allowing the channel to be pushed
-- we also atomically send a message to the channel, without dropping the lock on the subIDEs var
addNewSubIDEAndSend
  :: MultiIdeState
  -> FilePath
  -> LSP.FromClientMessage
  -> IO SubIDE
addNewSubIDEAndSend miState home msg = do
  debugPrint "Trying to make a SubIDE"
  ides <- atomically $ takeTMVar $ subIDEsVar miState

  let mExistingIde = mfilter ideActive $ Map.lookup home ides
  case mExistingIde of
    Just ide -> do
      debugPrint "SubIDE already exists"
      sendSubIDE ide msg
      atomically $ putTMVar (subIDEsVar miState) ides
      pure ide
    Nothing -> do
      debugPrint "Making a SubIDE"

      unitId <- either (\cErr -> error $ "Failed to get unit ID from daml.yaml: " <> show cErr) id <$> unitIdFromDamlYaml home

      subIdeProcess <- runSubProc home
      let inHandle = getStdin subIdeProcess
          outHandle = getStdout subIdeProcess

      -- Handles blocking the sender thread until the IDE is initialized.
      sendBlocker <- newEmptyMVar @()
      let unblock = putMVar sendBlocker ()
          onceUnblocked = (readMVar sendBlocker >>)

      --           ***** -> SubIDE
      toSubIDEChan <- atomically newTChan
      toSubIDE <- async $ onceUnblocked $ forever $ do
        msg <- atomically $ readTChan toSubIDEChan
        debugPrint "Pushing message to subIDE"
        putChunk inHandle msg

      --           Coord <- SubIDE
      subIDEToCoord <- async $ do
        -- Wait until our own IDE exists then pass it forward
        ide <- atomically $ fromMaybe (error "Failed to get own IDE") . mfilter ideActive . Map.lookup home <$> readTMVar (subIDEsVar miState)
        chunks <- getChunks outHandle
        mapM_ (subIDEMessageHandler miState unblock ide) chunks

      pid <- fromMaybe (error "SubIDE has no PID") <$> getPid (unsafeProcessHandle subIdeProcess)

      mInitParams <- tryReadMVar (initParamsVar miState)
      let !initParams = fromMaybe (error "Attempted to create a SubIDE before initialization!") mInitParams
          initId = LSP.IdString $ T.pack $ show pid
          (initMsg :: LSP.FromClientMessage) = LSP.FromClientMess LSP.SInitialize LSP.RequestMessage 
            { _id = initId
            , _method = LSP.SInitialize
            , _params = initParams
                { LSP._rootPath = Just $ T.pack home
                , LSP._rootUri = Just $ LSP.filePathToUri home
                }
            , _jsonrpc = "2.0"
            }
          ide = 
            SubIDE
              { ideInhandleAsync = toSubIDE
              , ideInHandle = inHandle
              , ideInHandleChannel = toSubIDEChan
              , ideOutHandleAsync = subIDEToCoord
              , ideProcess = subIdeProcess
              , ideProcessID = pid
              , ideHomeDirectory = home
              , ideMessageIdPrefix = T.pack $ show pid
              , ideActive = True
              , ideUnitId = unitId
              }


      putReqMethodSingleFromClient (fromClientMethodTrackerVar miState) initId LSP.SInitialize
      putChunk inHandle $ Aeson.encode initMsg
      -- Dangerous call is okay here because we're already holding the subIDEsVar lock
      sendSubIDE ide msg

      atomically $ putTMVar (subIDEsVar miState) $ Map.insert home ide ides

      pure ide

runSubProc :: FilePath -> IO (Process Handle Handle ())
runSubProc home = do
  assistantPath <- getEnv "DAML_ASSISTANT"

  startProcess $
    proc assistantPath ["ide"] &
      setStdin createPipe &
      setStdout createPipe &
      -- setStderr (useHandleOpen stderr) &
      setStderr nullStream &
      setWorkingDir home

-- Spin-down logic

-- Sends a shutdown message and sets active to false, disallowing any further messages to be sent to the subIDE
-- given queue nature of TChan, all other pending messages will be sent first before handling shutdown
shutdownIde :: MultiIdeState -> SubIDE -> IO ()
shutdownIde miState ide = do
  ides <- atomically $ takeTMVar (subIDEsVar miState)
  let shutdownId = LSP.IdString $ T.pack $ show (ideProcessID ide) <> "-shutdown"
      (shutdownMsg :: LSP.FromClientMessage) = LSP.FromClientMess LSP.SShutdown LSP.RequestMessage 
        { _id = shutdownId
        , _method = LSP.SShutdown
        , _params = LSP.Empty
        , _jsonrpc = "2.0"
        }
  
  putReqMethodSingleFromClient (fromClientMethodTrackerVar miState) shutdownId LSP.SShutdown
  sendSubIDE ide shutdownMsg

  atomically $ putTMVar (subIDEsVar miState) $ Map.adjust (\ide' -> ide' {ideActive = False}) (ideHomeDirectory ide) ides

-- To be called once we receive the Shutdown response
-- Safe to assume that the sending channel is empty, so we can end the thread and send the final notification directly on the handle
handleExit :: MultiIdeState -> SubIDE -> IO ()
handleExit miState ide = do
  let (exitMsg :: LSP.FromClientMessage) = LSP.FromClientMess LSP.SExit LSP.NotificationMessage
        { _method = LSP.SExit
        , _params = LSP.Empty
        , _jsonrpc = "2.0"
        }
  -- This will cause the subIDE process to exit
  putChunk (ideInHandle ide) $ Aeson.encode exitMsg
  atomically $ modifyTMVar (subIDEsVar miState) $ Map.delete (ideHomeDirectory ide)
  cancel $ ideInhandleAsync ide
  cancel $ ideOutHandleAsync ide

-- Communication logic

-- Dangerous as does not hold the subIDEsVar lock. If a shutdown is called whiled this is running, the message may not be sent.
sendSubIDE :: SubIDE -> LSP.FromClientMessage -> IO ()
sendSubIDE ide = atomically . writeTChan (ideInHandleChannel ide) . Aeson.encode

sendClient :: MultiIdeState -> LSP.FromServerMessage -> IO ()
sendClient miState = atomically . writeTChan (toClientChan miState) . Aeson.encode

sendAllSubIDEs :: MultiIdeState -> LSP.FromClientMessage -> IO [FilePath]
sendAllSubIDEs miState msg = atomically $ do
  idesUnfiltered <- takeTMVar (subIDEsVar miState)
  let ides = Map.filter ideActive idesUnfiltered
  when (null ides) $ error "Got a broadcast to nothing :("
  homes <- forM (Map.elems ides) $ \ide -> ideHomeDirectory ide <$ writeTChan (ideInHandleChannel ide) (Aeson.encode msg)
  putTMVar (subIDEsVar miState) idesUnfiltered
  pure homes

sendAllSubIDEs_ :: MultiIdeState -> LSP.FromClientMessage -> IO ()
sendAllSubIDEs_ miState = void . sendAllSubIDEs miState

sendSubIDEByPath :: MultiIdeState -> FilePath -> LSP.FromClientMessage -> IO ()
sendSubIDEByPath miState path msg = do
  mHome <- sendSubIDEByPath_ path msg
  -- Lock is dropped then regained here for new IDE. This is acceptable as it's impossible for a shutdown
  -- of the new ide to be sent before its created.
  -- Note that if sendSubIDEByPath is called multiple times concurrently for a new IDE, addNewSubIDEAndSend may be called twice for the same home
  --   addNewSubIDEAndSend handles this internally with its own checks, so this is acceptable.
  forM_ mHome $ \home -> addNewSubIDEAndSend miState home msg
  where
    -- If a SubIDE is needed, returns the path out of the STM transaction
    sendSubIDEByPath_ :: FilePath -> LSP.FromClientMessage -> IO (Maybe FilePath)
    sendSubIDEByPath_ path msg = atomically $ do
      idesUnfiltered <- takeTMVar (subIDEsVar miState)
      let ides = Map.filter ideActive idesUnfiltered
          mHome = find (`isPrefixOf` path) $ Map.keys ides
          mIde = mHome >>= flip Map.lookup ides

      case mIde of
        Just ide -> do
          writeTChan (ideInHandleChannel ide) (Aeson.encode msg)
          unsafeIOToSTM $ debugPrint $ "Found relevant SubIDE: " <> ideHomeDirectory ide
          putTMVar (subIDEsVar miState) idesUnfiltered
          pure Nothing
        Nothing -> do
          putTMVar (subIDEsVar miState) idesUnfiltered
          -- Safe as findHome only does reads
          mHome <- unsafeIOToSTM $ findHome path
          case mHome of
            -- Returned out of the transaction to be handled in IO
            Just home -> pure $ Just home
            Nothing -> do
              -- We get here if we cannot find a daml.yaml file for a file mentioned in a request
              -- if we're sending a response, ignore it, as this means the server that sent the request has been killed already.
              -- if we're sending a request, respond to the client with an error.
              -- if we're sending a notification, ignore it - theres nothing the protocol allows us to do to signify notification failures.
              let replyError :: forall (m :: LSP.Method 'LSP.FromClient 'LSP.Request). LSP.SMethod m -> LSP.LspId m -> STM ()
                  replyError method id =
                    writeTChan (toClientChan miState) $ Aeson.encode $
                      LSP.FromServerRsp method $ LSP.ResponseMessage "2.0" (Just id) $ Left $
                        LSP.ResponseError LSP.InvalidParams "Could not find daml.yaml for this file." Nothing
              case msg of
                LSP.FromClientMess method params ->
                  case (LSP.splitClientMethod method, params) of
                    (LSP.IsClientReq, LSP.RequestMessage {_id}) -> Nothing <$ replyError method _id
                    (LSP.IsClientEither, LSP.ReqMess (LSP.RequestMessage {_id})) -> Nothing <$ replyError method _id
                    _ -> pure Nothing
                _ -> pure Nothing

parseCustomResult :: Aeson.FromJSON a => String -> Either LSP.ResponseError Aeson.Value -> Either LSP.ResponseError a
parseCustomResult name =
  fmap $ either (\err -> error $ "Failed to parse response of " <> name <> ": " <> err) id 
    . Aeson.parseEither Aeson.parseJSON 

-- Handlers

subIDEMessageHandler :: MultiIdeState -> IO () -> SubIDE -> B.ByteString -> IO ()
subIDEMessageHandler miState unblock ide bs = do
  debugPrint "Called subIDEMessageHandler"
  -- BSC.hPutStrLn stderr bs

  -- Decode a value, parse
  let val :: Aeson.Value
      val = er "eitherDecode" $ Aeson.eitherDecodeStrict bs
  mMsg <- either error id <$> parseServerMessageWithTracker (fromClientMethodTrackerVar miState) (ideHomeDirectory ide) val

  -- Adds the various prefixes needed for from server messages to not clash with those from other IDEs
  mPrefixedMsg <-
    mapM 
      ( addProgressTokenPrefixToServerMessage (serverCreatedProgressTokensVar miState) (ideHomeDirectory ide) (ideMessageIdPrefix ide)
      . addLspPrefixToServerMessage ide
      )
      mMsg

  forM_ mPrefixedMsg $ \msg -> do
    -- If its a request (builtin or custom), save it for response handling.
    putServerReq (fromServerMethodTrackerVar miState) (ideHomeDirectory ide) msg

    debugPrint "About to thunk message"
    case msg of
      LSP.FromServerRsp LSP.SInitialize LSP.ResponseMessage {_result} -> do
        debugPrint "Got initialization reply, sending initialized and unblocking"
        -- Dangerous call here is acceptable as this only happens while the ide is booting, before unblocking
        sendSubIDE ide $ LSP.FromClientMess LSP.SInitialized $ LSP.NotificationMessage "2.0" LSP.SInitialized (Just LSP.InitializedParams)
        unblock
      LSP.FromServerRsp LSP.SShutdown _ -> handleExit miState ide

      LSP.FromServerRsp (LSP.SCustomMethod "daml/tryGetDefinition") LSP.ResponseMessage {_id, _result} -> do
        debugPrint "Got tryGetDefinition response, handling..."
        let parsedResult = parseCustomResult @(Maybe TryGetDefinitionResult) "daml/tryGetDefinition" _result
            reply :: Either LSP.ResponseError (LSP.ResponseResult 'LSP.TextDocumentDefinition) -> IO ()
            reply rsp = do
              debugPrint $ "Replying directly to client with " <> show rsp
              sendClient miState $ LSP.FromServerRsp LSP.STextDocumentDefinition $ LSP.ResponseMessage "2.0" (castLspId <$> _id) rsp
            replyLocations :: [LSP.Location] -> IO ()
            replyLocations = reply . Right . LSP.InR . LSP.InL . LSP.List
        case parsedResult of
          Left err -> reply $ Left err
          Right Nothing -> replyLocations []
          Right (Just (TryGetDefinitionResult loc Nothing)) -> replyLocations [loc]
          Right (Just (TryGetDefinitionResult loc (Just name))) -> do
            debugPrint $ "Got name in result! Backup location is " <> show loc
            let mHome = Map.lookup (tgdnPackageUnitId name) $ multiPackageMapping miState
            case mHome of
              Nothing -> replyLocations [loc]
              Just home -> do
                debugPrint $ "Found unit ID in multi-package mapping, forwarding to " <> home
                let method = LSP.SCustomMethod "daml/gotoDefinitionByName"
                    lspId = maybe (error "No LspId provided back from tryGetDefinition") castLspId _id
                putReqMethodSingleFromClient (fromClientMethodTrackerVar miState) lspId method
                sendSubIDEByPath miState home $ LSP.FromClientMess method $ LSP.ReqMess $
                  LSP.RequestMessage "2.0" lspId method $ Aeson.toJSON $
                    GotoDefinitionByNameParams loc name
      
      LSP.FromServerRsp (LSP.SCustomMethod "daml/gotoDefinitionByName") LSP.ResponseMessage {_id, _result} -> do
        debugPrint "Got gotoDefinitionByName response, handling..."
        let parsedResult = parseCustomResult @GotoDefinitionByNameResult "daml/gotoDefinitionByName" _result
            reply :: Either LSP.ResponseError (LSP.ResponseResult 'LSP.TextDocumentDefinition) -> IO ()
            reply rsp = do
              debugPrint $ "Replying directly to client with " <> show rsp
              sendClient miState $ LSP.FromServerRsp LSP.STextDocumentDefinition $ LSP.ResponseMessage "2.0" (castLspId <$> _id) rsp
        case parsedResult of
          Left err -> reply $ Left err
          Right loc -> reply $ Right $ LSP.InR $ LSP.InL $ LSP.List [loc]

      LSP.FromServerMess method _ -> do
        debugPrint $ "Backwarding request " <> show method
        sendClient miState msg
      LSP.FromServerRsp method _ -> do
        debugPrint $ "Backwarding response to " <> show method
        sendClient miState msg

clientMessageHandler :: MultiIdeState -> B.ByteString -> IO ()
clientMessageHandler miState bs = do
  debugPrint "Called clientMessageHandler"

  -- Decode a value, parse
  let castFromClientMessage :: LSP.FromClientMessage' (Product LSP.SMethod (Const FilePath)) -> LSP.FromClientMessage
      castFromClientMessage = \case
        LSP.FromClientMess method params -> LSP.FromClientMess method params
        LSP.FromClientRsp (Pair method _) params -> LSP.FromClientRsp method params

      val :: Aeson.Value
      val = er "eitherDecode" $ Aeson.eitherDecodeStrict bs

  msg <- either error id <$> parseClientMessageWithTracker (fromServerMethodTrackerVar miState) val

  case msg of
    -- Store the initialize params for starting subIDEs, respond statically with what ghc-ide usually sends.
    LSP.FromClientMess LSP.SInitialize LSP.RequestMessage {_id, _method, _params} -> do
      putMVar (initParamsVar miState) _params
      sendClient miState $ LSP.FromServerRsp _method $ LSP.ResponseMessage "2.0" (Just _id) (Right initializeResult)
    LSP.FromClientMess LSP.SWindowWorkDoneProgressCancel notif -> do
      (newNotif, mHome) <- removeWorkDoneProgressCancelTokenPrefix (serverCreatedProgressTokensVar miState) notif
      let newMsg = LSP.FromClientMess LSP.SWindowWorkDoneProgressCancel newNotif
      case mHome of
        Nothing -> void $ sendAllSubIDEs miState newMsg
        Just home -> void $ sendSubIDEByPath miState home newMsg

    -- Special handing for STextDocumentDefinition to ask multiple IDEs
    LSP.FromClientMess LSP.STextDocumentDefinition req@LSP.RequestMessage {_id, _method, _params} -> do
      let path = filePathFromParamsWithTextDocument req
          lspId = castLspId _id
          method = LSP.SCustomMethod "daml/tryGetDefinition"
      putReqMethodSingleFromClient (fromClientMethodTrackerVar miState) lspId method
      sendSubIDEByPath miState path $ LSP.FromClientMess method $ LSP.ReqMess $
        LSP.RequestMessage "2.0" lspId method $ Aeson.toJSON $
          TryGetDefinitionParams (_params ^. LSP.textDocument) (_params ^. LSP.position)

    LSP.FromClientMess meth params ->
      case getMessageForwardingBehaviour meth params of
        ForwardRequest mess (Single path) -> do
          debugPrint $ "single req on method " <> show meth <> " over path " <> path
          let LSP.RequestMessage {_id, _method} = mess
          putReqMethodSingleFromClient (fromClientMethodTrackerVar miState) _id _method
          sendSubIDEByPath miState path (castFromClientMessage msg)

        ForwardRequest mess (AllRequest combine) -> do
          debugPrint $ "all req on method " <> show meth
          let LSP.RequestMessage {_id, _method} = mess
          ides <- sendAllSubIDEs miState (castFromClientMessage msg)
          putReqMethodAll (fromClientMethodTrackerVar miState) _id _method ides combine

        ForwardNotification _ (Single path) -> do
          debugPrint $ "single not on method " <> show meth <> " over path " <> path
          sendSubIDEByPath miState path (castFromClientMessage msg)

        ForwardNotification _ AllNotification -> do
          debugPrint $ "all not on method " <> show meth
          sendAllSubIDEs_ miState (castFromClientMessage msg)

        ExplicitHandler handler -> do
          handler (sendClient miState) (sendSubIDEByPath miState)
    LSP.FromClientRsp (Pair method (Const home)) rMsg -> 
      sendSubIDEByPath miState home $ LSP.FromClientRsp method $ 
        rMsg & LSP.id %~ fmap removeLspPrefix

getMultiPackageYamlMapping :: IO MultiPackageYamlMapping
getMultiPackageYamlMapping = do
  -- TODO: this will find the "closest" multi-package.yaml, but in a case where we have multiple referring to each other, we'll not see the outer one
  -- in that case, code jump won't work. Its unclear which the user would want, so we may want to prompt them with either closest or furthest (that links up)
  mPkgConfig <- findMultiPackageConfig $ ProjectPath "."
  case mPkgConfig of
    Nothing ->
      Map.empty <$ debugPrint "No multi-package.yaml found"
    Just path -> do
      debugPrint "Found multi-package.yaml"
      withMultiPackageConfig path $ \multiPackage -> do
        eUnitIds <- traverse unitIdFromDamlYaml (mpPackagePaths multiPackage)
        let eMapping = Map.fromList . flip zip (mpPackagePaths multiPackage) <$> sequence eUnitIds
        either throwIO pure eMapping

{-
Expect a multi-package.yaml at the workspace root
If we do not get one, we continue as normal (no popups) until the user attempts to open/use files in a different package to the first one
  When this occurs, this send a popup:
    Make a multi-package.yaml at the root and reload the editor please :)
    OR tell me where the multi-package.yaml(s) is
      if the user provides multiple, we union that lookup, allowing "cross project boundary" jumps
-}

-- Main loop logic

runMultiIde :: IO ()
runMultiIde = do
  multiPackageMapping <- getMultiPackageYamlMapping
  miState <- newMultiIdeState multiPackageMapping

  debugPrint "Listening for bytes"
  -- Client <- *****
  toClientThread <- async $ forever $ do
    msg <- atomically $ readTChan $ toClientChan miState
    debugPrint "Pushing message to client"
    -- BSLC.hPutStrLn stderr msg
    putChunk stdout msg

  -- Client -> Coord
  clientToCoordThread <- async $ do
    chunks <- getChunks stdin
    mapM_ (clientMessageHandler miState) chunks

  let killAll :: IO ()
      killAll = do
        debugPrint "Killing subIDEs"
        subIDEs <- atomically $ Map.filter ideActive <$> readTMVar (subIDEsVar miState)
        forM_ subIDEs (shutdownIde miState)

  handle (\(_ :: AsyncException) -> killAll) $ do
    atomically $ do
      unsafeIOToSTM $ debugPrint "Running main loop"
      subIDEs <- readTMVar $ subIDEsVar miState
      let asyncs = concatMap (\subIDE -> [ideInhandleAsync subIDE, ideOutHandleAsync subIDE]) subIDEs
      errs <- lefts . catMaybes <$> traverse pollSTM (asyncs ++ [toClientThread, clientToCoordThread])
      when (not $ null errs) $
        unsafeIOToSTM $ debugPrint $ "A thread handler errored with: " <> show (head errs)

      let procs = ideProcess <$> subIDEs
      exits <- catMaybes <$> traverse getExitCodeSTM (Map.elems procs)
      when (not $ null exits) $
        unsafeIOToSTM $ debugPrint $ "A subIDE finished with code: " <> show (head exits)

      when (null exits && null errs) retry

    -- If we get here, something failed/stopped, so stop everything
    killAll
