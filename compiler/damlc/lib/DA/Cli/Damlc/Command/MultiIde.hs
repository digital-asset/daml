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
import System.Posix.Signals (installHandler, sigTERM, Handler (Catch))
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
  :: StateVars
  -> FilePath
  -> LSP.FromClientMessage
  -> IO SubIDE
addNewSubIDEAndSend stateVars home msg = do
  debugPrint "Trying to make a SubIDE"
  ides <- atomically $ takeTMVar $ subIDEsVar stateVars

  let mExistingIde = mfilter ideActive $ Map.lookup home ides
  case mExistingIde of
    Just ide -> do
      debugPrint "SubIDE already exists"
      sendSubIDE ide msg
      atomically $ putTMVar (subIDEsVar stateVars) ides
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
        ide <- atomically $ fromMaybe (error "Failed to get own IDE") . mfilter ideActive . Map.lookup home <$> readTMVar (subIDEsVar stateVars)
        chunks <- getChunks outHandle
        mapM_ (subIDEMessageHandler stateVars unblock ide) chunks

      pid <- fromMaybe (error "SubIDE has no PID") <$> getPid (unsafeProcessHandle subIdeProcess)

      mInitParams <- tryReadMVar (initParamsVar stateVars)
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


      putReqMethodSingleFromClient (fromClientMethodTrackerVar stateVars) initId LSP.SInitialize
      putChunk inHandle $ Aeson.encode initMsg
      -- Dangerous call is okay here because we're already holding the subIDEsVar lock
      sendSubIDE ide msg

      atomically $ putTMVar (subIDEsVar stateVars) $ Map.insert home ide ides

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
shutdownIde :: StateVars -> SubIDE -> IO ()
shutdownIde stateVars ide = do
  ides <- atomically $ takeTMVar (subIDEsVar stateVars)
  let shutdownId = LSP.IdString $ T.pack $ show (ideProcessID ide) <> "-shutdown"
      (shutdownMsg :: LSP.FromClientMessage) = LSP.FromClientMess LSP.SShutdown LSP.RequestMessage 
        { _id = shutdownId
        , _method = LSP.SShutdown
        , _params = LSP.Empty
        , _jsonrpc = "2.0"
        }
  
  putReqMethodSingleFromClient (fromClientMethodTrackerVar stateVars) shutdownId LSP.SShutdown
  sendSubIDE ide shutdownMsg

  atomically $ putTMVar (subIDEsVar stateVars) $ Map.adjust (\ide' -> ide' {ideActive = False}) (ideHomeDirectory ide) ides

-- To be called once we receive the Shutdown response
-- Safe to assume that the sending channel is empty, so we can end the thread and send the final notification directly on the handle
handleExit :: StateVars -> SubIDE -> IO ()
handleExit stateVars ide = do
  let (exitMsg :: LSP.FromClientMessage) = LSP.FromClientMess LSP.SExit LSP.NotificationMessage
        { _method = LSP.SExit
        , _params = LSP.Empty
        , _jsonrpc = "2.0"
        }
  -- This will cause the subIDE process to exit
  putChunk (ideInHandle ide) $ Aeson.encode exitMsg
  atomically $ modifyTMVar (subIDEsVar stateVars) $ Map.delete (ideHomeDirectory ide)
  cancel $ ideInhandleAsync ide
  cancel $ ideOutHandleAsync ide

-- Communication logic

-- Dangerous as does not hold the subIDEsVar lock. If a shutdown is called whiled this is running, the message may not be sent.
sendSubIDE :: SubIDE -> LSP.FromClientMessage -> IO ()
sendSubIDE ide = atomically . writeTChan (ideInHandleChannel ide) . Aeson.encode

sendClient :: StateVars -> LSP.FromServerMessage -> IO ()
sendClient stateVars = atomically . writeTChan (toClientChan stateVars) . Aeson.encode

sendAllSubIDEs :: StateVars -> LSP.FromClientMessage -> IO [FilePath]
sendAllSubIDEs stateVars msg = atomically $ do
  idesUnfiltered <- takeTMVar (subIDEsVar stateVars)
  let ides = Map.filter ideActive idesUnfiltered
  if null ides then error "Got a broadcast to nothing :(" else pure ()
  homes <- forM (Map.elems ides) $ \ide -> ideHomeDirectory ide <$ writeTChan (ideInHandleChannel ide) (Aeson.encode msg)
  putTMVar (subIDEsVar stateVars) idesUnfiltered
  pure homes

sendAllSubIDEs_ :: StateVars -> LSP.FromClientMessage -> IO ()
sendAllSubIDEs_ stateVars = void . sendAllSubIDEs stateVars

sendSubIDEByPath :: StateVars -> FilePath -> LSP.FromClientMessage -> IO ()
sendSubIDEByPath stateVars path msg = do
  mHome <- sendSubIDEByPath_ path msg
  -- Lock is dropped then regained here for new IDE. This is acceptable as it's impossible for a shutdown
  -- of the new ide to be sent before its created.
  -- Note that if sendSubIDEByPath is called multiple times concurrently for a new IDE, addNewSubIDEAndSend may be called twice for the same home
  --   addNewSubIDEAndSend handles this internally with its own checks, so this is acceptable.
  forM_ mHome $ \home -> addNewSubIDEAndSend stateVars home msg
  where
    -- If a SubIDE is needed, returns the path out of the STM transaction
    sendSubIDEByPath_ :: FilePath -> LSP.FromClientMessage -> IO (Maybe FilePath)
    sendSubIDEByPath_ path msg = atomically $ do
      idesUnfiltered <- takeTMVar (subIDEsVar stateVars)
      let ides = Map.filter ideActive idesUnfiltered
          mHome = find (`isPrefixOf` path) $ Map.keys ides
          mIde = mHome >>= flip Map.lookup ides

      case mIde of
        Just ide -> do
          writeTChan (ideInHandleChannel ide) (Aeson.encode msg)
          unsafeIOToSTM $ debugPrint $ "Found relevant SubIDE: " <> ideHomeDirectory ide
          putTMVar (subIDEsVar stateVars) idesUnfiltered
          pure Nothing
        Nothing -> do
          putTMVar (subIDEsVar stateVars) idesUnfiltered
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
                    writeTChan (toClientChan stateVars) $ Aeson.encode $
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

subIDEMessageHandler :: StateVars -> IO () -> SubIDE -> B.ByteString -> IO ()
subIDEMessageHandler stateVars unblock ide bs = do
  debugPrint "Called subIDEMessageHandler"
  -- BSC.hPutStrLn stderr bs

  -- Decode a value, parse
  let val :: Aeson.Value
      val = er "eitherDecode" $ Aeson.eitherDecodeStrict bs
  mMsg <- either error id <$> parseServerMessageWithTracker (fromClientMethodTrackerVar stateVars) (ideHomeDirectory ide) val

  -- Adds the various prefixes needed for from server messages to not clash with those from other IDEs
  mPrefixedMsg <-
    mapM 
      ( addProgressTokenPrefixToServerMessage (serverCreatedProgressTokensVar stateVars) (ideHomeDirectory ide) (ideMessageIdPrefix ide)
      . addLspPrefixToServerMessage ide
      )
      mMsg

  forM_ mPrefixedMsg $ \msg -> do
    -- If its a request (builtin or custom), save it for response handling.
    putServerReq (fromServerMethodTrackerVar stateVars) (ideHomeDirectory ide) msg

    debugPrint "About to thunk message"
    case msg of
      LSP.FromServerRsp LSP.SInitialize LSP.ResponseMessage {_result} -> do
        debugPrint "Got initialization reply, sending initialized and unblocking"
        -- Dangerous call here is acceptable as this only happens while the ide is booting, before unblocking
        sendSubIDE ide $ LSP.FromClientMess LSP.SInitialized $ LSP.NotificationMessage "2.0" LSP.SInitialized (Just LSP.InitializedParams)
        unblock
      LSP.FromServerRsp LSP.SShutdown _ -> handleExit stateVars ide

      LSP.FromServerRsp (LSP.SCustomMethod "daml/tryGetDefinition") LSP.ResponseMessage {_id, _result} -> do
        debugPrint "Got tryGetDefinition response, handling..."
        let parsedResult = parseCustomResult @(Maybe TryGetDefinitionResult) "daml/tryGetDefinition" _result
            reply :: Either LSP.ResponseError (LSP.ResponseResult 'LSP.TextDocumentDefinition) -> IO ()
            reply rsp = do
              debugPrint $ "Replying directly to client with " <> show rsp
              sendClient stateVars $ LSP.FromServerRsp LSP.STextDocumentDefinition $ LSP.ResponseMessage "2.0" (castLspId <$> _id) rsp
            replyLocations :: [LSP.Location] -> IO ()
            replyLocations = reply . Right . LSP.InR . LSP.InL . LSP.List
        case parsedResult of
          Left err -> reply $ Left err
          Right Nothing -> replyLocations []
          Right (Just (TryGetDefinitionResult loc Nothing)) -> replyLocations [loc]
          Right (Just (TryGetDefinitionResult loc (Just name))) -> do
            debugPrint $ "Got name in result! Backup location is " <> show loc
            idesUnfiltered <- atomically $ takeTMVar (subIDEsVar stateVars)
            let ides = Map.filter ideActive idesUnfiltered
                mIde = find (\ide -> ideUnitId ide == tgdnPackageUnitId name) ides
            case mIde of
              Nothing -> replyLocations [loc]
              Just ide -> do
                debugPrint $ "We have a SubIDE with the given unitId, forwarding to " <> ideHomeDirectory ide
                let method = LSP.SCustomMethod "daml/gotoDefinitionByName"
                    lspId = maybe (error "No LspId provided back from tryGetDefinition") castLspId _id
                    msg = LSP.FromClientMess method $ LSP.ReqMess $ LSP.RequestMessage "2.0" lspId method
                            $ Aeson.toJSON $ GotoDefinitionByNameParams loc name
                              
                putReqMethodSingleFromClient (fromClientMethodTrackerVar stateVars) lspId method
                sendSubIDE ide msg
            atomically $ putTMVar (subIDEsVar stateVars) idesUnfiltered
      
      LSP.FromServerRsp (LSP.SCustomMethod "daml/gotoDefinitionByName") LSP.ResponseMessage {_id, _result} -> do
        debugPrint "Got gotoDefinitionByName response, handling..."
        let parsedResult = parseCustomResult @GotoDefinitionByNameResult "daml/gotoDefinitionByName" _result
            reply :: Either LSP.ResponseError (LSP.ResponseResult 'LSP.TextDocumentDefinition) -> IO ()
            reply rsp = do
              debugPrint $ "Replying directly to client with " <> show rsp
              sendClient stateVars $ LSP.FromServerRsp LSP.STextDocumentDefinition $ LSP.ResponseMessage "2.0" (castLspId <$> _id) rsp
        case parsedResult of
          Left err -> reply $ Left err
          Right loc -> reply $ Right $ LSP.InR $ LSP.InL $ LSP.List [loc]

      LSP.FromServerMess method _ -> do
        debugPrint $ "Backwarding request " <> show method
        sendClient stateVars msg
      LSP.FromServerRsp method _ -> do
        debugPrint $ "Backwarding response to " <> show method
        sendClient stateVars msg

clientMessageHandler :: StateVars -> B.ByteString -> IO ()
clientMessageHandler stateVars bs = do
  debugPrint "Called clientMessageHandler"

  -- Decode a value, parse
  let castFromClientMessage :: LSP.FromClientMessage' (Product LSP.SMethod (Const FilePath)) -> LSP.FromClientMessage
      castFromClientMessage = \case
        LSP.FromClientMess method params -> LSP.FromClientMess method params
        LSP.FromClientRsp (Pair method _) params -> LSP.FromClientRsp method params

      val :: Aeson.Value
      val = er "eitherDecode" $ Aeson.eitherDecodeStrict bs

  msg <- either error id <$> parseClientMessageWithTracker (fromServerMethodTrackerVar stateVars) val

  case msg of
    -- Store the initialize params for starting subIDEs, respond statically with what ghc-ide usually sends.
    LSP.FromClientMess LSP.SInitialize LSP.RequestMessage {_id, _method, _params} -> do
      putMVar (initParamsVar stateVars) _params
      sendClient stateVars $ LSP.FromServerRsp _method $ LSP.ResponseMessage "2.0" (Just _id) (Right initializeResult)
    LSP.FromClientMess LSP.SWindowWorkDoneProgressCancel notif -> do
      (newNotif, mHome) <- removeWorkDoneProgressCancelTokenPrefix (serverCreatedProgressTokensVar stateVars) notif
      let newMsg = LSP.FromClientMess LSP.SWindowWorkDoneProgressCancel newNotif
      case mHome of
        Nothing -> void $ sendAllSubIDEs stateVars newMsg
        Just home -> void $ sendSubIDEByPath stateVars home newMsg

    -- Special handing for STextDocumentDefinition to ask multiple IDEs
    LSP.FromClientMess LSP.STextDocumentDefinition req@LSP.RequestMessage {_id, _method, _params} -> do
      let path = filePathFromParamsWithTextDocument req
          lspId = castLspId _id
          method = LSP.SCustomMethod "daml/tryGetDefinition"
      putReqMethodSingleFromClient (fromClientMethodTrackerVar stateVars) lspId method
      sendSubIDEByPath stateVars path $ LSP.FromClientMess method $ LSP.ReqMess $
        LSP.RequestMessage "2.0" lspId method $ Aeson.toJSON $
          TryGetDefinitionParams (_params ^. LSP.textDocument) (_params ^. LSP.position)

    LSP.FromClientMess meth params ->
      case getMessageForwardingBehaviour meth params of
        ForwardRequest mess (Single path) -> do
          debugPrint $ "single req on method " <> show meth <> " over path " <> path
          let LSP.RequestMessage {_id, _method} = mess
          putReqMethodSingleFromClient (fromClientMethodTrackerVar stateVars) _id _method
          sendSubIDEByPath stateVars path (castFromClientMessage msg)

        ForwardRequest mess (AllRequest combine) -> do
          debugPrint $ "all req on method " <> show meth
          let LSP.RequestMessage {_id, _method} = mess
          ides <- sendAllSubIDEs stateVars (castFromClientMessage msg)
          putReqMethodAll (fromClientMethodTrackerVar stateVars) _id _method ides combine

        ForwardNotification _ (Single path) -> do
          debugPrint $ "single not on method " <> show meth <> " over path " <> path
          sendSubIDEByPath stateVars path (castFromClientMessage msg)

        ForwardNotification _ AllNotification -> do
          debugPrint $ "all not on method " <> show meth
          sendAllSubIDEs_ stateVars (castFromClientMessage msg)

        ExplicitHandler handler -> do
          handler (sendClient stateVars) (sendSubIDEByPath stateVars)
    LSP.FromClientRsp (Pair method (Const home)) rMsg -> 
      sendSubIDEByPath stateVars home $ LSP.FromClientRsp method $ 
        rMsg & LSP.id %~ fmap removeLspPrefix

-- Main loop logic

runMultiIde :: [String] -> IO ()
runMultiIde sourceDeps = do
  hPrint stderr sourceDeps

  stateVars <- newStateVars

  debugPrint "Listening for bytes"
  -- Client <- *****
  toClientThread <- async $ forever $ do
    msg <- atomically $ readTChan $ toClientChan stateVars
    debugPrint "Pushing message to client"
    -- BSLC.hPutStrLn stderr msg
    putChunk stdout msg

  -- Client -> Coord
  clientToCoordThread <- async $ do
    chunks <- getChunks stdin
    mapM_ (clientMessageHandler stateVars) chunks

  let killAll :: IO ()
      killAll = do
        subIDEs <- atomically $ Map.filter ideActive <$> readTMVar (subIDEsVar stateVars)
        forM_ subIDEs (shutdownIde stateVars)

  installHandler sigTERM (Catch killAll) Nothing

  atomically $ do
    unsafeIOToSTM $ debugPrint "Running main loop"
    subIDEs <- readTMVar $ subIDEsVar stateVars
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

  pure ()
