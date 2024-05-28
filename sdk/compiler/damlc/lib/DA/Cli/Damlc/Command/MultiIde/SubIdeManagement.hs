-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE DataKinds #-}

module DA.Cli.Damlc.Command.MultiIde.SubIdeManagement (
  module DA.Cli.Damlc.Command.MultiIde.SubIdeManagement,
  module DA.Cli.Damlc.Command.MultiIde.SubIdeCommunication,
) where

import Control.Concurrent.Async (async)
import Control.Concurrent.STM.TChan
import Control.Concurrent.STM.TMVar
import Control.Concurrent.STM.TVar
import Control.Concurrent.MVar
import Control.Lens
import Control.Monad
import Control.Monad.STM
import DA.Cli.Damlc.Command.MultiIde.ClientCommunication
import DA.Cli.Damlc.Command.MultiIde.Util
import DA.Cli.Damlc.Command.MultiIde.Parsing
import DA.Cli.Damlc.Command.MultiIde.Types
import DA.Cli.Damlc.Command.MultiIde.SubIdeCommunication
import Data.Foldable (traverse_)
import qualified Data.Map as Map
import Data.Maybe (fromMaybe, isJust)
import qualified Data.Set as Set
import qualified Data.Text as T
import qualified Data.Text.Extended as TE
import qualified Data.Text.IO as T
import qualified Language.LSP.Types as LSP
import System.Environment (getEnv, getEnvironment)
import System.IO.Extra
import System.Info.Extra (isWindows)
import System.Process (getPid, terminateProcess)
import System.Process.Typed (
  Process,
  StreamSpec,
  getStderr,
  getStdin,
  getStdout,
  mkPipeStreamSpec,
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
-- we also atomically send a message to the channel, without dropping the lock on the subIdes var
-- Note that messages sent here should _already_ be in the fromClientMessage tracker
addNewSubIdeAndSend
  :: MultiIdeState
  -> PackageHome
  -> Maybe LSP.FromClientMessage
  -> IO ()
addNewSubIdeAndSend miState home mMsg =
  withIDEs_ miState $ \ides -> unsafeAddNewSubIdeAndSend miState ides home mMsg

-- Unsafe as does not acquire SubIdesVar, instead simply transforms it
unsafeAddNewSubIdeAndSend
  :: MultiIdeState
  -> SubIdes
  -> PackageHome
  -> Maybe LSP.FromClientMessage
  -> IO SubIdes
unsafeAddNewSubIdeAndSend miState ides home mMsg = do
  logDebug miState "Trying to make a SubIde"

  let ideData = lookupSubIde home ides
  case ideDataMain ideData of
    Just ide -> do
      logDebug miState "SubIde already exists"
      forM_ mMsg $ unsafeSendSubIde ide
      pure ides
    Nothing | ideShouldDisable ideData || ideDataDisabled ideData -> do
      when (ideShouldDisable ideData) $ logDebug miState $ "SubIde failed twice within " <> show ideShouldDisableTimeout <> ", disabling SubIde"

      responses <- getUnrespondedRequestsFallbackResponses miState ideData home
      logDebug miState $ "Found " <> show (length responses) <> " unresponded messages, sending empty replies."
      
      -- Doesn't include mMsg, as if it was request, it'll already be in the tracker, so a reply for it will be in `responses`
      -- As such, we cannot send this on every failed message, 
      let ideData' = ideData {ideDataDisabled = True, ideDataFailTimes = []}
          -- Only add diagnostic messages for first fail to start.
          -- Diagnostic messages trigger the client to send a codeAction request, which would create an infinite loop if we sent
          -- diagnostics with its reply
          messages = responses <> if ideShouldDisable ideData then disableIdeDiagnosticMessages ideData else []

      atomically $ traverse_ (sendClientSTM miState) messages
      pure $ Map.insert home ideData' ides
    Nothing -> do
      logInfo miState $ "Creating new SubIde for " <> unPackageHome home
      traverse_ (sendClient miState) $ clearIdeDiagnosticMessages ideData

      unitId <- either (\cErr -> error $ "Failed to get unit ID from daml.yaml: " <> show cErr) fst <$> unitIdAndDepsFromDamlYaml home

      subIdeProcess <- runSubProc miState home
      let inHandle = getStdin subIdeProcess
          outHandle = getStdout subIdeProcess
          errHandle = getStderr subIdeProcess

      ideErrText <- newTVarIO @T.Text ""

      -- Handles blocking the sender thread until the IDE is initialized.
      (onceUnblocked, unblock) <- makeIOBlocker

      --           ***** -> SubIde
      toSubIdeChan <- atomically newTChan
      let pushMessageToSubIde :: IO ()
          pushMessageToSubIde = do
            msg <- atomically $ readTChan toSubIdeChan
            logDebug miState "Pushing message to subIde"
            putChunk inHandle msg
      toSubIde <- async $ do
        -- Allow first message (init) to be sent before unblocked
        pushMessageToSubIde
        onceUnblocked $ forever pushMessageToSubIde

      --           Coord <- SubIde
      subIdeToCoord <- async $ do
        -- Wait until our own IDE exists then pass it forward
        ide <- atomically $ fromMaybe (error "Failed to get own IDE") . ideDataMain . lookupSubIde home <$> readTMVar (misSubIdesVar miState)
        onChunks outHandle $ misSubIdeMessageHandler miState unblock ide

      pid <- fromMaybe (error "SubIde has no PID") <$> getPid (unsafeProcessHandle subIdeProcess)

      ideErrTextAsync <- async $
        let go = do
              text <- T.hGetChunk errHandle
              unless (text == "") $ do
                atomically $ modifyTVar' ideErrText (<> text)
                logDebug miState $ "[SubIde " <> show pid <> "] " <> T.unpack text
                go
         in go

      mInitParams <- tryReadMVar (misInitParamsVar miState)
      let ide = 
            SubIdeInstance
              { ideInhandleAsync = toSubIde
              , ideInHandle = inHandle
              , ideInHandleChannel = toSubIdeChan
              , ideOutHandle = outHandle
              , ideOutHandleAsync = subIdeToCoord
              , ideErrHandle = errHandle
              , ideErrText = ideErrText
              , ideErrTextAsync = ideErrTextAsync
              , ideProcess = subIdeProcess
              , ideHome = home
              , ideMessageIdPrefix = T.pack $ show pid
              , ideUnitId = unitId
              }
          ideData' = ideData {ideDataMain = Just ide}
          !initParams = fromMaybe (error "Attempted to create a SubIde before initialization!") mInitParams
          initMsg = initializeRequest initParams ide

      -- Must happen before the initialize message is added, else it'll delete that
      unrespondedRequests <- getUnrespondedRequestsToResend miState ideData home

      logDebug miState "Sending init message to SubIde"
      putSingleFromClientMessage miState home initMsg
      unsafeSendSubIde ide initMsg

      -- Dangerous calls are okay here because we're already holding the misSubIdesVar lock
      -- Send the open file notifications
      logDebug miState "Sending open files messages to SubIde"
      forM_ (ideDataOpenFiles ideData') $ \path -> do
        content <- TE.readFileUtf8 $ unDamlFile path
        unsafeSendSubIde ide $ openFileNotification path content
      

      -- Resend all pending requests
      -- No need for re-prefixing or anything like that, messages are stored with the prefixes they need
      -- Note that we want to remove the message we're sending from this list, to not send it twice
      let mMsgLspId = mMsg >>= fromClientRequestLspId
          requestsToResend = filter (\req -> fromClientRequestLspId req /= mMsgLspId) unrespondedRequests
      logDebug miState $ "Found " <> show (length requestsToResend) <> " unresponded messages, resending:\n"
        <> show (fmap (\r -> (fromClientRequestMethod r, fromClientRequestLspId r)) requestsToResend)

      traverse_ (unsafeSendSubIde ide) requestsToResend

      logDebug miState $ "Sending intended message to SubIde: " <> show ((\r -> (fromClientRequestMethod r, fromClientRequestLspId r)) <$> mMsg)
      -- Send the intended message
      forM_ mMsg $ unsafeSendSubIde ide

      pure $ Map.insert home ideData' ides

runSubProc :: MultiIdeState -> PackageHome -> IO (Process Handle Handle Handle)
runSubProc miState home = do
  assistantPath <- getEnv "DAML_ASSISTANT"
  -- Need to remove some variables so the sub-assistant will pick them up from the working dir/daml.yaml
  assistantEnv <- filter (flip notElem ["DAML_PROJECT", "DAML_SDK_VERSION", "DAML_SDK"] . fst) <$> getEnvironment

  startProcess $
    proc assistantPath ("ide" : misSubIdeArgs miState) &
      setStdin createPipeNoClose &
      setStdout createPipeNoClose &
      setStderr createPipeNoClose &
      setWorkingDir (unPackageHome home) &
      setEnv assistantEnv
  where
    createPipeNoClose :: StreamSpec streamType Handle
    createPipeNoClose = mkPipeStreamSpec $ \_ h -> pure (h, pure ())

-- Spin-down logic
rebootIdeByHome :: MultiIdeState -> PackageHome -> IO ()
rebootIdeByHome miState home = withIDEs_ miState $ \ides -> do
  ides' <- unsafeShutdownIdeByHome miState ides home
  unsafeAddNewSubIdeAndSend miState ides' home Nothing

-- Version of rebootIdeByHome that only spins up IDEs that were either active, or disabled.
-- Does not spin up IDEs that were naturally shutdown/never started
lenientRebootIdeByHome :: MultiIdeState -> PackageHome -> IO ()
lenientRebootIdeByHome miState home = withIDEs_ miState $ \ides -> do
  let ideData = lookupSubIde home ides
      shouldBoot = isJust (ideDataMain ideData) || ideDataDisabled ideData
  ides' <- unsafeShutdownIdeByHome miState ides home
  if shouldBoot 
    then unsafeAddNewSubIdeAndSend miState ides' home Nothing
    else pure ides'

-- Checks if a shutdown message LspId originated from the multi-ide coordinator
isCoordinatorShutdownLspId :: LSP.LspId 'LSP.Shutdown -> Bool
isCoordinatorShutdownLspId (LSP.IdString str) = "-shutdown" `T.isSuffixOf` str
isCoordinatorShutdownLspId _ = False

-- Sends a shutdown message and moves SubIdeInstance to `ideDataClosing`, disallowing any further client messages to be sent to the subIde
-- given queue nature of TChan, all other pending messages will be sent first before handling shutdown
shutdownIdeByHome :: MultiIdeState -> PackageHome -> IO ()
shutdownIdeByHome miState home = withIDEs_ miState $ \ides -> unsafeShutdownIdeByHome miState ides home

-- Unsafe as does not acquire SubIdesVar, instead simply transforms it
unsafeShutdownIdeByHome :: MultiIdeState -> SubIdes -> PackageHome -> IO SubIdes
unsafeShutdownIdeByHome miState ides home = do
  let ideData = lookupSubIde home ides
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
      
      logDebug miState $ "Sending shutdown message to " <> unPackageHome (ideDataHome ideData)

      putSingleFromClientMessage miState home shutdownMsg
      unsafeSendSubIde ide shutdownMsg
      pure $ Map.adjust (\ideData' -> ideData' 
        { ideDataMain = Nothing
        , ideDataClosing = Set.insert ide $ ideDataClosing ideData
        , ideDataFailTimes = []
        , ideDataDisabled = False
        }) home ides
    Nothing ->
      pure $ Map.adjust (\ideData -> ideData {ideDataFailTimes = [], ideDataDisabled = False}) home ides

-- To be called once we receive the Shutdown response
-- Safe to assume that the sending channel is empty, so we can end the thread and send the final notification directly on the handle
handleExit :: MultiIdeState -> SubIdeInstance -> IO ()
handleExit miState ide =
  if isWindows
    then do
      -- On windows, ghc-ide doesn't close correctly on exit messages (even terminating the process leaves subprocesses behind)
      -- Instead, we close the handle its listening on, and terminate the process.
      logDebug miState $ "(windows) Closing handle and terminating " <> unPackageHome (ideHome ide)
      hTryClose $ ideInHandle ide
      terminateProcess $ unsafeProcessHandle $ ideProcess ide
    else do
      let (exitMsg :: LSP.FromClientMessage) = LSP.FromClientMess LSP.SExit LSP.NotificationMessage
            { _method = LSP.SExit
            , _params = LSP.Empty
            , _jsonrpc = "2.0"
            }
      logDebug miState $ "Sending exit message to " <> unPackageHome (ideHome ide)
      -- This will cause the subIde process to exit
      -- Able to be unsafe as no other messages can use this IDE once it has been shutdown
      unsafeSendSubIde ide exitMsg

-- This function lives here instead of SubIdeCommunication because it can spin up new subIDEs
sendSubIdeByPath :: MultiIdeState -> FilePath -> LSP.FromClientMessage -> IO ()
sendSubIdeByPath miState path msg = do
  home <- atomically $ getSourceFileHome miState path
  putSingleFromClientMessage miState home msg

  withIDEs_ miState $ \ides -> do
    let ideData = lookupSubIde home ides
    case ideDataMain ideData of
      -- Here we already have a subIde, so we forward our message to it before dropping the lock
      Just ide -> do
        unsafeSendSubIde ide msg
        logDebug miState $ "Found relevant SubIde: " <> unPackageHome (ideDataHome ideData)
        pure ides
      -- This path will create a new subIde at the given home
      Nothing -> do
        unsafeAddNewSubIdeAndSend miState ides home $ Just msg
