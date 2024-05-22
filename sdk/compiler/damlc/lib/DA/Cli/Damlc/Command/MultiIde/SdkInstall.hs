-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE DataKinds #-}

module DA.Cli.Damlc.Command.MultiIde.SdkInstall (
  allowSdkInstall,
  ensureSdkInstalled,
  handleSdkInstallCancelled,
  handleShowMessageResponse,
  untrackPackageSdkInstall,
) where

import Control.Concurrent.Async
import Control.Concurrent.MVar
import Control.Concurrent.STM.TMVar
import Control.Exception (SomeException, tryJust, displayException, fromException)
import Control.Lens ((^.))
import Control.Monad (foldM, forM_)
import Control.Monad.STM
import Data.Aeson (fromJSON, toJSON)
import Data.Either.Extra (eitherToMaybe)
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Data.Text as T
import DA.Cli.Damlc.Command.MultiIde.ClientCommunication
import DA.Cli.Damlc.Command.MultiIde.OpenFiles
import DA.Cli.Damlc.Command.MultiIde.Parsing
import DA.Cli.Damlc.Command.MultiIde.Types
import DA.Cli.Damlc.Command.MultiIde.Util
import DA.Daml.Assistant.Env
import DA.Daml.Assistant.Install
import DA.Daml.Assistant.Types
import DA.Daml.Assistant.Util (tryConfig)
import DA.Daml.Assistant.Version
import DA.Daml.Project.Config
import qualified Language.LSP.Types as LSP
import qualified Language.LSP.Types.Lens as LSP

-- Check if an ask is installed, transform the subIDE data to disable it if needed
ensureSdkInstalled :: MultiIdeState -> UnresolvedReleaseVersion -> PackageHome -> SubIdeData -> IO SubIdeData
ensureSdkInstalled miState ver home ideData = do
  installDatas <- atomically $ takeTMVar $ misSdkInstallDatasVar miState
  let installData = getSdkInstallData ver installDatas
  (newInstallDatas, mDisableDiagnostic) <- case sidStatus installData of
    SISCanAsk -> do
      damlPath <- getDamlPath
      installedVersions <- getInstalledSdkVersions damlPath
      let versionIsInstalled = any ((unwrapUnresolvedReleaseVersion ver==) . releaseVersionFromReleaseVersion) installedVersions

      if versionIsInstalled
        then pure (installDatas, Nothing)
        else do
          -- Ask the user if they want to install
          let verStr = T.pack $ unresolvedReleaseVersionToString ver
              lspId = LSP.IdString $ verStr <> "-sdk-install-request"
              messageContent = "This package uses the release version " <> verStr <> " which is not installed on this system.\n"
                <> "The IDE cannot give intelligence on this package without this SDK. Would you like to install it?"
              message = showMessageRequest lspId LSP.MtError messageContent ["Install SDK " <> verStr, "Do not install"]
              installData' = 
                installData
                  { sidPendingHomes = Set.insert home $ sidPendingHomes installData
                  , sidStatus = SISAsking
                  }

          putFromServerCoordinatorMessage miState message
          sendClient miState message
          pure (Map.insert ver installData' installDatas, Just (LSP.DsError, missingSdkIdeDiagnosticMessage ver))
    -- If the home is already in the set, the diagnostic has already been sent
    _ | Set.member home $ sidPendingHomes installData -> pure (installDatas, Nothing)
    _ ->
      let message = 
            case sidStatus installData of
              SISInstalling _ -> (LSP.DsInfo, installingSdkIdeDiagnosticMessage ver)
              SISFailed log err -> (LSP.DsError, failedInstallIdeDiagnosticMessage ver log err)
              _ -> (LSP.DsError, missingSdkIdeDiagnosticMessage ver)
       in pure (Map.insert ver (installData {sidPendingHomes = Set.insert home $ sidPendingHomes installData}) installDatas, Just message)
  atomically $ do
    putTMVar (misSdkInstallDatasVar miState) newInstallDatas
    case mDisableDiagnostic of
      Just (severity, message) -> do
        let ideData' = ideData {ideDataDisabled = IdeDataDisabled severity message}
        sendPackageDiagnostic miState ideData'
        pure ideData'
      Nothing -> pure ideData

missingSdkIdeDiagnosticMessage :: UnresolvedReleaseVersion -> T.Text
missingSdkIdeDiagnosticMessage ver =
  let verText = T.pack $ unresolvedReleaseVersionToString ver
   in "Missing required Daml SDK version " <> verText <> " to create development environment.\n"
        <> "Install this version via `daml install " <> verText <> "`, or save the daml.yaml to be prompted"

installingSdkIdeDiagnosticMessage :: UnresolvedReleaseVersion -> T.Text
installingSdkIdeDiagnosticMessage ver =
  "Installing Daml SDK version " <> T.pack (unresolvedReleaseVersionToString ver)

failedInstallIdeDiagnosticMessage :: UnresolvedReleaseVersion -> T.Text -> SomeException -> T.Text
failedInstallIdeDiagnosticMessage ver outputLog err =
  "Failed to install Daml SDK version " <> T.pack (unresolvedReleaseVersionToString ver) <> " due to the following:\n"
    <> (if outputLog == "" then "" else outputLog <> "\n")
    <> T.pack (displayException err)

updateSdkStatus :: MultiIdeState -> SdkInstallDatas -> UnresolvedReleaseVersion -> LSP.DiagnosticSeverity -> T.Text -> SdkInstallStatus -> IO SdkInstallDatas
updateSdkStatus miState installDatas ver severity message newStatus = do
  let installData = getSdkInstallData ver installDatas
      homes = sidPendingHomes installData
      disableIde :: SubIdes -> PackageHome -> STM SubIdes
      disableIde ides home = do
        let ideData = (lookupSubIde home ides) {ideDataDisabled = IdeDataDisabled severity message}
        sendPackageDiagnostic miState ideData
        pure $ Map.insert home ideData ides
  withIDEsAtomic miState $ \ides -> do
    ides' <- foldM disableIde ides homes
    pure (ides', Map.insert ver (installData {sidStatus = newStatus}) installDatas)

releaseVersionFromLspId :: LSP.LspId 'LSP.WindowShowMessageRequest -> Maybe UnresolvedReleaseVersion
releaseVersionFromLspId (LSP.IdString lspIdStr) = T.stripSuffix "-sdk-install-request" lspIdStr >>= eitherToMaybe . parseUnresolvedVersion
releaseVersionFromLspId _ = Nothing

handleShowMessageResponse :: MultiIdeState -> LSP.LspId 'LSP.WindowShowMessageRequest -> Either LSP.ResponseError (Maybe LSP.MessageActionItem) -> IO ()
handleShowMessageResponse miState (releaseVersionFromLspId -> Just ver) res = do
  installDatas <- atomically $ takeTMVar $ misSdkInstallDatasVar miState
  let installData = getSdkInstallData ver installDatas
      changeSdkStatus :: LSP.DiagnosticSeverity -> T.Text -> SdkInstallStatus -> IO ()
      changeSdkStatus severity message newStatus = do
        installDatas' <- updateSdkStatus miState installDatas ver severity message newStatus
        atomically $ putTMVar (misSdkInstallDatasVar miState) installDatas'
      disableSdk = changeSdkStatus LSP.DsError (missingSdkIdeDiagnosticMessage ver) SISDenied

  case (sidStatus installData, res) of
    (SISAsking, Right (Just (LSP.MessageActionItem (T.stripPrefix "Install SDK" -> Just _)))) -> do
      -- Install accepted, start install process
      installThread <- async $ do
        setupSdkInstallReporter miState ver
        outputLogVar <- newMVar ""
        res <- tryForwardAsync $ installSdk ver outputLogVar $ updateSdkInstallReporter miState ver
        handleInstallResult miState ver outputLogVar $ either Just (const Nothing) res
        finishSdkInstallReporter miState ver
      changeSdkStatus LSP.DsInfo (installingSdkIdeDiagnosticMessage ver) (SISInstalling installThread)
    (SISAsking, _) -> disableSdk
    (_, _) -> atomically $ putTMVar (misSdkInstallDatasVar miState) installDatas
handleShowMessageResponse _ _ _ = pure ()

-- try @SomeException that doesn't catch AsyncCancelled exceptions
tryForwardAsync :: IO a -> IO (Either SomeException a)
tryForwardAsync = tryJust @SomeException $ \case
  (fromException -> Just AsyncCancelled) -> Nothing
  e -> Just e

handleSdkInstallCancelled :: MultiIdeState -> LSP.NotificationMessage 'LSP.CustomMethod -> IO ()
handleSdkInstallCancelled miState notif = do
  forM_ (fromJSON $ notif ^. LSP.params) $ \message -> do
    let ver = sicSdkVersion message
    installDatas <- atomically $ takeTMVar $ misSdkInstallDatasVar miState
    let installData = getSdkInstallData ver installDatas
    installDatas' <- case sidStatus installData of
      SISInstalling thread -> do
        logDebug miState $ "Killing install thread for " <> unresolvedReleaseVersionToString ver
        cancel thread
        updateSdkStatus miState installDatas ver LSP.DsError (missingSdkIdeDiagnosticMessage ver) SISDenied
      _ -> pure installDatas
    atomically $ putTMVar (misSdkInstallDatasVar miState) installDatas'

handleInstallResult :: MultiIdeState -> UnresolvedReleaseVersion -> MVar T.Text -> Maybe SomeException -> IO ()
handleInstallResult miState ver outputLogVar mError = do
  installDatas <- atomically $ takeTMVar $ misSdkInstallDatasVar miState
  let installData = getSdkInstallData ver installDatas
  case mError of
    Nothing -> do
      let homes = sidPendingHomes installData
          installDatas' = Map.delete ver installDatas
          disableIde :: SubIdes -> PackageHome -> IO SubIdes
          disableIde ides home =
            let ides' = Map.insert home ((lookupSubIde home ides) {ideDataDisabled = IdeDataNotDisabled}) ides
             in misUnsafeAddNewSubIdeAndSend miState ides' home Nothing
      atomically $ putTMVar (misSdkInstallDatasVar miState) installDatas'
      withIDEs_ miState $ \ides -> foldM disableIde ides homes
    Just err -> do
      outputLog <- takeMVar outputLogVar
      let errText = failedInstallIdeDiagnosticMessage ver outputLog err
      installDatas' <- updateSdkStatus miState installDatas ver LSP.DsError errText (SISFailed outputLog err)
      sendClient miState $ showMessage LSP.MtError errText
      atomically $ putTMVar (misSdkInstallDatasVar miState) installDatas'

-- Given a version, and a progress computation, install an sdk (blocking)
installSdk :: UnresolvedReleaseVersion -> MVar Text -> (Int -> IO ()) -> IO ()
installSdk unresolvedVersion outputLogVar report = do
  damlPath <- getDamlPath
  cachePath <- getCachePath
  let useCache = mkUseCache cachePath damlPath

  version <- resolveReleaseVersionUnsafe useCache unresolvedVersion
  damlConfigE <- tryConfig $ readDamlConfig damlPath
  let env = InstallEnv
        { options = InstallOptions
            { iTargetM = Nothing
            , iSnapshots = False
            , iAssistant = InstallAssistant No
            , iForce = ForceInstall True
            , iQuiet = QuietInstall False
            , iSetPath = SetPath No
            , iBashCompletions = BashCompletions No
            , iZshCompletions = ZshCompletions No
            , iInstallWithInternalVersion = InstallWithInternalVersion False
            , iInstallWithCustomVersion = InstallWithCustomVersion Nothing
            }
        , targetVersionM = version
        , assistantVersion = Nothing
        , damlPath = damlPath
        , useCache = useCache
        , missingAssistant = False
        , installingFromOutside = False
        , projectPathM = Nothing
        , artifactoryApiKeyM = queryArtifactoryApiKey =<< eitherToMaybe damlConfigE
        , output = \str -> modifyMVar_ outputLogVar $ pure . (<> T.pack str)
        , downloadProgressObserver = Just report
        }
  versionInstall env

sendSdkInstallProgress :: MultiIdeState -> UnresolvedReleaseVersion -> DamlSdkInstallProgressNotificationKind -> Int -> IO ()
sendSdkInstallProgress miState ver kind progress =
  sendClient miState $ LSP.FromServerMess (LSP.SCustomMethod damlSdkInstallProgressMethod) $ LSP.NotMess $
    LSP.NotificationMessage "2.0" (LSP.SCustomMethod damlSdkInstallProgressMethod) $ toJSON $ DamlSdkInstallProgressNotification
      { sipSdkVersion = ver
      , sipKind = kind
      , sipProgress = progress
      }

setupSdkInstallReporter :: MultiIdeState -> UnresolvedReleaseVersion -> IO ()
setupSdkInstallReporter miState ver = sendSdkInstallProgress miState ver InstallProgressBegin 0
  
updateSdkInstallReporter :: MultiIdeState -> UnresolvedReleaseVersion -> Int -> IO ()
updateSdkInstallReporter miState ver = sendSdkInstallProgress miState ver InstallProgressReport

finishSdkInstallReporter :: MultiIdeState -> UnresolvedReleaseVersion -> IO ()
finishSdkInstallReporter miState ver = sendSdkInstallProgress miState ver InstallProgressEnd 100

untrackPackageSdkInstall :: MultiIdeState -> PackageHome -> IO ()
untrackPackageSdkInstall miState home = atomically $ modifyTMVar (misSdkInstallDatasVar miState) $
  fmap $ \installData -> installData {sidPendingHomes = Set.delete home $ sidPendingHomes installData}

allowSdkInstall :: MultiIdeState -> PackageHome -> IO ()
allowSdkInstall miState home = do
  ePackageSummary <- packageSummaryFromDamlYaml home
  forM_ ePackageSummary $ \ps ->
    atomically $ modifyTMVar (misSdkInstallDatasVar miState) $ Map.adjust (\installData ->
      installData 
        { sidStatus = case sidStatus installData of
            SISDenied -> SISCanAsk
            SISFailed _ _ -> SISCanAsk
            _ -> sidStatus installData
        }
    ) (psReleaseVersion ps)
