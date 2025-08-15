-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE DataKinds #-}

module DA.Cli.Damlc.Command.MultiIde.SdkInstall (
  allowIdeSdkInstall,
  ensureIdeSdkInstalled,
  handleSdkInstallClientCancelled,
  handleSdkInstallPromptResponse,
  untrackPackageSdkInstall,
  updateResolutionFileForChanged,
) where

import Control.Applicative ((<|>))
import Control.Concurrent.Async
import Control.Concurrent.MVar
import Control.Concurrent.STM.TMVar
import Control.Exception (SomeException, displayException)
import Control.Lens ((^.), (&))
import Control.Monad (foldM, forM_, void, when)
import Control.Monad.STM
import Data.Aeson (fromJSON, toJSON)
import qualified Data.Yaml as Y
import qualified Data.ByteString.Char8 as BSC
import Data.Either.Extra (eitherToMaybe, fromRight, mapLeft)
import Data.List.Extra (nubOrd)
import qualified Data.Map as Map
import Data.Maybe (catMaybes, fromMaybe, listToMaybe)
import qualified Data.Set as Set
import qualified Data.Text as T
import DA.Cli.Damlc.Command.MultiIde.ClientCommunication
import DA.Cli.Damlc.Command.MultiIde.OpenFiles
import DA.Cli.Damlc.Command.MultiIde.Parsing
import DA.Cli.Damlc.Command.MultiIde.Types
import DA.Cli.Damlc.Command.MultiIde.Util
import DA.Daml.Assistant.Cache (CacheTimeout (..))
import DA.Daml.Assistant.Env
import DA.Daml.Assistant.Install
import DA.Daml.Assistant.Types
import DA.Daml.Assistant.Util (tryConfig)
import DA.Daml.Assistant.Version
import DA.Daml.Project.Config
import DA.Daml.Resolution.Config
import qualified Language.LSP.Types as LSP
import qualified Language.LSP.Types.Lens as LSP
import System.Exit (ExitCode (..))
import System.Environment (getEnv, getEnvironment)
import System.FilePath (takeDirectory)
import System.Process

-- Check if an SDK is installed, transform the subIDE data to disable it if needed
-- If using DPM, returns the damlc location
ensureIdeSdkInstalled :: MultiIdeState -> PackageSummary -> PackageHome -> SubIdeData -> IO (SubIdeData, Maybe (ValidPackageResolution, FilePath))
ensureIdeSdkInstalled miState packageSummary home ideData = do
  mResolutionData <- tryReadMVar (misResolutionData miState)
  case mResolutionData of
    Just resolutionData -> ensureIdeSdkInstalledDPM miState resolutionData packageSummary home ideData
    Nothing -> (,Nothing) <$> ensureIdeSdkInstalledDamlAssistant miState (psReleaseVersion packageSummary) home ideData

ensureIdeSdkInstalledDPM :: MultiIdeState -> MultiIdeResolutionData -> PackageSummary -> PackageHome -> SubIdeData -> IO (SubIdeData, Maybe (ValidPackageResolution, FilePath))
ensureIdeSdkInstalledDPM miState resolutionData packageSummary home ideData = do
  shouldDisable <- addPackageHomeIfErrored miState home

  let handleResolution :: PackageResolutionData -> Either (LSP.DiagnosticSeverity, T.Text) (ValidPackageResolution, FilePath)
      handleResolution = \case
        ErrorPackageResolutionData errs ->
          Left (LSP.DsError, "Failed to obtain version information from DPM:\n" <> T.pack (unlines $ show <$> errs))
        ValidPackageResolutionData packageResolution -> do
          let mDamlcPath = Map.lookup "damlc-binary" (imports packageResolution) >>= listToMaybe
          case mDamlcPath of
            Nothing -> Left (LSP.DsError, "Couldn't extract damlc-binary from DPM, your installation may be broken.")
            Just damlcPath -> pure (packageResolution, damlcPath)

  diagOrResolution <-
    if shouldDisable
      then pure $ Left (LSP.DsError, "Failed to load information from multi-package.yaml, check this file for diagnostics")
      else
        Map.lookup home (mainPackages resolutionData) <|> Map.lookup home (orphanPackages resolutionData) & \case
          Just res -> pure $ handleResolution res
          Nothing -> do
            ePackageResolution <- addOrphanResolution miState home
            pure $ do
              resolution <- mapLeft (LSP.DsError,) ePackageResolution
              handleResolution resolution

  let ideData' = ideData {ideDataUsingLocalComponents = psUsingLocalComponents packageSummary}
  case diagOrResolution of
    Left (severity, message) -> do
      logDebug miState $ "Failed to find damlc for package " <> unPackageHome home <> ": " <> T.unpack message
      let ideDataWithError = ideData' {ideDataDisabled = IdeDataDisabled severity message}
      -- Sending diagnostics causes client to send a request, which would then send diagnostics again without this check
      when (ideDataDisabled ideDataWithError /= ideDataDisabled ideData') $
        atomically $ sendPackageDiagnostic miState ideDataWithError
      pure (ideDataWithError, Nothing)
    Right (resolution, damlcPath) -> do
      logDebug miState $ "Found damlc for package " <> unPackageHome home <> ": " <> damlcPath
      pure(ideData', Just (resolution, damlcPath))

ensureIdeSdkInstalledDamlAssistant :: MultiIdeState -> UnresolvedReleaseVersion -> PackageHome -> SubIdeData -> IO SubIdeData
ensureIdeSdkInstalledDamlAssistant miState ver home ideData = do
  installDatas <- atomically $ takeTMVar $ misSdkInstallDatasVar miState
  let installData = getSdkInstallData ver installDatas
  (newInstallDatas, mDisableDiagnostic) <- case sidStatus installData of
    SISCanAsk -> do
      damlPath <- getDamlPath
      installedVersions <- getInstalledSdkVersions damlPath
      let versionIsInstalled = any ((unwrapUnresolvedReleaseVersion ver==) . releaseVersionFromReleaseVersion) installedVersions

      if versionIsInstalled
        then pure (installDatas, Nothing)
        else if unresolvedReleaseVersionToString ver == "0.0.0" then do
            let errText = "Version 0.0.0 is not installed, it can be installed by building daml-head locally."
                installData' = 
                  installData
                    { sidPendingHomes = Set.insert home $ sidPendingHomes installData
                    , sidStatus = SISFailed errText Nothing
                    }
            pure (Map.insert ver installData' installDatas, Just (LSP.DsError, errText))
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

failedInstallIdeDiagnosticMessage :: UnresolvedReleaseVersion -> T.Text -> Maybe SomeException -> T.Text
failedInstallIdeDiagnosticMessage ver outputLog mErr =
  T.unlines $ catMaybes
    [ Just $ "Failed to install Daml SDK version " <> T.pack (unresolvedReleaseVersionToString ver) <> " due to the following:"
    , if outputLog == "" then Nothing else Just outputLog
    , T.pack . displayException <$> mErr
    ]

-- Set the install status for a version, updating all pending packages with diagnostics if needed
updateSdkInstallStatus :: MultiIdeState -> SdkInstallDatas -> UnresolvedReleaseVersion -> LSP.DiagnosticSeverity -> T.Text -> SdkInstallStatus -> IO SdkInstallDatas
updateSdkInstallStatus miState installDatas ver severity message newStatus = do
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

data DpmResolveMode = DpmResolveMultiPackage | DpmResolveSinglePackage PackageHome
  deriving Show

dpmResolveModeEnvVar :: MultiIdeState -> DpmResolveMode -> (String, String)
dpmResolveModeEnvVar miState DpmResolveMultiPackage = ("DPM_MULTI_PACKAGE", misMultiPackageHome miState)
dpmResolveModeEnvVar _ (DpmResolveSinglePackage home) = ("DAML_PROJECT", unPackageHome home)

callDpmResolve :: MultiIdeState -> DpmResolveMode -> IO (Either String (Map.Map PackageHome PackageResolutionData))
callDpmResolve miState dpmResolveMode = do
  dpmBinary <- getEnv "DPM_BIN_PATH"
  logDebug miState $ "DPM Resolving " <> show dpmResolveMode
  currentEnv <- getEnvironment
  (exit, resolutionStr, err) <-
    readCreateProcessWithExitCode ((proc dpmBinary ["resolve"])
      { cwd = Just $ takeDirectory $ unPackageHome $ misDefaultPackagePath miState
      , env = Just $ dpmResolveModeEnvVar miState dpmResolveMode : currentEnv
      }) ""
  logDebug miState $ if exit == ExitSuccess then "Successful" else "Failed"
  case exit of
    ExitSuccess -> do
      let result = Map.mapKeys PackageHome $ packages
            $ fromRight (error "Couldn't decode resolution data, something went wrong")
            $ Y.decodeEither' @ResolutionData $ BSC.pack resolutionStr
      logDebug miState $ show result
      pure $ Right result
    ExitFailure _ ->
      pure $ Left err

-- Adds resolution data for "orphan packages", i.e. packages not in the multi-package
-- This includes single package projects (with no multi-package) and unpacked dars
addOrphanResolution :: MultiIdeState -> PackageHome -> IO (Either T.Text PackageResolutionData)
addOrphanResolution miState home = do
  logDebug miState $ "Adding orphan package resolution for " <> unPackageHome home
  eMainPackages <- callDpmResolve miState $ DpmResolveSinglePackage home
  case eMainPackages of
    Right mainPackages -> do
      let !packageResolution = fromMaybe (error "Package resolution didn't include main package, something went wrong") $ Map.lookup home mainPackages
      modifyMVar_ (misResolutionData miState) $ \resolutionData -> pure $ resolutionData {orphanPackages = Map.insert home packageResolution $ orphanPackages resolutionData}
      pure $ Right packageResolution
    Left err ->
      pure $ Left $ "DPM failed to resolve package:\n" <> T.pack err

-- Updates the resolution data and reports packages that have been changed or removed
-- Takes the package home of the currently updated daml.yaml, if there is one
updateResolutionFileForChanged :: MultiIdeState -> Maybe PackageHome -> IO [PackageHome]
updateResolutionFileForChanged miState mHome = do
  mResolutionData <- tryReadMVar (misResolutionData miState)
  case mResolutionData of
    Nothing -> pure []
    Just resolutionData -> do
      logDebug miState "Running project DPM resolution"
      eNewMainPackages <- callDpmResolve miState DpmResolveMultiPackage
      case eNewMainPackages of
        Right newMainPackages -> do
          ides <- atomically $ readTMVar $ misSubIdesVar miState
          let newPackages = Map.keys $ newMainPackages Map.\\ mainPackages resolutionData
              removedPackages = Map.keys $ mainPackages resolutionData Map.\\ newMainPackages
              ideShouldRestart :: PackageHome -> (PackageResolutionData, PackageResolutionData) -> Bool
              ideShouldRestart home resolutions = uncurry (/=) resolutions || ideDataUsingLocalComponents (lookupSubIde home ides)
              changedPackages = Map.keys $ Map.filterWithKey ideShouldRestart $ Map.intersectionWith (,) (mainPackages resolutionData) newMainPackages
          void $ swapMVar (misResolutionData miState) $ resolutionData {mainPackages = newMainPackages}
          recoveredPackages <- reportResolutionError miState Nothing

          -- For package changes outside the multi-package scope, update that specific package and report failures on the package, rather than the project
          forM_ mHome $ \home -> when (Map.notMember home newMainPackages) $ do
            res <- addOrphanResolution miState home
            case res of
              Left err -> do
                atomically $ modifyTMVarM_ (misSubIdesVar miState) $ \ides -> do
                  let ideData' = (lookupSubIde home ides) {ideDataDisabled = IdeDataDisabled LSP.DsError err}
                  sendPackageDiagnostic miState ideData'
                  pure $ Map.insert home ideData' ides
              _ -> pure ()

          pure $ nubOrd $ changedPackages <> newPackages <> removedPackages <> recoveredPackages
        Left err -> do
          void $ reportResolutionError miState $ Just err
          pure []

-- Need function to update the resolution globally
--   should be able to decide if a package has changed, give back the list of packages
--   should report any packages that have local paths, determine from the daml.yaml in the package summary maybe?
-- need function to add to orphaned resolutions

-- Handle the Client -> Coordinator response to the "Would you like to install ..." message
handleSdkInstallPromptResponse :: MultiIdeState -> LSP.LspId 'LSP.WindowShowMessageRequest -> Either LSP.ResponseError (Maybe LSP.MessageActionItem) -> IO ()
handleSdkInstallPromptResponse miState (releaseVersionFromLspId -> Just ver) res = do
  installDatas <- atomically $ takeTMVar $ misSdkInstallDatasVar miState
  let installData = getSdkInstallData ver installDatas
      changeSdkStatus :: LSP.DiagnosticSeverity -> T.Text -> SdkInstallStatus -> IO ()
      changeSdkStatus severity message newStatus = do
        installDatas' <- updateSdkInstallStatus miState installDatas ver severity message newStatus
        atomically $ putTMVar (misSdkInstallDatasVar miState) installDatas'
      disableSdk = changeSdkStatus LSP.DsError (missingSdkIdeDiagnosticMessage ver) SISDenied

  case (sidStatus installData, res) of
    (SISAsking, Right (Just (LSP.MessageActionItem (T.stripPrefix "Install SDK" -> Just _)))) -> do
      -- Install accepted, start install process
      installThread <- async $ do
        setupSdkInstallReporter miState ver
        outputLogVar <- newMVar ""
        res <- tryForwardAsync $ installSdk ver outputLogVar $ updateSdkInstallReporter miState ver
        onSdkInstallerFinished miState ver outputLogVar $ either Just (const Nothing) res
        finishSdkInstallReporter miState ver
      changeSdkStatus LSP.DsInfo (installingSdkIdeDiagnosticMessage ver) (SISInstalling installThread)
    (SISAsking, _) -> disableSdk
    (_, _) -> atomically $ putTMVar (misSdkInstallDatasVar miState) installDatas
handleSdkInstallPromptResponse _ _ _ = pure ()

-- Handle the Client -> Coordinator notification for cancelling an sdk installation
handleSdkInstallClientCancelled :: MultiIdeState -> LSP.NotificationMessage 'LSP.CustomMethod -> IO ()
handleSdkInstallClientCancelled miState notif = do
  forM_ (fromJSON $ notif ^. LSP.params) $ \message -> do
    let ver = sicSdkVersion message
    installDatas <- atomically $ takeTMVar $ misSdkInstallDatasVar miState
    let installData = getSdkInstallData ver installDatas
    installDatas' <- case sidStatus installData of
      SISInstalling thread -> do
        logDebug miState $ "Killing install thread for " <> unresolvedReleaseVersionToString ver
        cancel thread
        updateSdkInstallStatus miState installDatas ver LSP.DsError (missingSdkIdeDiagnosticMessage ver) SISDenied
      _ -> pure installDatas
    atomically $ putTMVar (misSdkInstallDatasVar miState) installDatas'

-- Update sdk install data + boot pending ides for when an installation finishes (regardless of success)
onSdkInstallerFinished :: MultiIdeState -> UnresolvedReleaseVersion -> MVar T.Text -> Maybe SomeException -> IO ()
onSdkInstallerFinished miState ver outputLogVar mError = do
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
      let errText = failedInstallIdeDiagnosticMessage ver outputLog (Just err)
      installDatas' <- updateSdkInstallStatus miState installDatas ver LSP.DsError errText (SISFailed outputLog $ Just err)
      sendClient miState $ showMessage LSP.MtError errText
      atomically $ putTMVar (misSdkInstallDatasVar miState) installDatas'

-- Given a version, logging MVar and progress handler, install an sdk (blocking)
installSdk :: UnresolvedReleaseVersion -> MVar Text -> (Int -> IO ()) -> IO ()
installSdk unresolvedVersion outputLogVar report = do
  damlPath <- getDamlPath
  cachePath <- getCachePath
  -- Override the cache timeout to 5 minutes, to be sure we have a recent cache
  let useCache = (mkUseCache cachePath damlPath) {overrideTimeout = Just $ CacheTimeout 300}

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
untrackPackageSdkInstall miState home = atomically $ modifyTMVar_ (misSdkInstallDatasVar miState) $
  fmap $ \installData -> installData {sidPendingHomes = Set.delete home $ sidPendingHomes installData}

-- Unblock an ide's sdk from being installed if it was previously denied or failed.
allowIdeSdkInstall :: MultiIdeState -> PackageHome -> IO ()
allowIdeSdkInstall miState home = do
  ePackageSummary <- packageSummaryFromDamlYaml home
  forM_ ePackageSummary $ \ps ->
    atomically $ modifyTMVar_ (misSdkInstallDatasVar miState) $ Map.adjust (\installData ->
      installData 
        { sidStatus = case sidStatus installData of
            SISDenied -> SISCanAsk
            SISFailed _ _ -> SISCanAsk
            status -> status
        }
    ) (psReleaseVersion ps)
