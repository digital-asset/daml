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
import Control.Exception (SomeException, displayException, throwIO)
import Control.Lens ((^.), (&))
import Control.Monad (foldM, forM_, void, unless, when)
import Control.Monad.STM
import Data.Aeson (fromJSON, toJSON)
import qualified Data.Yaml as Y
import qualified Data.ByteString.Char8 as BSC
import Data.Either.Extra (eitherToMaybe, fromRight)
import Data.Foldable (traverse_)
import Data.List.Extra (nubOrd, find)
import qualified Data.Map as Map
import Data.Maybe (catMaybes, fromMaybe, isJust, listToMaybe, maybeToList)
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
import DA.Daml.Project.Consts (projectPathEnvVar, packagePathEnvVar)
import DA.Daml.Resolution.Config
import qualified Language.LSP.Types as LSP
import qualified Language.LSP.Types.Lens as LSP
import System.Exit (ExitCode (..))
import System.Environment (getEnv, getEnvironment)
import System.FilePath (takeDirectory)
import System.Process

import Data.Hashable (hash)

-- Check if an SDK is installed, transform the subIDE data to disable it if needed
-- If using DPM, returns the resolution and damlc location
ensureIdeSdkInstalled :: MultiIdeState -> PackageSummary -> PackageHome -> SubIdeData -> IO (SubIdeData, Maybe (ValidPackageResolution, FilePath))
ensureIdeSdkInstalled miState packageSummary home ideData = do
  mResolutionData <- tryReadMVar (misResolutionData miState)
  case mResolutionData of
    Just resolutionData -> ensureIdeSdkInstalledDPM miState resolutionData packageSummary home ideData
    Nothing -> (,Nothing) <$> ensureIdeSdkInstalledDamlAssistant miState (psSdkVersionData packageSummary) home ideData

-- Called from unsafeAddNewSubIdeAndSend, which is called with subIDE lock, so this function should be safe for IDE lock
ensureIdeSdkInstalledDPM :: MultiIdeState -> MultiIdeResolutionData -> PackageSummary -> PackageHome -> SubIdeData -> IO (SubIdeData, Maybe (ValidPackageResolution, FilePath))
ensureIdeSdkInstalledDPM miState resolutionData packageSummary home ideData = do
  globalErrorStatus <- getGlobalErrorStatusWithPackageRestart miState home

  let handleResolution :: PackageResolutionData -> IO (Either (LSP.DiagnosticSeverity, T.Text) (ValidPackageResolution, FilePath))
      handleResolution = \case
        -- If error is NOT_INSTALLED, request installation
        ErrorPackageResolutionData [err] | code err == "SDK_NOT_INSTALLED" -> do
          Left <$> tryAskForSdkInstall miState missingSdkIdeDiagnosticMessageDpm (psSdkVersionData packageSummary) home
        ErrorPackageResolutionData errs ->
          -- For all other errors, display to user
          pure $ Left (LSP.DsError, "Failed to obtain version information from DPM:\n" <> T.pack (unlines $ show <$> errs))
        ValidPackageResolutionData packageResolution -> do
          -- If successful, find the damlc-binary. If this fails, forward this to user
          let mDamlcPath = Map.lookup "damlc-binary" (imports packageResolution) >>= listToMaybe
          case mDamlcPath of
            Nothing -> pure $ Left (LSP.DsError, "Couldn't extract damlc-binary from DPM, your installation may be broken.")
            Just damlcPath -> pure $ Right (packageResolution, damlcPath)

  -- First check for global errors, if any present, do not try to lookup information for this package.
  diagOrResolution <-
    case globalErrorStatus of
      HasGlobalError ->
        pure $ Left (LSP.DsError, "Failed to load information from multi-package.yaml, check this file for diagnostics")
      NoGlobalError ->
        -- First try main packages, then orphan packages
        Map.lookup home (mainPackages resolutionData) <|> Map.lookup home (orphanPackages resolutionData) & \case
          Just res -> handleResolution res
          Nothing -> do
            -- When no package found, attempt to add as an orphan package
            ePackageResolution <- addOrphanResolution miState home
            case ePackageResolution of
              Right resolution -> handleResolution resolution
              Left errMessage -> pure $ Left (LSP.DsError, errMessage)

  let ideData' = ideData {ideDataUsingLocalComponents = sdkVersionDataUsingLocalOverrides $ psSdkVersionData packageSummary}
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

ensureIdeSdkInstalledDamlAssistant :: MultiIdeState -> SdkVersionData -> PackageHome -> SubIdeData -> IO SubIdeData
ensureIdeSdkInstalledDamlAssistant miState ver home ideData = do
  damlPath <- getDamlPath
  versionIsInstalled <-
    case svdVersion ver of
      Nothing -> pure True
      Just ver -> do
        installedVersions <- getInstalledSdkVersions damlPath
        return $ any (\installedVer -> unwrapUnresolvedReleaseVersion ver == releaseVersionFromReleaseVersion installedVer) installedVersions

  globalErrorStatus <- getGlobalErrorStatusWithPackageRestart miState home

  -- First check for global errors, if any present, do not try to ask for install
  mDisableDiagnostic <- case (versionIsInstalled, globalErrorStatus) of
    (True, _) -> pure Nothing
    (False, HasGlobalError) -> pure $ Just (LSP.DsError, "Failed to load information from multi-package.yaml, check this file for diagnostics")
    (False, NoGlobalError) -> Just <$> tryAskForSdkInstall miState missingSdkIdeDiagnosticMessageDamlAssistant ver home

  case mDisableDiagnostic of
    Nothing -> pure ideData
    Just (severity, message) -> do
      let ideDataWithError = ideData {ideDataDisabled = IdeDataDisabled severity message}
      -- Must prevent repeat diagnostic messages, as sending diagnostics triggers a
      -- code actions request from the editor, which would fail from missing
      -- SDK and trigger this code again, leading to a loop.
      when (ideDataDisabled ideDataWithError /= ideDataDisabled ideData) $
        atomically $ sendPackageDiagnostic miState ideDataWithError
      pure ideDataWithError

-- Attempts to ask a user to install a package.
-- No-op if the user has already been asked, or the previous attempt failed
-- Returns the severity and error text for the diagnostic to show on the relevant package while installing/if failed to install
tryAskForSdkInstall :: MultiIdeState -> (SdkVersionData -> T.Text) -> SdkVersionData -> PackageHome -> IO (LSP.DiagnosticSeverity, T.Text)
tryAskForSdkInstall miState makeMissingSdkIdeDiagnosticMessage ver home = do
  installDatas <- atomically $ takeTMVar $ misSdkInstallDatasVar miState
  let installData = getSdkInstallData ver installDatas
      addHomeAndReturn :: SdkInstallData -> (LSP.DiagnosticSeverity, T.Text) -> IO (SdkInstallDatas, (LSP.DiagnosticSeverity, T.Text))
      addHomeAndReturn installData' message =
        pure (Map.insert ver (installData' {sidPendingHomes = Set.insert home $ sidPendingHomes installData}) installDatas, message)
  
  (newInstallDatas, disableDiagnostic) <- case sidStatus installData of
    SISCanAsk -> do  
        if "0.0.0" `elem` fmap unresolvedReleaseVersionToString (svdVersion ver) then do
          let errText = "Version 0.0.0 is not installed, it can be installed by building daml-head locally."
              installData' = installData {sidStatus = SISFailed errText Nothing}
          addHomeAndReturn installData' (LSP.DsError, errText)
        else do
          -- Ask the user if they want to install
          let lspId = sdkVersionDataToLspId ver
              verStr = renderSdkVersionData ver
              messageContent = "This package uses the release version " <> verStr <> " which is not installed on this system.\n"
                <> "The IDE cannot give intelligence on this package without this SDK. Would you like to install it?"
              message = showMessageRequest lspId LSP.MtError messageContent ["Install SDK " <> verStr, "Do not install"]
              installData' = installData {sidStatus = SISAsking}
          putFromServerCoordinatorMessage miState message
          sendClient miState message
          addHomeAndReturn installData' (LSP.DsError, makeMissingSdkIdeDiagnosticMessage ver)
    SISInstalling _ -> addHomeAndReturn installData (LSP.DsInfo, installingSdkIdeDiagnosticMessage ver)
    SISFailed log err -> addHomeAndReturn installData (LSP.DsError, failedInstallIdeDiagnosticMessage ver log err)
    _ -> addHomeAndReturn installData (LSP.DsError, makeMissingSdkIdeDiagnosticMessage ver)
  atomically $ putTMVar (misSdkInstallDatasVar miState) newInstallDatas
  pure disableDiagnostic

missingSdkIdeDiagnosticMessage :: T.Text -> SdkVersionData -> T.Text
missingSdkIdeDiagnosticMessage installCommand ver =
  "Missing required Daml SDK version " <> renderSdkVersionData ver <> " to create development environment.\n"
    <> "Install this version via " <> installCommand <> ", or save the daml.yaml to be prompted"

missingSdkIdeDiagnosticMessageDamlAssistant :: SdkVersionData -> T.Text
missingSdkIdeDiagnosticMessageDamlAssistant ver = missingSdkIdeDiagnosticMessage ("`daml install " <> renderSdkVersionData ver <> "`") ver

missingSdkIdeDiagnosticMessageDpm :: SdkVersionData -> T.Text
missingSdkIdeDiagnosticMessageDpm = missingSdkIdeDiagnosticMessage "`dpm install package` from the package directory"

installingSdkIdeDiagnosticMessage :: SdkVersionData -> T.Text
installingSdkIdeDiagnosticMessage ver =
  "Installing Daml SDK version " <> renderSdkVersionData ver

failedInstallIdeDiagnosticMessage :: SdkVersionData -> T.Text -> Maybe SomeException -> T.Text
failedInstallIdeDiagnosticMessage ver outputLog mErr =
  T.unlines $ catMaybes
    [ Just $ "Failed to install Daml SDK version " <> renderSdkVersionData ver <> " due to the following:"
    , if outputLog == "" then Nothing else Just outputLog
    , T.pack . displayException <$> mErr
    ]

-- Set the install status for a version, updating all pending packages with diagnostics if needed
updateSdkInstallStatus :: MultiIdeState -> SdkInstallDatas -> SdkVersionData -> LSP.DiagnosticSeverity -> T.Text -> SdkInstallStatus -> IO SdkInstallDatas
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

-- LSPIds for version installation messages must be unique for given overrides
-- we use the version and a hash of the overrides to ensure this
-- of the form `${version}-with-overrides-${overrides-hash}`
sdkVersionDataToVersionIdentifier :: SdkVersionData -> T.Text
sdkVersionDataToVersionIdentifier ver =
  let verStr = renderOptionalUnresolvedReleaseVersion (svdVersion ver)
   in verStr <> "-with-overrides-" <> sdkVersionDataOverridesHash ver

-- Takes version identifier of form `3.3.0-with-overrides-<overrides-hash>` and finds the correct sdkInstallData
getSdkInstallDataFromIdentifier :: T.Text -> SdkInstallDatas -> Maybe SdkInstallData
getSdkInstallDataFromIdentifier (T.splitOn "-with-overrides-" -> [verStr, overridesHash]) installDatas = do
  ver <- eitherToMaybe $ parseUnresolvedVersion verStr
  let keyCandidates = filter (elem ver . svdVersion) $ Map.keys installDatas
  sdkVersionData <- find ((==overridesHash) . sdkVersionDataOverridesHash) keyCandidates
  Map.lookup sdkVersionData installDatas
getSdkInstallDataFromIdentifier _ _ = Nothing

sdkVersionDataToLspId :: SdkVersionData -> LSP.LspId 'LSP.WindowShowMessageRequest
sdkVersionDataToLspId ver =
  LSP.IdString $ "sdk-install-request-" <> sdkVersionDataToVersionIdentifier ver

getSdkInstallDataFromLspId :: LSP.LspId 'LSP.WindowShowMessageRequest -> SdkInstallDatas -> Maybe SdkInstallData
getSdkInstallDataFromLspId (LSP.IdString (T.stripPrefix "sdk-install-request-" -> Just verIdentifier)) installDatas =
  getSdkInstallDataFromIdentifier verIdentifier installDatas
getSdkInstallDataFromLspId _ _ = Nothing

sdkVersionDataOverridesHash :: SdkVersionData -> T.Text
sdkVersionDataOverridesHash = T.pack . show . hash . svdOverrides

-- When running `dpm resolve` in a package that sits under a multi-package.yaml, but isn't listed in it
-- dpm will generate a resolution for the multi-package, and won't include the single package we care about
-- we combat this by running dpm in `/tmp` and providing the multi-package or single package path via environment variables
data DpmResolveMode = DpmResolveMultiPackage | DpmResolveSinglePackage PackageHome
  deriving Show

dpmResolveModeEnvVar :: MultiIdeState -> DpmResolveMode -> [(String, String)]
dpmResolveModeEnvVar miState DpmResolveMultiPackage = [("DPM_MULTI_PACKAGE", misMultiPackageHome miState)]
dpmResolveModeEnvVar _ (DpmResolveSinglePackage home) = [(projectPathEnvVar, unPackageHome home), (packagePathEnvVar, unPackageHome home)]

callDpmResolve :: MultiIdeState -> DpmResolveMode -> IO (Either String (Map.Map PackageHome PackageResolutionData))
callDpmResolve miState dpmResolveMode = do
  dpmBinary <- getEnv "DPM_BIN_PATH"
  logDebug miState $ "DPM Resolving " <> show dpmResolveMode
  currentEnv <- getEnvironment
  (exit, resolutionStr, err) <-
    readCreateProcessWithExitCode ((proc dpmBinary ["resolve"])
      { cwd = Just $ takeDirectory $ unPackageHome $ misDefaultPackagePath miState
      , env = Just $ dpmResolveModeEnvVar miState dpmResolveMode ++ currentEnv
      }) ""
  case exit of
    ExitSuccess -> do
      let result = Map.mapKeys PackageHome $ packages
            $ fromRight (error "Couldn't decode resolution data, something went wrong")
            $ Y.decodeEither' @ResolutionData $ BSC.pack resolutionStr
      logDebug miState $ "Successful: " <> show result
      pure $ Right result
    ExitFailure _ -> do
      logDebug miState $ "Failed: " <> err
      pure $ Left err

-- Adds resolution data for "orphan packages", i.e. packages not in the multi-package
-- This includes single package projects (with no multi-package) and unpacked dars
addOrphanResolution :: MultiIdeState -> PackageHome -> IO (Either T.Text PackageResolutionData)
addOrphanResolution miState home = do
  logDebug miState $ "Adding orphan package resolution for " <> unPackageHome home
  eMainPackages <- callDpmResolve miState $ DpmResolveSinglePackage home
  case eMainPackages of
    Right mainPackages -> do
      -- DpmResolveMode introduced to ensure the below error case can never happen
      let !packageResolution = fromMaybe (error "Package resolution didn't include main package, something went wrong") $ Map.lookup home mainPackages
      modifyMVar_ (misResolutionData miState) $ \resolutionData -> pure $ resolutionData {orphanPackages = Map.insert home packageResolution $ orphanPackages resolutionData}
      pure $ Right packageResolution
    Left err ->
      pure $ Left $ "DPM failed to resolve package:\n" <> T.pack err

-- Convenience wrapper for updateResolutionFileForManyChanged, which takes only one package
updateResolutionFileForChanged :: MultiIdeState -> Maybe PackageHome -> IO [PackageHome]
updateResolutionFileForChanged miState = updateResolutionFileForManyChanged miState . Set.fromList . maybeToList

-- Updates the resolution data and reports packages that have been changed or removed
-- Takes the package homes of any currently updated daml.yaml(s)
updateResolutionFileForManyChanged :: MultiIdeState -> Set.Set PackageHome -> IO [PackageHome]
updateResolutionFileForManyChanged miState homes = withIDEs miState $ \ides -> do
  mResolutionData <- tryReadMVar (misResolutionData miState)
  case mResolutionData of
    -- No resolution file means we're using legacy assistant, nothing to do here
    Nothing -> pure (ides, [])
    Just resolutionData -> do
      logDebug miState "Running project DPM resolution"
      eNewMainPackages <- callDpmResolve miState DpmResolveMultiPackage
      case eNewMainPackages of
        Right newMainPackages -> do
          -- New and removed packages should be restarted
          --   we don't simply shutdown removed packages as removed here simply means dropped from the resolution file
          --   this is most likely due to it being removed from the multi-package.yaml, but still existing on disk
          -- Changed packages are only restart if their resolution paths have changed, or if they are using local components
          let newPackages = Map.keys $ newMainPackages Map.\\ mainPackages resolutionData
              removedPackages = Map.keys $ mainPackages resolutionData Map.\\ newMainPackages
              ideShouldRestart :: PackageHome -> (PackageResolutionData, PackageResolutionData) -> Bool
              ideShouldRestart home resolutions = uncurry (/=) resolutions || ideDataUsingLocalComponents (lookupSubIde home ides)
              changedPackages = Map.keys $ Map.filterWithKey ideShouldRestart $ Map.intersectionWith (,) (mainPackages resolutionData) newMainPackages
          void $ swapMVar (misResolutionData miState) $ resolutionData {mainPackages = newMainPackages}
          recoveredPackages <- reportResolutionError miState Nothing

          -- For package changes outside the multi-package scope, update that specific package and report failures on the package, rather than as a global error
          let orphanPackages = Set.filter (`Map.notMember` newMainPackages) homes
          updatedIdes <-
            foldM (\ides' home -> do
              res <- addOrphanResolution miState home
              case res of
                Left err -> do
                  let ideData' = (lookupSubIde home ides') {ideDataDisabled = IdeDataDisabled LSP.DsError err}
                  atomically $ sendPackageDiagnostic miState ideData'
                  pure $ Map.insert home ideData' ides'
                _ -> pure ides'
            ) ides orphanPackages

          pure (updatedIdes, nubOrd $ changedPackages <> newPackages <> removedPackages <> recoveredPackages)
        Left err -> do
          void $ reportResolutionError miState $ Just err
          pure (ides, [])

-- Handle the Client -> Coordinator response to the "Would you like to install ..." message
handleSdkInstallPromptResponse :: MultiIdeState -> LSP.LspId 'LSP.WindowShowMessageRequest -> Either LSP.ResponseError (Maybe LSP.MessageActionItem) -> IO ()
handleSdkInstallPromptResponse miState lspId res = do
  installDatas <- atomically $ takeTMVar $ misSdkInstallDatasVar miState
  usingDpm <- isJust <$> tryReadMVar (misResolutionData miState)
  forM_ (getSdkInstallDataFromLspId lspId installDatas) $ \installData -> do
    let ver = sidVersionData installData
        changeSdkStatus :: LSP.DiagnosticSeverity -> T.Text -> SdkInstallStatus -> IO ()
        changeSdkStatus severity message newStatus = do
          installDatas' <- updateSdkInstallStatus miState installDatas ver severity message newStatus
          atomically $ putTMVar (misSdkInstallDatasVar miState) installDatas'
        makeMissingSdkIdeDiagnosticMessage = if usingDpm then missingSdkIdeDiagnosticMessageDpm else missingSdkIdeDiagnosticMessageDamlAssistant
        disableSdk = changeSdkStatus LSP.DsError (makeMissingSdkIdeDiagnosticMessage ver) SISDenied

    case (sidStatus installData, res) of
      (SISAsking, Right (Just (LSP.MessageActionItem (T.stripPrefix "Install SDK" -> Just _)))) -> do
        -- Install accepted, start install process
        installThread <- async $ do
          setupSdkInstallReporter miState ver
          outputLogVar <- newMVar ""
          -- For DPM, install at the first home, since all homes for a given SdkInstallData should install the same thing
          let installSdk = if usingDpm then installSdkDpm (Set.elemAt 0 $ sidPendingHomes installData) else installSdkDamlAssistant ver
          res <- tryForwardAsync $ installSdk outputLogVar $ updateSdkInstallReporter miState ver
          onSdkInstallerFinished miState ver outputLogVar $ either Just (const Nothing) res
          finishSdkInstallReporter miState ver
        changeSdkStatus LSP.DsInfo (installingSdkIdeDiagnosticMessage ver) (SISInstalling installThread)
      (SISAsking, _) -> disableSdk
      (_, _) -> atomically $ putTMVar (misSdkInstallDatasVar miState) installDatas

-- Handle the Client -> Coordinator notification for cancelling an sdk installation
handleSdkInstallClientCancelled :: MultiIdeState -> LSP.NotificationMessage 'LSP.CustomMethod -> IO ()
handleSdkInstallClientCancelled miState notif = do
  forM_ (fromJSON $ notif ^. LSP.params) $ \message -> do
    let verIdentifier = sicSdkVersionIdentifier message
    installDatas <- atomically $ takeTMVar $ misSdkInstallDatasVar miState
    forM_ (getSdkInstallDataFromIdentifier verIdentifier installDatas) $ \installData -> do
      let ver = sidVersionData installData
      installDatas' <- case sidStatus installData of
        SISInstalling thread -> do
          logDebug miState $ "Killing install thread for " <> T.unpack (renderSdkVersionData ver)
          cancel thread
          updateSdkInstallStatus miState installDatas ver LSP.DsError (missingSdkIdeDiagnosticMessageDamlAssistant ver) SISDenied
        _ -> pure installDatas
      atomically $ putTMVar (misSdkInstallDatasVar miState) installDatas'

-- Update sdk install data + boot pending ides for when an installation finishes (regardless of success)
onSdkInstallerFinished :: MultiIdeState -> SdkVersionData -> MVar T.Text -> Maybe SomeException -> IO ()
onSdkInstallerFinished miState ver outputLogVar mError = do
  installDatas <- atomically $ takeTMVar $ misSdkInstallDatasVar miState
  let installData = getSdkInstallData ver installDatas
  case mError of
    Nothing -> do
      let homes = sidPendingHomes installData
          installDatas' = Map.delete ver installDatas
          enableIde :: SubIdes -> PackageHome -> IO SubIdes
          enableIde ides home =
            let ides' = Map.insert home ((lookupSubIde home ides) {ideDataDisabled = IdeDataNotDisabled}) ides
             in misUnsafeAddNewSubIdeAndSend miState ides' home Nothing
      atomically $ putTMVar (misSdkInstallDatasVar miState) installDatas'
      updatedHomes <- updateResolutionFileForManyChanged miState homes
      -- Since updateResolutionFileForManyChanged calls DPM resolve, which can result in package changes outside the scope of
      -- this version, i.e. those with already running environments, we need to be able to reboot them
      -- For similar circular dependency reasons to the existence of misUnsafeAddNewSubIdeAndSend,
      -- we also use misRebootIdeByHome here, over the real `rebootIdeByHome` function.
      traverse_ (misRebootIdeByHome miState) $ Set.fromList updatedHomes Set.\\ homes
      withIDEs_ miState $ \ides -> foldM enableIde ides homes
    Just err -> do
      outputLog <- takeMVar outputLogVar
      let errText = failedInstallIdeDiagnosticMessage ver outputLog (Just err)
      installDatas' <- updateSdkInstallStatus miState installDatas ver LSP.DsError errText (SISFailed outputLog $ Just err)
      sendClient miState $ showMessage LSP.MtError errText
      atomically $ putTMVar (misSdkInstallDatasVar miState) installDatas'

-- Installs an SDK for a given package home
-- Currently does not log or report status, as DPM cannot report this information.
installSdkDpm :: PackageHome -> MVar Text -> (Int -> IO ()) -> IO ()
installSdkDpm packageHome _outputLogVar _report = do
  dpmBinary <- getEnv "DPM_BIN_PATH"
  (exit, _, err) <- readCreateProcessWithExitCode ((proc dpmBinary ["install", "package"]) {cwd = Just $ unPackageHome packageHome}) ""

  case exit of
    ExitSuccess -> pure ()
    ExitFailure _ -> throwIO $ assistantError $ T.pack err

-- Given a version, logging MVar and progress handler, install an sdk (blocking)
installSdkDamlAssistant :: SdkVersionData -> MVar Text -> (Int -> IO ()) -> IO ()
installSdkDamlAssistant versionData outputLogVar report = do
  unless (null $ svdOverrides versionData) $
    throwIO $ assistantError "Daml Assistant cannot install versions with overrides, use DPM by opening Studio with `dpm studio`."
  damlPath <- getDamlPath
  cachePath <- getCachePath
  -- Override the cache timeout to 5 minutes, to be sure we have a recent cache
  let useCache = (mkUseCache cachePath damlPath) {overrideTimeout = Just $ CacheTimeout 300}

  version <- 
      case svdVersion versionData of
        Just ver -> resolveReleaseVersionUnsafe useCache ver
        Nothing -> throwIO $ assistantError "Missing sdk-version, please specify sdk-version in daml.yaml or use DPM with overrides."
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
        , mPackagePath = Nothing
        , artifactoryApiKeyM = queryArtifactoryApiKey =<< eitherToMaybe damlConfigE
        , output = \str -> modifyMVar_ outputLogVar $ pure . (<> T.pack str)
        , downloadProgressObserver = Just report
        }
  versionInstall env

sendSdkInstallProgress :: MultiIdeState -> SdkVersionData -> DamlSdkInstallProgressNotificationKind -> Int -> IO ()
sendSdkInstallProgress miState ver kind progress =
  sendClient miState $ LSP.FromServerMess (LSP.SCustomMethod damlSdkInstallProgressMethod) $ LSP.NotMess $
    LSP.NotificationMessage "2.0" (LSP.SCustomMethod damlSdkInstallProgressMethod) $ toJSON $ DamlSdkInstallProgressNotification
      { sipSdkVersionIdentifier = sdkVersionDataToVersionIdentifier ver
      , sipSdkVersionRendered = renderSdkVersionData ver
      , sipKind = kind
      , sipProgress = progress
      }

setupSdkInstallReporter :: MultiIdeState -> SdkVersionData -> IO ()
setupSdkInstallReporter miState ver = sendSdkInstallProgress miState ver InstallProgressBegin 0
  
updateSdkInstallReporter :: MultiIdeState -> SdkVersionData -> Int -> IO ()
updateSdkInstallReporter miState ver = sendSdkInstallProgress miState ver InstallProgressReport

finishSdkInstallReporter :: MultiIdeState -> SdkVersionData -> IO ()
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
    ) (psSdkVersionData ps)
