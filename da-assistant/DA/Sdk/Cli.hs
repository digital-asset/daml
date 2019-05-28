-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE FlexibleInstances   #-}

module DA.Sdk.Cli (main) where

import           Control.Exception.Safe  (Handler (..), SomeException, MonadThrow, StringException (..), catches, throwString)
import qualified Control.Monad.Logger    as L
import qualified DA.Sdk.Cli.Changelog as Ch
import           DA.Sdk.Cli.Command
import           DA.Sdk.Cli.Command.Types ( SubscriptionOp(..)
                                          , TemplateListFormat(..)
                                          , TemplateArg(..)
                                          , templateArgToText
                                          , isMalformed)
import           DA.Sdk.Cli.Conf         (Conf (..), NameSpace(..),
                                          configHelp, getEffectiveConfigPath, getProject, getSdkTempDirPath,
                                          requireConf, prettyConfError, projectSDKVersion)
import           DA.Sdk.Cli.Conf.Migrate (migrateConfigFile)
import           DA.Sdk.Cli.Job          (Job (..), JobException, UserMessage(..), runJob, runSandbox,
                                          runCompile, runNavigator, start, stop, restart, copyDetailsFromOldConf)
import           DA.Sdk.Cli.Monad
import qualified DA.Sdk.Cli.Message      as M
import qualified DA.Sdk.Cli.Management      as Management
import           DA.Sdk.Cli.Paths        (PathError, showPath, runPackageMain)
import           DA.Sdk.Cli.Sdk          (sdkList, sdkUpgrade, sdkUse, installSdkM, sdkUninstall, SdkUninstallError, SdkError)
import           DA.Sdk.Cli.SelfUpgrade  (attemptSelfUpgrade, displayUpdateChannelInfo)
import           DA.Sdk.Cli.SelfUpgrade.Types (SelfUpgradeError(MigrationError))
import           DA.Sdk.Cli.Setup        (setup)
import           DA.Sdk.Cli.Status       (status)
import           DA.Sdk.Cli.Test         (runTestAction, runTestTemplates)
import           DA.Sdk.Cli.Version      (version)
import           DA.Sdk.Cli.Template     (templatePublish, listUsableTemplates,
                                          templateAdd, getTemplateInfo,
                                          checkNamespaceExists, createProject)
import           DA.Sdk.Cli.Repository.Types (TemplateType(..))
import           DA.Sdk.Cli.Repository   (daRepository, daExperimentalRepository)
import           DA.Sdk.Cli.Project      (installTemplate)
import           DA.Sdk.Cli.Command.Config (runConfigGet, runConfigSet, subscribe, listSubscription)
import qualified DA.Sdk.Cli.SdkVersion   as SdkVersion
import           DA.Sdk.Prelude
import qualified DA.Sdk.Pretty           as P
import           DA.Sdk.Version          (parseBuildVersion, showBuildVersion)
import           System.Exit             (ExitCode, exitWith)
import qualified System.IO               as IO
import           DA.Sdk.Cli.System       (unsupportedOnWin, exitFailure, isSudo)
import           Control.Monad.Catch     ( MonadMask )
import           DA.Sdk.Cli.Monad.FileSystem (MonadFS)
import           DA.Sdk.Cli.Monad.Repository
import           DA.Sdk.Cli.Monad.MockIO
import           DA.Sdk.Cli.Monad.UserInteraction
import           Control.Monad.Reader
import           Control.Monad.Except
import           DA.Sdk.Cli.Monad.Timeout (MonadTimeout)
import           Data.Foldable (for_)
import qualified Data.Text.Extended as T
import           DA.Sdk.Cli.Package
import           Control.Monad.Extra (whenM)
import DA.Sdk.Cli.Monad.Locations
import qualified Data.Text as DT
import Data.Either.Combinators

main :: IO ()
main = runAssistant `catches`
    [ Handler $ \(code :: ExitCode)    -> exitWith code
    , Handler $ \(message :: StringException) -> do
        case message of
          (StringException m _) -> displayStr m
        exitFailure
    , Handler $ \(suEx :: SelfUpgradeError) -> do
        displayPretty suEx
        exitFailure
    , Handler $ \(jobEx :: JobException) -> do
        displayPretty jobEx
        exitFailure
    , Handler $ \(pe :: PathError) -> do
        displayPretty pe
        exitFailure
    , Handler $ \(um :: UserMessage) ->
        displayPretty um
    , Handler $ \(ex :: SomeException) -> do
        displayStr . unlines $
            [ "An unknown error occurred."
            , "Please raise this issue on our support page:"
            , ""
            , "    https://docs.daml.com/support/support.html"
            , ""
            , "Please include these error details:"
            , displayException ex
            ]
        exitFailure
    ]

-- | Perform an upgrade if necessary

runAssistant :: IO ()
runAssistant = do
  -- Make stdout unbuffered so output written appears immediately to the user
  IO.hSetBuffering IO.stdout IO.NoBuffering
  whenM isSudo $ do
    displayStr $ "Warning: The SDK Assistant should be run as a normal user. " <>
                 "Now it is run as root."
    displayNewLine


  IO.hPutStr IO.stderr . unlines $
    [ "WARNING: The da assistant is deprecated in favor of the new daml assistant."
    , "Please follow the installation instructions for the new daml assistant:"
    , ""
    , "    https://docs.daml.com/getting-started/installation.html"
    , ""
    , "And consult the migration guide at:"
    , ""
    , "    https://docs.daml.com/support/new-assistant.html"
    , ""
    ]

  -- First parse CLI argument into a structured command (or exit).
  command <- getCommand

  -- Now that we have the CLI argument we can run the assistant
  runLogging (commandConfigLogLevel command) (runAssistant' command)

data CliError = CliTextError Text
              | CliUpgradeError SelfUpgradeError
              | CliFetchError FetchError
              | CliUninstallError SdkUninstallError
              | CliSdkError SdkError
              | CliChangelogError Ch.Error

throwCliError :: MonadThrow m => CliError -> m a
throwCliError (CliTextError t) =
    throwString $ T.unpack t
throwCliError (CliUpgradeError e) =
    throwM e
throwCliError (CliFetchError e) =
    throwString $ T.unpack $ P.renderPlain $ P.pretty e
throwCliError (CliUninstallError e) =
    throwString $ T.unpack $ P.renderPlain $ P.pretty e
throwCliError (CliSdkError e) =
    throwString $ T.unpack $ P.renderPlain $ P.pretty e
throwCliError (CliChangelogError e) =
    case e of
      Ch.EUserError er ->
        throwString $ T.unpack $ P.renderPlain $ P.pretty er
      Ch.EInternalError er -> do
        throwString . unlines $
            [ "An internal error occurred."
            , "Please raise this issue on our support page:"
            , ""
            , "    https://docs.daml.com/support/support.html"
            , ""
            , "Please include these error details:"
            , T.unpack (P.renderPlain (P.pretty er))
            ]

runAssistant' :: (MonadUserInput m, MonadUserOutput m, L.MonadLogger m, MonadRepo m,
                  MonadFS m, MockIO m, MonadMask m, MonadTimeout m, MonadLocations m, MonadIO m)
              => Command -> m ()
runAssistant' command = do
    let action = commandAction command

    case action of
        Primitive ShowVersion -> do
            display $ "Installed SDK Assistant version: " <> versionTxt
            displayNewLine
            display "Type `da update-info` to see information about update channels."
        Primitive ShowConfigHelp ->
            displayDoc configHelp
        Primitive DoSetup ->
            getConfigFile >>= setup

        -- Hidden test actions
        Primitive (Test test) ->
            mockIO_ $ runTestAction test

        Normal normalAction -> do
            let cliProps = commandConfigProps command
            configFile <- getConfigFile
            errOrConf <- requireConf configFile cliProps
            case errOrConf of
                Left e ->
                    L.logErrorN $ P.renderPlain $ prettyConfError e
                Right conf -> do
                    errOrProject <- getProject cliProps
                    maybeProject <- case errOrProject of
                        Left (M.Error e) ->
                            throwString . T.unpack . P.renderPlain $ P.pretty e
                        -- We ignore the warning here, since it's okay to be outside of
                        -- a project.
                        Left (M.Warning _w) ->
                            pure Nothing
                        Right p -> pure $ Just p
                    errOrOk <- handle (Env conf maybeProject) configFile normalAction
                    case errOrOk of
                      Right () -> return ()
                      Left err -> throwCliError err
  where
    getConfigFile = getEffectiveConfigPath . commandConfigPath $ command
    versionTxt = maybe "Invalid version" showBuildVersion $ parseBuildVersion version

handle :: forall m. (MonadUserOutput m, L.MonadLogger m, MonadRepo m, MonadFS m,
           MockIO m, MonadMask m, MonadUserInput m, MonadTimeout m, MonadLocations m, MonadIO m)
       => Env -> FilePath -> NormalAction -> m (Either CliError ())
handle env configFile action = do
  let conf = envConf env
      proj = envProject env
      defaultSdkVsn = confDefaultSDKVersion $ envConf env
      isScript = confIsScript . envConf $ env
      installSdkIfMissing :: m ()
      installSdkIfMissing = unless isScript $ do
          for_ (envProject env) $ \project -> do
            -- We are inside a project
            errOrInstalledVsns <- SdkVersion.getInstalledSdkVersions
            let currentSdkVersion = projectSDKVersion project
            installedVersions <- either (\_ -> throwString "Sdk Uninstall: Failed to list installed SDK versions.")
                                        return errOrInstalledVsns
            unless (currentSdkVersion `elem` installedVersions) $ do
                errOrSuccess <- installSdkM conf daRepository currentSdkVersion
                case errOrSuccess of
                    Left er -> displayPretty er
                    Right () -> pure ()

      attemptUpgrades :: m (Either SelfUpgradeError ())
      attemptUpgrades =
        runExceptT . unless isScript $ do
            withExceptT MigrationError $ ExceptT $ migrateConfigFile configFile
            ExceptT $ attemptSelfUpgrade env configFile False False
            lift installSdkIfMissing

      windowsSupportWrapper = if isSupportedOnWin action then id else unsupportedOnWin

      tryCreateAndListIfUnsuccessful mbTemplate target = do
        created <- createProject mbTemplate target
        when (not created) $ listUsableTemplates TemplateListAsTable (Just Project)
      tryAddAndListIfUnsuccessful template mbTarget = do
        added <- templateAdd template mbTarget
        when (not added) $ listUsableTemplates TemplateListAsTable (Just Addon)

  logDebug . T.show $ conf
  logDebug . T.show $ proj
  windowsSupportWrapper $ runExceptT $ do
    -- Some actions avoid automatic upgrade attempts by default, because their
    -- use-cases are such that. Other actions should upgrade automatically.

    when (shouldAttemptUpgradesFirst action) $
        withExceptT CliUpgradeError $ ExceptT attemptUpgrades

    -- TODO (GH): Use a common bintray handle for all these operations
    case action of
        FetchPackage fetchArg mbTarget dontCheckFetchTag -> do
            daTempDir <- getSdkTempDirPath
            withExceptT CliFetchError $ ExceptT $
                fetchPackage daTempDir conf fetchArg mbTarget dontCheckFetchTag

        SendFeedback ->
            displayStr . unlines $
                [ "We'd love to hear your feedback. Please visit our support and feedback page:"
                , ""
                , "    https://docs.daml.com/support/support.html"
                ]

        ShowPath pathName exec   ->
            showPath pathName exec proj defaultSdkVsn
        RunExec mbArgs           ->
            runPackageMain mbArgs proj defaultSdkVsn
        DisplayUpdateChannelInfo ->
            displayUpdateChannelInfo conf
        ShowDocs               ->
            display docsHelp
        ShowStatus             ->
            notYetRefactored status
        CreateProject (Just (Qualified ns tn)) path ->
            notYetRefactored $ installTemplate ns tn Project path
        CreateProject mtn path ->
            notYetRefactored $ tryCreateAndListIfUnsuccessful (templateArgToText <$> mtn) path
        TemplateAdd (Qualified ns tn) (Just path) ->
            notYetRefactored $ installTemplate ns tn Addon path
        TemplateAdd tn mpath ->
            notYetRefactored $ tryAddAndListIfUnsuccessful (templateArgToText tn) mpath
            -- TODO is this bad in the qualified case?
        SdkUse ver             ->
            withExceptT CliSdkError $ ExceptT $
                sdkUse conf daRepository ver
        SdkUseExperimental ver ->
            withExceptT CliSdkError $ ExceptT $
                sdkUse conf daExperimentalRepository ver
        SdkUpgrade             -> do
            withExceptT CliUpgradeError $ ExceptT $
                attemptSelfUpgrade env configFile True True
            withExceptT CliSdkError $ ExceptT $
                sdkUpgrade proj defaultSdkVsn conf
        SdkUninstall target    ->
            withExceptT CliUninstallError $ ExceptT $ sdkUninstall proj defaultSdkVsn target isScript
        SdkList                ->
            notYetRefactored $ throwErr sdkList
        Start svc              ->
            deprecated $ start svc
        Stop svc               ->
            deprecated $ stop svc
        Restart svc            ->
            deprecated $ restart svc
        Studio                 ->
            deprecated $ runJob (JobStudio ".")
        Migrate -> do
            res <- liftIO $ copyDetailsFromOldConf proj
            whenLeft res $ \ex -> throwCliError (CliTextError (DT.pack $ "Could no copy over the details from da.yaml" ++ show ex))
        Navigator              ->
            deprecated runNavigator
        Sandbox                ->
            deprecated runSandbox
        Compile                ->
            deprecated runCompile
        TemplatePublish (ns, tn, rl) ->
            deprecated $ templatePublish ns tn rl
        TemplateList fmt mty   ->
            deprecated $ listUsableTemplates fmt mty
        TemplateInfo (ns, tn, rl) ->
            deprecated $ getTemplateInfo ns tn rl
        Changelog _        ->
            display changelogLink
        ConfigGet mbKey        ->
            notYetRefactored $ runConfigGet configFile mbKey
        ConfigSet isProject mbKey val ->
            withExceptT CliTextError $ ExceptT $
                runConfigSet (if isProject then Nothing else Just configFile) mbKey val
        Subscribe ns           ->
            deprecated $ checkNamespaceExists ns >>=
                                    (\exists -> if exists
                                                then errorUnhandled $ subscribe configFile ns OpSubscribe
                                                else logError ("Namespace does not exist: " <> unwrapNameSpace ns))
        Unsubscribe (Just ns)  ->
            deprecated $ throwErr $ subscribe configFile ns OpUnsubscribe
        Unsubscribe Nothing    ->
            deprecated $ throwErr $ listSubscription configFile
        TestTemplates          ->
            notYetRefactored runTestTemplates
        Sdk sdkAction          ->
            notYetRefactored $ Management.sdkManagement sdkAction
  where
    notYetRefactored = mockCliM_ env
    deprecated = mockCliM_ env
    errorUnhandled = void
    throwErr a = a >>= either (throwString . show) return

isSupportedOnWin :: NormalAction -> Bool
isSupportedOnWin = \case
    Start _   -> False
    Stop _    -> False
    Restart _ -> False
    Navigator -> False
    Sandbox   -> False
    Compile   -> False
    _         -> True

shouldAttemptUpgradesFirst :: NormalAction -> Bool
shouldAttemptUpgradesFirst = \case
    SendFeedback             -> False
    ShowPath _ _             -> False
    RunExec _                -> False
    DisplayUpdateChannelInfo -> False
    CreateProject tn _       -> maybe True isMalformed tn -- it's going to fail
    TemplateAdd tn _         -> isMalformed tn -- it's going to fail
    SdkUpgrade               -> False -- special upgrade logic
    SdkUninstall _           -> False
    ConfigGet _              -> False
    ConfigSet _ _ _          -> False
    Subscribe _              -> False
    Unsubscribe _            -> False
    TestTemplates            -> False
    Sdk _                    -> False
    _                        -> True
