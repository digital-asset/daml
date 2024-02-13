-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module DA.Daml.Assistant
    ( main
    ) where

import DA.Signals
import qualified DA.Service.Logger as L
import qualified DA.Service.Logger.Impl.Pure as L
import qualified DA.Service.Logger.Impl.GCP as L
import DA.Daml.Project.Config
import DA.Daml.Project.Consts (sdkVersionEnvVar)
import DA.Daml.Assistant.Types
import DA.Daml.Assistant.Env
import DA.Daml.Assistant.Command
import DA.Daml.Assistant.Version
import DA.Daml.Assistant.Install
import DA.Daml.Assistant.Util
import DA.Daml.Assistant.Cache
import System.Environment (getArgs, lookupEnv)
import System.FilePath
import System.Directory
import System.Process.Typed
import System.Exit
import System.IO
import Control.Exception (throw)
import Control.Exception.Safe hiding (throw)
import qualified Data.Aeson as A
import qualified Data.Aeson.Key as A
import qualified Data.Aeson.KeyMap as A
import Data.Char
import Data.Maybe
import Data.List.Extra
import Data.Either.Extra
import qualified Data.Set as S
import qualified Data.Text as T
import Control.Monad.Extra
import Safe

import SdkVersion (SdkVersioned, sdkVersion, withSdkVersions)

-- | Run the assistant and exit.
main :: IO ()
-- Note that we do not close on stdin here.
-- The reason for this is that this would result in us terminating the child
-- process, e.g., daml-helper using TerminateProcess on Windows which does
-- not give it a chance to cleanup. Therefore, we only do this in daml-helper
-- which starts long-running server processes like sandbox via run-jar.
-- This means that closing stdin won’t work for things like daml test
-- but that seems acceptable for now.
-- In theory, process groups or job control might provide a solution
-- but as Ben Gamari noticed, this is horribly unreliable
-- https://gitlab.haskell.org/ghc/ghc/issues/17777
-- so we are likely to make things worse rather than better.
main = withSdkVersions $ do
    damlPath <- handleErrors L.makeNopHandle getDamlPath
    withLogger damlPath $ \logger -> handleErrors logger $ do
        installSignalHandlers
        builtinCommandM <- tryBuiltinCommand
        case builtinCommandM of
            Just builtinCommand -> do
                env <- getDamlEnv damlPath (commandWantsProjectPath builtinCommand)
                handleCommand env logger builtinCommand
            Nothing -> do
                env@Env{..} <- autoInstall =<< getDamlEnv damlPath (LookForProjectPath True)

                -- We already know we can't parse the command without an installed SDK.
                -- So if we can't find it, let the user know. This will happen whenever
                -- auto-install is disabled and the project or environment specify a
                -- missing SDK version.
                case envSdkPath of
                    Nothing -> do
                        let installTarget
                                | Just v <- envSdkVersion = versionToString v
                                | otherwise = "latest"
                        hPutStr stderr . unlines $
                            [ "SDK not installed. Cannot run command without SDK."
                            , "To proceed, please install the SDK by running:"
                            , ""
                            , "    daml install " <> installTarget
                            , ""
                            ]
                        exitFailure
                    Just sdkPath -> do
                        sdkConfig <- readSdkConfig sdkPath
                        enriched <- hasEnrichedCompletion <$> getArgs
                        sdkCommands <- fromRightM throwIO (listSdkCommands sdkPath enriched sdkConfig)
                        userCommand <- getCommand sdkCommands
                        versionChecks env
                        handleCommand env logger userCommand

commandWantsProjectPath :: Command -> LookForProjectPath
commandWantsProjectPath cmd = LookForProjectPath $
    case cmd of
        Builtin (Install InstallOptions{..})
            | Just RawInstallTarget_Project <- iTargetM -> True
            | otherwise -> False
        Builtin Uninstall{} -> False
        _ -> True

-- | Perform version checks, i.e. warn user if project SDK version or assistant SDK
-- versions are out of date with the latest known release.
versionChecks :: Env -> IO ()
versionChecks Env{..} = do
    envFreshStableSdkVersionForCheck <- envFreshStableSdkVersionForCheck
    whenJust envFreshStableSdkVersionForCheck $ \latestVersion -> do
        let isHead = maybe False isHeadVersion envSdkVersion
            projectSdkVersionIsOld = isJust envProjectPath && envSdkVersion < Just latestVersion
            assistantVersionIsOld = isJust envDamlAssistantSdkVersion &&
                fmap unwrapDamlAssistantSdkVersion envDamlAssistantSdkVersion <
                Just latestVersion

        -- Project SDK version is outdated.
        when (not isHead && projectSdkVersionIsOld) $ do
            hPutStr stderr . unlines $
                [ "SDK " <> versionToString latestVersion <> " has been released!"
                , "See https://github.com/digital-asset/daml/releases/tag/v"
                  <> versionToString latestVersion <> " for details."
                -- Carefully crafted wording to make sure it’s < 80 characters so
                -- we do not get a line break.
                , ""
                ]

        -- Daml assistant is outdated.
        when (not isHead && not projectSdkVersionIsOld && assistantVersionIsOld) $ do
            hPutStr stderr . unlines $
                [ "WARNING: Using an outdated version of the Daml assistant."
                , "Please upgrade to the latest stable version by running:"
                , ""
                , "    daml install latest"
                , ""
                ]

-- | Perform auto-install if SDK version is given but SDK path is missing,
-- and auto-installs are not disabled in the $DAML_HOME/daml-config.yaml.
-- Returns the Env updated with the installed SdkPath.
autoInstall :: Env -> IO Env
autoInstall env@Env{..} = do
    damlConfigE <- tryConfig $ readDamlConfig envDamlPath
    let doAutoInstallE = queryDamlConfigRequired ["auto-install"] =<< damlConfigE
        doAutoInstall = fromRight True doAutoInstallE
        artifactoryApiKeyM = queryArtifactoryApiKey =<< eitherToMaybe damlConfigE

    if doAutoInstall && isJust envSdkVersion && isNothing envSdkPath then do
        -- sdk is missing, so let's install it!
        let sdkVersion = fromJust envSdkVersion
            options = InstallOptions
                { iTargetM = Nothing
                , iSnapshots = False
                , iQuiet = QuietInstall False
                , iAssistant = InstallAssistant Auto
                , iActivate = ActivateInstall False
                , iForce = ForceInstall False
                , iSetPath = SetPath Auto
                , iBashCompletions = BashCompletions Auto
                , iZshCompletions = ZshCompletions Auto
                , iInstallWithInternalVersion = InstallWithInternalVersion False
                , iInstallWithCustomVersion = InstallWithCustomVersion Nothing
                }
            installEnv = InstallEnv
                { options = options
                , damlPath = envDamlPath
                , useCache = envUseCache env
                , targetVersionM = sdkVersion
                , missingAssistant = False
                , installingFromOutside = False
                , projectPathM = Nothing
                , assistantVersion = envDamlAssistantSdkVersion
                , artifactoryApiKeyM
                , output = hPutStrLn stderr
                    -- Print install messages to stderr since the install
                    -- is only happening because of some other command,
                    -- and we don't want to mess up the other command's
                    -- output / have the install messages be gobbled
                    -- up by a pipe.
                }
        versionInstall installEnv
        pure env { envSdkPath = Just (defaultSdkPath envDamlPath sdkVersion) }

    else
        pure env

handleCommand :: SdkVersioned => Env -> L.Handle IO -> Command -> IO ()
handleCommand env@Env{..} logger command = do
    runCommand env command
    args' <- anonimizeArgs
    L.logJson logger L.Telemetry $ mkLogTable
        [ ("event", "command")
        , ("assistant-version", A.String (maybe ""
            (versionToText . unwrapDamlAssistantSdkVersion)
            envDamlAssistantSdkVersion))
        , ("sdk-version", A.String (maybe "" versionToText envSdkVersion))
        , ("args", A.toJSON args')
        ]

runCommand :: SdkVersioned => Env -> Command -> IO ()
runCommand env@Env{..} = \case
    Builtin (Version VersionOptions{..}) -> do
        let useCache =
              UseCache
                { cachePath = envCachePath
                , damlPathUnsafe = envDamlPath
                , overrideTimeout = if vForceRefresh then Just (CacheTimeout 1) else Nothing
                }
        installedVersionsE <- tryAssistant $ getInstalledSdkVersions envDamlPath
        snapshotVersionsEUnfiltered <- tryAssistant $ fst <$> getAvailableSdkSnapshotVersions useCache
        case snapshotVersionsEUnfiltered of
          Left e -> putStrLn $ "WARNING Couldn't update cache:\n" <> displayException e
          _ -> pure ()
        let snapshotVersionsE = if vSnapshots then snapshotVersionsEUnfiltered else pure []
            availableVersionsE = extractReleasesFromSnapshots <$> snapshotVersionsEUnfiltered
        defaultVersionM <- tryAssistantM $ getDefaultSdkVersion envDamlPath
        projectVersionM <- mapM (getSdkVersionFromProjectPath useCache) envProjectPath
        envSelectedVersionM <- lookupEnv sdkVersionEnvVar

        let asstVersion = unwrapDamlAssistantSdkVersion <$> envDamlAssistantSdkVersion
            envVersions = catMaybes
                [ envSdkVersion
                , guard vAssistant >> asstVersion
                , projectVersionM
                , defaultVersionM
                ]

            latestVersionM = maximumMay $ concat
                [ fromRight [] availableVersionsE
                , fromRight [] installedVersionsE
                , envVersions
                ]

            isNotInstalled = -- defaults to False if "installed version" list is not available
                case installedVersionsE of
                    Left _ -> const False
                    Right vs -> (`notElem` vs)

            isAvailable = -- defaults to False if "available version" list is not available
                case availableVersionsE of
                    Left _ -> const False
                    Right vs -> (`elem` vs)

            versionAttrs v = catMaybes
                [ ("selected by env var " <> pack sdkVersionEnvVar)
                    <$ guard (Just (unpack $ versionToText v) == envSelectedVersionM)
                , "project SDK version from daml.yaml"
                    <$ guard (Just v == projectVersionM && isJust envProjectPath)
                , "default SDK version for new projects"
                    <$ guard (Just v == defaultVersionM && isNothing envProjectPath)
                , "daml assistant version"
                    <$ guard (Just v == asstVersion && vAssistant)
                , "latest release"
                    <$ guard (Just v == latestVersionM && isNotInstalled v && isAvailable v)
                , "not installed"
                    <$ guard (isNotInstalled v)
                ]

            versions = nubSort . concat $
                [ envVersions
                , maybeToList latestVersionM
                , fromRight [] installedVersionsE
                , if vAll then fromRight [] availableVersionsE else []
                , fromRight [] snapshotVersionsE
                ]
            versionTable = [ (versionToText v, versionAttrs v) | v <- versions ]
            versionWidth = maximum (1 : map (T.length . fst) versionTable)
            versionLines =
                [ T.concat
                    [ "  "
                    , v
                    , T.replicate (versionWidth - T.length v) " "
                    , if null attrs
                        then ""
                        else "  (" <> T.intercalate ", " attrs <> ")"
                    ]
                | (v,attrs) <- versionTable ]

        putStr . unpack $ T.unlines ("SDK versions:" : versionLines)

    Builtin (Install options) -> wrapErr "Installing the SDK." $ do
        install options envDamlPath (envUseCache env) envProjectPath envDamlAssistantSdkVersion

    Builtin (Uninstall unresolvedVersion) -> do
        versionOrErr <- resolveReleaseVersion (envUseCache env) unresolvedVersion
        uninstallVersion env versionOrErr

    Builtin (Exec cmd args) -> do
        wrapErr "Running executable in daml environment." $ do
            path <- fromMaybe cmd <$> findExecutable cmd
            dispatch env path args

    Dispatch SdkCommandInfo{..} cmdArgs -> do
        wrapErr ("Running " <> unwrapSdkCommandName sdkCommandName <> " command.") $ do
            sdkPath <- required "Could not determine SDK path." envSdkPath
            let path = unwrapSdkPath sdkPath </> unwrapSdkCommandPath sdkCommandPath
                args = unwrapSdkCommandArgs sdkCommandArgs ++ unwrapUserCommandArgs cmdArgs
            dispatchWithEnvVarWarning env path args

envVarAddedVersion :: ReleaseVersion
envVarAddedVersion = either throw id $ unsafeParseOldReleaseVersion "2.9.0-snapshot.20240210.0"

dispatchWithEnvVarWarning :: SdkVersioned => Env -> FilePath -> [String] -> IO ()
dispatchWithEnvVarWarning env@Env{..} path args = do
  case (envSdkVersion, envProjectPath) of
    (Just ver, Just projectPath) -> do
        hasEnvVars <- projectConfigUsesEnvironmentVariables projectPath
        let sdkMissingEnvSupport = ver < envVarAddedVersion && not (isHeadVersion ver)
        if hasEnvVars && sdkMissingEnvSupport
            then do
                putStrLn $ "WARNING: Using an SDK (" <> versionToString ver
                  <> ") that does not support Environment Variable Interpolation with a daml.yaml containing Environment Variables."
                putStrLn "  Something will almost certainly break."
                putStrLn $ "  Consider updating your package to an SDK version that supports Environment Variable Interpolation, such as " 
                  <> sdkVersion <> " (your activated assistant version)"
                exitCode <- dispatchForExitCode env path args
                case exitCode of
                  ExitFailure _ -> do
                    putStrLn $ "This failure may be due to the environment variables in your daml.yaml. The selected SDK Version ("
                      <> versionToString ver <> ") does not support this feature."
                    throw exitCode
                  _ -> pure ()
            else dispatch env path args
    _ -> dispatch env path args

dispatch :: Env -> FilePath -> [String] -> IO ()
dispatch env path args = dispatchForExitCode env path args >>= throwIO

dispatchForExitCode :: Env -> FilePath -> [String] -> IO ExitCode
dispatchForExitCode env path args = do
    dispatchEnv <- getDispatchEnv env
    requiredIO "Failed to spawn command subprocess." $
        runProcess (setEnv dispatchEnv $ proc path args)

handleErrors :: forall t. L.Handle IO -> IO t -> IO t
handleErrors logger m = m `catches`
    [ Handler (go . displayException @AssistantError)
    , Handler (go . displayException @ConfigError)
    ]
  where
    go :: String -> IO t
    go err = do
        hPutStrLn stderr err
        L.logJson logger L.Error $ mkLogTable
            [ ("event", "error")
            , ("message", A.String (T.pack err))
            ]
        exitFailure

withLogger :: SdkVersioned => DamlPath -> (L.Handle IO -> IO ()) -> IO ()
withLogger (DamlPath damlPath) k = do
    cache <- getCachePath
    let cachePath = unwrapCachePath cache
    let logOfInterest prio = prio `elem` [L.Telemetry, L.Warning, L.Error]
        gcpConfig = L.GCPConfig
            { gcpConfigTag = "assistant"
            , gcpConfigCachePath = Just cachePath
            , gcpConfigDamlPath = Just damlPath
            }
    isOptedIn <- L.isOptedIn cachePath
    isOptedOut <- L.isOptedOut cachePath
    if isOptedIn && not isOptedOut
      then
        L.withGcpLogger gcpConfig logOfInterest L.makeNopHandle $ \gcpStateM logger -> do
            whenJust gcpStateM $ \gcpState -> L.logMetaData gcpState
            k logger
      else
        k L.makeNopHandle

-- | Get the arguments to `daml` and anonimize all but the first.
-- That way, the daml command doesn't get accidentally anonimized.
anonimizeArgs :: IO [T.Text]
anonimizeArgs = do
    args <- map T.pack <$> getArgs
    case args of
        [] -> pure []
        argsHead : argsTail -> do
            argsTail' <- concatMapM anonimizeArg argsTail
            pure (argsHead : argsTail')

-- | Anonimize an argument to `daml`.
anonimizeArg :: T.Text -> IO [T.Text]
anonimizeArg arg = do
    forM (T.splitOn "=" arg) $ \part -> do
        let partStr = T.unpack part
        isPath <- doesPathExist partStr
        pure $ if (part `S.member` argWhitelist) || (isFlag partStr && not isPath)
            then part
            else ""
  where
    isFlag :: [Char] -> Bool
    isFlag ['-', _] = True
    isFlag ('-':'-':cs) = all isFlagChar cs
    isFlag _ = False

    isFlagChar :: Char -> Bool
    isFlagChar c = isAlphaNum c || c == '-'

argWhitelist :: S.Set T.Text
argWhitelist = S.fromList
    [ "version", "yes", "no", "auto"
    , "install", "latest", "project"
    , "uninstall"
    , "studio", "never", "always", "published"
    , "new", "skeleton", "empty-skeleton", "quickstart-java", "copy-trigger", "gsg-trigger"
    , "daml-intro-1", "daml-intro-2", "daml-intro-3", "daml-intro-4"
    , "daml-intro-5", "daml-intro-6", "daml-intro-7", "script-example"
    , "daml-intro-13"
    , "migrate"
    , "init"
    , "build"
    , "test"
    , "start", "none"
    , "clean"
    , "damlc", "ide", "license", "package", "docs", "visual", "visual-web", "inspect-dar", "validate-dar", "doctest", "lint"
    , "sandbox", "INFO", "TRACE", "DEBUG", "WARN", "ERROR"
    , "navigator", "server", "console", "dump-graphql-schema", "create-config", "static", "simulated", "wallclock"
    , "ledger", "list-parties", "allocate-parties", "upload-dar", "fetch-dar"
    , "codegen", "java", "js"
    , "deploy"
    , "json-api"
    , "trigger", "trigger-service", "list"
    , "oauth2-middleware"
    , "script"
    ]

mkLogTable :: [(A.Key, A.Value)] -> A.Value
mkLogTable fields = A.Object . A.fromList $
    (A.fromString "source", A.String "daml-assistant") : fields
