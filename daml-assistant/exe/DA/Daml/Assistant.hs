-- Copyright (c) 2020 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module DA.Daml.Assistant
    ( main
    ) where

import DA.Signals
import DA.Daml.Project.Config
import DA.Daml.Assistant.Types
import DA.Daml.Assistant.Env
import DA.Daml.Assistant.Command
import DA.Daml.Assistant.Version
import DA.Daml.Assistant.Install
import DA.Daml.Assistant.Util
import System.Environment (getArgs)
import System.FilePath
import System.Directory
import System.Process.Typed
import System.Exit
import System.IO
import Control.Exception.Safe
import Data.Maybe
import Data.List.Extra
import Data.Either.Extra
import qualified Data.Text as T
import Control.Monad.Extra
import Safe

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
-- but as Ben Gamari noticed, this is horribly unreliable https://gitlab.haskell.org/ghc/ghc/issues/17777
-- so we are likely to make things worse rather than better.
main = displayErrors $ do
    installSignalHandlers
    builtinCommandM <- tryBuiltinCommand
    case builtinCommandM of
        Just builtinCommand -> do
            env <- getDamlEnv
            handleCommand env builtinCommand
        Nothing -> do
            env@Env{..} <- autoInstall =<< getDamlEnv

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
                        [ "DAML SDK not installed. Cannot run command without SDK."
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
                    handleCommand env userCommand

-- | Perform version checks, i.e. warn user if project SDK version or assistant SDK
-- versions are out of date with the latest known release.
versionChecks :: Env -> IO ()
versionChecks Env{..} =
    whenJust envLatestStableSdkVersion $ \latestVersion -> do
        let isHead = maybe False isHeadVersion envSdkVersion
            projectSdkVersionIsOld = isJust envProjectPath && envSdkVersion < Just latestVersion
            assistantVersionIsOld = isJust envDamlAssistantSdkVersion &&
                fmap unwrapDamlAssistantSdkVersion envDamlAssistantSdkVersion <
                Just latestVersion

        -- Project SDK version is outdated.
        when (not isHead && projectSdkVersionIsOld) $ do
            hPutStr stderr . unlines $
                [ "WARNING: Using an outdated version of the DAML SDK in project."
                , "To migrate to the latest DAML SDK, please set the sdk-version"
                , "field in daml.yaml to " <> versionToString latestVersion
                , ""
                ]

        -- DAML assistant is outdated.
        when (not isHead && not projectSdkVersionIsOld && assistantVersionIsOld) $ do
            hPutStr stderr . unlines $
                [ "WARNING: Using an outdated version of the DAML assistant."
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

    if doAutoInstall && isJust envSdkVersion && isNothing envSdkPath then do
        -- sdk is missing, so let's install it!
        let sdkVersion = fromJust envSdkVersion
            options = InstallOptions
                { iTargetM = Nothing
                , iQuiet = QuietInstall False
                , iAssistant = InstallAssistant Auto
                , iActivate = ActivateInstall False
                , iForce = ForceInstall False
                , iSetPath = SetPath True
                , iBashCompletions = BashCompletions Auto
                , iZshCompletions = ZshCompletions Auto
                }
            installEnv = InstallEnv
                { options = options
                , damlPath = envDamlPath
                , targetVersionM = Just sdkVersion
                , missingAssistant = False
                , installingFromOutside = False
                , projectPathM = Nothing
                , assistantVersion = envDamlAssistantSdkVersion
                , output = hPutStrLn stderr
                    -- Print install messages to stderr since the install
                    -- is only happening because of some other command,
                    -- and we don't want to mess up the other command's
                    -- output / have the install messages be gobbled
                    -- up by a pipe.
                }
        versionInstall installEnv sdkVersion
        pure env { envSdkPath = Just (defaultSdkPath envDamlPath sdkVersion) }

    else
        pure env

handleCommand :: Env -> Command -> IO ()
handleCommand env@Env{..} = \case

    Builtin (Version VersionOptions{..}) -> do
        installedVersionsE <- tryAssistant $ getInstalledSdkVersions envDamlPath
        availableVersionsE <- tryAssistant $ refreshAvailableSdkVersions envDamlPath
        defaultVersionM <- tryAssistantM $ getDefaultSdkVersion envDamlPath
        projectVersionM <- mapM getSdkVersionFromProjectPath envProjectPath

        let asstVersion = unwrapDamlAssistantSdkVersion <$> envDamlAssistantSdkVersion
            envVersions = catMaybes
                [ envSdkVersion
                , envLatestStableSdkVersion
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
                [ "project SDK version from daml.yaml"
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
                , fromRight [] installedVersionsE
                , if vAll then fromRight [] availableVersionsE else []
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

        putStr . unpack $ T.unlines ("DAML SDK versions:" : versionLines)

    Builtin (Install options) -> wrapErr "Installing the SDK." $ do
        install options envDamlPath envProjectPath envDamlAssistantSdkVersion

    Builtin (Uninstall version) -> do
        uninstallVersion env version

    Builtin (Exec cmd args) -> do
        wrapErr "Running executable in daml environment." $ do
            path <- fromMaybe cmd <$> findExecutable cmd
            dispatch env path args

    Dispatch SdkCommandInfo{..} cmdArgs -> do
        wrapErr ("Running " <> unwrapSdkCommandName sdkCommandName <> " command.") $ do
            sdkPath <- required "Could not determine SDK path." envSdkPath
            let path = unwrapSdkPath sdkPath </> unwrapSdkCommandPath sdkCommandPath
                args = unwrapSdkCommandArgs sdkCommandArgs ++ unwrapUserCommandArgs cmdArgs
            dispatch env path args

dispatch :: Env -> FilePath -> [String] -> IO ()
dispatch env path args = do
    dispatchEnv <- getDispatchEnv env
    requiredIO "Failed to spawn command subprocess." $
        runProcess_ (setEnv dispatchEnv $ proc path args)

displayErrors :: IO () -> IO ()
displayErrors m = m `catches`
    [ Handler $ \ (e :: AssistantError) -> do
        hPutStrLn stderr (displayException e)
        exitFailure
    , Handler $ \ (e :: ConfigError) -> do
        hPutStrLn stderr (displayException e)
        exitFailure
    ]
