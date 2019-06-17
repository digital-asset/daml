-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DAML.Assistant.Command
    ( Command(..)
    , BuiltinCommand(..)
    , SdkCommandInfo(..)
    , SdkCommandName(..)
    , SdkCommandArgs(..)
    , SdkCommandPath(..)
    , UserCommandArgs(..)
    , tryBuiltinCommand
    , getCommand
    ) where

import DAML.Assistant.Types
import Data.List
import Data.Maybe
import Data.Foldable
import Options.Applicative.Extended
import System.Environment
import Data.Either.Extra
import Control.Exception.Safe

-- | Parse command line arguments without SDK command info. Returns Nothing if
-- any error occurs, meaning the command may not be parseable without SDK command
-- info, or it might just be an error.
tryBuiltinCommand :: IO (Maybe Command)
tryBuiltinCommand = do
    args <- getArgs
    pure $ getParseResult (getCommandPure [] args)

-- | Parse command line arguments with SDK command info, exiting on failure.
getCommand :: [SdkCommandInfo] -> IO Command
getCommand sdkCommands = do
    args <- getArgs
    handleParseResult (getCommandPure sdkCommands args)

getCommandPure :: [SdkCommandInfo] -> [String] -> ParserResult Command
getCommandPure sdkCommands args =
    let parserInfo = info (commandParser sdkCommands <**> helper) forwardOptions
        parserPrefs = prefs showHelpOnError
    in execParserPure parserPrefs parserInfo args

subcommand :: Text -> Text -> InfoMod Command -> Parser Command -> Mod CommandFields Command
subcommand name desc infoMod parser =
    command (unpack name) (info parser (infoMod <> progDesc (unpack desc)))

builtin :: Text -> Text -> InfoMod Command -> Parser BuiltinCommand -> Mod CommandFields Command
builtin name desc mod parser =
    subcommand name desc mod (Builtin <$> parser)

isHidden :: SdkCommandInfo -> Bool
isHidden = isNothing . sdkCommandDesc

dispatch :: SdkCommandInfo -> Mod CommandFields Command
dispatch info = subcommand
    (unwrapSdkCommandName $ sdkCommandName info)
    (fromMaybe "" $ sdkCommandDesc info)
    forwardOptions
    (Dispatch info . UserCommandArgs <$>
        many (strArgument (metavar "ARGS")))

commandParser :: [SdkCommandInfo] -> Parser Command
commandParser cmds | (hidden, visible) <- partition isHidden cmds = asum
    [ subparser -- visible commands
        $  builtin "version" "Display DAML version information" mempty (Version <$> versionParser <**> helper)
        <> builtin "install" "Install the specified DAML SDK version" mempty (Install <$> installParser <**> helper)
        <> builtin "uninstall" "Uninstall the specified DAML SDK version" mempty (Uninstall <$> uninstallParser <**> helper)
        <> foldMap dispatch visible
    , subparser -- hidden commands
        $  internal
        <> builtin "exec" "Execute command with daml environment." forwardOptions
            (Exec <$> strArgument (metavar "CMD") <*> many (strArgument (metavar "ARGS")))
        <> foldMap dispatch hidden
    ]

versionParser :: Parser VersionOptions
versionParser = VersionOptions
    <$> flagYesNoAuto "all" False "Display all available versions." idm
    <*> flagYesNoAuto "assistant" False "Display DAML assistant version." idm

installParser :: Parser InstallOptions
installParser = InstallOptions
    <$> optional (RawInstallTarget <$> argument str (metavar "TARGET" <> help "The SDK version to install. Use 'latest' to download and install the latest stable SDK version available. Run 'daml install' to see the full set of options."))
    <*> (InstallAssistant <$> flagYesNoAuto' "install-assistant" "Install specified DAML assistant version" idm)
    <*> iflag ActivateInstall "activate" hidden "Activate installed version of daml"
    <*> iflag ForceInstall "force" (short 'f') "Overwrite existing installation"
    <*> iflag QuietInstall "quiet" (short 'q') "Don't display installation messages"
    <*> fmap SetPath (flagYesNoAuto "set-path" True "Adjust PATH automatically. This option only has an effect on Windows." idm)
    where
        iflag p name opts desc = fmap p (switch (long name <> help desc <> opts))

uninstallParser :: Parser SdkVersion
uninstallParser =
    argument readSdkVersion (metavar "VERSION" <> help "The SDK version to uninstall.")

readSdkVersion :: ReadM SdkVersion
readSdkVersion =
    eitherReader (mapLeft displayException . parseVersion . pack)


