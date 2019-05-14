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
    , getCommand
    ) where

import DAML.Assistant.Types
import Data.List
import Data.Maybe
import Data.Foldable
import Options.Applicative

getCommand :: [SdkCommandInfo] -> IO Command
getCommand sdkCommands =
    customExecParser parserPrefs $ info (commandParser sdkCommands <**> helper) forwardOptions
    where parserPrefs = prefs showHelpOnError

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
        $  builtin "version" "Display DAML version information" mempty (pure Version <**> helper)
        <> builtin "install" "Install the specified DAML SDK version" mempty (Install <$> installParser <**> helper)
        <> foldMap dispatch visible
    , subparser -- hidden commands
        $  internal
        <> builtin "exec" "Execute command with daml environment." forwardOptions
            (Exec <$> strArgument (metavar "CMD") <*> many (strArgument (metavar "ARGS")))
        <> foldMap dispatch hidden
    ]


installParser :: Parser InstallOptions
installParser = InstallOptions
    <$> optional (RawInstallTarget <$> argument str (metavar "TARGET" <> help "The SDK version to install. Use 'latest' to download and install the latest stable SDK version available. Run 'daml install' to see the full set of options."))
    <*> iflag ActivateInstall "activate" mempty "Activate installed version of daml"
    <*> iflag ForceInstall "force" (short 'f') "Overwrite existing installation"
    <*> iflag QuietInstall "quiet" (short 'q') "Don't display installation messages"
    <*> fmap SetPath (flagWithAuto "set-path" True "Adjust PATH automatically. This option only has an effect on Windows.")
    where
        iflag p name opts desc = fmap p (switch (long name <> help desc <> opts))

-- | This constructs flags that can be set to yes, no, or auto to control a boolean value
-- with auto using the default.
flagWithAuto :: String -> Bool -> String -> Parser Bool
flagWithAuto flagName defaultValue helpText =
    option reader (long flagName <> value defaultValue <> help (helpText <> commonHelp))
  where reader = eitherReader $ \case
            "yes" -> Right True
            "no" -> Right False
            "auto" -> Right defaultValue
            s -> Left ("Expected \"yes\", \"no\" or \"auto\" but got " <> show s)
        commonHelp = " Can be set to \"yes\", \"no\" or \"auto\" to select the default (" <> show defaultValue <> ")"
