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
    <*> enableDisableSwitch SetPath True "set-path"
            "automatic modification of PATH. This only has an effect on Windows and is enabled by default."
            mempty
    where
        iflag p name opts desc = fmap p (switch (long name <> help desc <> opts))

-- Sadly, optparse-applicative does not have an easy way to get
-- switches to enable and disable an option, see
-- https://github.com/pcapriotti/optparse-applicative/issues/148.
-- These helpers are taken from stack
-- https://github.com/commercialhaskell/stack/blob/f6258124cff9a7e92bcb5704164a70e149080e88/src/Options/Applicative/Builder/Extra.hs
enableDisableSwitch :: (Bool -> a) -> Bool -> String -> String -> Mod FlagFields Bool -> Parser a
enableDisableSwitch f defaultValue name helpSuffix mods =
    fmap f $ enableDisableFlags defaultValue True False name helpSuffix mods

-- | Enable/disable flags for any type.
enableDisableFlags
    :: a -- ^ Default value
    -> a -- ^ Enabled value
    -> a -- ^ Disabled value
    -> String -- ^ Name
    -> String -- ^ Help suffix
    -> Mod FlagFields a
    -> Parser a
enableDisableFlags defaultValue enabledValue disabledValue name helpSuffix mods =
    enableDisableFlagsNoDefault enabledValue disabledValue name helpSuffix mods <|>
    pure defaultValue

-- | Enable/disable flags for any type, without a default (to allow chaining with '<|>')
enableDisableFlagsNoDefault
    :: a -- ^ Enabled value
    -> a -- ^ Disabled value
    -> String -- ^ Name
    -> String -- ^ Help suffix
    -> Mod FlagFields a
    -> Parser a
enableDisableFlagsNoDefault enabledValue disabledValue name helpSuffix mods =
    last <$> some
        ((flag'
             enabledValue
             (hidden <>
              internal <>
              long name <>
              help helpSuffix <>
              mods) <|>
         flag'
             disabledValue
             (hidden <>
              internal <>
              long ("no-" ++ name) <>
              help helpSuffix <>
              mods)) <|>
         flag'
             disabledValue
             (long ("[no-]" ++ name) <>
              help ("Enable/disable " ++ helpSuffix) <>
              mods))
