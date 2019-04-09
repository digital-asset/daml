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
    execParser $ info (commandParser sdkCommands <**> helper) forwardOptions

subcommand :: Text -> Text -> InfoMod Command -> Parser Command -> Mod CommandFields Command
subcommand name desc infoMod parser =
    command (unpack name) (info parser (infoMod <> progDesc (unpack desc)))

builtin :: Text -> Text -> Parser BuiltinCommand -> Mod CommandFields Command
builtin name desc parser =
    subcommand name desc mempty (Builtin <$> parser <**> helper)

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
        $  builtin "version" "Display SDK version" (pure Version)
        <> builtin "install" "Install SDK version" (Install <$> installParser)
        <> foldMap dispatch visible
    , subparser -- hidden commands
        $  internal
        <> foldMap dispatch hidden
    ]


installParser :: Parser InstallOptions
installParser = InstallOptions
    <$> optional (RawInstallTarget <$> argument str (metavar "CHANNEL|VERSION|PATH"))
    <*> iflag ActivateInstall "activate" mempty "Activate installed version of daml"
    <*> iflag InitialInstall "initial" mempty "Create daml home folder as well"
    <*> iflag ForceInstall "force" (short 'f') "Overwrite existing installation"
    <*> iflag QuietInstall "quiet" (short 'q') "Quiet verbosity"
    where
        iflag p name opts desc = fmap p (switch (long name <> help desc <> opts))
