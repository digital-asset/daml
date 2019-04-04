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
import Control.Monad
import Data.List
import Data.Maybe
import Data.Foldable
import qualified Data.Text as T

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
    <$> optional (argument readInstallTarget (metavar "CHANNEL|VERSION|PATH"))
    <*> switch (long "force" <> short 'f' <> help "Overwrite existing installation")
    <*> switch (long "quiet" <> short 'q' <> help "Do not show informative messages")
    <*> switch (long "activate" <> help "Activate the installed version of daml")
    <*> switch (long "initial" <> help "Perform initial installation of daml home folder")

readInstallTarget :: ReadM InstallTarget
readInstallTarget =
    InstallVersion <$> readVersion
    <|> InstallChannel <$> readChannel
    <|> InstallPath <$> readPath

validSdkVersion :: SdkVersion -> Bool
validSdkVersion v =
    let (c,sv) = splitVersion v
    in validSdkChannel c && validSdkSubVersion sv

validSdkChannel :: SdkChannel -> Bool
validSdkChannel (SdkChannel ch)
    =  not (T.null ch)
    && ch == T.strip ch
    && and ['a' <= c && c <= 'z' | c <- unpack ch]

validSdkSubVersion :: SdkSubVersion -> Bool
validSdkSubVersion (SdkSubVersion sv)
    =  not (T.null sv)
    && sv == T.strip sv
    && and [ not (T.null p) && and ['0' <= c && c <= '9' | c <- unpack p]
           | p <- T.splitOn "." sv ]

readVersion :: ReadM SdkVersion
readVersion = do
    v <- SdkVersion . pack <$> str
    guard (validSdkVersion v)
    pure v

readChannel :: ReadM SdkChannel
readChannel = do
    c <- SdkChannel . pack <$> str
    guard (validSdkChannel c)
    pure c

readPath :: ReadM FilePath
readPath = str
