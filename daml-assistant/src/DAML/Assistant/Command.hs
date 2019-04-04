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
import Data.Foldable
import qualified Data.Text as T

import Options.Applicative

getCommand :: [SdkCommandInfo] -> IO Command
getCommand sdkCommands =
    execParser $ info (commandParser sdkCommands <**> helper) forwardOptions

subcommand :: Text -> Text -> InfoMod Command -> Parser Command -> Mod CommandFields Command
subcommand name desc infoMod parser =
    command (unpack name) (info parser (infoMod <> progDesc (unpack desc)))

commandParser :: [SdkCommandInfo] -> Parser Command
commandParser sdkCommands = asum
    [ subparser . fold $ -- visible commands
        [ subcommand "version" "Display SDK version" mempty (versionCommandParser <**> helper)
        , subcommand "install" "Install SDK version" mempty (installCommandParser <**> helper) ] ++
        [ subcommand name desc forwardOptions (sdkCommandParser cmd)
        | cmd <- sdkCommands
        , SdkCommandName name <- pure (sdkCommandName cmd)
        , Just desc <- pure (sdkCommandDesc cmd)
        ]
    , subparser . (internal <>) . fold $ -- hidden commands
        [ subcommand name "" forwardOptions (sdkCommandParser cmd)
        | cmd <- sdkCommands
        , SdkCommandName name <- pure (sdkCommandName cmd)
        , Nothing <- pure (sdkCommandDesc cmd)
        ]
    ]

    where
        versionCommandParser = pure $ BuiltinCommand Version
        installCommandParser =
            BuiltinCommand . Install
                <$> installParser
        sdkCommandParser sdkCommand =
            SdkCommand sdkCommand . UserCommandArgs
                <$> many (strArgument (metavar "ARGS"))


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
