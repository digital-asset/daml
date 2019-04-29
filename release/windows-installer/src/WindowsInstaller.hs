-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}
module Main (main) where

import Data.String
import Development.NSIS
import System.Environment

import SdkVersion

main :: IO ()
main = do
    [installerFile, sdkTarball, pluginDir] <- getArgs
    writeFile installerFile $ nsis $ installer sdkTarball pluginDir

installer :: FilePath -> FilePath -> Action SectionId
installer sdkTarball pluginDir = do
    name "DAML SDK installer"
    outFile "daml-sdk-installer.exe"
    addPluginDir (fromString pluginDir)
    section "" [] $ do
        setOutPath "$TEMP"
        file [] (fromString sdkTarball)
        plugin "untgz" "extract" ["-d" :: Exp String, "$TEMP", "$TEMP/sdk-release-tarball.tar.gz"]
        let dir = "$TEMP/sdk-" <> sdkVersion
        execWait $ fromString $ "\"" <> dir <> "\\daml\\daml.exe\" install " <> dir <> " --activate"
