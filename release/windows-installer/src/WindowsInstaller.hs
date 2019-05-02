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
    [installerFile, sdkDir] <- getArgs
    writeFile installerFile $ nsis $ installer sdkDir

installer :: FilePath -> Action SectionId
installer sdkDir = do
    name "DAML SDK installer"
    outFile "daml-sdk-installer.exe"
    section "" [] $ do
        setOutPath "$TEMP"
        file [Recursive] (fromString (sdkDir <> "\\*.*"))
        let dir = "$TEMP/sdk-" <> sdkVersion
        execWait $ fromString $ "\"" <> dir <> "\\daml\\daml.exe\" install " <> dir <> " --activate"
