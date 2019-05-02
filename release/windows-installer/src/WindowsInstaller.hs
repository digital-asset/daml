-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}
module Main (main) where

import Data.String
import Development.NSIS
import System.Environment
import System.FilePath

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
        -- We use PLUGINSDIR as an easy way to get a temporary directory
        -- that nsis will cleanup automatically.
        unsafeInject "InitPluginsDir"
        let dir = "$PLUGINSDIR" </> "daml-sdk-" <> sdkVersion
        setOutPath (fromString dir)
        file [Recursive] (fromString (sdkDir <> "\\*.*"))
        -- install --activate will copy the SDK to the final location.
        execWait $ fromString $ "\"" <> dir </> "daml" </> "daml.exe" <> "\" install " <> dir <> " --activate"
