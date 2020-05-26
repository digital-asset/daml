-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Main (main) where

import Data.String
import Development.NSIS
import System.Environment
import System.FilePath

import SdkVersion

main :: IO ()
main = do
    [installerFile, sdkDir, logo] <- getArgs
    writeFile installerFile $ nsis $ installer sdkDir logo

installer :: FilePath -> FilePath -> Action SectionId
installer sdkDir logo = do
    name "DAML SDK"
    outFile "daml-sdk-installer.exe"
    installIcon (fromString logo)
    requestExecutionLevel User
    unsafeInjectGlobal "!insertmacro MUI_PAGE_WELCOME"
    page InstFiles
    page $ Finish finishOptions
        { finLinkText = "Open the DAML Quickstart guide"
        , finLink = "https://docs.daml.com/getting-started/quickstart.html"
        }
    section "" [] $ do
        -- We use PLUGINSDIR as an easy way to get a temporary directory
        -- that nsis will cleanup automatically.
        unsafeInject "InitPluginsDir"
        iff_ (fileExists "$APPDATA/daml") $ do
            answer <- messageBox [MB_YESNO] "DAML SDK is already installed. Do you want to remove the installed SDKs before installing this one?"
            iff (answer %== "YES")
                (rmdir [Recursive] "$APPDATA/daml")
                (abort "Existing installation detected.")
        let dir = "$PLUGINSDIR" </> "daml-sdk-" <> sdkVersion
        setOutPath (fromString dir)
        file [Recursive] (fromString (sdkDir <> "\\*.*"))
        -- install --install-assistant=yes will copy the SDK to the final location.
        plugin "nsExec" "ExecToLog"
            [ fromString $ unwords
                  [ "\"" <> dir </> "daml" </> "daml.exe\""
                  , "install"
                  , "\"" <> dir <> "\""
                  , "--install-assistant=yes"
                  ] :: Exp String
            ]
