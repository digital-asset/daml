-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DA.Test.Daml2jsUtils (
    TsLibrary (..),
    Workspaces (..),
    allTsLibraries,
    setupYarnEnv,
    addTestDependencies,
    ) where

import qualified Data.Text.Extended as T
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString as BS
import Control.Monad
import DA.Bazel.Runfiles
import DA.Directory
import Data.Aeson
import Data.Aeson.Extra.Merge
import System.FilePath

data TsLibrary
    = DamlLedger
    | DamlReact
    | DamlTypes
    deriving (Bounded, Enum)

newtype Workspaces = Workspaces [FilePath]

allTsLibraries :: [TsLibrary]
allTsLibraries = [minBound .. maxBound]

tsLibraryName :: TsLibrary -> String
tsLibraryName = \case
    DamlLedger -> "daml-ledger"
    DamlReact -> "daml-react"
    DamlTypes -> "daml-types"

-- NOTE(MH): In some tests we need our TS libraries like `@daml/types` in
-- scope. We achieve this by putting a `package.json` file further up in the
-- directory tree. This file sets up a yarn workspace that includes the TS
-- libraries via the `resolutions` field.
setupYarnEnv :: FilePath -> Workspaces -> [TsLibrary] -> IO ()
setupYarnEnv rootDir (Workspaces workspaces) tsLibs = do
    tsLibsRoot <- locateRunfiles $ mainWorkspace </> "language-support" </> "ts"
    forM_  tsLibs $ \tsLib -> do
        let name = tsLibraryName tsLib
        copyDirectory (tsLibsRoot </> name </> "npm_package") (rootDir </> name)
    BSL.writeFile (rootDir </> "package.json") $ encode $ object
        [ "private" .= True
        , "workspaces" .= workspaces
        , "resolutions" .= object
            [ pkgName .= ("file:./" ++ name)
            | tsLib <- tsLibs
            , let name = tsLibraryName tsLib
            , let pkgName = "@" <> T.replace "-" "/"  (T.pack name)
            ]
        ]

testDependencies :: Value
testDependencies = object
    [ "devDependencies" .= object
        [ ("@types/jest", String "^25.2.1")
        , ("@types/node", String "^13.11.1")
        , ("@types/puppeteer", String "^2.0.1")
        , ("@types/wait-on", String "^4.0.0")
        , ("puppeteer", String "^2.1.1")
        , ("wait-on", String "^4.0.2")
        ]
    ]

addTestDependencies :: FilePath -> IO ()
addTestDependencies uiDir = do
    packageJson <- BS.readFile (uiDir </> "package.json")
    let packageJsonObj = case decode (BSL.fromStrict packageJson) of
            Nothing -> error "Could not decode package.json"
            Just pJ -> pJ
        newPackageJson = lodashMerge packageJsonObj testDependencies
    BSL.writeFile (uiDir </> "package.json") (encode newPackageJson)
