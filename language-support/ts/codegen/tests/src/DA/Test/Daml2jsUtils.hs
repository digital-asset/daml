-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DA.Test.Daml2jsUtils (
    TsLibrary (..),
    Workspaces (..),
    allTsLibraries,
    tsLibraryName,
    setupYarnEnv,
    ) where

{- HLINT ignore "locateRunfiles/package_app" -}

import Control.Monad
import DA.Bazel.Runfiles
import DA.Directory
import Data.Aeson
import Data.Aeson.Key qualified as Aeson
import Data.ByteString.Lazy qualified as BSL
import Data.Text.Extended qualified as T
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
            [ Aeson.fromText pkgName .= ("file:./" ++ name)
            | tsLib <- tsLibs
            , let name = tsLibraryName tsLib
            , let pkgName = "@" <> T.replace "-" "/"  (T.pack name)
            ]
        ]
