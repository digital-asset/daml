-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DA.Daml.Assistant.IntegrationTestUtils
  ( withDpmSdkResource
  , withDpmSdkExtraVerResource
  , SandboxPorts(..)
  , sandboxPorts
  , throwError
  , TsLibrary (..)
  , Workspaces (..)
  , allTsLibraries
  , tsLibraryName
  , setupYarnEnv
  , tokenFor
  , decodeCantonSandboxPort
  ) where

{- HLINT ignore "locateRunfiles/package_app" -}

import Conduit hiding (connect)
import Control.Monad (forM_, join)
import DA.Bazel.Runfiles
import DA.Directory
import DA.Test.Process (callProcessSilent)
import DA.Test.Util
import Data.Aeson ((.=))
import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Key as Aeson
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as BSL8
import qualified Data.Conduit.Tar.Extra as Tar.Conduit.Extra
import qualified Data.Conduit.Zlib as Zlib
import Data.List.Extra
import qualified Data.Map as Map
import qualified Data.Text as T
import Network.Socket.Extended (PortNumber, getFreePort)
import System.Environment.Blank
import System.FilePath
import System.Directory.Extra
import System.IO.Extra
import Test.Tasty
import qualified Web.JWT as JWT
import ComponentVersion.Class (ComponentVersioned, componentVersionString)

-- | Install the SDK in a temporary directory and provide the path to the SDK directory.
-- This also adds the bin directory to PATH so calling assistant commands works without
-- special hacks.
withDpmSdkResource :: (IO FilePath -> TestTree) -> TestTree
withDpmSdkResource =
  _withSdkResource (mainWorkspace </> "release" </> "dpm-sdk-release-tarball.tar.gz") "DPM_HOME" $ \extractDir targetDir ->
    callProcessSilent "cp" ["-a", extractDir </> ".", targetDir]

-- | Like withDpmSdkResource but also installs "10.0.0" with the same components. useful for multi-package testing
withDpmSdkExtraVerResource :: ComponentVersioned => (IO FilePath -> TestTree) -> TestTree
withDpmSdkExtraVerResource =
  _withSdkResource (mainWorkspace </> "release" </> "dpm-sdk-release-tarball.tar.gz") "DPM_HOME" $ \extractDir targetDir -> do
    callProcessSilent "cp" ["-a", extractDir </> ".", targetDir]
    -- Take a copy of 0.0.0.yaml and rename/replace its version field over to 10.0.0
    assemblyContent <- readFile $ targetDir </> "cache" </> "sdk" </> "open-source" </> componentVersionString <> ".yaml"
    let updatedAssemblyContent = replace ("\n  version: " <> componentVersionString) "\n  version: 10.0.0" assemblyContent
    writeFile (targetDir </> "cache" </> "sdk" </> "open-source" </> "10.0.0.yaml") updatedAssemblyContent

-- Takes path to tarball, HOME variable name, and installation action
_withSdkResource :: FilePath -> String -> (FilePath -> FilePath -> IO ()) -> (IO FilePath -> TestTree) -> TestTree
_withSdkResource tarball homeName install f = do
    withTempDirResource $ \getDir ->
      withResource (installSdk =<< getDir) restoreEnv (const $ f $ (</> "installation") <$> getDir)
  where installSdk tmpDir = do
            let targetDir = tmpDir </> "installation"
                cacheDir = tmpDir </> "cache"
            createDirectory targetDir
            createDirectory cacheDir

            releaseTarball <- locateRunfiles tarball
            oldPath <- getSearchPath
            setEnv "DAML_CACHE" cacheDir True
            setEnv homeName targetDir True

            withTempDir $ \extractDir -> do
                runConduitRes
                    $ sourceFileBS releaseTarball
                    .| Zlib.ungzip
                    .| Tar.Conduit.Extra.untar (Tar.Conduit.Extra.restoreFile throwError extractDir)
                setPermissions cacheDir emptyPermissions
                install extractDir targetDir
                -- We restrict the permissions of the `homeName` directory to make sure everything
                -- still works when the directory is read-only.
                allFiles <- listFilesRecursive targetDir
                forM_ allFiles $ \file -> do
                  getPermissions file >>= \p -> setPermissions file $ p {writable = False}
                setPermissions targetDir emptyPermissions {executable = True, readable = True, searchable = True}
                setPermissions cacheDir emptyPermissions {readable = True, writable = True, searchable = True}
            setEnv "PATH" (intercalate [searchPathSeparator] ((targetDir </> "bin") : oldPath)) True
            pure oldPath
        restoreEnv oldPath = do
            setEnv "PATH" (intercalate [searchPathSeparator] oldPath) True
            unsetEnv "DAML_CACHE"
            unsetEnv homeName

data SandboxPorts = SandboxPorts
  { ledger :: PortNumber
  , admin :: PortNumber
  , sequencerPublic :: PortNumber
  , sequencerAdmin :: PortNumber
  , mediatorAdmin :: PortNumber
  , jsonApi :: PortNumber
  }

sandboxPorts :: IO SandboxPorts
sandboxPorts = SandboxPorts <$> getFreePort <*> getFreePort <*> getFreePort <*> getFreePort <*> getFreePort <*> getFreePort

throwError :: MonadFail m => T.Text -> T.Text -> m ()
throwError msg e = fail (T.unpack $ msg <> " " <> e)

data TsLibrary
    = DamlTypes
    deriving (Bounded, Enum)

newtype Workspaces = Workspaces [FilePath]

allTsLibraries :: [TsLibrary]
allTsLibraries = [minBound .. maxBound]

tsLibraryName :: TsLibrary -> String
tsLibraryName = \case
    DamlTypes -> "daml-types"

-- NOTE(MH): In some tests we need our TS libraries like `@daml/types` in
-- scope. We achieve this by putting a `package.json` file further up in the
-- directory tree. This file sets up a yarn workspace that includes the TS
-- libraries via the `resolutions` field.
setupYarnEnv :: FilePath -> Workspaces -> [TsLibrary] -> IO ()
setupYarnEnv rootDir (Workspaces workspaces) tsLibs = do
    jsLibsRoot <- locateRunfiles $ mainWorkspace </> "language-support" </> "js"
    forM_  tsLibs $ \tsLib -> do
        let name = tsLibraryName tsLib
        copyDirectory (jsLibsRoot </> name </> "npm_package") (rootDir </> name)
    BSL.writeFile (rootDir </> "package.json") $ Aeson.encode $ Aeson.object
        [ "private" .= True
        , "workspaces" .= workspaces
        , "resolutions" .= Aeson.object
            [ Aeson.fromText pkgName .= ("file:./" ++ name)
            | tsLib <- tsLibs
            , let name = tsLibraryName tsLib
            , let pkgName = "@" <> T.replace "-" "/"  (T.pack name)
            ]
        ]

tokenFor :: T.Text -> T.Text
tokenFor user =
  JWT.encodeSigned
    (JWT.EncodeHMACSecret "secret")
    mempty
    mempty
      { JWT.sub = JWT.stringOrURI user
        , JWT.unregisteredClaims =
          JWT.ClaimsMap $
          Map.fromList
            [ ("scope", Aeson.String "daml_ledger_api")
          ]
      }

decodeCantonPort :: String -> String -> Maybe Int
decodeCantonPort participantName json = do
    participants :: Map.Map String (Map.Map String (Maybe Int)) <- Aeson.decode (BSL8.pack json)
    ports <- Map.lookup participantName participants
    join $ Map.lookup "ledgerApi" ports

decodeCantonSandboxPort :: String -> Maybe Int
decodeCantonSandboxPort = decodeCantonPort "sandbox"
