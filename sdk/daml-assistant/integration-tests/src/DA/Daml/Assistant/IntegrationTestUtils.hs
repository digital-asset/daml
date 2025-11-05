-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DA.Daml.Assistant.IntegrationTestUtils
  ( withSdkResource
  , withDpmSdkResource
  , SandboxPorts(..)
  , sandboxPorts
  , throwError
  ) where

{- HLINT ignore "locateRunfiles/package_app" -}

import Conduit hiding (connect)
import Control.Monad (forM_)
import qualified Data.Conduit.Tar.Extra as Tar.Conduit.Extra
import qualified Data.Conduit.Zlib as Zlib
import Data.List.Extra
import qualified Data.Text as T
import Network.Socket.Extended (PortNumber, getFreePort)
import System.Environment.Blank
import System.FilePath
import System.Directory.Extra
import System.IO.Extra
import System.Info.Extra
import Test.Tasty

import DA.Bazel.Runfiles
import DA.Test.Process (callProcessSilent)
import DA.Test.Util

-- | Install the SDK in a temporary directory and provide the path to the SDK directory.
-- This also adds the bin directory to PATH so calling assistant commands works without
-- special hacks.
withSdkResource :: (IO FilePath -> TestTree) -> TestTree
withSdkResource = _withSdkResource (mainWorkspace </> "release" </> "sdk-release-tarball-ce.tar.gz") "DAML_HOME" $ \extractDir _ ->
  if isWindows
    then callProcessSilent
        (extractDir </> "daml" </> damlInstallerName)
        ["install", "--install-assistant=yes", "--set-path=no", "--install-with-internal-version=yes", extractDir]
    else callProcessSilent (extractDir </> "install.sh") ["--install-with-internal-version=yes"]

withDpmSdkResource :: (IO FilePath -> TestTree) -> TestTree
withDpmSdkResource =
  _withSdkResource (mainWorkspace </> "release" </> "dpm-sdk-release-tarball.tar.gz") "DPM_HOME" $ \extractDir targetDir ->
    callProcessSilent "cp" ["-a", extractDir </> ".", targetDir]

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

-- from DA.Daml.Helper.Util
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

damlInstallerName :: String
damlInstallerName
    | isWindows = "daml.exe"
    | otherwise = "daml"
