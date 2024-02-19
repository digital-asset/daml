-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DA.Daml.Assistant.IntegrationTestUtils
  ( withSdkResource
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
withSdkResource f =
    withTempDirResource $ \getDir ->
    withResource (installSdk =<< getDir) restoreEnv (const $ f getDir)
  where installSdk targetDir = do
            releaseTarball <- locateRunfiles (mainWorkspace </> "release" </> "sdk-release-tarball-ce.tar.gz")
            oldPath <- getSearchPath
            withTempDir $ \cacheDir -> do
            withTempDir $ \extractDir -> do
                runConduitRes
                    $ sourceFileBS releaseTarball
                    .| Zlib.ungzip
                    .| Tar.Conduit.Extra.untar (Tar.Conduit.Extra.restoreFile throwError extractDir)
                setEnv "DAML_HOME" targetDir True
                setPermissions cacheDir emptyPermissions
                setEnv "DAML_CACHE" cacheDir True
                if isWindows
                    then callProcessSilent
                        (extractDir </> "daml" </> damlInstallerName)
                        ["install", "--install-assistant=yes", "--set-path=no", "--install-with-internal-version=yes", extractDir]
                    else callProcessSilent (extractDir </> "install.sh") ["--install-with-internal-version=yes"]
                -- We restrict the permissions of the DAML_HOME directory to make sure everything
                -- still works when the directory is read-only.
                allFiles <- listFilesRecursive targetDir
                forM_ allFiles $ \file -> do
                  getPermissions file >>= \p -> setPermissions file $ p {writable = False}
                setPermissions targetDir emptyPermissions {executable = True}
            setEnv "PATH" (intercalate [searchPathSeparator] ((targetDir </> "bin") : oldPath)) True
            pure oldPath
        restoreEnv oldPath = do
            setEnv "PATH" (intercalate [searchPathSeparator] oldPath) True
            unsetEnv "DAML_HOME"

-- from DA.Daml.Helper.Util
data SandboxPorts = SandboxPorts
  { ledger :: PortNumber
  , admin :: PortNumber
  , sequencerPublic :: PortNumber
  , sequencerAdmin :: PortNumber
  , mediatorAdmin :: PortNumber
  }

sandboxPorts :: IO SandboxPorts
sandboxPorts = SandboxPorts <$> getFreePort <*> getFreePort <*> getFreePort <*> getFreePort <*> getFreePort

throwError :: MonadFail m => T.Text -> T.Text -> m ()
throwError msg e = fail (T.unpack $ msg <> " " <> e)

damlInstallerName :: String
damlInstallerName
    | isWindows = "daml.exe"
    | otherwise = "daml"
