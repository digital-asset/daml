-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DA.Daml.Assistant.IntegrationTestUtils (withSdkResource, throwError) where

import Conduit hiding (connect)
import Control.Monad.Fail (MonadFail)
import qualified Data.Conduit.Tar.Extra as Tar.Conduit.Extra
import qualified Data.Conduit.Zlib as Zlib
import Data.List.Extra
import qualified Data.Text as T
import System.Environment.Blank
import System.FilePath
import System.IO.Extra
import System.Info.Extra
import Test.Tasty

import DA.Bazel.Runfiles
import DA.Test.Process (callCommandSilent,callProcessSilent)
import DA.Test.Util

-- | Install the SDK in a temporary directory and provide the path to the SDK directory.
-- This also adds the bin directory to PATH so calling assistant commands works without
-- special hacks.
withSdkResource :: (IO FilePath -> TestTree) -> TestTree
withSdkResource f =
    withTempDirResource $ \getDir ->
    withResource (installSdk =<< getDir) restoreEnv (const $ f getDir)
  where installSdk targetDir = do
            releaseTarball <- locateRunfiles (mainWorkspace </> "release" </> "sdk-release-tarball.tar.gz")
            oldPath <- getSearchPath
            withTempDir $ \extractDir -> do
                runConduitRes
                    $ sourceFileBS releaseTarball
                    .| Zlib.ungzip
                    .| Tar.Conduit.Extra.untar (Tar.Conduit.Extra.restoreFile throwError extractDir)
                setEnv "DAML_HOME" targetDir True
                if isWindows
                    then callProcessSilent
                        (extractDir </> "daml" </> damlInstallerName)
                        ["install", "--install-assistant=yes", "--set-path=no", extractDir]
                    else callCommandSilent $ extractDir </> "install.sh"
            setEnv "PATH" (intercalate [searchPathSeparator] ((targetDir </> "bin") : oldPath)) True
            pure oldPath
        restoreEnv oldPath = do
            setEnv "PATH" (intercalate [searchPathSeparator] oldPath) True
            unsetEnv "DAML_HOME"

throwError :: MonadFail m => T.Text -> T.Text -> m ()
throwError msg e = fail (T.unpack $ msg <> " " <> e)

damlInstallerName :: String
damlInstallerName
    | isWindows = "daml.exe"
    | otherwise = "daml"
