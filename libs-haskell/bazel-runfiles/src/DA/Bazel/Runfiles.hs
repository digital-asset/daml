-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- There is a library in rules_haskell that does something similar
-- but since we also want support for locating runfiles in JARs
-- it is simpler to have all code located here.
module DA.Bazel.Runfiles
  ( locateRunfiles
  , mainWorkspace
  , exe
  ) where

import qualified Bazel.Runfiles
import GHC.Stack
import System.Directory
import System.Environment
import System.FilePath
import System.Info (os)

mainWorkspace :: String
mainWorkspace = "com_github_digital_asset_daml"

exe :: FilePath -> FilePath
exe | os == "mingw32" = (<.> "exe")
    | otherwise       = id

-- | Return the resources directory for the given runfiles dependency.
--
-- In packaged application, using @bazel_tools/packaging/packaging.bzl@
-- this corresponds to the top-level @resources@ directory. In a @bazel run@ or
-- @bazel test@ target this corresponds to the @runfiles@ path of the given
-- @data@ dependency.
locateRunfiles :: HasCallStack => FilePath -> IO FilePath
locateRunfiles fp = do
  -- If the current executable was packaged using @package_app@, then
  -- data files are stored underneath the resources directory.
  -- See @bazel_tools/packaging/packaging.bzl@.
  -- This is based on Buck's runfiles behavior and users of locateRunfiles
  -- expect the resources directory itself.
  execPath <- getExecutablePath
  let jarResources = takeDirectory execPath </> "resources"
  hasJarResources <- doesDirectoryExist jarResources
  if hasJarResources
      then pure jarResources
      else do
          runfiles <- Bazel.Runfiles.create
          pure $! Bazel.Runfiles.rlocation runfiles fp
