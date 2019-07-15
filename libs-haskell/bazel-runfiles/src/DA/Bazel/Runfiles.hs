-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- There is a library in rules_haskell that does something similar
-- but since we also want support for locating runfiles in JARs
-- it is simpler to have all code located here.
module DA.Bazel.Runfiles
  ( locateRunfiles
  , mainWorkspace
  ) where

import qualified Bazel.Runfiles
import System.Directory
import System.Environment
import System.FilePath

mainWorkspace :: String
mainWorkspace = "com_github_digital_asset_daml"

locateRunfiles :: FilePath -> IO FilePath
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
