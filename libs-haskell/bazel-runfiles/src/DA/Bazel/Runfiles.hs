-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- There is a library in rules_haskell that does something similar
-- but since we also want support for locating runfiles in JARs
-- it is simpler to have all code located here.
module DA.Bazel.Runfiles
  ( Resource (..)
  , locateResource
  , locateRunfiles
  , setRunfilesEnv
  , mainWorkspace
  , exe
  ) where

import qualified Bazel.Runfiles
import Control.Exception.Safe (MonadCatch, SomeException, try)
import Control.Monad (forM_)
import GHC.Stack
import System.Directory
import System.Environment.Blank
import System.FilePath
import System.Info (os)

mainWorkspace :: String
mainWorkspace = "com_github_digital_asset_daml"

exe :: FilePath -> FilePath
exe | os == "mingw32" = (<.> "exe")
    | otherwise       = id

data Resource = Resource
  { runfilesPath :: FilePath
  , resourcesPath :: FilePath
  }
  deriving (Eq, Ord, Show)

-- | Return the path for the given resource dependency.
--
-- In a packaged application, using @bazel_tools/packaging/packaging.bzl@,
--
-- > locateResource Resource { runfilesPath, resourcesPath }
--
-- returns the path
--
-- > topLevelResourcesDir </> resourcesPath
--
-- where @topLevelResourcesDir@ is the top level @resources@ directory, a
-- sibling of the packaged executable.
--
-- In a @bazel run@ or @bazel test@ target, the same expression returns
--
-- > topLevelResourceDir </> runfilesPath
--
-- where @topLevelResourceDir@ is the top level @runfiles@ directory assigned
-- by Bazel.
--
locateResource :: HasCallStack => Resource -> IO FilePath
locateResource Resource { runfilesPath, resourcesPath } = do
  -- If the current executable was packaged using @package_app@, then
  -- data files are stored underneath the resources directory.
  -- See @bazel_tools/packaging/packaging.bzl@.
  -- This is based on Buck's runfiles behavior.
  execPath <- getExecutablePath
  let jarResources = takeDirectory execPath </> "resources"
  hasJarResources <- doesDirectoryExist jarResources
  if hasJarResources
      then pure (jarResources </> resourcesPath)
      else do
          runfiles <- Bazel.Runfiles.create
          pure $! Bazel.Runfiles.rlocation runfiles runfilesPath

-- | Return the resources directory for the given runfiles dependency.
--
-- In a packaged application, using @bazel_tools/packaging/packaging.bzl@
-- this corresponds to the top-level @resources@ directory. In a @bazel run@ or
-- @bazel test@ target this corresponds to the @runfiles@ path of the given
-- @data@ dependency.
--
-- WARNING: In a packaged application this function __ignores the argument__
-- and simply returns the resources directory itself - even if the
-- corresponding @runfiles@ path is for a file.
-- Prefer 'locateResource' for codepaths that will be used from both packaged
-- applications and @bazel run@ or @bazel test@ targets.
-- Users of this function should handle the result accordingly.
--
locateRunfiles :: HasCallStack => FilePath -> IO FilePath
locateRunfiles runfilesPath =
  locateResource Resource
    { runfilesPath
    , resourcesPath = ""
    }

-- | Store the detected runfiles in the environment.
--
-- Detects runfiles as in 'locateRunfiles' and, if found, stores the
-- outcome in the environment. Do this at the beginning of the program
-- if you intend to change working directory or use `withProgName` or
-- `withArgs`.
--
-- Failure to detect runfiles will be ignored.
setRunfilesEnv :: HasCallStack => IO ()
setRunfilesEnv = do
  execPath <- getExecutablePath
  let jarResources = takeDirectory execPath </> "resources"
  hasJarResources <- doesDirectoryExist jarResources
  if hasJarResources
      then pure ()
      else try_ $ do
          runfiles <- Bazel.Runfiles.create
          let vars = Bazel.Runfiles.env runfiles
          forM_ vars $ \(name, value) ->
              setEnv name value True

try_ :: MonadCatch m => m () -> m ()
try_ m = do
  _ <- try @_ @SomeException m
  pure ()
