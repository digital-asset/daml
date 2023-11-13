-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

import Bazel.Runfiles qualified
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

-- | Identifies a runtime resource.
data Resource = Resource
  { resourcesPath :: FilePath
    -- ^ Path of the resource relative to the @"resources"@ directory of the
    -- packaged application where it's used.
  , runfilesPathPrefix :: FilePath
    -- ^ Prefix which must be prepended to @resourcesPath@ in order to find the
    -- same resource in the @"runfiles"@ directory when the application is used
    -- as a Bazel target.
  }
  deriving (Eq, Ord, Show)

-- | The runfiles path of a Resource is simply its 'resourcesPath' prepended
-- with its 'runfilesPathPrefix'.
runfilesPath :: Resource -> FilePath
runfilesPath res = runfilesPathPrefix res </> resourcesPath res

-- | Return the path for the given resource dependency.
--
-- In a packaged application, using @bazel_tools/packaging/packaging.bzl@,
--
-- > locateResource Resource { resourcesPath, runfilesPathPrefix }
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
-- > topLevelRunfilesDir </> runfilesPathPrefix </> resourcesPath
--
-- where @topLevelRunfilesDir@ is the top level @runfiles@ directory assigned
-- by Bazel.
--
locateResource :: HasCallStack => Resource -> IO FilePath
locateResource res = do
  -- If the current executable was packaged using @package_app@, then
  -- data files are stored underneath the resources directory.
  -- See @bazel_tools/packaging/packaging.bzl@.
  -- This is based on Buck's runfiles behavior.
  execPath <- getExecutablePath
  let jarResources = takeDirectory execPath </> "resources"
  hasJarResources <- doesDirectoryExist jarResources
  if hasJarResources
      then pure (jarResources </> resourcesPath res)
      else locateRunfiles (runfilesPath res)

-- | Return the path for the given runfiles dependency.
--
-- WARNING: The runfiles directory only exists when the executable is a
-- @bazel run@ or @bazel test@ target. In particular, this function will
-- fail at runtime when used from a packaged application
-- (@bazel_tools/packaging/packaging.bzl@).
--
-- Instead, use 'locateResource' for codepaths that will be used from both
-- packaged applications and @bazel run@ or @bazel test@ targets.
--
locateRunfiles :: HasCallStack => FilePath -> IO FilePath
locateRunfiles fp = do
  runfiles <- Bazel.Runfiles.create
  pure $! Bazel.Runfiles.rlocation runfiles fp

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
