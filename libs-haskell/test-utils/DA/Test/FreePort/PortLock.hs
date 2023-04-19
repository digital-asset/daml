-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Test.FreePort.PortLock (lockPort) where

import DA.Test.FreePort.OS (os, OS (Windows))
import System.Directory (getHomeDirectory, createDirectoryIfMissing)
import System.FileLock (tryLockFile, FileLock, SharedExclusive (Exclusive))
import System.FilePath ((</>))

lockPort :: Int -> IO (Maybe FileLock)
lockPort p = do
  tmpDir <- getTmpDir
  createDirectoryIfMissing True tmpDir
  tryLockFile (tmpDir </> show p) Exclusive

getTmpDir :: IO FilePath
getTmpDir = do
  tmpDir <- case os of
    Windows -> do
      home <- getHomeDirectory
      pure $ home </> "Appdata" </> "Local" </> "Temp"
    _ -> pure "/tmp"
  pure $ tmpDir </> "daml" </> "build" </> "postgresql-testing" </> "ports"

