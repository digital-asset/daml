module DA.Test.FreePort.PortLock (withPortLock) where

import DA.Test.FreePort.OS (os, OS (Windows))
import System.Directory (getHomeDirectory, createDirectoryIfMissing)
import System.FileLock (withTryFileLock, SharedExclusive (Exclusive))
import System.FilePath ((</>))

-- This should try to create a port lock file and run your comp if it works
-- Should use a determinsitic path (for windows or unix based)
withPortLock :: Int -> IO a -> IO (Maybe a)
withPortLock p k = do
  tmpDir <- getTmpDir
  createDirectoryIfMissing True tmpDir
  withTryFileLock (tmpDir </> show p) Exclusive (const k)

getTmpDir :: IO FilePath
getTmpDir = do
  tmpDir <- case os of
    Windows -> do
      home <- getHomeDirectory
      pure $ home </> "Appdata" </> "Local" </> "Temp"
    _ -> pure "/tmp"
  pure $ tmpDir </> "daml" </> "build" </> "postgresql-testing" </> "ports"
