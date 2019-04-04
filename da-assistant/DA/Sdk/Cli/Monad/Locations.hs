-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}

module DA.Sdk.Cli.Monad.Locations where

import           DA.Sdk.Prelude
import           DA.Sdk.Cli.System           (installationDir)
import           DA.Sdk.Version              (SemVersion)
import qualified DA.Sdk.Cli.Locations as L
import           DA.Sdk.Cli.Monad.FileSystem
import           DA.Sdk.Cli.Conf.Types       (Project(..))
import           DA.Sdk.Prelude.Path (stringToPath)
import qualified DA.Sdk.Cli.Monad.FileSystem as FS
import qualified Control.Monad.Logger as Log
import Control.Monad.Trans.Except (ExceptT (..), runExceptT) 
import qualified Control.Monad.Except as EE
import Control.Monad.Trans (lift)
import System.Directory (getAppUserDataDirectory)
import Control.Monad.Extra

data GetVsCodeExtensionPathFailed = GetVsCodeExtensionPathFailed FilePath deriving (Eq, Show)

class Monad m => MonadLocations m where
    getInstallationDir :: m L.FilePathOfInstallationDir
    getVscodeExtensionPath :: m (Either GetVsCodeExtensionPathFailed FilePath)

    default getInstallationDir :: MonadIO m => m L.FilePathOfInstallationDir
    getInstallationDir = installationDir

    default getVscodeExtensionPath :: MonadIO m => m (Either GetVsCodeExtensionPathFailed FilePath) 
    getVscodeExtensionPath = liftIO $ runExceptT $ do
      vscodePath <- lift $ stringToPath <$> getAppUserDataDirectory "vscode"
      whenM (notM $ FS.testdir' vscodePath) $
          EE.throwError $ GetVsCodeExtensionPathFailed vscodePath
      return $ vscodePath </> "extensions" </> "da-vscode-daml-extension"

instance MonadLocations IO

instance MonadLocations (Log.LoggingT IO)

instance MonadLocations m => MonadLocations (ExceptT e m) where
    getInstallationDir = lift getInstallationDir
    getVscodeExtensionPath = lift getVscodeExtensionPath

getDefaultHomeDirPath :: MonadLocations m => m L.FilePathOfDaHomeDir
getDefaultHomeDirPath = L.daHomePath <$> getInstallationDir

getDaHomePath :: MonadLocations m => m L.FilePathOfDaHomeDir
getDaHomePath = getDefaultHomeDirPath

getDefaultConfigFilePath :: MonadLocations m => m L.FilePathOfDefaultConfigFile
getDefaultConfigFilePath = L.defaultConfigFilePath <$> getDefaultHomeDirPath

getSdkTempDirPath :: (MonadFS m, MonadLocations m) => m L.FilePathOfSdkTmpDir
getSdkTempDirPath = getDefaultHomeDirPath >>= \homeDir -> do
  let tempPath = L.addPathSuffix "tmp" homeDir
  void $ mktree tempPath -- TODO (GH) we should propagate the error
  pure tempPath

getLastCheckPath :: MonadLocations m => m L.FilePathOfLastCheckFile
getLastCheckPath = L.lastCheckPath <$> getDaHomePath

getLastNotifiedVsnFilePath :: MonadLocations m => m L.FilePathOfLastNotifiedVsnFile
getLastNotifiedVsnFilePath = L.lastNotifiedVsnFilePath <$> getDaHomePath

getLastUpgradeReminderTimeFilePath :: MonadLocations m => m L.FilePathOfLastUpgradeReminderFile
getLastUpgradeReminderTimeFilePath = L.lastUpgradeReminderTimeFilePath <$> getDaHomePath

getDaBinDirPath :: MonadLocations m => m L.FilePathOfDaBinaryDir
getDaBinDirPath = L.daBinDirPath <$> getDaHomePath

getDaBinPath :: MonadLocations m => m L.FilePathOfDaBinary
getDaBinPath = L.daBinPath <$> getDaBinDirPath

getProcPath :: MonadLocations m => m L.FilePathOfProcDir
getProcPath = L.procPath <$> getDaHomePath

getPackagesPath :: MonadLocations m => m L.FilePathOfPackagesDir
getPackagesPath = L.packagesPath <$> getDaHomePath

getProjectLogPath :: (MonadLocations m, MonadFS m) => Project -> m (Either MkTreeFailed FilePath)
getProjectLogPath project = runExceptT $ do
    ExceptT $ mktree' logPath
    return logPath
  where
    logPath = projectPath project </> "logs"

getSdkPackagePath :: MonadLocations m => m L.FilePathOfSdkPackage
getSdkPackagePath = L.sdkPackagePath <$> getPackagesPath

getSdkPackageVersionDirPath :: MonadLocations m => SemVersion -> m L.FilePathOfSdkPackageVersionDir
getSdkPackageVersionDirPath v = L.sdkPackageVersionDirPath v <$> getPackagesPath

getSdkPackageChangelogPath :: MonadLocations m => SemVersion -> m L.FilePathOfSdkPackageChangelog
getSdkPackageChangelogPath v = L.sdkPackageChangelogPath v <$> getPackagesPath

getSdkPackageTemplateDirPath :: MonadLocations m => SemVersion -> m L.FilePathOfSdkPackageVersionTemplatesDir
getSdkPackageTemplateDirPath v = L.sdkPackageTemplateDirPath v <$> getPackagesPath
