-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}

module DA.Sdk.Cli.Locations
  ( lastCheckPath
  , daBinDirPath
  , daBinPath
  , procPath
  , packagesPath

  , lastNotifiedVsnFilePath
  , lastUpgradeReminderTimeFilePath

  , sdkPackagePath
  , sdkPackageVersionDirPath
  , sdkPackageTemplateDirPath
  , sdkPackageChangelogPath
  , packagePath
  , packageVersionDirPath

  , defaultConfigFileName
  , defaultConfigFilePath
  , daHomePath

  , module DA.Sdk.Cli.Locations.Types
  ) where

import           DA.Sdk.Prelude

import           DA.Sdk.Cli.Locations.Types
import           DA.Sdk.Cli.System           (daDir, daRelativeBinDir, daRelativeBinName)
import           DA.Sdk.Version              (SemVersion, showSemVersion)

defaultConfigFileName :: FilePath
defaultConfigFileName = "da.yaml"

defaultHomeDirPath :: FilePathOfInstallationDir -> FilePathOfDaHomeDir
defaultHomeDirPath = addPathSuffix daDir

daHomePath :: FilePathOfInstallationDir -> FilePathOfDaHomeDir
daHomePath = defaultHomeDirPath

defaultConfigFilePath :: FilePathOfDaHomeDir -> FilePathOfDefaultConfigFile
defaultConfigFilePath = addPathSuffix defaultConfigFileName

lastCheckPath :: FilePathOfDaHomeDir -> FilePathOfLastCheckFile
lastCheckPath = addPathSuffix "last-check"

lastNotifiedVsnFilePath :: FilePathOfDaHomeDir -> FilePathOfLastNotifiedVsnFile
lastNotifiedVsnFilePath = addPathSuffix "last-notified-vsn"

lastUpgradeReminderTimeFilePath :: FilePathOfDaHomeDir -> FilePathOfLastUpgradeReminderFile
lastUpgradeReminderTimeFilePath = addPathSuffix "last-upgrade-reminder"

daBinDirPath :: FilePathOfDaHomeDir -> FilePathOfDaBinaryDir
daBinDirPath = addPathSuffix daRelativeBinDir

daBinPath :: FilePathOfDaBinaryDir -> FilePathOfDaBinary
daBinPath = addPathSuffix daRelativeBinName

procPath :: FilePathOfDaHomeDir -> FilePathOfProcDir
procPath = addPathSuffix "proc"

packagesPath :: FilePathOfDaHomeDir -> FilePathOfPackagesDir
packagesPath = addPathSuffix "packages"

sdkPackagePath :: FilePathOfPackagesDir -> FilePathOfSdkPackage
sdkPackagePath = addPathSuffix "sdk"

sdkPackageVersionDirPath :: SemVersion -> FilePathOfPackagesDir -> FilePathOfSdkPackageVersionDir
sdkPackageVersionDirPath v = addPathSuffix (textToPath (showSemVersion v)) . sdkPackagePath

sdkPackageChangelogPath :: SemVersion -> FilePathOfPackagesDir -> FilePathOfSdkPackageChangelog
sdkPackageChangelogPath v = addPathSuffix "CHANGELOG" . sdkPackageVersionDirPath v

sdkPackageTemplateDirPath :: SemVersion -> FilePathOfPackagesDir -> FilePathOfSdkPackageVersionTemplatesDir
sdkPackageTemplateDirPath v = addPathSuffix "templates" . sdkPackageVersionDirPath v

packagePath :: FilePathOfPackagesDir -> FilePath -> FilePathOfSomePackage
packagePath pkgsDir pkgName = addPathSuffix pkgName pkgsDir

packageVersionDirPath :: FilePathOfPackagesDir -> FilePath -> SemVersion -> FilePathOfSomePackageVersion
packageVersionDirPath pkgsDir pkgName v = addPathSuffix (textToPath (showSemVersion v)) $ packagePath pkgsDir pkgName
