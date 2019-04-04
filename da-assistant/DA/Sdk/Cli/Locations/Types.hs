-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE DataKinds, KindSignatures, FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}

module DA.Sdk.Cli.Locations.Types
    ( FilePathOf (..)
    , FilePathTypes (..)
    , PackageName (..)
    , FilePathOfInstallationDir
    , FilePathOfDaHomeDir
    , FilePathOfProcDir
    , FilePathOfSdkTmpDir
    , FilePathOfPackagesDir
    , FilePathOfProjectDir
    , FilePathOfProjectLogDir
    , FilePathOfLastCheckFile
    , FilePathOfLastNotifiedVsnFile
    , FilePathOfLastUpgradeReminderFile
    , FilePathOfDaBinaryDir
    , FilePathOfDaBinary
    , FilePathOfSdkPackage
    , FilePathOfSdkPackageVersionDir
    , FilePathOfSdkPackageVersionTemplatesDir
    , FilePathOfSdkPackageVersionMetafile
    , FilePathOfSdkPackageChangelog
    , FilePathOfSomePackage
    , FilePathOfSomePackageVersion
    , FilePathOfPackage
    , FilePathOfPackageVersion
    , FilePathOfDefaultConfigFile
    , FilePathOfSomePackageVersionBinDir
    , FilePathOfSomePackageVersionBin
    , FilePathOfDaBinaryDirPackageBin
    , IsDirPath
    , IsFilePath
    , DirContains(..)
    , pathOfToText
    , pathOfToString
    ) where

import DA.Sdk.Prelude
import Turtle ((</>), IsString)

-- | File path of a specific destination. This is a newtype over the 'FilePath'
-- of the prelude, designed to prevent equivocation of different types of
-- filepaths. For instance, a file path to the DA home directory is very
-- different from the file path to the DA binary.
newtype FilePathOf (a :: FilePathTypes)
    = FilePathOf { unwrapFilePathOf :: FilePath }
    deriving (Eq, Ord, Show, IsString)

-- | Supported destinations for 'FilePathOf'. Feel free to add to this list and
-- add a type alias below.
data FilePathTypes
    = InstallationDir
    | DaHomeDir
    | ProcDir
    | PackagesDir
    | ProjectDir
    | ProjectLogDir
    | LastCheckFile
    | LastNotifiedVsnFile
    | LastUpgradeReminderFile
    | DaBinaryDir
    | DaBinary
    | DaBinaryDirPackageBin
    | SdkPackageChangelog
    | SdkTmpDir
    | SomePackageDir PackageName
    | SomePackageVersionDir PackageName
    | SdkPackageVersionMetafile
    | SdkPackageVersionPackageDir
    | DaConfigFile
    | SomePackageVersionBinDir
    | SomePackageVersionBin

data PackageName
    = SdkPackage
    | OtherPackage

-- | @IsDirPath a@ means that @a@ is a type of paths for directories.
class IsDirPath (a :: FilePathTypes)
instance IsDirPath 'InstallationDir
instance IsDirPath 'DaHomeDir
instance IsDirPath 'DaBinaryDir
instance IsDirPath 'ProcDir
instance IsDirPath 'PackagesDir
instance IsDirPath 'ProjectDir
instance IsDirPath 'ProjectLogDir
instance IsDirPath 'SdkTmpDir
instance IsDirPath ('SomePackageDir p)
instance IsDirPath ('SomePackageVersionDir p)
instance IsDirPath 'SdkPackageVersionPackageDir
instance IsDirPath 'SomePackageVersionBinDir

-- | @IsFilePath a@ means that @a@ is a type of paths for non-directory files.
class IsFilePath (a :: FilePathTypes)
instance IsFilePath 'LastCheckFile
instance IsFilePath 'LastNotifiedVsnFile
instance IsFilePath 'LastUpgradeReminderFile
instance IsFilePath 'DaBinary
instance IsFilePath 'SdkPackageChangelog
instance IsFilePath 'SdkPackageVersionMetafile
instance IsFilePath 'DaConfigFile
instance IsFilePath 'SomePackageVersionBin
instance IsFilePath 'DaBinaryDirPackageBin

-- | @DirContains a b@ means that the file or directory of path type @b@ is
-- usually contained in the directory of path type @a@, either directly or
-- in a recursive subdirectory of @a@ (i.e. indirect containment). We only
-- allow one @a@ for any given @b@, meaning that a @b@ should almost always
-- be constructed from an @a@ using 'addPathSuffix', if not supplied by the
-- user.
class IsDirPath a => DirContains a b | b -> a where
    addPathSuffix :: FilePath -> FilePathOf a -> FilePathOf b
    addPathSuffix t = FilePathOf . (</> t) . unwrapFilePathOf

instance DirContains 'InstallationDir      'DaHomeDir
instance DirContains 'DaHomeDir            'ProcDir
instance DirContains 'DaHomeDir            'SdkTmpDir
instance DirContains 'DaHomeDir            'PackagesDir
instance DirContains 'DaHomeDir            'LastCheckFile
instance DirContains 'DaHomeDir            'LastNotifiedVsnFile
instance DirContains 'DaHomeDir            'LastUpgradeReminderFile
instance DirContains 'DaHomeDir            'DaBinaryDir
instance DirContains 'DaBinaryDir          'DaBinary
instance DirContains 'DaBinaryDir          'DaBinaryDirPackageBin
instance DirContains 'DaHomeDir            'DaConfigFile
instance DirContains 'ProjectDir           'ProjectLogDir
instance DirContains 'PackagesDir          ('SomePackageDir p)
instance DirContains ('SomePackageDir p) ('SomePackageVersionDir p)
instance DirContains ('SomePackageVersionDir 'SdkPackage) 'SdkPackageChangelog
instance DirContains ('SomePackageVersionDir 'SdkPackage) 'SdkPackageVersionMetafile
instance DirContains ('SomePackageVersionDir 'SdkPackage) 'SdkPackageVersionPackageDir
instance DirContains ('SomePackageVersionDir 'OtherPackage) 'SomePackageVersionBinDir
instance DirContains 'SomePackageVersionBinDir 'SomePackageVersionBin

type FilePathOfInstallationDir         = FilePathOf 'InstallationDir
-- | File path of the DA home directory.
type FilePathOfDaHomeDir               = FilePathOf 'DaHomeDir
type FilePathOfProcDir                 = FilePathOf 'ProcDir
type FilePathOfSdkTmpDir               = FilePathOf 'SdkTmpDir
type FilePathOfPackagesDir             = FilePathOf 'PackagesDir
type FilePathOfProjectDir              = FilePathOf 'ProjectDir
type FilePathOfProjectLogDir           = FilePathOf 'ProjectLogDir
type FilePathOfLastCheckFile           = FilePathOf 'LastCheckFile
type FilePathOfLastNotifiedVsnFile     = FilePathOf 'LastNotifiedVsnFile
type FilePathOfLastUpgradeReminderFile = FilePathOf 'LastUpgradeReminderFile
-- | File path of the DA binary.
type FilePathOfDaBinary                = FilePathOf 'DaBinary
type FilePathOfDaBinaryDir             = FilePathOf 'DaBinaryDir
type FilePathOfSdkPackage              = FilePathOf ('SomePackageDir 'SdkPackage)
type FilePathOfSdkPackageVersionDir    = FilePathOf ('SomePackageVersionDir 'SdkPackage)
type FilePathOfSdkPackageVersionMetafile = FilePathOf 'SdkPackageVersionMetafile
type FilePathOfSdkPackageChangelog     = FilePathOf 'SdkPackageChangelog
type FilePathOfSomePackage                 = FilePathOf ('SomePackageDir 'OtherPackage)
type FilePathOfSomePackageVersion       = FilePathOf ('SomePackageVersionDir 'OtherPackage)
type FilePathOfSdkPackageVersionTemplatesDir = FilePathOf 'SdkPackageVersionPackageDir

type FilePathOfPackage p = FilePathOf ('SomePackageDir p)
type FilePathOfPackageVersion p = FilePathOf ('SomePackageVersionDir p)

type FilePathOfDefaultConfigFile = FilePathOf 'DaConfigFile

type FilePathOfSomePackageVersionBinDir = FilePathOf 'SomePackageVersionBinDir
type FilePathOfSomePackageVersionBin = FilePathOf 'SomePackageVersionBin
type FilePathOfDaBinaryDirPackageBin = FilePathOf 'DaBinaryDirPackageBin

-- | Convert type-safe path to text.
pathOfToText :: FilePathOf t -> Text
pathOfToText = pathToText . unwrapFilePathOf

-- | Convert type-safe path to string.
pathOfToString :: FilePathOf t -> String
pathOfToString = pathToString . unwrapFilePathOf
