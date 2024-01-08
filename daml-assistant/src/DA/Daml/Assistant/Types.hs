-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE UndecidableInstances #-}

module DA.Daml.Assistant.Types
    ( module DA.Daml.Assistant.Types
    , module DA.Daml.Project.Types
    , YesNoAuto (..)
    , Text, pack, unpack -- convenient re-exports
    ) where

import DA.Daml.Project.Types
import Data.Text (Text, pack, unpack)
import Options.Applicative.Extended (YesNoAuto (..))
import Data.Functor.Identity
import Data.Maybe (isJust)

data EnvF f = Env
    { envDamlPath      :: DamlPath
    , envCachePath :: CachePath
    , envDamlAssistantPath :: DamlAssistantPath
    , envDamlAssistantSdkVersion :: Maybe DamlAssistantSdkVersion
    , envProjectPath   :: Maybe ProjectPath
    , envSdkPath       :: Maybe SdkPath
    , envSdkVersion    :: Maybe ReleaseVersion
    , envFreshStableSdkVersionForCheck :: f (Maybe ReleaseVersion)
    }

deriving instance Eq (f (Maybe ReleaseVersion)) => Eq (EnvF f)
deriving instance Show (f (Maybe ReleaseVersion)) => Show (EnvF f)

type Env = EnvF IO

forceEnv :: Monad m => EnvF m -> m (EnvF Identity)
forceEnv Env{..} = do
  envFreshStableSdkVersionForCheck <- fmap Identity envFreshStableSdkVersionForCheck
  pure Env{..}

data BuiltinCommand
    = Version VersionOptions
    | Exec String [String]
    | Install InstallOptions
    | Uninstall UnresolvedReleaseVersion
    deriving (Eq, Show)

newtype LookForProjectPath = LookForProjectPath
    { unLookForProjectPath :: Bool }

data Command
    = Builtin BuiltinCommand
    | Dispatch SdkCommandInfo UserCommandArgs
    deriving (Eq, Show)

newtype UserCommandArgs = UserCommandArgs
    { unwrapUserCommandArgs :: [String]
    } deriving (Eq, Show)

-- | Command-line options for daml version command.
data VersionOptions = VersionOptions
    { vAll :: Bool -- ^ show all versions (stable + snapshot)
    , vSnapshots :: Bool -- ^ show all snapshot versions
    , vAssistant :: Bool -- ^ show assistant version
    , vForceRefresh :: Bool -- ^ force refresh available versions, don't use 1-day cache
    } deriving (Eq, Show)

-- | Command-line options for daml install command.
data InstallOptions = InstallOptions
    { iTargetM :: Maybe RawInstallTarget -- ^ version to install
    , iSnapshots :: Bool -- ^ include snapshots for latest target
    , iAssistant :: InstallAssistant -- ^ install the assistant
    , iActivate :: ActivateInstall -- ^ install the assistant if true (deprecated, delete with 0.14.x)
    , iForce :: ForceInstall -- ^ force reinstall if already installed
    , iQuiet :: QuietInstall -- ^ don't print messages
    , iSetPath :: SetPath -- ^ set the user's PATH (on Windows)
    , iBashCompletions :: BashCompletions -- ^ install bash completions for the daml assistant
    , iZshCompletions :: ZshCompletions -- ^ install Zsh completions for the daml assistant
    , iInstallWithInternalVersion :: InstallWithInternalVersion -- ^ install using the internal version of the tarball
    , iInstallWithCustomVersion :: InstallWithCustomVersion -- ^ install using the custom version
    } deriving (Eq, Show)

newtype InstallWithInternalVersion = InstallWithInternalVersion { unInstallWithInternalVersion :: Bool } deriving (Eq, Show)
newtype InstallWithCustomVersion = InstallWithCustomVersion { unInstallWithCustomVersion :: Maybe String } deriving (Eq, Show)

newtype RawInstallTarget = RawInstallTarget String deriving (Eq, Show)
newtype ForceInstall = ForceInstall { unForceInstall :: Bool } deriving (Eq, Show)
newtype QuietInstall = QuietInstall { unQuietInstall :: Bool } deriving (Eq, Show)
newtype ActivateInstall = ActivateInstall { unActivateInstall :: Bool } deriving (Eq, Show)
newtype SetPath = SetPath {unwrapSetPath :: YesNoAuto} deriving (Eq, Show)
newtype InstallAssistant = InstallAssistant { unwrapInstallAssistant :: YesNoAuto } deriving (Eq, Show)
newtype BashCompletions = BashCompletions { unwrapBashCompletions :: YesNoAuto } deriving (Eq, Show)
newtype ZshCompletions = ZshCompletions { unwrapZshCompletions :: YesNoAuto } deriving (Eq, Show)

bothInstallWithVersion :: InstallOptions -> Bool
bothInstallWithVersion InstallOptions{..} =
    unInstallWithInternalVersion iInstallWithInternalVersion &&
        isJust (unInstallWithCustomVersion iInstallWithCustomVersion)
