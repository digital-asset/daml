-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}

module DA.Sdk.Cli.SelfUpgrade.Types
    ( NameSpace (..)
    , Conf (..)
    , Project (..)
    , RepositoryURLs (..)
    , UpdateSetting (..)
    , UpdateChoice (..)
    , UpdateDecisionHandle (..)
    , UpdateChannel (..)
    , UpdateChannelReason (..)
    , UpdateChannelWithReason (..)
    , SelfUpgradeError (..)
    , parseUpdateSetting
    ) where

import           DA.Sdk.Cli.Conf.Types
import           DA.Sdk.Prelude
import           DA.Sdk.Version                (BuildVersion)
import qualified Data.Text                     as T
import           Control.Exception.Safe        (Exception (..), Typeable)
import qualified DA.Sdk.Cli.Repository.Types   as Repo
import qualified DA.Sdk.Pretty                 as P
import           DA.Sdk.Cli.Monad.UserInteraction
import qualified Data.Text.Extended            as T
import           DA.Sdk.Cli.Monad.FileSystem

-- | Custom exception type for CLI tool self-upgrades
data SelfUpgradeError
    = Timeout Int
    | BadLocalVersion Text
    | UnknownOS Text
    | SetConfigError Text
    | RepoError Repo.Error
    | MigrationError ConfigMigrationError
    | UserApprovalError ReadInputLineFailed
    | LastCheckFileError TouchFailed
    | CannotSaveNotifTime WriteFileFailed
    | CannotSaveLastNotifVsn WriteFileFailed
    | CannotCreateTmpDir WithSysTempDirectoryFailed
    | CannotReadLastNotifVsn ReadFileUtf8Failed
    | CannotCheckInterval ReadFileUtf8Failed
    deriving (Typeable, Show)
  
instance Exception SelfUpgradeError

data UpdateDecisionHandle m = UpdateDecisionHandle
    { getUpdateSetting :: m UpdateSetting
    , setUpdateSetting :: UpdateSetting -> m (Either SelfUpgradeError ())
    , getCurrentVsn :: m (Either SelfUpgradeError BuildVersion)
    , getLatestAvailVsn :: m (Either SelfUpgradeError BuildVersion)
    , getLastNotifiedVsn :: m (Either SelfUpgradeError BuildVersion)
    , setLastNotifiedVsn :: BuildVersion -> m (Either SelfUpgradeError ())
    , saveNotificationTime :: m (Either SelfUpgradeError ())
    , checkNotificationInterval :: m (Either SelfUpgradeError Bool)
    , presentVsnInfo :: BuildVersion -> m ()
    , askUserForApproval :: BuildVersion -> m (Either SelfUpgradeError UpdateChoice)
    }

-- | Update channel of a package.
data UpdateChannel = Production | AltUpdateChannel Text deriving (Eq)
data UpdateChannelReason = ReasonDaEmail | ReasonDefault | ReasonConfigSet deriving (Eq, Show)
data UpdateChannelWithReason = UpdateChannelWithReason
  { getChannel :: UpdateChannel
  , _getReason :: UpdateChannelReason
  } deriving (Eq, Show)

instance Show UpdateChannel where
  show Production = "production"
  show (AltUpdateChannel c) = T.unpack c

instance P.Pretty SelfUpgradeError where
  pretty = \case
    Timeout _t ->
        P.t "Self upgrade timed out"
    BadLocalVersion v ->
      P.t ("Self upgrade: can't parse current, local SDK version (" <> v <> ")")
    UnknownOS os ->
      P.t ("Self upgrade: your operating system is not compatible with the SDK, unknwon OS: " <> os)
    RepoError repoErr ->
      P.pretty repoErr
    SetConfigError setConfErr ->
      P.t ("Self upgrade: There was a problem when trying to set the update-setting parameter in the config: "
                    <> setConfErr)
    MigrationError _err -> -- TODO (GH): Print 'err' as well.
      P.t "Self upgrade: Failed to migrate the config file."
    UserApprovalError (ReadInputLineFailed ioErr) -> -- TODO (GH): Print 'err' as well.
      P.t ("Self upgrade: Failed to migrate the config file." <> T.show ioErr)
    LastCheckFileError (TouchFailed _f fErr) ->
      P.t $ "Self upgrade: Failed to create the last check file: " <> T.show fErr
    CannotSaveNotifTime _ ->
      P.t "Self upgrade: Unable to save notification time."
    CannotSaveLastNotifVsn _ ->
      P.t "Self upgrade: Unable to save last version the user was notified about."
    CannotCreateTmpDir _ ->
      P.t "Self upgrade: Cannot create temporary directory."
    CannotReadLastNotifVsn _ ->
      P.t "Self upgrade: Unable to read last version the user was notified about."
    CannotCheckInterval _ ->
      P.t "Self upgrade: Cannot check whether the notification interval has passed."