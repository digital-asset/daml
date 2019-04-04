-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}
{-# LANGUAGE TemplateHaskell   #-}

module DA.Sdk.Cli.Conf.Types where

import           Control.Lens                  (makePrisms)
import qualified DA.Sdk.Cli.Message            as M
import           Control.Monad.Logger          (LogLevel (..))
import           DA.Sdk.Cli.Credentials        (Credentials (..))
import           DA.Sdk.Prelude
import           DA.Sdk.Version                (SemVersion (..))
import           Servant.Client                (BaseUrl)
import qualified Data.Yaml                     as Yaml
import qualified Data.Aeson.Types              as Aeson
import qualified Data.Text                     as T
import qualified Text.Read                     as TR
import           DA.Sdk.Cli.Monad.FileSystem.Types

newtype NameSpace = NameSpace
    { unwrapNameSpace :: Text
    } deriving (Eq, Ord, Show)

-- | Fully specified configuration properties; expected to be the result of
-- merging defaults with various sources of user config.
data Conf = Conf
    { confVersion                :: Int
    , confLogLevel               :: LogLevel
    , confIsScript               :: Bool
    , confUserEmail              :: Maybe Text
    , confCredentials            :: Credentials
    , confRepositoryURLs         :: RepositoryURLs
    , confDefaultSDKVersion      :: Maybe SemVersion
    , confSDKIncludedTags        :: [Text]
    , confSDKExcludedTags        :: [Text]
    , confTermWidth              :: Maybe Int
    , confUpgradeIntervalSeconds :: Int
    , confUpdateChannel          :: Maybe Text
    , confNameSpaces             :: [NameSpace]
    , confUpdate                 :: UpdateSetting
    , confOutputPath             :: Maybe FilePath
    } deriving (Show, Eq)

-- | Fully specified project configuration.
data Project = Project
    { projectPath            :: FilePath
    , projectProjectName     :: Text
    , projectSDKVersion      :: SemVersion
    , projectDAMLSource      :: Text -- TODO: to FilePath + JSON instances
    , projectDAMLScenario    :: Maybe Text
    , projectParties         :: [Text]
    , projectDarDependencies :: [FilePath]
    } deriving (Show, Eq)

-- | Bintray repository URLs, it contains the API URL and the DA download URL
data RepositoryURLs = RepositoryURLs
    { repositoryAPIURL      :: BaseUrl
    , repositoryDownloadURL :: BaseUrl
    } deriving (Show, Eq)

data UpdateSetting = Always | Never | RemindLater | RemindNext deriving (Eq, Read, Show)
data UpdateChoice  = Remembered UpdateSetting | YesAndAskNext

instance Yaml.ToJSON NameSpace where
    toJSON (NameSpace n) = Yaml.toJSON n

instance Yaml.FromJSON NameSpace where
    parseJSON = Yaml.withText "NameSpace" (pure . NameSpace)

instance Yaml.ToJSON UpdateSetting where
    toJSON updS = Yaml.toJSON $ show updS

instance Yaml.FromJSON UpdateSetting where
    parseJSON y@(Yaml.String v) =
        either (\_l -> Aeson.typeMismatch "UpdateSetting" y)
               (\r  -> pure r)
            $ parseUpdateSetting v
    parseJSON invalid = Aeson.typeMismatch "UpdateSetting" invalid

parseUpdateSetting :: Text -> Either Text UpdateSetting
parseUpdateSetting v =
    case TR.readMaybe $ T.unpack v of
      Just updS -> Right updS
      Nothing   -> Left "UpdateSetting"

data ConfigMigrationError = MigrationWriteFailed ConfigWriteFailed
                          | MigrationEnsureModeFailed SetFileModeFailed
                          | ConfigDirListingError LsFailed
                          | MigrationBackupFailed CpFailed
                          deriving (Show)

data ConfigUpdateError = UpdateBackupFailed CpFailed
                       | UpdateConfigWriteFailed ConfigWriteFailed
                       | ConfigUpdateDecodeError DecodeYamlFileFailed
                       deriving (Show)

data ConfigWriteFailed = ConfigWriteSetModeFailed SetFileModeFailed
                    | ConfigWriteFailed WriteFileFailed
                    deriving (Show)


-- | An individual configuration property. These are usually put into lists,
-- merged, and then converted into a fully specified 'Conf'.
data Prop
    = PropConfVersion Int
    | PropLogLevel LogLevel
    | PropIsScript Bool
    | PropUserEmail Text
    | PropProjectName Text
    | PropProjectSDKVersion SemVersion
    | PropProjectParties [Text]
    | PropDAMLSource Text
    | PropDAMLScenario (Maybe Text)
    | PropBintrayUsername Text
    | PropBintrayKey Text
    | PropBintrayAPIURL BaseUrl
    | PropBintrayDownloadURL BaseUrl
    | PropSDKDefaultVersion (Maybe SemVersion)
    | PropSDKIncludedTags [Text]
    | PropSDKExcludedTags [Text]
    | PropTermWidth (Maybe Int)
    -- | seconds that need to pass between two updates
    | PropUpgradeIntervalSeconds Int
    | PropUpdateChannel (Maybe Text)
    | PropNameSpaces [NameSpace]
    | PropSelfUpdate UpdateSetting
    | PropOutputPath (Maybe FilePath)
    deriving (Show, Eq)

makePrisms ''Prop

-- | Reify config property names so we can talk about missing ones in error
-- reporting and when mapping to names in config files and similar.
data Name
    = NameConfVersion
    | NameLogLevel
    | NameHomePath
    | NameIsScript
    | NameUpdateChannel
    | NameUserEmail
    | NameProjectName
    | NameProjectSDKVersion
    | NameProjectParties
    | NameDAMLSource
    | NameDAMLScenario
    | NameBintrayUsername
    | NameBintrayKey
    | NameBintrayAPIURL
    | NameBintrayDownloadURL
    | NameSDKDefaultVersion
    | NameSDKIncludedTags
    | NameSDKExcludedTags
    | NameTermWidth
    | NameUpgradeIntervalSeconds
    | NameNameSpaces
    | NameSelfUpdate
    | NameOutputPath
    deriving (Show, Eq, Bounded, Enum)

newtype Props = Props { fromProps :: [Prop] }
    deriving (Show, Eq)

data NameInfo = NameInfo
    { infoKeys :: [Text]
    , infoHelp :: Maybe Text
    -- ^ Help for config. Only has help if it is a valid config file property.
    } deriving (Show, Eq)

data ConfError
    = ConfErrorMissingFile FilePath
    | ConfErrorMalformedFile FilePath
    | ConfErrorMissingProperties [Name]

data ProjectError
    = ProjectErrorMalformedYAMLFile FilePath
    | ProjectErrorMissingProps [Name]
    | ProjectErrorDarDepMalformedFile FilePath Text
    | ProjectErrorDarDepFilePathClash Text FilePath FilePath
    | ProjectErrorCannotPwd
  deriving (Show, Eq)

data ProjectWarning
    = ProjectWarningNoProjectFound
  deriving (Show, Eq)

type ProjectMessage = M.Message ProjectError ProjectWarning
