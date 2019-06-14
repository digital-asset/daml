-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE ConstraintKinds  #-}
{-# LANGUAGE OverloadedStrings #-}
module Types (
    AllArtifacts(..),
    ArtifactId,
    ArtifactType,
    CIException(..),
    Classifier,
    BintrayPackage(..),
    GitRev,
    GroupId,
    MavenAllowUnsecureTls(..),
    MavenCoords(..),
    MavenUpload(..),
    MavenUploadConfig(..),
    MonadCI,
    OS(..),
    PerformUpload(..),
    PlatformDependent(..),
    TextVersion,
    Version(..),
    VersionChange(..),
    (#),
    dropFileName,
    parseVersion,
    pathToString,
    pathToText,
    renderVersion,
    throwIO,
    tshow,
  ) where

import qualified Control.Concurrent.Async.Lifted.Safe as Async
import qualified Control.Exception                    as E
import           Control.Monad.Catch                  (MonadMask)
import           Control.Monad.IO.Class               (MonadIO, liftIO)
import           Control.Monad.IO.Unlift              (MonadUnliftIO)
import           Control.Monad.Logger
import           Control.Monad.Trans.Control          (MonadBaseControl)
import           Data.Aeson
import           Data.Maybe
import           Data.Text                            (Text)
import qualified Data.Text                            as T
import           Data.Typeable                        (Typeable)
import           Path
import           Path.Internal
import qualified System.FilePath                      as FP
import           Control.Monad (guard, (>=>))
import           Safe (readMay)

data BintrayPackage
  = PkgSdkComponents
  | PkgSdk
  deriving (Eq, Ord, Show, Read, Enum, Bounded)

instance FromJSON BintrayPackage where
    parseJSON = withText "BintrayPackage" $ \t ->
        case t of
            "sdk-components" -> pure PkgSdkComponents
            "sdk" -> pure PkgSdk
            _ -> fail $ "Unknown bintray package " <> show t


type TextVersion = Text
type GroupId = [Text]
type ArtifactId = Text
type Classifier = Text
type ArtifactType = Text

-- Fully qualified coordinates for a Maven artifact.
data MavenCoords = MavenCoords
    { groupId :: !GroupId
    , artifactId :: !ArtifactId
    , version :: !TextVersion
    , classifier :: Maybe ArtifactType
    , artifactType :: !ArtifactType
    } deriving Show

newtype PlatformDependent = PlatformDependent{getPlatformDependent :: Bool}
    deriving (Eq, Show, FromJSON)

newtype MavenUpload = MavenUpload { getMavenUpload :: Bool }
    deriving (Eq, Show, FromJSON)

-- | If this is True, we produce all artifacts even platform independent artifacts on MacOS.
-- This is useful for testing purposes.
newtype AllArtifacts = AllArtifacts Bool
    deriving (Eq, Show)

-- execution
type MonadCI m = (MonadIO m, MonadMask m, MonadLogger m,
                  MonadUnliftIO m, MonadBaseControl IO m, Async.Forall (Async.Pure m))


data CIException = CIException Text
  deriving (Show, Typeable)
instance E.Exception CIException


throwIO :: (MonadIO m, E.Exception e) => e -> m a
throwIO = liftIO . E.throwIO

-- prelude
-- --------------------------------------------------------------------

tshow :: Show a => a -> Text
tshow = T.pack . show


pathToString :: Path a b -> String
pathToString = T.unpack . pathToText


pathToText :: Path a b -> Text
pathToText = T.pack . toFilePath


(#) :: Text -> Text -> Text
(#) = (<>)

dropFileName :: Path a b -> Path a Dir
dropFileName (Path x) = Path (FP.dropFileName x)


-- os
-- --------------------------------------------------------------------

data OS =
    Linux
  | MacOS
  deriving (Eq, Show, Read)


type GitRev = Text

newtype PerformUpload = PerformUpload{getPerformUpload :: Bool}
    deriving (Eq, Show)

-- versions
-- --------------------------------------------------------------------

--
-- | Version number bumping is fully automated using the @VERSION@
--   files that can be found in the root directory of the repo.
data Version = Version
  { versionMajor :: Int
  , versionMinor :: Int
  , versionPatch :: Int
  } deriving (Eq, Ord, Show, Read)

data VersionChange =
    VCPatch
  | VCMinor
  | VCMajor
  deriving (Eq, Ord, Show, Read)

parseVersion :: Text -> Maybe Version
parseVersion (T.strip -> txt) = do
  let positive n = n <$ guard (n >= 0)
  [versionMajor, versionMinor, versionPatch] <-
    traverse ((readMay >=> positive) . T.unpack) (T.split (=='.') txt)
  return Version{..}

renderVersion :: Version -> Text
renderVersion (Version maj min_ patch) = T.intercalate "." [tshow maj, tshow min_, tshow patch]

newtype MavenAllowUnsecureTls = MavenAllowUnsecureTls { getAllowUnsecureTls :: Bool }
    deriving (Eq, Show, FromJSON)

data MavenUploadConfig = MavenUploadConfig
  { mucUrl :: !Text
  , mucUser :: !Text
  , mucPassword :: !Text
  , mucAllowUnsecureTls :: !MavenAllowUnsecureTls
-- ^^ For testing with an Artifactory (or similar) instance using a self-signed SSL certificate.
-- This flag should NEVER be set in production.
  , mucSigningKey :: String
} deriving (Eq, Show)

instance FromJSON MavenUploadConfig where
    parseJSON = withObject "MavenUploadConfig" $ \o -> MavenUploadConfig
        <$> o .: "url"
        <*> o .: "user"
        <*> o .: "password"
        <*> (fromMaybe (MavenAllowUnsecureTls False) <$> o .:? "allowUnsecureTls")
        <*> o .: "signingKey"
