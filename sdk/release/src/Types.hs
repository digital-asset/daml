-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE ConstraintKinds  #-}
module Types (
    ArtifactId,
    ArtifactType,
    CIException(..),
    Classifier,
    GitRev,
    GroupId,
    MavenAllowUnsecureTls(..),
    MavenCoords(..),
    MavenUploadConfig(..),
    MonadCI,
    IncludeDocs(..),
    IncludeTypescript(..),
    OS(..),
    PerformUpload(..),
    (#),
    dropFileName,
    groupIdString,
    pathToString,
    pathToText,
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
import qualified Data.List                            as List
import           Data.Maybe
import           Data.SemVer (Version)
import           Data.Text                            (Text)
import qualified Data.Text                            as T
import           Data.Typeable                        (Typeable)
import           Path
import           Path.Internal
import qualified System.FilePath                      as FP

type GroupId = [Text]
type ArtifactId = Text
type Classifier = Text
type ArtifactType = Text

-- Fully qualified coordinates for a Maven artifact.
data MavenCoords = MavenCoords
    { groupId :: !GroupId
    , artifactId :: !ArtifactId
    , version :: !Version
    , classifier :: Maybe ArtifactType
    , artifactType :: !ArtifactType
    } deriving Show

groupIdString :: GroupId -> String
groupIdString gid = List.intercalate "." (map T.unpack gid)

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

-- | Whether documentation should be included as well.
-- This is useful to disable if you run this via daml-sdk-head where you often
-- don’t care about documentation.
newtype IncludeDocs = IncludeDocs{includeDocs :: Bool}
    deriving (Eq, Show)

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

-- | Whether typescript packages should also be built and uploaded.
newtype IncludeTypescript = IncludeTypescript { getIncludeTypescript :: Bool}
  deriving (Eq, Show)
