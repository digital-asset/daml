-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

-- | This module defines a function that will run subcommands in the background.
module DA.Sdk.Cli.Job.Types
  ( Job (..)
  , Navigator (..)
  , NavigatorUrl (..)
  , Sandbox (..)
  , Process (..)
  , ProjectPath (..)
  , DarDependencyFile(..)
  , DarDependency(..)
  , UserMessage(..)
  , describeJob
  , jobName
  ) where

import           DA.Sdk.Prelude
import qualified DA.Sdk.Pretty    as P
import           Data.Aeson.Types (defaultOptions, genericParseJSON, genericToEncoding)
import qualified Data.Aeson.Types as Aeson
import qualified Data.Yaml        as Y
import           Data.Yaml        ((.:))
import qualified Data.Text.Extended as T
import           GHC.Generics
import           Control.Exception

newtype ProjectPath = ProjectPath { fromProjectPath :: FilePath }
  deriving (Show, Eq, Generic)

instance Y.ToJSON ProjectPath where
  toJSON (ProjectPath p) = Y.toJSON $ pathToText p

instance Y.FromJSON ProjectPath where
  parseJSON v = Aeson.withText "Invalid project" (\t -> return (ProjectPath (textToPath t))) v

data Job
  = JobNavigator Navigator
  | JobSandbox Sandbox
  | JobStudio Text
  | JobWebsite Text
  | JobResource Text
  deriving (Show, Eq, Generic)

instance Y.ToJSON Job where
  toEncoding = genericToEncoding defaultOptions
instance Y.FromJSON Job where
  parseJSON = genericParseJSON defaultOptions

-- | The job name is used to generate a name for the PID file and is only
-- relevant for daemon processes. We currently assume there can only be one
-- daemon per type per project.
jobName :: Job -> Text
jobName (JobNavigator _) = "navigator"
jobName (JobSandbox s)   = "sandbox" <> (if sandboxUseJavaVersion s then "-java" else "")
jobName (JobStudio _)    = "studio"
jobName (JobWebsite _)   = "web"
jobName (JobResource _)  = "resource"

describeJob :: Job -> P.Doc ann
describeJob (JobNavigator Navigator {..}) = P.fillSep
    [ P.reflow "Navigator web app using"
    , P.t $ prettyNavUrl navigatorUrl
    , P.reflow "and binding to port"
    , P.pretty navigatorPort
    ]
describeJob (JobSandbox Sandbox {..}) = P.fillSep
    [ P.reflow "Sandbox ledger server"
    , P.t sandboxDAMLFile
    , maybe
        (P.reflow "with no scenario")
        (\s -> P.fillSep [P.reflow "with scenario", P.pretty s])
        sandboxScenario
    , P.reflow "and binding to port"
    , P.pretty sandboxPort
    ]
describeJob (JobStudio path) =
    P.sep ["DAML Studio on the path", P.pretty path]
describeJob (JobWebsite url) =
    P.sep ["web browser on URL", P.pretty url]
describeJob (JobResource name) = P.fillSep
    [ P.t "resource"
    , P.dquotes $ P.t name
    , P.t "using default OS application"
    ]

data Navigator = Navigator
  { navigatorPort       :: Int
  , navigatorUrl        :: NavigatorUrl
  , navigatorConfigFile :: Text
  -- TODO: Add ability to pass in arbitrary extra CLI arguments
  } deriving (Show, Eq, Generic)

data NavigatorUrl = OldApiUrlWithPort Text
                  | NewApiUrlAndPort Text Int
                  deriving (Show, Eq, Generic)
prettyNavUrl :: NavigatorUrl -> Text
prettyNavUrl (OldApiUrlWithPort urlWPort) = urlWPort
prettyNavUrl (NewApiUrlAndPort url port)  = url <> " (" <> T.show port <> ")"

instance Y.ToJSON Navigator where
  toEncoding = genericToEncoding defaultOptions
instance Y.ToJSON NavigatorUrl where
  toEncoding = genericToEncoding defaultOptions
instance Y.FromJSON Navigator where
  parseJSON = genericParseJSON defaultOptions

data Sandbox = Sandbox
  { sandboxUseJavaVersion :: Bool
  , sandboxProjectName    :: Text
  , sandboxProjectRoot    :: Text
  , sandboxDAMLFile       :: Text -- TODO: Change to internal Path newtype I think
  , sandboxDarDeps        :: [Text]
  , sandboxPort           :: Int
  , sandboxScenario       :: Maybe Text
  -- TODO: Add ability to pass in arbitrary extra CLI arguments
  } deriving (Show, Eq, Generic)

data UserMessage = UserError Text
                 | UserInfo Text
    deriving (Show, Eq, Generic)

instance Exception UserMessage

instance P.Pretty UserMessage where
  pretty (UserError errTxt) = P.sep [P.t "Error:", P.pretty errTxt]
  pretty (UserInfo infoTxt) = P.pretty infoTxt

instance Y.ToJSON Sandbox where
  toEncoding = genericToEncoding defaultOptions
instance Y.FromJSON Sandbox where
  parseJSON = genericParseJSON defaultOptions
instance Y.FromJSON NavigatorUrl where
  parseJSON = genericParseJSON defaultOptions

data Process = Process
  { processPid         :: Int -- TODO: newtype ProcessID, needs ToJSON instance
  , processProjectPath :: ProjectPath
  , processJob         :: Job
  } deriving (Show, Eq, Generic)

data DarDependencyFile = DarDependencyFile
  { darDepFileDependencies :: [DarDependency]
  } deriving (Show, Eq, Generic)

data DarDependency = DarDependency
  { darDepsFile         :: FilePath
  , darDepsName         :: Text
  , darDepsDependencies :: [DarDependency]
  } deriving (Show, Eq, Generic)

instance Y.ToJSON Process where
  toEncoding = genericToEncoding defaultOptions
instance Y.FromJSON Process where
  parseJSON = genericParseJSON defaultOptions
instance Y.FromJSON DarDependencyFile where
  parseJSON (Y.Object v) =
    DarDependencyFile <$> v .: "dependencies"
  parseJSON invalid = Aeson.typeMismatch "DarDependencyFile" invalid
instance Y.FromJSON DarDependency where
  parseJSON (Y.Object v) = do
    f <- v .: "file"
    name <- v .: "name"
    deps <- v .: "dependencies"
    return $ DarDependency (textToPath f) name deps
  parseJSON invalid = Aeson.typeMismatch "DarDependency" invalid
