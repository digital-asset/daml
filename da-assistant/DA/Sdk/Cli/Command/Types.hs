-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

module DA.Sdk.Cli.Command.Types where

import           Control.Monad.Logger        (LogLevel)
import           DA.Sdk.Prelude
import           DA.Sdk.Cli.Conf.Types       (Prop (..))
import qualified DA.Sdk.Version              as V
import qualified DA.Sdk.Cli.Repository.Types as Rty

data SubscriptionOp = OpSubscribe | OpUnsubscribe deriving (Eq)

data Command = Command
  { commandConfigPath  :: Maybe FilePath
  , commandConfigLogLevel :: LogLevel
  , commandConfigIsScript :: Bool
  , commandConfigTermWidth :: Maybe Int
  , commandAction      :: Action
  } deriving (Show, Eq)

commandConfigProps :: Command -> [Prop]
commandConfigProps Command {..} =
  [ PropLogLevel commandConfigLogLevel
  , PropIsScript commandConfigIsScript
  , PropTermWidth commandConfigTermWidth
  ]

data Action
  = Primitive PrimitiveAction
  | Normal NormalAction
  deriving (Show, Eq)

data PrimitiveAction
  = ShowVersion
  | DoSetup
  | ShowConfigHelp
  | Test TestAction
  deriving (Show, Eq)

data TestAction
    = ParseSDKYaml FilePath
    | DownloadDocs Text Text FilePath FilePath
    deriving (Show, Eq)

data SdkTagsAction
    = SdkTagsActionList (Maybe V.SemVersion)
    | SdkTagsActionAdd V.SemVersion [Text]
    | SdkTagsActionRemove V.SemVersion [Text]
    | SdkTagsActionSync FilePath
    deriving (Show, Eq)

newtype SdkAction
    = SdkActionTags SdkTagsAction
    deriving (Show, Eq)

-- | Do you want the path to the package, or to the executable?
data PackagePathQuery
    = PPQExecutable
    | PPQPackage
    deriving (Show, Eq)

-- | Known Services that can be started and stopped
data Service
    = SandboxService
    | NavigatorServices
    | All
    deriving (Show, Eq, Ord)

data SdkUninstallAction
    = UninstallAll
    | Uninstall [V.SemVersion]
    deriving (Show, Eq)

data NormalAction
  = ShowStatus
  | DisplayUpdateChannelInfo
  | ShowDocs
  | Start Service
  | Stop Service
  | Restart Service
  | SdkUpgrade
  | SdkList
  | SdkUse V.SemVersion
  | SdkUseExperimental V.SemVersion
  | SdkUninstall SdkUninstallAction
  | CreateProject (Maybe TemplateArg) FilePath
  | TemplateAdd TemplateArg (Maybe FilePath)
  | TemplatePublish (Rty.NameSpace, Rty.TemplateName, Rty.ReleaseLine)
    -- ^ @TemplatePublish contains a subject (Bintray location), a repository,
    -- a tuple of (name, version) and a list of attributes
  | TemplateList TemplateListFormat (Maybe Rty.TemplateType)
  | TemplateInfo (Rty.NameSpace, Rty.TemplateName, Rty.ReleaseLine)
  | SendFeedback
  | Studio
  | Migrate
  | Navigator
  | Sandbox
  | Compile
  | ConfigGet (Maybe Text)
  | ConfigSet Bool Text Text
    -- ^ @ConfigSet isProjectSpecific key val@
  | ShowPath (Maybe Text) PackagePathQuery
    -- ^ @ShowPath mbPackageName executablePath@
    -- Show the path to a package or all of them if no package has been passed.
    -- If @executablePath@ has been passed, show the path to the executable
    -- instead of the package surrounding it.
  | RunExec (Maybe (Text, [Text]))
  | Changelog (Maybe V.SemVersion)
  | TestTemplates
  | Sdk SdkAction
    -- ^ Hidden command used for SDK management.
  | Subscribe Rty.NameSpace
  | Unsubscribe (Maybe Rty.NameSpace)
  | FetchPackage Rty.FetchArg (Maybe FilePath) Bool
  deriving (Show, Eq)

data TemplateArg
  = Qualified Rty.NameSpace Rty.TemplateName -- FIXME(FM): Rename to NameSpaced?
  | BuiltIn   Text -- FIXME(FM): Text is suspicious, use TemplateName?
  | Malformed Text
  deriving (Show, Eq) -- FIXME(FM): Add third constructor to include release line?

templateArgToText :: TemplateArg -> Text
templateArgToText (Qualified ns tn) =
  Rty.unwrapNameSpace ns <> "/" <> Rty.unwrapTemplateName tn
templateArgToText (BuiltIn t) = t
templateArgToText (Malformed t) = t

isMalformed :: TemplateArg -> Bool
isMalformed (Malformed _) = True
isMalformed _ = False

data TemplateListFormat = TemplateListAsTuples
                        | TemplateListAsTable
                        deriving (Show, Eq)
