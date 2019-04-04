-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

module DA.Sdk.Cli.Changelog
    ( Changelog(..)
    , Change(..)
    , Error(..)

      -- * Commands
    , changelog
    , readChange
    ) where

import qualified DA.Sdk.Cli.Conf       as Conf
import qualified DA.Sdk.Cli.Monad.Locations as L
import qualified DA.Sdk.Cli.Locations as Loc
import qualified DA.Sdk.Cli.Project    as Project
import qualified DA.Sdk.Cli.SdkVersion as SdkVersion
import           DA.Sdk.Prelude
import qualified DA.Sdk.Pretty         as P
import qualified DA.Sdk.Version        as Version
import qualified Data.Aeson.Types      as Aeson
import qualified Data.Vector           as V
import           Data.Yaml             ((.:))
import qualified Data.Yaml             as Yaml
import qualified Data.Text             as T
import           DA.Sdk.Cli.Monad.UserInteraction
import           Control.Monad.Except
import           DA.Sdk.Cli.Monad.FileSystem (MonadFS, testfile, decodeYamlFile
                                             , DecodeYamlFileFailed(..))
import           Control.Monad.Logger       (MonadLogger)

--------------------------------------------------------------------------------
-- Types
--------------------------------------------------------------------------------

data Changelog = Changelog
    { unreleased :: Maybe Change
    , changes    :: [Change]
    } deriving (Show)

data Change = Change
    { changeVersion :: Version.SemVersion
    , changeDate    :: Text
    , changeNote    :: Text
    } deriving (Show)

-- | The user made a mistake.
data UserError
    = UESDKVersionNotInstalled Version.SemVersion [Version.SemVersion]
    | UESDKLegacyVersion Version.SemVersion
    | UEVersionNotFoundInChangelog Version.SemVersion [Version.SemVersion]
     -- ^ The version could not be found in the changelog.
    | UENotInProjectDir

-- | Errors that should not happen if the user did everything right.
data InternalError
    = IEYamlParseError DecodeYamlFileFailed
      -- ^ There was an error while parsing the changelog file.
    | IEChangelogFileNotFound Loc.FilePathOfSdkPackageChangelog
      -- ^ The changelog file could not be found.
    | IECannotListSdkVersions

-- | An error is either a user error, or an internal one.
data Error
    = EUserError UserError
    | EInternalError InternalError

--------------------------------------------------------------------------------
-- Commands
--------------------------------------------------------------------------------

changelog :: (MonadLogger m, L.MonadLocations m, MonadFS m, MonadUserOutput m)
          => Maybe Conf.Project -> Maybe Version.SemVersion -> m (Either Error ())
changelog mbProj mbVersion = runExceptT $ do
    change <- ExceptT $ readChange mbProj mbVersion
    displayPretty change

readChange :: (MonadLogger m, L.MonadLocations m, MonadFS m)
           => Maybe Conf.Project -> Maybe Version.SemVersion -> m (Either Error Change)
readChange mbProj = \case
    Just version -> runExceptT $ do
        installedVersions <- withExceptT (const $ EInternalError IECannotListSdkVersions) $
                                ExceptT SdkVersion.getInstalledSdkVersions
        if version `elem` installedVersions
        then
            ExceptT $ readChangelog version
        else
            throwError
                $ EUserError
                $ UESDKVersionNotInstalled version installedVersions

    Nothing -> runExceptT $ do
        project <- withExceptT (const $ EUserError UENotInProjectDir) $
                        ExceptT $ Project.requireProject' mbProj
        let version = Conf.projectSDKVersion project
        ExceptT $ readChangelog version
  where
    -- readChangelog :: Version.SemVersion -> m (Either Error Change)
    readChangelog version
      -- SDK versions before 0.2.5 do not include a proper changelog.
      | version < Version.SemVersion 0 2 5 Nothing = do
            pure $ Left $ EUserError $ UESDKLegacyVersion version

      | otherwise = do
            sdkChangelogPath <- L.getSdkPackageChangelogPath version
            errOrChangelog <- readChangelogFile sdkChangelogPath
            case errOrChangelog of
                Left er ->
                    pure $ Left $ EInternalError er
                Right (Changelog _ c) ->
                    case filter ((== version) . changeVersion) c of
                        [] ->
                            pure
                                $ Left
                                $ EUserError
                                $ UEVersionNotFoundInChangelog version
                                    [ changeVersion ch
                                    | ch <- c
                                    ]
                        ch : _ -> pure $ Right ch

--------------------------------------------------------------------------------
-- Reading
--------------------------------------------------------------------------------

-- | Reads an sdk changelog file.
-- Hides the error, because we don't want to show exaclty what went wrong
-- to the user, but makes it possible to log it when in debug mode.
readChangelogFile :: MonadFS m => Loc.FilePathOfSdkPackageChangelog -> m (Either InternalError Changelog)
readChangelogFile filePath = do
    changelogExists <- testfile filePath
    if changelogExists
    then
        mapError <$> decodeYamlFile filePath
    else
        pure $ Left $ IEChangelogFileNotFound filePath
  where
    mapError ::
           Either DecodeYamlFileFailed Changelog
        -> Either InternalError Changelog
    mapError = \case
        Left er -> Left $ IEYamlParseError er
        Right v -> Right v

--------------------------------------------------------------------------------
-- Pretty printing
--------------------------------------------------------------------------------

prettyChange :: Change -> P.Doc ann
prettyChange (Change cVersion cDate cNote) = P.nest 2 $ P.vsep
    [ P.pretty cVersion
    , P.vsep
        [ P.label "Date" $ P.pretty cDate
         , P.t "Changelog:"
         ,  P.vcat $ P.pretty <$> T.lines cNote
        ]
    ]

prettyUserError :: UserError -> P.Doc ann
prettyUserError = \case
    UESDKVersionNotInstalled wantedVersion installedVersions -> P.sep
        [
          P.t "SDK Version "
          P.<+> P.pretty wantedVersion
          P.<+> P.t " is not installed."
        , "Installed versions:"
        , P.nest 2 $ P.hcat $ intersperse (P.t ", ") $ fmap P.pretty installedVersions
        ]

    UESDKLegacyVersion version -> P.sep
        [ P.t "No changelog found for SDK release"
        , P.pretty version
        ]

    UEVersionNotFoundInChangelog wantedVersion foundVersions ->  P.sep
        [
          P.t "SDK Version "
          P.<+> P.pretty wantedVersion
          P.<+> P.t " has no associated entries in changelog."
        , "Versions with changes in changelog are:"
        , P.nest 2 $ P.hcat $ intersperse (P.t ", ") $ fmap P.pretty foundVersions
        ]
    UENotInProjectDir -> P.t "This command needs to be run in a project directory."


prettyInternalError :: InternalError -> P.Doc ann
prettyInternalError = \case
    IEYamlParseError (DecodeYamlFileFailed _f parseError) -> P.sep
        [
          P.t "Unable to parse changelog file:"
        , P.pretty $ Yaml.prettyPrintParseException parseError
        ]

    IEChangelogFileNotFound filePath ->
        "No changelog could be found at:" P.<+> P.t (Loc.pathOfToText filePath)
    
    IECannotListSdkVersions ->
        P.t "Cannot list SDK versions."


--------------------------------------------------------------------------------
-- Instances
--------------------------------------------------------------------------------

instance Aeson.FromJSON Changelog where
    parseJSON (Aeson.Object chL) =
        Changelog <$> (chL .: "Unreleased")
                  <*> (chL .: "Released")
    parseJSON (Aeson.Array arr)  =
        Changelog <$> pure Nothing
                  <*> (V.toList <$> traverse Aeson.parseJSON arr)
    parseJSON other              =
        Aeson.typeMismatch "Changelog" other

instance Aeson.FromJSON Change where
    parseJSON = Aeson.withObject "Change" $ \v ->
        Change <$> v .: "version" <*> v .: "date" <*> v .: "note"

instance P.Pretty UserError where
    pretty = prettyUserError

instance P.Pretty InternalError where
    pretty = prettyInternalError

instance P.Pretty Change where
    pretty = prettyChange
