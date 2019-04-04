-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell   #-}

module DA.Sdk.Cli.Metadata
    ( Packaging(..)
    , Package(..)
    , Group(..)
    , Metadata(..)
    , TarExt(..)

    , mVersion, mGroups
    , gVersion, gGroupId, gPackages
    , pMain, pPackaging, pTemplate, pDoc

      -- * Utils
    , findPackage
    , allPackages
    , packagingToFileEnding

    , ReadMetadataError(..)
    , readSdkMetadata
    ) where

import           Control.Lens (makeLenses)
import           DA.Sdk.Prelude
import qualified DA.Sdk.Version as Version
import qualified Data.Aeson.Types as Aeson
import qualified Data.Map.Strict as MS
import qualified Data.HashMap.Lazy as HML
import qualified Data.Text as T
import           Data.Yaml ((.:), (.:?))
import qualified Data.Yaml as Yaml
import           DA.Sdk.Cli.Monad.FileSystem as FS
import qualified DA.Sdk.Cli.Locations as L
import qualified DA.Sdk.Pretty as P
import qualified Data.Text.Extended as TE

data TarExt
    = TarGz
    | Tgz
    deriving (Show, Eq)

data Packaging
    = PackagingTarGz TarExt
      -- ^ A ".tar.gz" file that will have to be extracted.
    | PackagingJar
      -- ^ A java ".jar" file.
    | PackagingExtension Text
      -- ^ A file with a custom extension.
    deriving (Show, Eq)

type GroupId = [Text]

-- | A release group is a set of artefacts that share the group-id
-- and version. Their primary purpose is to deduplicate version
-- information.
data Group = Group
    { _gVersion  :: !Version.SemVersion
    , _gGroupId  :: !GroupId
    , _gPackages :: !(MS.Map Text Package)
    } deriving (Show, Eq)

-- | Package in a release group
data Package = Package
    { _pPackaging :: !Packaging
      -- ^ The packaging kind. Used for correctly referring to the artifact
      -- and for identifying how to handle the package in generic commands.
    , _pMain      :: !(Maybe FilePath)
      -- ^ Relative path to the main entry point of the package,
      -- e.g. a binary.
    , _pDoc       :: !Bool
      -- ^ True if the package is a documentation package. Documentation
      -- packages are not used by the SDK assistant itself, but only when
      -- assembling the SDK documentation. The documentation packages
      -- are part of the metadata to provide a single source of truth for
      -- version information by making them part of release groups.
    , _pTemplate  :: !Bool
      -- ^ True if the package is a template package. Template packages
      -- are installable into projects using the 'template' subcommands.
    } deriving (Show, Eq)

data Metadata = Metadata
    { _mVersion :: !Int
    , _mGroups  :: !(MS.Map Text Group)
    } deriving (Show, Eq)

--------------------------------------------------------------------------------
-- Utilities
--------------------------------------------------------------------------------
-- | Convert a packaging format to the corresponding file ending.
packagingToFileEnding :: Packaging -> Text
packagingToFileEnding (PackagingTarGz TarGz) = ".tar.gz"
packagingToFileEnding (PackagingTarGz Tgz) = ".tgz"
packagingToFileEnding PackagingJar = ".jar"
packagingToFileEnding (PackagingExtension e) = "." <> e

allPackages :: Metadata -> MS.Map Text (Group, Package)
allPackages = foldl' (\a g -> a `MS.union` MS.map (g,) (_gPackages g)) MS.empty . _mGroups

findPackage :: Metadata -> Text -> Maybe (Group, Package)
findPackage metadata packageName = MS.lookup packageName $ allPackages metadata

--------------------------------------------------------------------------------
-- Instances
--------------------------------------------------------------------------------

instance Yaml.FromJSON Packaging where
    parseJSON (Yaml.String p) =
        pure $ case p of
            "tar.gz" -> PackagingTarGz TarGz
            "tgz" -> PackagingTarGz Tgz
            "jar" -> PackagingJar
            _ -> PackagingExtension p
    parseJSON invalid = Aeson.typeMismatch "Packaging" invalid

instance Yaml.FromJSON Package where
    parseJSON (Yaml.Object o) = do
        packaging <- o .: "packaging"
        mainPath <- fmap textToPath <$> o .:? "main"
        doc <- fromMaybe False <$> o .:? "doc"
        template <- fromMaybe False <$> o .:? "template"
        pure $ Package packaging mainPath doc template
    parseJSON invalid = Aeson.typeMismatch "Package" invalid

instance Yaml.FromJSON Group where
    parseJSON (Yaml.Object o) = do
        version <- o .: "version"
        groupId <- T.splitOn "." <$> o .: "group-id"
        packages <- o .: "packages"
        pure $ Group version groupId packages
    parseJSON invalid = Aeson.typeMismatch "Group" invalid

instance Yaml.FromJSON Metadata where
    parseJSON o@(Yaml.Object v) = do
      version <- fromMaybe (0 :: Int) <$> v .:? "metadata_version"
      case version of
        0 -> parseVersion0 v
        1 -> parseVersion1 v
        _ -> Aeson.typeMismatch "Metadata: Unknown version" o
    parseJSON invalid = Aeson.typeMismatch "Metadata" invalid

parseVersion1 :: Yaml.Object -> Yaml.Parser Metadata
parseVersion1 obj =
    Metadata <$> pure 1 <*> obj .: "groups"

-- | Parser for the legacy sdk.yaml format which does not
-- support groups.
parseVersion0 :: Yaml.Object -> Yaml.Parser Metadata
parseVersion0 obj =
    obj .: "packages" >>= parsePackages
  where
    parsePackages packages = do
      groups <- forM (HML.toList packages) $ \(artifactId, package) -> do
        version   <- package .: "version"
        groupId   <- package .:? "group-id" >>= \case
          Nothing  -> fail "group-id missing"
          Just gid -> pure $ T.splitOn "." gid
        packaging <- package .: "packaging"
        mainPath  <- fmap textToPath <$> package .:? "main"
        return
          ( artifactId
          , Group
            { _gVersion = version
            , _gGroupId = groupId
            , _gPackages = MS.singleton artifactId (Package packaging mainPath False False)
            }
          )
      return $ Metadata 0 (MS.fromList groups)


data ReadMetadataError
    = MetadataFileNotFound L.FilePathOfSdkPackageVersionMetafile
    | YamlError Yaml.ParseException
    deriving (Show)

instance P.Pretty ReadMetadataError where
    pretty (MetadataFileNotFound f) =
        P.t $ "Error when reading metadata, file not found: " <> L.pathOfToText f
    pretty (YamlError yamlErr) =
        P.t $ "Error when reading metadata, yaml error: " <> TE.show yamlErr

-- | Read the metadata from an sdk folder.
readSdkMetadata :: MonadFS m => L.FilePathOfSdkPackageVersionDir -> m (Either ReadMetadataError Metadata)
readSdkMetadata path = do
    let metadataPath = L.addPathSuffix "sdk.yaml" path
    metadataExists <- FS.testfile metadataPath
    if metadataExists
    then do
        errOrMetadata <- FS.decodeYamlFile metadataPath
        case errOrMetadata of
            Left (DecodeYamlFileFailed _ er) -> pure $ Left $ YamlError er -- FIXME: possibly hold on to the context?
            Right metadata -> pure $ Right metadata
    else
        pure $ Left $ MetadataFileNotFound metadataPath

-- Optics
---------

makeLenses ''Package
makeLenses ''Group
makeLenses ''Metadata
