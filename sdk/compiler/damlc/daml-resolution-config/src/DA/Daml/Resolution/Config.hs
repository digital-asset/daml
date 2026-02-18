-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE ImportQualifiedPost #-}

module DA.Daml.Resolution.Config
  ( findPackageResolutionData
  , expandSdkPackagesDpm
  , getResolutionData
  , resolutionFileEnvVar
  , ExpandedSdkPackages (..)
  , ResolutionData (..)
  , PackageResolutionData (..)
  , ErrorPackageResolution (..)
  , ValidPackageResolution (..)
  , ResolutionError (..)
  , DPMUnsupportedError (..)
  ) where

import "zip-archive" Codec.Archive.Zip qualified as ZipArchive
import Control.Exception
import Control.Monad
import DA.Daml.Compiler.ExtractDar
import DA.Daml.LF.Ast qualified as LF
import DA.Daml.LF.Proto3.Archive.Decode qualified as Archive
import DA.Daml.Project.Types
import Data.Aeson qualified as Aeson
import Data.Bifunctor (first)
import Data.ByteString.Lazy qualified as BSL
import Data.Either.Extra (eitherToMaybe, fromRight)
import Data.Function ((&))
import Data.List.Extra
import Data.Map qualified as Map
import Data.Maybe
import Data.Ord (comparing)
import Data.Text qualified as T
import Data.Time.Clock
import Data.Typeable (Typeable)
import Data.Yaml
import System.Directory (createDirectoryIfMissing)
import System.Environment (lookupEnv)
import System.FilePath

newtype DalfInfoCache = DalfInfoCache {getDalfInfoCacheMap :: Map.Map LF.PackageId DalfInfoCacheEntry}
  deriving newtype (ToJSON, FromJSON, Semigroup, Monoid)

data DalfInfoCacheEntry = DalfInfoCacheEntry
  { diPackageName :: LF.PackageName
  , diPackageVersion :: LF.PackageVersion
  , diLfVersion :: LF.Version
  , diTimestamp :: UTCTime
  }

instance ToJSON DalfInfoCacheEntry where
  toJSON DalfInfoCacheEntry{..} = object
    [ "package_name" .= diPackageName
    , "package_version" .= diPackageVersion
    , "lf_version" .= LF.renderVersion diLfVersion
    , "timestamp" .= diTimestamp
    ]
instance FromJSON DalfInfoCacheEntry where
  parseJSON = withObject "DalfInfoCacheEntry" $ \o -> do
    mVer <- LF.parseVersion <$> o .: "lf_version"
    DalfInfoCacheEntry
      <$> o .: "package_name"
      <*> o .: "package_version"
      <*> maybe (fail "Invalid lf version") pure mVer
      <*> o .: "timestamp"

darInfoCacheKey :: String
darInfoCacheKey = "builtin_dar_info.json"

darInfoCacheLifetime :: NominalDiffTime
darInfoCacheLifetime = 30 * nominalDay

darInfoCachePath :: CachePath -> FilePath
darInfoCachePath p = unwrapCachePath p </> darInfoCacheKey

-- For "internal" failures in resolution
newtype ResolutionError = ResolutionError {unResolutionError :: String}
  deriving newtype Show
  deriving stock Typeable

instance Exception ResolutionError

newtype DPMUnsupportedError = DPMUnsupportedError {unsupportedThing :: String}
  deriving stock Typeable

instance Show DPMUnsupportedError where
  show (DPMUnsupportedError thing) = "DPM does not currently support " <> thing <> ". Support may be added in a future version."
instance Exception DPMUnsupportedError

getDarHeaderInfos :: CachePath -> ValidPackageResolution -> IO (Map.Map FilePath DalfInfoCacheEntry)
getDarHeaderInfos cachePath (ValidPackageResolution _ imports) = do
  let paths = fromMaybe [] $ Map.lookup "dars" imports
  extractedDars <- traverse (\p -> (p,) <$> extractDar p) paths
  let extractedDarsWithPackageIds = (\(path, ed) -> (path, ed, mainPackageIdFromPaths ed)) <$> extractedDars

  dalfInfoCache <- handle @IOException (const $ pure mempty) $  
    fromRight mempty <$> Aeson.eitherDecodeFileStrict @DalfInfoCache (darInfoCachePath cachePath)

  currentTime <- getCurrentTime

  let dalfInfoCacheWithoutStale =
        DalfInfoCache $ Map.filter ((> currentTime) . addUTCTime darInfoCacheLifetime . diTimestamp) $ getDalfInfoCacheMap dalfInfoCache
      missingDars = filter (\(_, _, pkgId) -> Map.notMember pkgId $ getDalfInfoCacheMap dalfInfoCacheWithoutStale) extractedDarsWithPackageIds
      missingCacheEntries = DalfInfoCache $ Map.fromList $ flip mapMaybe missingDars $ \(_, ed, pkgId) -> do
        (_, package) <-
          eitherToMaybe $ Archive.decodeArchive Archive.DecodeAsDependency $ BSL.toStrict $ ZipArchive.fromEntry $ edMain ed
        pure
          ( pkgId
          , DalfInfoCacheEntry
              (LF.packageName $ LF.packageMetadata package)
              (LF.packageVersion $ LF.packageMetadata package)
              (LF.packageLfVersion package) 
              currentTime
          )
      newCache = dalfInfoCacheWithoutStale <> missingCacheEntries
      pathEntries :: Map.Map FilePath DalfInfoCacheEntry
      pathEntries = Map.fromList $ mapMaybe (\(path, _, pkgId) -> (path,) <$> Map.lookup pkgId (getDalfInfoCacheMap newCache)) extractedDarsWithPackageIds

  -- Ensure path exists before writing file
  let diCachePath = darInfoCachePath cachePath
  createDirectoryIfMissing True $ takeDirectory diCachePath
  encodeFile diCachePath newCache

  pure pathEntries

-- Deprecated in all of 3x, remove later for 3.4
hardcodedPackageRenames :: T.Text -> T.Text
hardcodedPackageRenames "daml-script-lts" = "daml-script"
hardcodedPackageRenames "daml3-script" = "daml-script"
hardcodedPackageRenames name = name

-- Packages that cannot be data deps (currently only daml-script)
-- as they use CallStack
unsupportedAsDataDep :: [T.Text]
unsupportedAsDataDep = ["daml-script"]

data ExpandedSdkPackages = ExpandedSdkPackages
  { espRegularDeps :: [FilePath]
  , -- For deps that shouldn't check the SDK version, i.e. deps from DPM that cannot be data deps
    -- currently this is just daml-script
    espRegularUncheckedDeps :: [FilePath]
  , espDataDeps :: [FilePath]
  }

-- Mimics (mostly) signature of expandSdkPackages in Options.hs
-- Returns two lists, first is regular deps, second is data deps
expandSdkPackagesDpm :: CachePath -> ValidPackageResolution -> LF.Version -> [FilePath] -> IO ExpandedSdkPackages
expandSdkPackagesDpm cachePath pkgResolution lfVersion paths = do
  darInfos <- getDarHeaderInfos cachePath pkgResolution
  let isSdkPackage fp = takeExtension fp `notElem` [".dar", ".dalf"]
      (sdkPackages, purePaths) = partition isSdkPackage paths
      resolvedSdkPackagesWithPaths = traverse (\fp -> findDarInDarInfos darInfos (T.pack fp) lfVersion) sdkPackages
  case resolvedSdkPackagesWithPaths of
    Left err -> throwIO $ ResolutionError $ T.unpack err
    Right resolvedSdkPackages -> do
      let (asDataDeps, asDeps) = partition snd resolvedSdkPackages
      pure $ ExpandedSdkPackages purePaths (fmap fst asDeps) (fmap fst asDataDeps)

-- Returns the path to the dar found, as well as a bool for whether this dar can be a data-dep (see `unsupportedAsDataDep`)
findDarInDarInfos :: Map.Map FilePath DalfInfoCacheEntry -> T.Text -> LF.Version -> Either T.Text (FilePath, Bool)
findDarInDarInfos darInfos rawName lfVersion = do
  let name = hardcodedPackageRenames rawName
      lfCondition name = if name `elem` unsupportedAsDataDep then (==) else LF.canDependOn
      candidates =
        Map.filter (\darInfo ->
          (LF.unPackageName $ diPackageName darInfo) == name
            && lfCondition name lfVersion (diLfVersion darInfo)
        ) darInfos
      availableVersions = nubOrd $ diPackageVersion <$> Map.elems candidates
  case availableVersions of
    [] -> do
      let allPackageNames = T.intercalate "," . nubOrd $ LF.unPackageName . diPackageName <$> Map.elems darInfos
      Left $ "Package " <> rawName <> " could not be found, available packages are:\n"
        <> allPackageNames <> "\nIf your package is shown, it may not be compatible with your LF version."
    [_] ->
      -- Major LF versions aren't cross compatible, so all will be same major here due to canDependOn check above
      -- as such, we take maximum by minor version
      Right (fst $ maximumBy (comparing (LF.versionMinor . diLfVersion . snd)) $ Map.toList candidates, name `notElem` unsupportedAsDataDep)
    _ ->
      Left $ "Multiple package versions for " <> rawName <> " were found:\n" <> (T.intercalate "," $ LF.unPackageVersion <$> availableVersions)
        <> "\nYour daml installation may be broken."

findPackageResolutionData :: FilePath -> ResolutionData -> Either ResolutionError ValidPackageResolution
findPackageResolutionData path (ResolutionData packages) =
  Map.lookup (toPosixFilePath path) packages & \case
    Just (ErrorPackageResolutionData errs) -> Left $ ResolutionError $ "Couldn't resolve package " <> path <> ":\n" <> unlines (show <$> errs)
    Just (ValidPackageResolutionData res) -> Right res
    Nothing -> Left $ ResolutionError $ "DPM did not provide information for package at " <> path <> ". Is this a valid package? If you have a multi-package.yaml, is this package included?"

resolutionFileEnvVar :: String
resolutionFileEnvVar = "DPM_RESOLUTION_FILE"

getResolutionData :: IO (Maybe ResolutionData)
getResolutionData = do
  mPath <- lookupEnv resolutionFileEnvVar
  forM mPath $ \path -> do
    eResolutionData <- decodeFileEither path
    case eResolutionData of
      Right resolutionData -> pure resolutionData
      Left err -> throwIO $ ResolutionError $ "Failed to decode " <> resolutionFileEnvVar <> " at " <> path <> "\n" <> show err

data ResolutionData = ResolutionData
  { packages :: Map.Map FilePath PackageResolutionData
  }
  deriving (Eq, Show)

data PackageResolutionData
  = ErrorPackageResolutionData [ErrorPackageResolution]
  | ValidPackageResolutionData ValidPackageResolution
  deriving (Eq, Show)

data ValidPackageResolution = ValidPackageResolution
  { components :: Map.Map String FilePath
  , imports :: Map.Map String [FilePath]
  }
  deriving (Eq, Show)

data ErrorPackageResolution = ErrorPackageResolution
  { cause :: String
  , code :: String
  }
  deriving Eq

-- Changes backslashes to forward slashes, lowercases the drive
-- Need native filepath for splitDrive, as Posix version just takes first n `/`s
toPosixFilePath :: FilePath -> FilePath
toPosixFilePath = uncurry joinDrive . first lower . splitDrive . replace "\\" "/"

toPosixFilePathValidPackageResolution :: ValidPackageResolution -> ValidPackageResolution
toPosixFilePathValidPackageResolution (ValidPackageResolution components imports) =
  ValidPackageResolution (fmap toPosixFilePath components) (fmap (fmap toPosixFilePath) imports)

instance Show ErrorPackageResolution where
  show ErrorPackageResolution {..} = code <> ": " <> cause

instance Aeson.FromJSON ResolutionData where
  parseJSON = Aeson.withObject "ResolutionData" $ \obj ->
    ResolutionData . Map.mapKeys toPosixFilePath <$> obj .: "packages"

instance Aeson.FromJSON PackageResolutionData where
  parseJSON = Aeson.withObject "PackageResolutionData" $ \obj -> do
    mErrors <- obj .:? "errors"
    case mErrors of
      Just errs -> pure $ ErrorPackageResolutionData errs
      Nothing -> fmap ValidPackageResolutionData $ Aeson.parseJSON $ Aeson.Object obj

instance Aeson.FromJSON ValidPackageResolution where
  parseJSON = Aeson.withObject "ValidPackageResolution" $ \obj ->
    fmap toPosixFilePathValidPackageResolution $
      ValidPackageResolution
        <$> obj .:? "components" .!= mempty
        <*> obj .:? "imports" .!= mempty

instance Aeson.FromJSON ErrorPackageResolution where
  parseJSON = Aeson.withObject "ErrorPackageResolution" $ \obj ->
    ErrorPackageResolution
      <$> obj .: "cause"
      <*> obj .: "code"

instance Aeson.ToJSON ResolutionData where
  toJSON (ResolutionData packages) =
    Aeson.object ["packages" .= packages]

instance Aeson.ToJSON PackageResolutionData where
  toJSON (ErrorPackageResolutionData errs) =
    Aeson.object ["errors" .= errs]
  toJSON (ValidPackageResolutionData validResolutionData) =
    Aeson.toJSON validResolutionData

instance Aeson.ToJSON ValidPackageResolution where
  toJSON (ValidPackageResolution components imports) =
    Aeson.object ["components" .= components, "imports" .= imports]

instance Aeson.ToJSON ErrorPackageResolution where
  toJSON (ErrorPackageResolution cause code) =
    Aeson.object ["cause" .= cause, "code" .= code]
