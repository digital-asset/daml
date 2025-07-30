-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE ImportQualifiedPost #-}

module DA.Daml.Resolution.Config
  ( findPackageResolutionData
  , expandSdkPackagesDpm
  , getResolutionData
  , ResolutionData (..)
  , PackageResolutionData (..)
  , ValidPackageResolution (..)
  ) where

import "zip-archive" Codec.Archive.Zip qualified as ZipArchive
import Control.Exception
import Control.Monad
import DA.Daml.Compiler.ExtractDar
import DA.Daml.LF.Ast qualified as LF
import DA.Daml.LF.Proto3.Archive.Decode qualified as Archive
import DA.Daml.Project.Types
import Data.Aeson qualified as Aeson
import Data.ByteString.Lazy qualified as BSL
import Data.Either.Extra (eitherToMaybe)
import Data.Functor
import Data.List.Extra
import Data.Map qualified as Map
import Data.Maybe
import Data.Ord (comparing)
import Data.Text qualified as T
import Data.Time.Clock
import Data.Yaml
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

getDarHeaderInfos :: CachePath -> ValidPackageResolution -> IO (Map.Map FilePath DalfInfoCacheEntry)
getDarHeaderInfos cachePath (ValidPackageResolution _ imports) = do
  let paths = fromMaybe [] $ Map.lookup "dars" imports
  extractedDars <- traverse (\p -> (p,) <$> extractDar p) paths
  let extractedDarPackageId :: ExtractedDar -> LF.PackageId
      extractedDarPackageId ed = LF.PackageId $ T.pack $ last $ linesBy (=='-') $ takeBaseName $ ZipArchive.eRelativePath $ edMain ed
      extractedDarsWithPackageIds = (\(path, ed) -> (path, ed, extractedDarPackageId ed)) <$> extractedDars

  dalfInfoCache <- handle @SomeException (const $ pure mempty) $  
    Aeson.eitherDecodeFileStrict @DalfInfoCache (darInfoCachePath cachePath) >>= either fail pure

  currentTime <- getCurrentTime

  let missingDars = filter (\(_, _, pkgId) -> Map.notMember pkgId $ getDalfInfoCacheMap dalfInfoCache) extractedDarsWithPackageIds
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
      removeOldEntries :: DalfInfoCache -> DalfInfoCache
      removeOldEntries (DalfInfoCache m) = DalfInfoCache $ Map.filter ((> currentTime) . addUTCTime darInfoCacheLifetime . diTimestamp) m
      newCache = removeOldEntries dalfInfoCache <> missingCacheEntries
      pathEntries :: Map.Map FilePath DalfInfoCacheEntry
      pathEntries = Map.fromList $ mapMaybe (\(path, _, pkgId) -> (path,) <$> Map.lookup pkgId (getDalfInfoCacheMap newCache)) extractedDarsWithPackageIds

  encodeFile (darInfoCachePath cachePath) newCache

  pure pathEntries

hardcodedPackageRenames :: T.Text -> T.Text
hardcodedPackageRenames "daml-script-lts" = "daml-script"
hardcodedPackageRenames "daml3-script" = "daml-script"
hardcodedPackageRenames name = name

-- Packages that cannot be data deps (currently only daml-script)
-- as they use CallStack
unsupportedAsDataDep :: [T.Text]
unsupportedAsDataDep = ["daml-script"]

-- Mimics (mostly) signature of expandSdkPackages in Options.hs
-- Returns two lists, first is regular deps, second is data deps
expandSdkPackagesDpm :: CachePath -> ValidPackageResolution -> LF.Version -> [FilePath] -> IO ([FilePath], [FilePath])
expandSdkPackagesDpm cachePath pkgResolution lfVersion paths = do
  darInfos <- getDarHeaderInfos cachePath pkgResolution
  let isSdkPackage fp = takeExtension fp `notElem` [".dar", ".dalf"]
      (sdkPackages, purePaths) = partition isSdkPackage paths
      resolvedSdkPackagesWithPaths = traverse (\fp -> findDarInDarInfos darInfos (T.pack fp) lfVersion) sdkPackages
  case resolvedSdkPackagesWithPaths of
    Left err -> fail $ T.unpack err
    Right resolvedSdkPackages -> do
      let (asDataDeps, asDeps) = partition snd resolvedSdkPackages
      pure (purePaths <> fmap fst asDeps, fmap fst asDataDeps)

-- Bool is if it can be a data-dep
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
      allPackageNames = T.intercalate "," . nubOrd $ LF.unPackageName . diPackageName <$> Map.elems darInfos
  case availableVersions of
    [] ->
      Left $ "Package " <> rawName <> " could not be found, available packages are:\n"
        <> allPackageNames <> "\nIf your package is shown, it may not be compatible with your LF version."
    [_] ->
      -- Major LF versions aren't cross compatible, so all will be same major here due to canDependOn check above
      -- as such, we take maximum by minor version
      Right (fst $ maximumBy (comparing (LF.versionMinor . diLfVersion . snd)) $ Map.toList candidates, name `notElem` unsupportedAsDataDep)
    _ ->
      Left $ "Multiple package versions for " <> rawName <> " were found:\n" <> (T.intercalate "," $ LF.unPackageVersion <$> availableVersions)
        <> "\nYour daml installation may be broken."

findPackageResolutionData :: FilePath -> ResolutionData -> Maybe ValidPackageResolution
findPackageResolutionData path (ResolutionData packages) =
  Map.lookup path packages <&> \case
    ErrorPackageResolutionData err -> error $ unlines err
    ValidPackageResolutionData res -> res

getResolutionData :: IO (Maybe ResolutionData)
getResolutionData = do
  mPath <- lookupEnv "UNIFI_ASSISTANT_RESOLUTION_FILE"
  forM mPath $ \path ->
    either (\err -> error $ "Failed to decode UNIFI_ASSISTANT_RESOLUTION_FILE at " <> path <> "\n" <> show err) id <$> decodeFileEither path

data ResolutionData = ResolutionData
  { packages :: Map.Map FilePath PackageResolutionData
  }
  deriving (Eq, Show)

data PackageResolutionData
  = ErrorPackageResolutionData [String]
  | ValidPackageResolutionData ValidPackageResolution
  deriving (Eq, Show)

data ValidPackageResolution = ValidPackageResolution
  { components :: Map.Map String FilePath
  , imports :: Map.Map String [FilePath]
  }
  deriving (Eq, Show)

instance Aeson.FromJSON ResolutionData where
  parseJSON = Aeson.withObject "ResolutionData" $ \obj ->
    ResolutionData <$> obj .: "packages"

instance Aeson.FromJSON PackageResolutionData where
  parseJSON = Aeson.withObject "PackageResolutionData" $ \obj -> do
    mErrors <- obj .:? "error"
    case mErrors of
      Just errs -> pure $ ErrorPackageResolutionData errs
      Nothing ->
        fmap ValidPackageResolutionData $ ValidPackageResolution
          <$> obj .: "components"
          <*> obj .: "imports"
