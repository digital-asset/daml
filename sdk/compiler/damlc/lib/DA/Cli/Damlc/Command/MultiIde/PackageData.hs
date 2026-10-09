-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Cli.Damlc.Command.MultiIde.PackageData (updatePackageData, addDpmPackageDependencyDarsFromDatabase) where

import qualified "zip-archive" Codec.Archive.Zip as Zip
import Control.Applicative ((<|>))
import Control.Concurrent.STM.TMVar
import Control.Exception(SomeException, displayException, try)
import Control.Lens
import Control.Monad
import Control.Monad.Loops (maximumOnM)
import Control.Monad.STM
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.State.Strict (StateT, runStateT, gets, modify')
import qualified Data.ByteString.Lazy as BSL
import DA.Cli.Damlc.Command.MultiIde.ClientCommunication
import DA.Cli.Damlc.Command.MultiIde.Util
import DA.Cli.Damlc.Command.MultiIde.Types
import qualified DA.Daml.LF.Ast.Base as LF
import DA.Daml.LF.Reader (DalfManifest(..), readDalfManifest)
import DA.Daml.Options.Types (packageDependenciesDatabasePath)
import DA.Daml.Package.Config (MultiPackageConfigFields(..), findMultiPackageConfig, withMultiPackageConfig)
import DA.Daml.Project.Consts (getCachePath, packageConfigName)
import DA.Daml.Project.Types (PackagePath (..))
import DA.Daml.Resolution.Config (DependencyPackages (..), PackageResolutionData (..), ValidPackageResolution, getDpmDependencyPaths, readDependencyPackagesFromResolution)
import Data.Either.Extra (eitherToMaybe, fromRight)
import Data.Foldable (traverse_)
import Data.List.Extra (nubOrd)
import qualified Data.Map as Map
import Data.Maybe (catMaybes, fromMaybe, isJust, mapMaybe)
import qualified Data.Set as Set
import qualified Data.Text.Extended as T
import System.Directory.Extra (doesFileExist, getModificationTime, listDirectories)
import System.FilePath.Posix (takeExtension, takeFileName, (</>))

{-
TODO: refactor multi-package.yaml discovery logic
Expect a multi-package.yaml at the workspace root
If we do not get one, we continue as normal (no popups) until the user attempts to open/use files in a different package to the first one
  When this occurs, this send a popup:
    Make a multi-package.yaml at the root and reload the editor please :)
    OR tell me where the multi-package.yaml(s) is
      if the user provides multiple, we union that lookup, allowing "cross project boundary" jumps
-}
-- Updates the unit-id to package/dar mapping, as well as the dar to dependent packages mapping
-- for any daml.yamls or dars that are invalid, the ide home paths are returned, and their data is not added to the mapping
-- Attempts to use resolution imports to find resolved dependencies, so resolution file should be up-to-date before calling this
updatePackageData :: MultiIdeState -> IO [PackageHome]
updatePackageData miState = do
  logInfo miState "Updating package data"
  let ideRoot = misMultiPackageHome miState

  -- Take locks, throw away current data
  -- Find current running IDEs for which we know the package-db is up-to-date
  runningHomes <- atomically $ do
    void $ takeTMVar (misMultiPackageMappingVar miState)
    void $ takeTMVar (misDarDependentPackagesVar miState)
    fmap ideHome . mapMaybe ideDataMain . Map.elems <$> readTMVar (misSubIdesVar miState)
  
  mPkgConfig <- findMultiPackageConfig $ PackagePath ideRoot
  case mPkgConfig of
    Nothing -> do
      logDebug miState "No multi-package.yaml found"
      damlYamlExists <- doesFileExist $ ideRoot </> packageConfigName
      if damlYamlExists
        then do
          isFullDamlYaml <- shouldHandleDamlYamlChange <$> T.readFileUtf8 (ideRoot </> packageConfigName)
          if isFullDamlYaml
            then do
              logDebug miState "Found daml.yaml"
              -- Treat a workspace with only daml.yaml as a multi-package project with only one package
              deriveAndWriteMappings runningHomes [PackageHome ideRoot] []
            else do
              logDebug miState "Found daml.yaml, but not full package."
              -- Treat as though no daml.yaml exists
              deriveAndWriteMappings runningHomes [] []    
        else do
          logDebug miState "No daml.yaml found either"
          -- Without a multi-package or daml.yaml, no mappings can be made. Passing empty lists here will give empty mappings
          deriveAndWriteMappings runningHomes [] []
    Just path -> do
      logDebug miState "Found multi-package.yaml"
      (eRes :: Either SomeException [PackageHome]) <- try @SomeException $ withMultiPackageConfig path $ \multiPackage ->
        deriveAndWriteMappings
          runningHomes
          (PackageHome . toPosixFilePath <$> mpPackagePaths multiPackage)
          (DarFile . toPosixFilePath <$> mpDars multiPackage)
      case eRes of
        Right paths -> do
          -- On success, clear the global error for updatePackage
          toReboot <- reportUpdatePackageError miState Nothing
          pure $ nubOrd $ paths <> toReboot
        Left err -> do
          -- If the computation fails, the mappings may be empty, so ensure the TMVars have values
          atomically $ do
            void $ tryPutTMVar (misMultiPackageMappingVar miState) Map.empty
            void $ tryPutTMVar (misDarDependentPackagesVar miState) Map.empty
          -- Report error via global errors, which will display on multi-package.yaml
          void $ reportUpdatePackageError miState $ Just $ "Error reading multi-package.yaml:\n" <> displayException err
          pure []
  where
    -- Gets the unit id of a dar if it can, caches result in stateT
    -- Returns Nothing (and stores) if anything goes wrong (dar doesn't exist, dar isn't archive, dar manifest malformed, etc.)
    getDarUnitId :: DarFile -> StateT (Map.Map DarFile (Maybe UnitId)) IO (Maybe UnitId)
    getDarUnitId dep = do
      cachedResult <- gets (Map.lookup dep)
      case cachedResult of
        Just res -> pure res
        Nothing -> do
          mUnitId <- lift $ darUnitIdFromPath $ unDarFile dep
          modify' $ Map.insert dep mUnitId
          pure mUnitId

    deriveAndWriteMappings :: [PackageHome] -> [PackageHome] -> [DarFile] -> IO [PackageHome]
    deriveAndWriteMappings runningHomes packagePaths darPaths = do
      packedMappingData <- flip runStateT mempty $ do
        -- load cache with all multi-package dars, so they'll be present in darUnitIds
        traverse_ getDarUnitId darPaths
        resolutionData <- lift $ readTVarIO $ misResolutionData miState
        fmap (bimap catMaybes catMaybes . unzip) $ forM packagePaths $ \packagePath -> do
          mPackageSummary <- lift $ fmap eitherToMaybe $ packageSummaryFromDamlYaml packagePath
          case mPackageSummary of
            Just packageSummary -> do
              deps <-
                case Map.lookup packagePath (mainPackages resolutionData) <|> Map.lookup packagePath (orphanPackages resolutionData) of
                  Just (ValidPackageResolutionData packageResolution@(readDependencyPackagesFromResolution -> Just depData)) -> do
                    additionalDars <-
                      if packagePath `elem` runningHomes
                        then lift $ darPathsFromPackageHome packageResolution packagePath
                        else pure []
                    pure $ fmap DarFile $ filter ((== ".dar") . takeExtension) $ dpRegularDeps depData <> dpDataDeps depData <> additionalDars
                  _ -> pure $ psDeps packageSummary
              allDepsValid <- isJust . sequence <$> traverse getDarUnitId deps
              pure (if allDepsValid then Nothing else Just packagePath, Just (packagePath, psUnitId packageSummary, deps))
            _ -> pure (Just packagePath, Nothing)

      let invalidHomes :: [PackageHome]
          validPackageDatas :: [(PackageHome, UnitId, [DarFile])]
          darUnitIds :: Map.Map DarFile (Maybe UnitId)
          ((invalidHomes, validPackageDatas), darUnitIds) = packedMappingData
          packagesOnDisk :: Map.Map UnitId PackageSourceLocation
          packagesOnDisk =
            Map.fromList $ (\(packagePath, unitId, _) -> (unitId, PackageOnDisk packagePath)) <$> validPackageDatas
          darMapping :: Map.Map UnitId PackageSourceLocation
          darMapping =
            Map.fromList $ fmap (\(packagePath, unitId) -> (unitId, PackageInDar packagePath)) $ Map.toList $ Map.mapMaybe id darUnitIds
          multiPackageMapping :: Map.Map UnitId PackageSourceLocation
          multiPackageMapping = packagesOnDisk <> darMapping
          darDependentPackages :: Map.Map DarFile (Set.Set PackageHome)
          darDependentPackages = foldr
            (\(packagePath, _, deps) -> Map.unionWith (<>) $ Map.fromList $ (,Set.singleton packagePath) <$> deps
            ) Map.empty validPackageDatas

      logDebug miState $ "Setting multi package mapping to:\n" <> show multiPackageMapping
      logDebug miState $ "Setting dar dependent packages to:\n" <> show darDependentPackages
      atomically $ do
        putTMVar (misMultiPackageMappingVar miState) multiPackageMapping
        putTMVar (misDarDependentPackagesVar miState) darDependentPackages

      pure invalidHomes

-- Gets the unit id of a dar at the given path
-- Returns Nothing if anything goes wrong (dar doesn't exist, dar isn't archive, dar manifest malformed, etc.)
darUnitIdFromPath :: FilePath -> IO (Maybe UnitId)
darUnitIdFromPath path = fmap eitherToMaybe $ try @SomeException $ do
  archive <- Zip.toArchive <$> BSL.readFile path
  manifest <- either fail pure $ readDalfManifest archive
  -- Manifest "packageName" is actually unit id
  maybe (fail $ "data-dependency " <> path <> " missing a package name") (pure . UnitId) $ packageName manifest

-- Each LF verison has its own dependencies "database". It's rare that users have more than one
-- but in the cases where they do, we take the most recent.
-- The dependencies "database" is build before GHC is invoked, and most failures occur.
-- The failures that can occur before this build things like missing deps, malformed daml.yaml
-- TODO: Look into a better way to select the correct package-db
latestPackageDependencyDatabase :: PackageHome -> IO (Maybe FilePath)
latestPackageDependencyDatabase packageHome = do
  let databaseHome = unPackageHome packageHome </> packageDependenciesDatabasePath
  packageDependencyDatabases <- fromRight [] <$> try @SomeException (listDirectories databaseHome)
  maximumOnM getModificationTime packageDependencyDatabases

darPathsFromPackageHome :: ValidPackageResolution -> PackageHome -> IO [FilePath]
darPathsFromPackageHome packageResolution packageHome = do
  mPackageDependencyDatabase <- latestPackageDependencyDatabase packageHome
  fmap (fromMaybe []) $ forM mPackageDependencyDatabase $ \packageDependencyDatabase -> do
    dependencyDirs <- fromRight [] <$> try @SomeException (listDirectories packageDependencyDatabase)
    -- Directory names in the database are package-ids
    let packageIds = LF.PackageId . T.pack . takeFileName . toPosixFilePath <$> dependencyDirs
    cachePath <- getCachePath
    getDpmDependencyPaths cachePath packageResolution packageIds

-- Given a package with a populated package database, read the db to find its direct dep
-- package-ids. Search the dpm deps list for these ids, and resolve their transitive deps.
-- Add all these dependency dars to the MultiPackageYamlMapping as PackageInDar references
addDpmPackageDependencyDarsFromDatabase :: MultiIdeState -> PackageHome -> IO ()
addDpmPackageDependencyDarsFromDatabase miState packageHome = do
  resolutionData <- readTVarIO $ misResolutionData miState
  case Map.lookup packageHome (mainPackages resolutionData) <|> Map.lookup packageHome (orphanPackages resolutionData) of
    Just (ValidPackageResolutionData packageResolution) -> do
      darPaths <- darPathsFromPackageHome packageResolution packageHome
      newMappings <- fmap (Map.fromList . catMaybes) $ forM darPaths $ \darPath -> do
        mUnitId <- darUnitIdFromPath darPath
        pure $ (, PackageInDar $ DarFile $ toPosixFilePath darPath) <$> mUnitId
      logDebug miState $ "Adding dpm dependency dars to multi package mapping:\n" <> show newMappings
      -- Should only replace existing PackageInDar definitions, not PackageOnDisks
      atomically $ modifyTMVar_ (misMultiPackageMappingVar miState) (<> newMappings)
    -- This function is called when an IDE responds to initialization, which means it should have a resolution
    -- There are niche cases where it won't though, so we don't error
    _ -> pure ()
