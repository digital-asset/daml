-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Cli.Damlc.Command.MultiIde.PackageData (updatePackageData) where

import qualified "zip-archive" Codec.Archive.Zip as Zip
import Control.Concurrent.STM.TMVar
import Control.Exception(SomeException, displayException, try)
import Control.Lens
import Control.Monad
import Control.Monad.STM
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.State.Strict (StateT, runStateT, gets, modify')
import qualified Data.ByteString.Lazy as BSL
import DA.Cli.Damlc.Command.MultiIde.ClientCommunication
import DA.Cli.Damlc.Command.MultiIde.Util
import DA.Cli.Damlc.Command.MultiIde.Types
import DA.Daml.LF.Reader (DalfManifest(..), readDalfManifest)
import DA.Daml.Package.Config (MultiPackageConfigFields(..), findMultiPackageConfig, withMultiPackageConfig)
import DA.Daml.Project.Consts (projectConfigName)
import DA.Daml.Project.Types (ProjectPath (..))
import Data.Either.Extra (eitherToMaybe)
import Data.Foldable (traverse_)
import qualified Data.Map as Map
import Data.Maybe (catMaybes, isJust)
import qualified Data.Set as Set
import qualified Language.LSP.Types as LSP
import System.Directory (doesFileExist)
import System.FilePath.Posix ((</>))

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
updatePackageData :: MultiIdeState -> IO [PackageHome]
updatePackageData miState = do
  logInfo miState "Updating package data"
  let ideRoot = misMultiPackageHome miState

  -- Take locks, throw away current data
  atomically $ do
    void $ takeTMVar (misMultiPackageMappingVar miState)
    void $ takeTMVar (misDarDependentPackagesVar miState)
  
  mPkgConfig <- findMultiPackageConfig $ ProjectPath ideRoot
  case mPkgConfig of
    Nothing -> do
      logDebug miState "No multi-package.yaml found"
      damlYamlExists <- doesFileExist $ ideRoot </> projectConfigName
      if damlYamlExists
        then do
          logDebug miState "Found daml.yaml"
          -- Treat a workspace with only daml.yaml as a multi-package project with only one package
          deriveAndWriteMappings [PackageHome ideRoot] []
        else do
          logDebug miState "No daml.yaml found either"
          -- Without a multi-package or daml.yaml, no mappings can be made. Passing empty lists here will give empty mappings
          deriveAndWriteMappings [] []
    Just path -> do
      logDebug miState "Found multi-package.yaml"
      (eRes :: Either SomeException [PackageHome]) <- try @SomeException $ withMultiPackageConfig path $ \multiPackage ->
        deriveAndWriteMappings
          (PackageHome . toPosixFilePath <$> mpPackagePaths multiPackage)
          (DarFile . toPosixFilePath <$> mpDars multiPackage)
      let multiPackagePath = toPosixFilePath $ unwrapProjectPath path </> "multi-package.yaml"
      case eRes of
        Right paths -> do
          -- On success, clear any diagnostics on the multi-package.yaml
          sendClient miState $ clearDiagnostics multiPackagePath
          pure paths
        Left err -> do
          -- If the computation fails, the mappings may be empty, so ensure the TMVars have values
          atomically $ do
            void $ tryPutTMVar (misMultiPackageMappingVar miState) Map.empty
            void $ tryPutTMVar (misDarDependentPackagesVar miState) Map.empty
          -- Show the failure as a diagnostic on the multi-package.yaml
          sendClient miState $ fullFileDiagnostic LSP.DsError ("Error reading multi-package.yaml:\n" <> displayException err) multiPackagePath
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
          mUnitId <- lift $ fmap eitherToMaybe $ try @SomeException $ do
            archive <- Zip.toArchive <$> BSL.readFile (unDarFile dep)
            manifest <- either fail pure $ readDalfManifest archive
            -- Manifest "packageName" is actually unit id
            maybe (fail $ "data-dependency " <> unDarFile dep <> " missing a package name") (pure . UnitId) $ packageName manifest
          modify' $ Map.insert dep mUnitId
          pure mUnitId

    deriveAndWriteMappings :: [PackageHome] -> [DarFile] -> IO [PackageHome]
    deriveAndWriteMappings packagePaths darPaths = do
      packedMappingData <- flip runStateT mempty $ do
        -- load cache with all multi-package dars, so they'll be present in darUnitIds
        traverse_ getDarUnitId darPaths
        fmap (bimap catMaybes catMaybes . unzip) $ forM packagePaths $ \packagePath -> do
          mPackageSummary <- lift $ fmap eitherToMaybe $ packageSummaryFromDamlYaml packagePath
          case mPackageSummary of
            Just packageSummary -> do
              allDepsValid <- isJust . sequence <$> traverse getDarUnitId (psDeps packageSummary)
              pure (if allDepsValid then Nothing else Just packagePath, Just (packagePath, psUnitId packageSummary, psDeps packageSummary))
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
