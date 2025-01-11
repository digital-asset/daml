-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE ApplicativeDo #-}
{-# LANGUAGE RankNTypes #-}
module DA.Cli.Damlc.Command.UpgradeCheck (runUpgradeCheck) where

import System.Exit (exitWith, ExitCode(..))
import System.IO
import DA.Pretty
import DA.Daml.Options.Types
import Control.Monad (guard, when)
import Development.IDE.Types.Location (NormalizedFilePath, fromNormalizedFilePath, toNormalizedFilePath')
import DA.Daml.Compiler.ExtractDar (extractDar, ExtractedDar(..), edDeps)
import qualified Data.ByteString.Lazy as BSL
import qualified "zip-archive" Codec.Archive.Zip as ZipArchive
import qualified DA.Daml.LF.Proto3.Archive as Archive
import qualified DA.Daml.LF.Ast as LF
import Control.Exception
import DA.Daml.LF.Ast.Util
import DA.Daml.LF.Ast.Version as LFV
import DA.Daml.LF.TypeChecker.Upgrade as Upgrade
import Data.List (tails)
import Data.Maybe (mapMaybe)
import Safe (maximumByMay, minimumByMay)
import Data.Function (on)
import Development.IDE.Types.Diagnostics (Diagnostic(..), showDiagnostics, ShowDiagnostic(..), DiagnosticSeverity(..))
import Control.Monad.Trans.Except
import qualified Data.Validation as Validation
import Data.Validation (Validation)
import Data.Functor.Compose
import DA.Daml.LF.TypeChecker.Error.WarningFlags

-- Monad in which all checking operations run
-- Occasionally we change to CheckMValidate
type CheckM = ExceptT [CheckingError] IO
type CheckMValidate = Compose IO (Validation [CheckingError])

fromValidate :: CheckMValidate a -> CheckM a
fromValidate = ExceptT . fmap Validation.toEither . getCompose

toValidate :: CheckM a -> CheckMValidate a
toValidate = Compose . fmap Validation.fromEither . runExceptT

fromEither :: (e -> CheckingError) -> Either e a -> CheckM a
fromEither mkErr act = withExceptT (pure . mkErr) $ ExceptT $ pure act

-- All possible errors that can occur while checking upgrades
data CheckingError
  = CECantReadDar NormalizedFilePath String
  | CECantReadDalf NormalizedFilePath FilePath Archive.ArchiveError
  | CEDependencyCycle [(LF.PackageId, LF.PackageName, Maybe LF.PackageVersion)]
  | CEDiagnostic NormalizedFilePath [Diagnostic]
  deriving (Show, Eq)

exitCode :: [CheckingError] -> ExitCode
exitCode errs
  | any isCECantReadDar errs = ExitFailure 4
  | any isCECantReadDalf errs = ExitFailure 3
  | any isCEDependencyCycle errs = ExitFailure 2
  | any isCEDiagnosticError errs = ExitFailure 1
  | otherwise = ExitSuccess
  where
  isCECantReadDar (CECantReadDar {}) = True
  isCECantReadDar _ = False
  isCECantReadDalf (CECantReadDalf {}) = True
  isCECantReadDalf _ = False
  isCEDependencyCycle (CEDependencyCycle {}) = True
  isCEDependencyCycle _ = False
  isCEDiagnosticError (CEDiagnostic _ errs) = Just DsError `elem` map _severity errs
  isCEDiagnosticError _ = False

instance Pretty CheckingError where
  pPrint (CECantReadDar darPath err) = "Error reading DAR at path " <> string (fromNormalizedFilePath darPath) <> ": " <> string err
  pPrint (CECantReadDalf darPath dalfPath err) =
    vcat
      [ "Error reading DALF " <> string dalfPath <> " inside DAR " <> string (fromNormalizedFilePath darPath) <> ":"
      , pPrint err
      ]
  pPrint (CEDependencyCycle deps) =
    vcat
      [ "Supplied packages form a dependency cycle:"
      , nest 2 $ vcat $ map pprintDep deps
      ]
    where
    pprintDep (pkgId, name, mbVersion) =
      let versionDoc = case mbVersion of
            Nothing -> ""
            Just version -> " v" <> pPrint version
      in
      pPrint pkgId <> "(" <> pPrint name <> versionDoc <> ")"
  pPrint (CEDiagnostic path errs) =
    vcat
      [ "Upgrade checking " <> string (fromNormalizedFilePath path) <> " gave following error(s):"
      , text (showDiagnostics (map (path, ShowDiag,) errs))
      ]

newtype CheckingErrors = CheckingErrors [CheckingError]
  deriving (Show, Eq)

instance Pretty CheckingErrors where
  pPrint (CheckingErrors errs) = vcat $ map pPrint errs

-- Read main & deps from a DAR path, annotate with name and version so that
-- upgrades knows which types to check against which
type Archive = (NormalizedFilePath, UpgradedPkgWithNameAndVersion, [UpgradedPkgWithNameAndVersion])

readPathToArchive
  :: NormalizedFilePath
  -> CheckM Archive
readPathToArchive path = do
  extractedDar <-
    catchIOException
      (CECantReadDar path . displayException)
      (extractDar (fromNormalizedFilePath path))
  (main, deps) <- fromValidate $ do
      main <- toValidate $ decodeEntryWithUnitId path Archive.DecodeAsMain (edMain extractedDar)
      deps <- traverse (toValidate . decodeEntryWithUnitId path Archive.DecodeAsDependency) (edDeps extractedDar)
      pure (main, deps)
  pure (path, main, deps)
    where
      catchIOException :: (IOException -> CheckingError) -> IO a -> CheckM a
      catchIOException mkErr io = withExceptT (pure . mkErr) $ ExceptT $ try io

      decodeEntryWithUnitId
        :: NormalizedFilePath -> Archive.DecodingMode -> ZipArchive.Entry
        -> CheckM UpgradedPkgWithNameAndVersion
      decodeEntryWithUnitId darPath decodeAs entry =
        fromEither (CECantReadDalf darPath (ZipArchive.eRelativePath entry)) $ do
            let bs = BSL.toStrict $ ZipArchive.fromEntry entry
            (pkgId, pkg) <- Archive.decodeArchive decodeAs bs
            let (pkgName, mbPkgVersion) = LF.safePackageMetadata pkg
            pure $ UpgradedPkgWithNameAndVersion pkgId pkg pkgName mbPkgVersion

sortPackagesParentFirstM :: [Archive] -> CheckM [Archive]
sortPackagesParentFirstM packages =
  fmap (map withoutIdAndPkg) $ fromEither mkErr $ sortPackagesParentFirst (map withIdAndPkg packages)
    where
      cyclePkgToDep (_, UpgradedPkgWithNameAndVersion {upwnavPkgId, upwnavName, upwnavVersion}, _) = (upwnavPkgId, upwnavName, upwnavVersion)
      mkErr cyclePkgs =
        CEDependencyCycle (map (cyclePkgToDep . withoutIdAndPkg) cyclePkgs)
      withIdAndPkg x@(_path, upgradedPkg, _deps) = (upwnavPkgId upgradedPkg, x, upwnavPkg upgradedPkg)
      withoutIdAndPkg (_pkgId, x, _pkg) = x

checkPackageAgainstPastPackages :: (Archive, [Archive]) -> CheckM ()
checkPackageAgainstPastPackages ((path, main, deps), pastPackages) = do
  case splitPackageVersion id <$> upwnavVersion main of
    Nothing -> pure ()
    Just (Left _) -> pure ()
    Just (Right rawVersion) -> do
      let pastPackageFilterVersion pred = flip mapMaybe pastPackages $ \case
            (_, pastPkg, pkgDeps) -> do
              guard (not (isUtilityPackage (upwnavPkg pastPkg)))
              guard (upwnavName pastPkg == upwnavName main)
              case splitPackageVersion id <$> upwnavVersion pastPkg of
                Just (Right rawVersion) -> do
                  guard (pred rawVersion)
                  pure (rawVersion, (pastPkg, pkgDeps))
                _ -> Nothing
      let ordFst = compare `on` fst
      case maximumByMay ordFst $ pastPackageFilterVersion (\v -> v < rawVersion) of
        Nothing -> pure ()
        Just (_, (closestPastPackageWithLowerVersion, closestPastPackageWithLowerVersionDeps)) -> do
          let errs =
                Upgrade.checkPackageToDepth
                  Upgrade.CheckAll
                  (upwnavPkg main) deps
                  LFV.version2_dev
                  (UpgradeInfo (Just (fromNormalizedFilePath path)) True)
                  (mkDamlWarningFlags damlWarningFlagParserTypeChecker [])
                  (Just (closestPastPackageWithLowerVersion, closestPastPackageWithLowerVersionDeps))
          when (not (null errs)) (throwE [CEDiagnostic path errs])
      case minimumByMay ordFst $ pastPackageFilterVersion (\v -> v > rawVersion) of
        Nothing -> pure ()
        Just (_, (closestPastPackageWithHigherVersion, closestPastPackageWithHigherVersionDeps)) -> do
          let errs =
                Upgrade.checkPackageToDepth
                  Upgrade.CheckAll
                  (upwnavPkg closestPastPackageWithHigherVersion) closestPastPackageWithHigherVersionDeps
                  LFV.version2_dev
                  (UpgradeInfo (Just (fromNormalizedFilePath path)) True)
                  (mkDamlWarningFlags damlWarningFlagParserTypeChecker [])
                  (Just (main, deps))
          when (not (null errs)) (throwE [CEDiagnostic path errs])

runUpgradeCheck :: [String] -> IO ()
runUpgradeCheck rawPaths = do
  let paths = map toNormalizedFilePath' rawPaths
  errsOrUnit <- runExceptT $ do
    packages <- fromValidate $ traverse (toValidate . readPathToArchive) paths
    parentsFirst <- sortPackagesParentFirstM packages
    let dependenciesFirst = reverse parentsFirst
    -- Given sorted packages p1, p2, p3, ... this gives you (p1, []), (p2,[p1]), (p3,[p2,p1]), ...
    let sortedPackagesWithPastPackages = mapMaybe go (reverse (tails (reverse dependenciesFirst)))
          where
          go [] = Nothing
          go (x:xs) = Just (x, xs)
    mapM_ checkPackageAgainstPastPackages sortedPackagesWithPastPackages
  case errsOrUnit of
    Left errs -> do
      hPutStrLn stderr (renderPretty (CheckingErrors errs))
      exitWith (exitCode errs)
    Right () -> pure ()

