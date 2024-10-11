-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE ApplicativeDo #-}
{-# LANGUAGE RankNTypes #-}
module DA.Cli.Damlc.Command.UpgradeCheck (runUpgradeCheck) where

import System.Exit
import DA.Pretty
import DA.Daml.Options.Types
import Control.Monad (guard, when)
import Development.IDE.Types.Location (NormalizedFilePath, fromNormalizedFilePath, toNormalizedFilePath')
import DA.Daml.Compiler.ExtractDar (extractDar,ExtractedDar(..))
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
import Development.IDE.Types.Diagnostics (Diagnostic, showDiagnostics, ShowDiagnostic(..))
import Control.Monad.Trans.Except

-- Monad in which all checking operations run
-- Occasionally we change to CollectErrs
type CheckM = ExceptT [CheckingError] IO

fromEither :: (e -> CheckingError) -> Either e a -> CheckM a
fromEither mkErr act = withExceptT (pure . mkErr) $ ExceptT $ pure act

-- All possible errors that can occur while checking upgrades
data CheckingError
  = CECantReadDar NormalizedFilePath String
  | CECantReadDalf NormalizedFilePath FilePath Archive.ArchiveError
  | CEDependencyCycle [(LF.PackageId, LF.PackageName, Maybe LF.PackageVersion)]
  | CEDiagnostic NormalizedFilePath [Diagnostic]
  deriving (Show, Eq)

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

-- Run as many applicative actions as possible, collecting errors as we go along
-- Used to collect errors for a list of monadic actions without aborting on the
-- first error.
newtype CollectErrs m e a = CollectErrs { runCollectErrs :: m (Either e a) }

instance (Functor m) => Functor (CollectErrs m e) where
  fmap f (CollectErrs mea) = CollectErrs ((fmap . fmap) f mea)

instance (Applicative m, Monoid e) => Applicative (CollectErrs m e) where
  pure = CollectErrs . pure . Right
  (<*>) :: forall m e a b. (Applicative m, Monoid e) => CollectErrs m e (a -> b) -> CollectErrs m e a -> CollectErrs m e b
  (<*>) (CollectErrs mf) (CollectErrs ma) = CollectErrs (go <$> mf <*> ma)
    where
      go :: Either e (a -> b) -> Either e a -> Either e b
      go f a =
        case (f, a) of
          (Left fErrs, Left aErrs) -> Left (fErrs <> aErrs)
          (Left fErrs, _) -> Left fErrs
          (_, Left aErrs) -> Left aErrs
          (Right f, Right a) -> Right (f a)

fromCollect :: CollectErrs IO [CheckingError] a -> CheckM a
fromCollect = ExceptT . runCollectErrs

toCollect :: CheckM a -> CollectErrs IO [CheckingError] a
toCollect = CollectErrs . runExceptT

-- Read main & deps from a DAR path, annotate with name and version so that
-- upgrades knows which types to check against which
type Archive = (NormalizedFilePath, UpgradedPkgWithNameAndVersion, [UpgradedPkgWithNameAndVersion])

readPathToArchive
  :: NormalizedFilePath
  -> CheckM Archive
readPathToArchive path = do
  ExtractedDar{edMain,edDalfs} <-
    catchIOException
      (CECantReadDar path . displayException)
      (extractDar (fromNormalizedFilePath path))
  (main, deps) <- fromCollect $ do
      main <- toCollect $ decodeEntryWithUnitId path Archive.DecodeAsMain edMain
      deps <- traverse (toCollect . decodeEntryWithUnitId path Archive.DecodeAsDependency) edDalfs
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
            pure (pkgId, pkg, pkgName, mbPkgVersion)

topoSortPackagesM :: [Archive] -> CheckM [Archive]
topoSortPackagesM packages =
  fmap (map withoutIdAndPkg) $ fromEither mkErr $ topoSortPackages (map withIdAndPkg packages)
    where
      cyclePkgToDep (_, (pkgId, _, name, mbVersion), _) = (pkgId, name, mbVersion)
      mkErr cyclePkgs =
        CEDependencyCycle (map (cyclePkgToDep . withoutIdAndPkg) cyclePkgs)
      withIdAndPkg x@(_path, (pkgId, pkg, _name, _version), _deps) = (pkgId, x, pkg)
      withoutIdAndPkg (_pkgId, x, _pkg) = x

checkPackageAgainstPastPackages :: (Archive, [Archive]) -> CheckM ()
checkPackageAgainstPastPackages ((path, main, deps), pastPackages) = do
  let (_mainId, mainPkg, mainName, mbVersion) = main
  case splitPackageVersion id <$> mbVersion of
    Nothing -> pure ()
    Just (Left _) -> pure ()
    Just (Right rawVersion) -> do
      let pastPackageFilterVersion pred = flip mapMaybe pastPackages $ \case
            (_, x@(_, pastPackage, name, mbVersion), y) -> do
              guard (not (isUtilityPackage pastPackage))
              guard (name == mainName)
              case splitPackageVersion id <$> mbVersion of
                Just (Right rawVersion) -> do
                  guard (pred rawVersion)
                  pure (rawVersion, (x, y))
                _ -> Nothing
      let ordFst = compare `on` fst
      let pastPackageWithLowerVersion =
            fmap snd $ maximumByMay ordFst $ pastPackageFilterVersion (\v -> v < rawVersion)
      let pastPackageWithHigherVersion =
            fmap snd $ minimumByMay ordFst $ pastPackageFilterVersion (\v -> v > rawVersion)
      let checkAgainst pkg =
            let errs =
                  Upgrade.checkPackage
                    mainPkg deps
                    LFV.version2_dev
                    (UpgradeInfo (Just (fromNormalizedFilePath path)) True True)
                    (Just pkg)
            in
            when (not (null errs)) (throwE [CEDiagnostic path errs])
      mapM_ checkAgainst pastPackageWithLowerVersion
      mapM_ checkAgainst pastPackageWithHigherVersion

runUpgradeCheck :: [String] -> IO ()
runUpgradeCheck rawPaths = do
  let paths = map toNormalizedFilePath' rawPaths
  errsOrUnit <- runExceptT $ do
    packages <- fromCollect $ traverse (toCollect . readPathToArchive) paths
    sortedPackages <- topoSortPackagesM packages
    let sortedPackagesWithPastPackages = mapMaybe go (reverse (tails (reverse sortedPackages)))
          where
          go [] = Nothing
          go (x:xs) = Just (x, xs)
    mapM_ checkPackageAgainstPastPackages sortedPackagesWithPastPackages
  case errsOrUnit of
    Left errs -> do
      putStrLn (renderPretty (CheckingErrors errs))
      exitFailure
    Right () -> pure ()


