module DA.Cli.Damlc.Command.CheckUpgrades where

import DA.Pretty
import DA.Daml.Options.Types
import Control.Monad (forM_, guard)
import Development.IDE.Types.Location (NormalizedFilePath, fromNormalizedFilePath, toNormalizedFilePath')
import DA.Daml.Compiler.ExtractDar (extractDar,ExtractedDar(..))
import qualified Data.ByteString.Lazy as BSL
import qualified "zip-archive" Codec.Archive.Zip as ZipArchive
import qualified DA.Daml.LF.Proto3.Archive as Archive
import qualified DA.Daml.LF.Ast as LF
import Control.Exception
import Data.Either.Extra (mapLeft, fromRight', lefts, rights)
import DA.Daml.LF.Ast.Util
import DA.Daml.LF.Ast.Version as LFV
import DA.Daml.LF.TypeChecker.Upgrade as Upgrade
import Data.List (tails)
import Data.Maybe (mapMaybe)
import Safe (maximumByMay, minimumByMay)
import Data.Function (on)
import Development.IDE.Types.Diagnostics (Diagnostic, showDiagnostics, ShowDiagnostic(..))

data DecodingError
  = DECantReadDar NormalizedFilePath String
  | DECantReadDalf NormalizedFilePath FilePath Archive.ArchiveError
  deriving (Show, Eq)

instance Pretty DecodingError where
  pPrint (DECantReadDar darPath err) = "Error reading DAR at path " <> string (fromNormalizedFilePath darPath) <> ": " <> string err
  pPrint (DECantReadDalf darPath dalfPath err) =
    vcat
      [ "Error reading DALF " <> string dalfPath <> " inside DAR " <> string (fromNormalizedFilePath darPath) <> ":"
      , pPrint err
      ]

data CheckingError
  = CEDependencyCycle [(LF.PackageId, LF.PackageName, Maybe LF.PackageVersion)]
  | CEDiagnostic NormalizedFilePath [Diagnostic]
  deriving (Show, Eq)

instance Pretty CheckingError where
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

printErr :: Pretty a => a -> IO ()
printErr = putStrLn . renderPretty

runCheckUpgrades :: [String] -> IO ()
runCheckUpgrades rawPaths = do
  let paths = map toNormalizedFilePath' rawPaths
  let decodeEntryWithUnitId darPath decodeAs entry =
        mapLeft (DECantReadDalf darPath (ZipArchive.eRelativePath entry)) $ do
            let bs = BSL.toStrict $ ZipArchive.fromEntry entry
            (pkgId, pkg) <- Archive.decodeArchive decodeAs bs
            let (pkgName, mbPkgVersion) = LF.safePackageMetadata pkg
            pure (pkgId, pkg, pkgName, mbPkgVersion)
  let readPathToArchive :: NormalizedFilePath -> IO (Either [DecodingError]
            (NormalizedFilePath, (LF.PackageId, LF.Package, LF.PackageName, Maybe LF.PackageVersion),
             [(LF.PackageId, LF.Package, LF.PackageName, Maybe LF.PackageVersion)]))
      readPathToArchive path = do
        errExtractedDar <- try @IOException $ extractDar (fromNormalizedFilePath path)
        pure $ case errExtractedDar of
          Right (ExtractedDar{edMain,edDalfs}) ->
            let errMain = decodeEntryWithUnitId path Archive.DecodeAsMain edMain
                errDalfs = map (decodeEntryWithUnitId path Archive.DecodeAsDependency) edDalfs
                errs = lefts (errMain : errDalfs)
            in
            if not (null errs)
            then Left errs
            else Right (path, fromRight' errMain, map fromRight' errDalfs)
          Left err -> Left [DECantReadDar path (displayException err)]
  errDars <- traverse readPathToArchive paths
  let errDarsErrs = lefts errDars
  if not (null errDarsErrs)
    then mapM_ printErr errDarsErrs
    else do
      let packages = rights errDars
      let withIdAndPkg x@(_path, (pkgId, pkg, _name, _version), _deps) = (pkgId, x, pkg)
          withoutIdAndPkg (_pkgId, x, _pkg) = x
      case topoSortPackages (map withIdAndPkg packages) of
        Left cyclePkgs -> do
          let cyclePkgToDep (_, (pkgId, _, name, mbVersion), _) = (pkgId, name, mbVersion)
          printErr (CEDependencyCycle (map (cyclePkgToDep . withoutIdAndPkg) cyclePkgs))
        Right sortedWithPkgIdAndPkg -> do
          let sortedPackages = map withoutIdAndPkg sortedWithPkgIdAndPkg
          let sortedPackagesWithPastPackages = mapMaybe go (reverse (tails (reverse sortedPackages)))
                where
                go [] = Nothing
                go (x:xs) = Just (x, xs)
          forM_ sortedPackagesWithPastPackages $ \((path, main, deps), pastPackages) -> do
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
                let pastPackageWithLowerVersion = fmap snd $ maximumByMay ordFst $ pastPackageFilterVersion (\v -> v < rawVersion)
                let pastPackageWithHigherVersion = fmap snd $ minimumByMay ordFst $ pastPackageFilterVersion (\v -> v > rawVersion)
                case pastPackageWithLowerVersion of
                  Just past ->
                    printErr $ CEDiagnostic path $ Upgrade.checkPackage mainPkg deps LFV.version2_dev (UpgradeInfo (Just (fromNormalizedFilePath path)) True True) (Just past)
                  Nothing -> pure ()
                case pastPackageWithHigherVersion of
                  Just past ->
                    printErr $ CEDiagnostic path $ Upgrade.checkPackage mainPkg deps LFV.version2_dev (UpgradeInfo (Just (fromNormalizedFilePath path)) True True) (Just past)
                  Nothing -> pure ()
          pure ()
