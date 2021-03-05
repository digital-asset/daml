-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Cli.Damlc.DependencyDb
    ( installDependencies
    , dependenciesDir
    , queryDependencies
    , queryDependencyDalfs
    , queryDependencyMain
    , queryDataDependencies
    , mainDir
    , dalfsDir
    , configDir
    , sourcesDir
    ) where

import qualified "zip-archive" Codec.Archive.Zip as ZipArchive
import Control.Exception.Safe (tryAny)
import Control.Monad
import DA.Daml.Compiler.Dar
import DA.Daml.Compiler.DecodeDar (DecodedDalf(..), decodeDalf)
import DA.Daml.Compiler.ExtractDar (ExtractedDar(..), extractDar)
import qualified DA.Daml.LF.Ast as LF
import DA.Daml.Options.Types
import DA.Daml.Package.Config
import qualified DA.Pretty
import Data.Aeson (eitherDecodeFileStrict', encode)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Data.List.Extra
import Data.Set (Set)
import qualified Data.Set as Set
import qualified Data.Text.Extended as T
import Development.IDE.Types.Location
import GHC.Fingerprint
import "ghc-lib-parser" Module (unitIdString)
import System.Directory.Extra
import System.FilePath
import System.IO.Extra

-- Constants / Conventions
--------------------------
dependenciesDir :: Options -> NormalizedFilePath -> FilePath
dependenciesDir opts projRoot =
    fromNormalizedFilePath projRoot </> projectDependenciesDatabase </>
    lfVersionString (optDamlLfVersion opts)

fingerprintFile :: FilePath -> FilePath
fingerprintFile depsDir = depsDir </> "fingerprint.json"

mainDir, dalfsDir, configDir, sourcesDir, sdkVersionFile, depMarkerFile, dataDepMarkerFile :: FilePath -> FilePath
mainDir depPath = depPath </> "main"
dalfsDir depPath = depPath </> "dalfs"
configDir depPath = depPath </> "config"
sourcesDir depPath = depPath </> "sources"
sdkVersionFile depPath = depPath </> "sdk-version"
depMarkerFile depPath = depPath </> "_dependency_"
dataDepMarkerFile depPath = depPath </> "_data_dependency_"



-- Dependency installation
--------------------------
-- | Install all dependencies to the .daml/dependencies directory.
installDependencies ::
       NormalizedFilePath -> Options -> PackageSdkVersion -> [String] -> [String] -> IO ()
installDependencies projRoot opts sdkVer@(PackageSdkVersion thisSdkVer) pDeps pDataDeps = do
    deps <- expandSdkPackages (optDamlLfVersion opts) (filter (`notElem` basePackages) pDeps)
    (needsUpdate, newFingerprint) <-
        depsNeedUpdate depsDir (deps ++ pDataDeps) thisSdkVer (show $ optDamlLfVersion opts)
    when needsUpdate $ do
        removePathForcibly depsDir
        createDirectoryIfMissing True depsDir
        -- install dependencies
        -----------------------
        depsExtracted <- mapM extractDar deps
        checkSdkVersions sdkVer depsExtracted
        forM_ depsExtracted $ installDar depsDir False
        -- install data-dependencies
        ----------------------------
        let (fpDars, fpDalfs) = partition ((== ".dar") . takeExtension) pDataDeps
        forM_ fpDars $ extractDar >=> installDar depsDir True
        forM_ fpDalfs $ installDataDepDalf depsDir
        -- write new fingerprint
        write (fingerprintFile depsDir) $ encode newFingerprint
  where
    depsDir = dependenciesDir opts projRoot

-- | Check that only one sdk version is present in dependencies and it equals this sdk version.
checkSdkVersions :: PackageSdkVersion -> [ExtractedDar] -> IO ()
checkSdkVersions (PackageSdkVersion thisSdkVer) depsExtracted = do
    let uniqSdkVersions = nubSort $ thisSdkVer : map edSdkVersions depsExtracted
    let depsSdkVersions = map edSdkVersions depsExtracted
    unless (all (== thisSdkVer) depsSdkVersions) $
        fail $
        "Package dependencies from different SDK versions: " ++ intercalate ", " uniqSdkVersions

-- Install a dar dependency
installDar :: FilePath -> Bool -> ExtractedDar -> IO ()
installDar depsPath isDataDep ExtractedDar {..} = do
    let bs = BSL.toStrict $ ZipArchive.fromEntry edMain
    let fp = ZipArchive.eRelativePath edMain
    DecodedDalf {decodedUnitId, decodedDalfPkg} <- either fail pure $ decodeDalf Set.empty fp bs
    let relDepPath =
            unitIdString decodedUnitId <> "-" <>
            (T.unpack $ LF.unPackageId $ LF.dalfPackageId decodedDalfPkg)
    let depPath = depsPath </> relDepPath
    if isDataDep
        then write (dataDepMarkerFile depPath) ""
        else write (depMarkerFile depPath) ""
    let path = mainDir depPath </> (takeFileName $ ZipArchive.eRelativePath edMain)
    write path (ZipArchive.fromEntry edMain)
    forM_ edConfFiles $ \conf ->
        write
            (configDir depPath </> (takeFileName $ ZipArchive.eRelativePath conf))
            (ZipArchive.fromEntry conf)
    forM_ edSrcs $ \src ->
        write
            (sourcesDir depPath </> ZipArchive.eRelativePath src)
            (ZipArchive.fromEntry src)
    forM_ edDalfs $ \dalf ->
        write
            (dalfsDir depPath </> ZipArchive.eRelativePath dalf)
            (ZipArchive.fromEntry dalf)
    writeFileUTF8 (sdkVersionFile depPath) edSdkVersions

installDataDepDalf :: FilePath -> FilePath -> IO ()
installDataDepDalf depsDir fp = do
    bs <- BS.readFile fp
    DecodedDalf {decodedUnitId, decodedDalfPkg} <- either fail pure $ decodeDalf Set.empty fp bs
    let depDir =
            depsDir </> unitIdString decodedUnitId <> "-" <>
            (T.unpack $ LF.unPackageId $ LF.dalfPackageId decodedDalfPkg)
    copy fp (mainDir depDir </> takeFileName fp)
    copy fp (dalfsDir depDir </> takeFileName fp)
    write (dataDepMarkerFile depDir) ""

-- Updating/Fingerprint
-----------------------
depsNeedUpdate :: FilePath -> [FilePath] -> String -> String -> IO (Bool, Fingerprint)
depsNeedUpdate depsDir depFps sdkVersion damlLfVersion = do
    depsFps <- mapM getFileHash depFps
    let sdkVersionFp = fingerprintString sdkVersion
    let damlLfFp = fingerprintString damlLfVersion
    let fp = fingerprintFingerprints $ sdkVersionFp : damlLfFp : depsFps
  -- Read the metadata of an already existing package database and see if wee need to reinitialize.
    errOrFingerprint <- tryAny $ readDepsFingerprint depsDir
    pure $
        case errOrFingerprint of
            Left _err -> (True, fp)
            Right fp0 -> (fp0 /= fp, fp)

readDepsFingerprint :: FilePath -> IO Fingerprint
readDepsFingerprint depsDir = do
    errOrFp <- eitherDecodeFileStrict' (fingerprintFile depsDir)
    case errOrFp of
        Right fp -> pure fp
        Left err -> fail ("Could not decode fingerprint metadata: " <> err)

-- Queries
----------
queryDependencies :: FilePath -> IO [FilePath]
queryDependencies depsPath = do
    allDirs <- listDirectories depsPath
    filterM (\f -> do doesFileExist $ depMarkerFile f) allDirs

queryDataDependencies :: FilePath -> IO [FilePath]
queryDataDependencies depsPath = do
    allDirs <- listDirectories depsPath
    filterM (\f -> do doesFileExist $ dataDepMarkerFile f) allDirs

queryDependencyDalfFiles :: FilePath -> IO [FilePath]
queryDependencyDalfFiles depPath = do
    listFilesRecursive $ dalfsDir depPath

queryDependencyDalfs :: Set LF.PackageId -> FilePath -> IO [DecodedDalf]
queryDependencyDalfs pkgs dep = do
    fs <- queryDependencyDalfFiles dep
    forM fs $ \f -> do
        bs <- BS.readFile f
        either fail pure $ decodeDalf pkgs f bs

queryDependencyMain :: Set LF.PackageId -> FilePath -> IO DecodedDalf
queryDependencyMain pkgs dep = do
    fps <- listFilesRecursive $ mainDir dep
    let fp =
            headDef
                (fail "Corrupted dependency database. Please run `daml clean` and try again.")
                fps
    bs <- BS.readFile fp
    either fail pure $ decodeDalf pkgs fp bs

-- Utilities
------------
lfVersionString :: LF.Version -> String
lfVersionString = DA.Pretty.renderPretty

write :: FilePath -> BSL.ByteString -> IO ()
write fp bs = createDirectoryIfMissing True (takeDirectory fp) >> BSL.writeFile fp bs

copy :: FilePath -> FilePath -> IO ()
copy src target = do
  createDirectoryIfMissing True (takeDirectory target)
  copyFile src target
