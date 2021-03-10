-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Cli.Damlc.DependencyDb
    ( installDependencies
    , dependenciesDir
    , queryDalfsFromDependencies
    , queryDalfsFromDataDependencies
    , queryMainDalfs
    , mainsDir
    , dalfsDir
    , configsDir
    , sourcesDir
    , normalDepsDir
    ) where

import qualified "zip-archive" Codec.Archive.Zip as ZipArchive
import Control.Exception.Safe (tryAny)
import Control.Monad.Extra
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
import qualified Data.Set as Set
import qualified Data.Text.Extended as T
import Development.IDE.Types.Location
import GHC.Fingerprint
import System.Directory.Extra
import System.FilePath
import System.IO.Extra

-- Dependency Database Layout
-----------------------------
-- Here is an exemplary dependecy database:
-- .
-- ├── data-deps
-- │   ├── configs
-- │   │   └── proj2-0.0.1.conf
-- │   ├── dalfs
-- │   │   ├── 057eed1fd48c238491b8ea06b9b5bf85a5d4c9275dd3f6183e0e6b01730cc2ba
-- │   │   │   └── daml-stdlib-DA-Internal-Down-057eed1fd48c238491b8ea06b9b5bf85a5d4c9275dd3f6183e0e6b01730cc2ba.dalf
-- │   │   ├── 3c1af853a9bc7d6aa214b98f0e4d2099187bd5fa7ab07852b4165b84a82325d3
-- │   │   │   └── proj2-0.0.1-3c1af853a9bc7d6aa214b98f0e4d2099187bd5fa7ab07852b4165b84a82325d3.dalf
-- │   │   └── e9eeb8cf890e5a15bc65031f1a8373a4bb7f89229473dabaa595c25d35768b21
-- │   │       └── daml-prim-e9eeb8cf890e5a15bc65031f1a8373a4bb7f89229473dabaa595c25d35768b21.dalf
-- │   ├── mains
-- │   │   └── 3c1af853a9bc7d6aa214b98f0e4d2099187bd5fa7ab07852b4165b84a82325d3
-- │   │       └── proj2-0.0.1-3c1af853a9bc7d6aa214b98f0e4d2099187bd5fa7ab07852b4165b84a82325d3.dalf
-- │   ├── sdk-version
-- │   └── sources
-- │       └── proj2-0.0.1-3c1af853a9bc7d6aa214b98f0e4d2099187bd5fa7ab07852b4165b84a82325d3
-- │           ├── Baz.daml
-- │           ├── Baz.hi
-- │           └── Baz.hie
-- ├── deps
-- │   ├── configs
-- │   │   └── daml-script-0.0.0.conf
-- │   ├── dalfs
-- │   │   ├── 057eed1fd48c238491b8ea06b9b5bf85a5d4c9275dd3f6183e0e6b01730cc2ba
-- │   │   │   └── daml-stdlib-DA-Internal-Down-057eed1fd48c238491b8ea06b9b5bf85a5d4c9275dd3f6183e0e6b01730cc2ba.dalf
-- │   │   ├── 40f452260bef3f29dede136108fc08a88d5a5250310281067087da6f0baddff7
-- │   │   │   └── daml-prim-DA-Types-40f452260bef3f29dede136108fc08a88d5a5250310281067087da6f0baddff7.dalf
-- │   ├── mains
-- │   │   └── bf4a8f6f897b4d9639bab0f384ea7c06a7ad4b6b856cb0b511e1d79c9c46ecf2
-- │   │       └── daml-script-0.0.0-bf4a8f6f897b4d9639bab0f384ea7c06a7ad4b6b856cb0b511e1d79c9c46ecf2.dalf
-- │   ├── sdk-version
-- │   └── sources
-- │       └── daml-script-0.0.0-bf4a8f6f897b4d9639bab0f384ea7c06a7ad4b6b856cb0b511e1d79c9c46ecf2
-- │           └── Daml
-- │               ├── Script
-- │               │   ├── Free.daml
-- │               │   ├── Free.hi
-- │               │   └── Free.hie
-- │               ├── Script.daml
-- │               ├── Script.hi
-- │               └── Script.hie
-- └── fingerprint.json

-- The database is flattened, meaning that we collect all dalfs/mains under the `dalfs`/`mains`
-- directory and all sources under the `sources` directory. Dalf filepath are prefixed with their
-- package id like in `dalfs/package_id/name.dalf`.
--
-- The sdk-version file stores the used SDK version.
-- The fingerprint.json file detects changes to the dependencies/daml-lf-version/sdk-version and is
-- used for caching.
--
-- Normal dependencies are under the `deps` directory, while data-dependencies are under the
-- `data-deps` directory.

-- Constants / Conventions
--------------------------
dependenciesDir :: Options -> NormalizedFilePath -> FilePath
dependenciesDir opts projRoot =
    fromNormalizedFilePath projRoot </> projectDependenciesDatabase </>
    lfVersionString (optDamlLfVersion opts)

fingerprintFile :: FilePath -> FilePath
fingerprintFile depsDir = depsDir </> "fingerprint.json"

mainsDir, dalfsDir, configsDir, sourcesDir, sdkVersionFile, dataDepsDir, normalDepsDir :: FilePath -> FilePath
mainsDir depPath = depPath </> "mains"
dalfsDir depPath = depPath </> "dalfs"
configsDir depPath = depPath </> "configs"
sourcesDir depPath = depPath </> "sources"
sdkVersionFile depPath = depPath </> "sdk-version"
dataDepsDir depPath = depPath </> "data-deps"
normalDepsDir depPath = depPath </> "deps"

-- Dependency installation
--------------------------
-- | Install all dependencies to the .daml/dependencies directory.
installDependencies ::
   NormalizedFilePath
   -> Options
   -> PackageSdkVersion
   -> [String] -- Package dependencies. Can be base-packages, sdk-packages or filepath.
   -> [FilePath] -- Data Dependencies. Can be filepath to dars/dalfs.
   -> IO ()
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
    let depPath
            | isDataDep = dataDepsDir depsPath
            | otherwise = normalDepsDir depsPath
    fp <- dalfFileNameFromEntry edMain
    write (mainsDir depPath </> fp) (ZipArchive.fromEntry edMain)
    forM_ edConfFiles $ \conf -> do
        write
            (configsDir depPath </> (takeFileName $ ZipArchive.eRelativePath conf))
            (ZipArchive.fromEntry conf)
    forM_ edSrcs $ \src ->
        write (sourcesDir depPath </> ZipArchive.eRelativePath src) (ZipArchive.fromEntry src)
    forM_ edDalfs $ \dalf -> do
        fp <- dalfFileNameFromEntry dalf
        write (dalfsDir depPath </> fp) (ZipArchive.fromEntry dalf)
    writeFileUTF8 (sdkVersionFile depPath) edSdkVersions

dalfFileNameFromEntry :: ZipArchive.Entry -> IO FilePath
dalfFileNameFromEntry entry =
    dalfFileName (BSL.toStrict $ ZipArchive.fromEntry entry) (ZipArchive.eRelativePath entry)

dalfFileName :: BS.ByteString -> FilePath -> IO FilePath
dalfFileName bs fp = do
    DecodedDalf {decodedDalfPkg} <- either fail pure $ decodeDalf Set.empty fp bs
    let pkgId = T.unpack $ LF.unPackageId $ LF.dalfPackageId decodedDalfPkg
    pure $ pkgId </> takeFileName fp

installDataDepDalf :: FilePath -> FilePath -> IO ()
installDataDepDalf depsDir fp = do
    bs <- BS.readFile fp
    fileName <- dalfFileName bs fp
    let depDir = dataDepsDir depsDir
    copy fp (mainsDir depDir </> fileName)
    copy fp (dalfsDir depDir </> fileName)

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

queryDalfs :: FilePath -> Set.Set LF.PackageId -> IO [DecodedDalf]
queryDalfs dir pkgIds = do
    ifM (doesDirectoryExist dir)
        (do dalfs <- listFilesRecursive dir
            forM dalfs $ \dalf -> do
                bs <- BS.readFile dalf
                either fail pure $ decodeDalf pkgIds dalf bs)
        (pure [])

queryDalfsFromDependencies :: FilePath -> Set.Set LF.PackageId -> IO [DecodedDalf]
queryDalfsFromDependencies = queryDalfs . dalfsDir . normalDepsDir

queryDalfsFromDataDependencies :: FilePath -> Set.Set LF.PackageId -> IO [DecodedDalf]
queryDalfsFromDataDependencies = queryDalfs . dalfsDir . dataDepsDir

queryMainDalfs :: FilePath -> Set.Set LF.PackageId -> IO [DecodedDalf]
queryMainDalfs depsDir pkgIds = do
    ds0 <- queryDalfs (mainsDir $ normalDepsDir depsDir) pkgIds
    ds1 <- queryDalfs (mainsDir $ dataDepsDir depsDir) pkgIds
    pure $ ds0 ++ ds1

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
