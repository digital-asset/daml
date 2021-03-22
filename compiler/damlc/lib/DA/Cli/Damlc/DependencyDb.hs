-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Cli.Damlc.DependencyDb
    ( installDependencies
    , dependenciesDir
    , queryDalfs
    , mainMarker
    , depMarker
    , dataDepMarker
    ) where

import qualified "zip-archive" Codec.Archive.Zip as ZipArchive
import Control.Exception.Safe (tryAny)
import Control.Lens (toListOf)
import Control.Monad.Extra
import DA.Daml.Compiler.Dar
import DA.Daml.Compiler.DecodeDar (DecodedDalf(..), decodeDalf)
import DA.Daml.Compiler.ExtractDar (ExtractedDar(..), extractDar)
import DA.Daml.Helper.Ledger
import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.Ast.Optics as LF
import qualified DA.Daml.LF.Proto3.Archive as Archive
import DA.Daml.Options.Types
import DA.Daml.Package.Config
import qualified DA.Pretty
import Data.Aeson (eitherDecodeFileStrict', encode)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Data.List.Extra
import qualified Data.Set as Set
import qualified Data.Text as T
import Development.IDE.Types.Location
import GHC.Fingerprint
import System.Directory.Extra
import System.FilePath
import System.IO.Extra

-- Dependency Database Layout
-----------------------------
-- Here is an exemplary dependecy database:
-- .
-- ├── 057eed1fd48c238491b8ea06b9b5bf85a5d4c9275dd3f6183e0e6b01730cc2ba
-- │   ├── daml-stdlib-DA-Internal-Down-057eed1fd48c238491b8ea06b9b5bf85a5d4c9275dd3f6183e0e6b01730cc2ba.dalf
-- │   └── _pkg_
-- ├── 368191e500d560749859180bb4788d8d5dcfff0e6357ac54d0cc9ceaa5aeb1ce
-- │   ├── daml-script-0.0.0-368191e500d560749859180bb4788d8d5dcfff0e6357ac54d0cc9ceaa5aeb1ce
-- │   │   └── Daml
-- │   │       ├── Script
-- │   │       │   ├── Free.daml
-- │   │       │   ├── Free.hi
-- │   │       │   └── Free.hie
-- │   │       ├── Script.daml
-- │   │       ├── Script.hi
-- │   │       └── Script.hie
-- │   ├── daml-script-0.0.0-368191e500d560749859180bb4788d8d5dcfff0e6357ac54d0cc9ceaa5aeb1ce.dalf
-- │   ├── daml-script-0.0.0.conf
-- │   ├── _main_
-- │   ├── _pkg_
-- │   └── sdk-version
-- ├── 3811221efbc11637b3e6e36a4c28774e762f852b5f2cca144b5518a1e4b11b85
-- │   ├── _data_
-- │   ├── _main_
-- │   ├── _pkg_
-- │   ├── proj2-0.0.1-3811221efbc11637b3e6e36a4c28774e762f852b5f2cca144b5518a1e4b11b85
-- │   │   ├── Baz.daml
-- │   │   ├── Baz.hi
-- │   │   └── Baz.hie
-- │   ├── proj2-0.0.1-3811221efbc11637b3e6e36a4c28774e762f852b5f2cca144b5518a1e4b11b85.dalf
-- │   ├── proj2-0.0.1.conf
-- │   └── sdk-version
-- ├── 40f452260bef3f29dede136108fc08a88d5a5250310281067087da6f0baddff7
-- │   ├── daml-prim-DA-Types-40f452260bef3f29dede136108fc08a88d5a5250310281067087da6f0baddff7.dalf
-- │   └── _pkg_
-- └── fingerprint.json
--
-- There are three different marker files:
-- _main_: This directory contains the main dalf of a dependency.
-- _pkg: This directory contains a dalf coming from a normal dependency.
-- _data_: This directory contains a dalf coming from a data-dependency.
-- Sources and config files are stored in the directory containing the main dalf of the package.

-- Constants / Conventions
--------------------------
dependenciesDir :: Options -> NormalizedFilePath -> FilePath
dependenciesDir opts projRoot =
    fromNormalizedFilePath projRoot </> projectDependenciesDatabase </>
    lfVersionString (optDamlLfVersion opts)

fingerprintFile :: FilePath
fingerprintFile = "fingerprint.json"

sdkVersionFile :: FilePath
sdkVersionFile = "sdk-version"

mainMarker :: FilePath
mainMarker = "_main_"

depMarker :: FilePath
depMarker = "_pkg_"

dataDepMarker :: FilePath
dataDepMarker = "_data_"

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
    DataDeps {dataDepsDars, dataDepsDalfs, dataDepsPkgIds} <- readDataDeps pDataDeps
    (needsUpdate, newFingerprint) <-
        depsNeedUpdate
            depsDir
            (deps ++ dataDepsDars ++ dataDepsDalfs)
            dataDepsPkgIds
            thisSdkVer
            (show $ optDamlLfVersion opts)
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
        forM_ dataDepsDars $ extractDar >=> installDar depsDir True
        forM_ dataDepsDalfs $ \fp -> BS.readFile fp >>= installDataDepDalf True depsDir fp
        exclPkgIds <- queryPkgIds Nothing depsDir
        rdalfs <- getDalfsFromLedger dataDepsPkgIds exclPkgIds
        forM_ rdalfs $ \RemoteDalf {..} -> do
            installDataDepDalf remoteDalfIsMain depsDir (remoteDalfName <.> "dalf") remoteDalfBs
        -- Mark received packages as well as their transitive dependencies as data dependencies.
        markAsDataRec
            (Set.fromList [remoteDalfPkgId | RemoteDalf {remoteDalfPkgId} <- rdalfs])
            Set.empty
        -- write new fingerprint
        write (depsDir </> fingerprintFile) $ encode newFingerprint
  where
    markAsDataRec :: Set.Set LF.PackageId -> Set.Set LF.PackageId -> IO ()
    markAsDataRec pkgIds processed = do
        case Set.minView pkgIds of
            Nothing -> pure ()
            Just (pkgId, rest) -> do
                if pkgId `Set.member` processed
                    then markAsDataRec rest processed
                    else do
                        let pkgIdStr = T.unpack $ LF.unPackageId pkgId
                        let depDir = depsDir </> pkgIdStr
                        markDirWith dataDepMarker depDir
                        fs <- filter ("dalf" `isExtensionOf`) <$> listFilesRecursive depDir
                        forM_ fs $ \fp -> do
                            bs <- BS.readFile fp
                            (_pid, pkg) <-
                                either
                                    (const $ fail $ "Failed to decode dalf package " <> pkgIdStr)
                                    pure $
                                Archive.decodeArchive Archive.DecodeAsDependency bs
                            markAsDataRec
                                (packageRefs pkg `Set.union` rest)
                                (Set.insert pkgId processed)
    packageRefs pkg = Set.fromList [pid | LF.PRImport pid <- toListOf LF.packageRefs pkg]
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
    fp <- dalfFileNameFromEntry edMain
    let depPath = takeDirectory $ depsPath </> fp
    createDirectoryIfMissing True depPath
    if isDataDep
      then markDirWith dataDepMarker depPath
      else markDirWith depMarker depPath
    markDirWith mainMarker depPath
    forM_ edConfFiles $ \conf -> do
        write
            (depPath </> (takeFileName $ ZipArchive.eRelativePath conf))
            (ZipArchive.fromEntry conf)
    forM_ edSrcs $ \src ->
        write (depPath </> ZipArchive.eRelativePath src) (ZipArchive.fromEntry src)
    forM_ edDalfs $ \dalf -> do
        fp <- dalfFileNameFromEntry dalf
        let targetFp = depsPath </> fp
        let targetDir = takeDirectory targetFp
        if isDataDep
          then markDirWith dataDepMarker targetDir
          else markDirWith depMarker targetDir
        write targetFp (ZipArchive.fromEntry dalf)
    writeFileUTF8 (depPath </> sdkVersionFile) edSdkVersions

dalfFileNameFromEntry :: ZipArchive.Entry -> IO FilePath
dalfFileNameFromEntry entry =
    dalfFileName (BSL.toStrict $ ZipArchive.fromEntry entry) (ZipArchive.eRelativePath entry)

dalfFileName :: BS.ByteString -> FilePath -> IO FilePath
dalfFileName bs fp = do
    DecodedDalf {decodedDalfPkg} <- either fail pure $ decodeDalf Set.empty fp bs
    let pkgId = T.unpack $ LF.unPackageId $ LF.dalfPackageId decodedDalfPkg
    pure $ pkgId </> takeFileName fp

installDataDepDalf :: Bool -> FilePath -> FilePath -> BS.ByteString -> IO ()
installDataDepDalf isMain depsDir fp bs = do
    fileName <- dalfFileName bs fp
    let targetFp = depsDir </> fileName
    let targetDir = takeDirectory targetFp
    unlessM (doesDirectoryExist targetDir) $ write targetFp $ BSL.fromStrict bs
    -- if the directory exists, the dalf got already installed as a dependency
    when isMain $ markDirWith mainMarker targetDir
    markDirWith dataDepMarker targetDir

data DataDeps = DataDeps
    { dataDepsDars :: [FilePath]
    , dataDepsDalfs :: [FilePath]
    , dataDepsPkgIds :: [LF.PackageId]
    }

readDataDeps :: [String] -> IO DataDeps
readDataDeps fpOrIds = do
    pkgIds <- forM pkgIds0 validatePkgId
    pure $ DataDeps {dataDepsDars = dars, dataDepsDalfs = dalfs, dataDepsPkgIds = pkgIds}
  where
    (dars, rest) = partition ("dar" `isExtensionOf`) fpOrIds
    (dalfs, pkgIds0) = partition ("dalf" `isExtensionOf`) rest

-- | A check that no bad package ID's are present in the data-dependency section of daml.yaml.
validatePkgId :: String -> IO LF.PackageId
validatePkgId pkgId = do
    unless (length pkgId == 64 && all (`elem` (['a' .. 'f'] ++ ['0' .. '9'])) pkgId) $
        fail $ "Invalid package ID dependency in daml.yaml: " <> pkgId
    pure $ LF.PackageId $ T.pack pkgId

-- Ledger interactions
----------------------

getDalfsFromLedger :: [LF.PackageId] -> [LF.PackageId] -> IO [RemoteDalf]
getDalfsFromLedger = runLedgerGetDalfs (defaultLedgerFlags Grpc)

-- Updating/Fingerprint
-----------------------
depsNeedUpdate ::
       FilePath -> [FilePath] -> [LF.PackageId] -> String -> String -> IO (Bool, Fingerprint)
depsNeedUpdate depsDir depFps dataDepsPkgIds sdkVersion damlLfVersion = do
    depsFps <- mapM getFileHash depFps
    let sdkVersionFp = fingerprintString sdkVersion
    let damlLfFp = fingerprintString damlLfVersion
    let dataDepsPkgIdsFp =
            fingerprintFingerprints $
            [fingerprintString $ T.unpack $ LF.unPackageId d | d <- dataDepsPkgIds]
    let fp = fingerprintFingerprints $ sdkVersionFp : damlLfFp : dataDepsPkgIdsFp : depsFps
  -- Read the metadata of an already existing package database and see if wee need to reinitialize.
    errOrFingerprint <- tryAny $ readDepsFingerprint depsDir
    pure $
        case errOrFingerprint of
            Left _err -> (True, fp)
            Right fp0 -> (fp0 /= fp, fp)

readDepsFingerprint :: FilePath -> IO Fingerprint
readDepsFingerprint depsDir = do
    errOrFp <- eitherDecodeFileStrict' (depsDir </> fingerprintFile)
    case errOrFp of
        Right fp -> pure fp
        Left err -> fail ("Could not decode fingerprint metadata: " <> err)

-- Queries
----------

queryDalfs :: Maybe [FilePath] -> FilePath -> IO [FilePath]
queryDalfs markersM dir = do
    guardDefM [] (doesDirectoryExist dir) $ do
        dalfs <- filter ("dalf" `isExtensionOf`) <$> listFilesRecursive dir
        case markersM of
            Nothing -> pure dalfs
            Just markers -> do
                filterM
                    (\fp ->
                         fmap and $
                         forM markers $ \marker -> doesFileExist $ takeDirectory fp </> marker)
                    dalfs

queryPkgIds :: Maybe [FilePath] -> FilePath -> IO [LF.PackageId]
queryPkgIds markersM dir = do
    fps <- queryDalfs markersM dir
    pure
        [ LF.PackageId $ T.pack pkgId
        | fp <- fps
        , _dalf:pkgId:_rest <- [splitDirectories fp]
        ]

-- Utilities
------------
lfVersionString :: LF.Version -> String
lfVersionString = DA.Pretty.renderPretty

write :: FilePath -> BSL.ByteString -> IO ()
write fp bs = createDirectoryIfMissing True (takeDirectory fp) >> BSL.writeFile fp bs

markDirWith :: FilePath -> FilePath -> IO ()
markDirWith marker fp = write (fp </> marker) ""

guardDefM :: Monad m => a -> m Bool -> m a -> m a
guardDefM def pM m = ifM pM m (pure def)
