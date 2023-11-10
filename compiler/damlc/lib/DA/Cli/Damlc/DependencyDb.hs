-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Cli.Damlc.DependencyDb
    ( installDependencies
    , dependenciesDir
    , queryDalfs
    , mainMarker
    , depMarker
    , dataDepMarker
    ) where

import "zip-archive" Codec.Archive.Zip qualified as ZipArchive
import Control.Exception.Safe (tryAny)
import Control.Lens (toListOf)
import Control.Monad.Extra
import DA.Daml.Compiler.Dar
import DA.Daml.Compiler.ExtractDar (ExtractedDar(..), extractDar)
import DA.Daml.Helper.Ledger
import DA.Daml.LF.Ast qualified as LF
import DA.Daml.LF.Ast.Optics qualified as LF
import DA.Daml.LF.Proto3.Archive qualified as Archive
import DA.Daml.Options.Types
import DA.Daml.Package.Config
import DA.Service.Logger qualified as Logger
import DA.Pretty qualified
import Data.Aeson qualified as Aeson
import Data.Aeson (eitherDecodeFileStrict', encode)
import Data.ByteString qualified as BS
import Data.ByteString.Lazy qualified as BSL
import Data.Char
import Data.List.Extra
import Data.Map.Strict qualified as M
import Data.Maybe
import Data.Set qualified as Set
import Data.Text qualified as T
import Data.Yaml qualified as Yaml
import Development.IDE.Types.Location
import GHC.Fingerprint
import GHC.Generics
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

lockFile :: FilePath
lockFile = "daml.lock"

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
    logger <- getLogger opts "install-dependencies"
    deps <- expandSdkPackages logger (optDamlLfVersion opts) (filter (`notElem` basePackages) pDeps)
    DataDeps {dataDepsDars, dataDepsDalfs, dataDepsPkgIds, dataDepsNameVersion} <- readDataDeps pDataDeps
    (needsUpdate, newFingerprint) <-
        depsNeedUpdate
            depsDir
            (deps ++ dataDepsDars ++ dataDepsDalfs)
            dataDepsPkgIds
            dataDepsNameVersion
            thisSdkVer
            (show $ optDamlLfVersion opts)
    when needsUpdate $ do
        Logger.logDebug logger "Dependencies are not up2date, reinstalling"
        removePathForcibly depsDir
        createDirectoryIfMissing True depsDir
        -- install dependencies
        -----------------------
        Logger.logDebug logger "Extracting dependencies"
        depsExtracted <- mapM extractDar deps
        checkSdkVersions sdkVer depsExtracted
        Logger.logDebug logger "Installing dependencies"
        forM_ depsExtracted $ installDar depsDir False
        -- install data-dependencies
        ----------------------------
        Logger.logDebug logger "Extracting & installing data-dependency DARs"
        forM_ dataDepsDars $ extractDar >=> installDar depsDir True
        Logger.logDebug logger "Extracting & installing data-dependency DALFs"
        forM_ dataDepsDalfs $ \fp -> BS.readFile fp >>= installDataDepDalf False depsDir fp
        Logger.logDebug logger "Resolving package ids"
        resolvedPkgIds <- resolvePkgs projRoot opts dataDepsNameVersion
        Logger.logDebug logger "Querying package ids"
        exclPkgIds <- queryPkgIds Nothing depsDir
        Logger.logDebug logger "Fetching DALFs from ledger"
        rdalfs <- getDalfsFromLedger (optAccessTokenPath opts) (dataDepsPkgIds ++ M.elems resolvedPkgIds) exclPkgIds
        Logger.logDebug logger "Installing dalfs from ledger"
        forM_ rdalfs $ \RemoteDalf {..} -> do
            installDataDepDalf
                remoteDalfIsMain
                depsDir
                (packageNameToFp $ packageNameOrId remoteDalfPkgId remoteDalfName)
                remoteDalfBs
        -- Mark received packages as well as their transitive dependencies as data dependencies.
        Logger.logDebug logger "Mark data-dependencies"
        markAsDataRec
            (Set.fromList [remoteDalfPkgId | RemoteDalf {remoteDalfPkgId} <- rdalfs])
            Set.empty
        -- write new fingerprint
        Logger.logDebug logger "Updating fingerprint"
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
        unlessM (doesFileExist targetFp) $ do
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
    pkgId <- either (fail . DA.Pretty.renderPretty) pure $ Archive.decodeArchivePackageId bs
    pure $ T.unpack (LF.unPackageId pkgId) </> takeFileName fp

installDataDepDalf :: Bool -> FilePath -> FilePath -> BS.ByteString -> IO ()
installDataDepDalf isMain = installDalf ([dataDepMarker] ++ [mainMarker | isMain])

installDalf :: [FilePath] -> FilePath -> FilePath -> BS.ByteString -> IO ()
installDalf markers depsDir fp bs = do
    fileName <- dalfFileName bs fp
    let targetFp = depsDir </> fileName
    let targetDir = takeDirectory targetFp
    unlessM (doesDirectoryExist targetDir) $ write targetFp $ BSL.fromStrict bs
    forM_ markers $ \marker -> markDirWith marker targetDir

data DataDeps = DataDeps
    { dataDepsDars :: [FilePath]
    , dataDepsDalfs :: [FilePath]
    , dataDepsPkgIds :: [LF.PackageId]
    , dataDepsNameVersion :: [FullPkgName]
    }

readDataDeps :: [String] -> IO DataDeps
readDataDeps fpOrIds = do
    pkgIds <- forM pkgIds0 validatePkgId
    pure $
        DataDeps
            { dataDepsDars = dars
            , dataDepsDalfs = dalfs
            , dataDepsPkgIds = pkgIds
            , dataDepsNameVersion = pkgNameVersions
            }
  where
    (dars, rest) = partition ("dar" `isExtensionOf`) fpOrIds
    (dalfs, rest1) = partition ("dalf" `isExtensionOf`) rest
    (pkgNameVersions0, pkgIds0) = partition (':' `elem`) rest1
    pkgNameVersions =
        [ FullPkgName
            { pkgName = LF.PackageName $ T.pack $ strip pkgName
            , pkgVersion = LF.PackageVersion $ T.pack $ strip version
            }
        | pkgNameVersion <- pkgNameVersions0
        , (pkgName, _colon:version) <- [breakOn ":" pkgNameVersion]
        ]
    strip = dropWhileEnd isSpace . dropWhile isSpace


-- | A check that no bad package ID's are present in the data-dependency section of daml.yaml.
validatePkgId :: String -> IO LF.PackageId
validatePkgId pkgId = do
    unless (length pkgId == 64 && all (`elem` (['a' .. 'f'] ++ ['0' .. '9'])) pkgId) $
        fail $ "Invalid package ID dependency in daml.yaml: " <> pkgId
    pure $ LF.PackageId $ T.pack pkgId

-- Package resolution
---------------------

data FullPkgName = FullPkgName
    { pkgName :: !LF.PackageName
    , pkgVersion :: !LF.PackageVersion
    } deriving (Eq, Ord, Show)

data LockFile = LockFile
    { dependencies :: [DependencyInfo]
    } deriving Generic
instance Aeson.FromJSON LockFile
instance Aeson.ToJSON LockFile
data DependencyInfo = DependencyInfo
    { name :: LF.PackageName
    , version :: LF.PackageVersion
    , pkgId :: LF.PackageId
    } deriving Generic
instance Aeson.FromJSON DependencyInfo
instance Aeson.ToJSON DependencyInfo

-- | Resolves the given list of package names/versions to package IDs.
-- This will fail if any package can't be resolved.
-- Once all packages have been resolved, a new `daml.lock` file is noting the resolution.
resolvePkgs :: NormalizedFilePath -> Options -> [FullPkgName] -> IO (M.Map FullPkgName LF.PackageId)
resolvePkgs projRoot opts pkgs
    | null pkgs = pure M.empty
    | otherwise = do
        mbRes <- resolvePkgsWithLockFile lockFp pkgs
        resOrErr <- case mbRes of
          Nothing -> resolvePkgsWithLedger depsDir (optAccessTokenPath opts) pkgs
          Just res -> pure $ Right res
        case resOrErr of
            Left missing ->
                fail $
                unlines $
                "Unable to resolve the following packages in daml.yaml:" :
                [ (T.unpack $ LF.unPackageName pkgName) <> "-" <>
                (T.unpack $ LF.unPackageVersion pkgVersion)
                | FullPkgName {pkgName, pkgVersion} <- missing
                ]
            Right result -> do
              writeLockFile lockFile result
              pure result
  where
    depsDir = dependenciesDir opts projRoot
    lockFp = fromNormalizedFilePath projRoot </> lockFile

writeLockFile :: FilePath -> M.Map FullPkgName LF.PackageId -> IO ()
writeLockFile lockFp resolvedPkgs = do
    Yaml.encodeFile lockFp $
        LockFile
            [ DependencyInfo
                { name = pkgName
                , version = pkgVersion
                , pkgId = pkgId
                }
            | (FullPkgName {pkgName, pkgVersion}, pkgId) <- M.toList resolvedPkgs
            ]

-- | Read the package lock file and resolve the given packages.
-- Returns
--  Nothing -> At least one of the given packages could not be resolved.
--  Just map -> Resolution of all packages
--
-- It fails if the `daml.lock` file can't be parsed.
resolvePkgsWithLockFile :: FilePath -> [FullPkgName] -> IO (Maybe (M.Map FullPkgName LF.PackageId))
resolvePkgsWithLockFile lockFp pkgs = do
    hasLockFile <- doesFileExist lockFp
    if hasLockFile
        then do
            errOrLock <- Yaml.decodeFileEither lockFp
            case errOrLock of
                Left err ->
                    fail $
                    "Failed to parse daml.lock: " <> show err <> "\nTry to delete it an run again."
                Right LockFile {dependencies} -> do
                    let m =
                            M.fromList
                                [ ( FullPkgName
                                        { pkgName = name d
                                        , pkgVersion = version d
                                        }
                                  , pkgId d)
                                | d <- dependencies
                                ]
                    pure $
                        -- Check that all given packages could be resolved, return Nothing
                        -- otherwise.
                        if Set.fromList pkgs `Set.isSubsetOf` M.keysSet m
                            then Just $ M.restrictKeys m (Set.fromList pkgs)
                            else Nothing
        else pure Nothing

-- | Query the ledger for available packages and try to resolve the given packages. Returns either
-- the missing packages or a map from package names to package id.
resolvePkgsWithLedger ::
       FilePath -> Maybe FilePath -> [FullPkgName] -> IO (Either [FullPkgName] (M.Map FullPkgName LF.PackageId))
resolvePkgsWithLedger depsDir tokFpM pkgs = do
    ledgerPkgIds <- listLedgerPackages tokFpM
    rdalfs <- getDalfsFromLedger tokFpM ledgerPkgIds []
    forM_ rdalfs $ \RemoteDalf {..} ->
        installDalf
            []
            depsDir
            (packageNameToFp $ packageNameOrId remoteDalfPkgId remoteDalfName)
            remoteDalfBs
    let m =
            M.fromList
                [ ( FullPkgName
                        { pkgName = pkgName
                        , pkgVersion = version
                        }
                  , remoteDalfPkgId)
                | RemoteDalf {..} <- rdalfs
                , Just pkgName <- [remoteDalfName]
                , Just version <- [remoteDalfVersion]
                ]
    let m0 = M.restrictKeys m $ Set.fromList pkgs
    pure $
        if M.size m0 == length pkgs -- check that we resolved all packages.
            then Right m0
            else Left $ filter (\p -> p `M.notMember` m) pkgs

-- Ledger interactions
----------------------

getDalfsFromLedger :: Maybe FilePath -> [LF.PackageId] -> [LF.PackageId] -> IO [RemoteDalf]
getDalfsFromLedger tokFpM = runLedgerGetDalfs $ (defaultLedgerFlags Grpc) {fTokFileM = tokFpM}

listLedgerPackages :: Maybe FilePath -> IO [LF.PackageId]
listLedgerPackages tokFpM = runLedgerListPackages $ (defaultLedgerFlags Grpc) {fTokFileM = tokFpM}

-- Updating/Fingerprint
-----------------------
depsNeedUpdate ::
       FilePath
    -> [FilePath]
    -> [LF.PackageId]
    -> [FullPkgName]
    -> String
    -> String
    -> IO (Bool, Fingerprint)
depsNeedUpdate depsDir depFps dataDepsPkgIds dataDepsNameVersion sdkVersion damlLfVersion = do
    depsFps <- mapM getFileHash depFps
    let sdkVersionFp = fingerprintString sdkVersion
    let damlLfFp = fingerprintString damlLfVersion
    let dataDepsNameVersionFp =
            fingerprintFingerprints [fingerprintString $ show d | d <- dataDepsNameVersion]
    let dataDepsPkgIdsFp =
            fingerprintFingerprints $
            [fingerprintString $ T.unpack $ LF.unPackageId d | d <- dataDepsPkgIds]
    let fp =
            fingerprintFingerprints $
            sdkVersionFp : damlLfFp : dataDepsPkgIdsFp : dataDepsNameVersionFp : depsFps
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

packageNameToFp :: LF.PackageName -> FilePath
packageNameToFp n = (T.unpack $ LF.unPackageName n) <.> "dalf"

packageNameOrId :: LF.PackageId -> Maybe LF.PackageName -> LF.PackageName
packageNameOrId pkgId pkgNameM = fromMaybe (LF.PackageName $ LF.unPackageId pkgId) pkgNameM
