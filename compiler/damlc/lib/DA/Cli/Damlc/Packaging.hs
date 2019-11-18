-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Cli.Damlc.Packaging
  ( ExtractedDar(..)
  , extractDar

  , createProjectPackageDb

  , getEntry
  , mbErr
  , getUnitId
  ) where

import qualified "zip" Codec.Archive.Zip as Zip
import qualified "zip-archive" Codec.Archive.Zip as ZipArchive
import Control.Exception.Safe (handleIO)
import Control.Lens (toListOf)
import Control.Monad
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.UTF8 as BSUTF8
import Data.Graph
import Data.List.Extra
import qualified Data.Map.Strict as MS
import Data.Maybe
import qualified Data.Text.Extended as T
import Development.IDE.Core.Service (runAction)
import Development.IDE.Types.Location
import "ghc-lib-parser" Module (UnitId, primUnitId, stringToUnitId, unitIdString)
import System.Directory.Extra
import System.Exit
import System.FilePath
import System.Info.Extra
import System.IO.Extra
import System.Process (callProcess)

import DA.Bazel.Runfiles
import DA.Cli.Damlc.Base
import DA.Cli.Damlc.IdeState
import DA.Daml.Compiler.Dar
import DA.Daml.Compiler.Upgrade
import qualified DA.Daml.LF.Ast as LF
import DA.Daml.LF.Ast.Optics (packageRefs)
import qualified DA.Daml.LF.Proto3.Archive as Archive
import DA.Daml.LF.Reader
import DA.Daml.Options.Types
import DA.Daml.Project.Consts
import qualified DA.Pretty
import SdkVersion

-- | Create the project package database containing the given dar packages.
createProjectPackageDb ::
       Options -> String -> [FilePath] -> [FilePath] -> IO ()
createProjectPackageDb opts thisSdkVer deps0 dataDeps = do
    let dbPath = projectPackageDatabase </> (lfVersionString $ optDamlLfVersion opts)
    let
    -- Since we reinitialize the whole package db anyway,
    -- during `daml init`, we clear the package db before to avoid
    -- issues during SDk upgrades. Once we have a more clever mechanism than
    -- reinitializing everything, we probably want to change this.
    removePathForcibly dbPath
    createDirectoryIfMissing True $ dbPath </> "package.conf.d"
    -- Expand SDK package dependencies using the SDK root path.
    -- E.g. `daml-trigger` --> `$DAML_SDK/daml-libs/daml-trigger.dar`
    -- Or, fail if not run from DAML assistant.
    mbSdkPath <- handleIO (\_ -> pure Nothing) $ Just <$> getSdkPath
    let isSdkPackage fp = takeExtension fp `notElem` [".dar", ".dalf"]
        handleSdkPackages :: [FilePath] -> IO [FilePath]
        handleSdkPackages =
          let expand fp
                | isSdkPackage fp
                = case mbSdkPath of
                    Just sdkPath -> pure $! sdkPath </> "daml-libs" </> fp <.> "dar"
                    Nothing -> fail $ "Cannot resolve SDK dependency '" ++ fp ++ "'. Use daml-assistant."
                | otherwise
                = pure fp
          in mapM expand
    deps <- handleSdkPackages $ filter (`notElem` basePackages) deps0
    depsExtracted <- mapM extractDar deps
    let uniqSdkVersions = nubSort $ filter (/= "0.0.0") $ thisSdkVer : map edSdkVersions depsExtracted
    -- we filter the 0.0.0 version because otherwise integration tests fail that import SDK packages
    unless (length uniqSdkVersions <= 1) $
           fail $
           "Package dependencies from different SDK versions: " ++
           intercalate ", " uniqSdkVersions

    -- deal with data imports first
    let (fpDars, fpDalfs) = partition ((== ".dar") . takeExtension) dataDeps
    dars <- mapM extractDar fpDars
    let dalfs = concatMap edDalfs dars
    -- when we compile packages with different sdk versions or with dalf dependencies, we
    -- need to generate the interface files
    let dalfsFromDars =
            [ ( dropExtension $ takeFileName $ ZipArchive.eRelativePath e
              , BSL.toStrict $ ZipArchive.fromEntry e)
            | e <- dalfs
            ]
    dalfsFromFps <-
        forM fpDalfs $ \fp -> do
            bs <- B.readFile fp
            pure (dropExtension $ takeFileName fp, bs)
    let allDalfs = dalfsFromDars ++ dalfsFromFps
    pkgs <-
        forM allDalfs $ \(name, dalf) -> do
            (pkgId, package) <-
                either (fail . DA.Pretty.renderPretty) pure $
                Archive.decodeArchive Archive.DecodeAsMain dalf
            pure (pkgId, package, dalf, stringToUnitId name)
    -- mapping from package id's to unit id's. if the same package is imported with
    -- different unit id's, we would loose a unit id here.
    let pkgMap =
            MS.fromList
                [(pkgId, unitId) | (pkgId, _pkg, _bs, unitId) <- pkgs]
    -- order the packages in topological order
    let (depGraph, vertexToNode, _keyToVertex) =
            graphFromEdges $ do
                (pkgId, dalf, bs, unitId) <- pkgs
                let pkgRefs =
                        [ pid
                        | LF.PRImport pid <- toListOf packageRefs dalf
                        ]
                let getUid = getUnitId unitId pkgMap
                let src = generateSrcPkgFromLf getUid pkgId dalf
                let templInstSrc =
                        generateTemplateInstancesPkgFromLf
                            getUid
                            pkgId
                            dalf
                pure
                    ( (src, templInstSrc, unitId, dalf, bs)
                    , pkgId
                    , pkgRefs)
    let pkgIdsTopoSorted = reverse $ topSort depGraph
    dbPathAbs <- makeAbsolute dbPath
    projectPackageDatabaseAbs <- makeAbsolute projectPackageDatabase
    forM_ pkgIdsTopoSorted $ \vertex -> do
        let ((src, templInstSrc, uid, dalf, bs), pkgId, _) =
                vertexToNode vertex
        when (uid /= primUnitId) $ do
            let unitIdStr = unitIdString uid
            let instancesUnitIdStr = "instances-" <> unitIdStr
            let pkgIdStr = T.unpack $ LF.unPackageId pkgId
            let (pkgName, mbPkgVersion) =
                    fromMaybe (unitIdStr, Nothing) $ do
                        (uId, ver) <- stripInfixEnd "-" unitIdStr
                        guard $ all (`elem` '.' : ['0' .. '9']) ver
                        Just (uId, Just ver)
            let deps =
                    [ unitIdString uId <.> "dalf"
                    | ((_src, _templSrc, uId, _dalf, _bs), pId, _) <-
                          map vertexToNode $ reachable depGraph vertex
                    , pkgId /= pId
                    ]
            let workDir = dbPath </> unitIdStr <> "-" <> pkgIdStr
            createDirectoryIfMissing True workDir
            -- write the dalf package
            B.writeFile (workDir </> unitIdStr <.> "dalf") bs
            generateAndInstallIfaceFiles
                dalf
                src
                opts
                workDir
                dbPath
                projectPackageDatabase
                unitIdStr
                pkgIdStr
                pkgName
                mbPkgVersion
                deps

            unless (null templInstSrc) $
                generateAndInstallInstancesPkg
                    templInstSrc
                    opts
                    dbPathAbs
                    projectPackageDatabaseAbs
                    unitIdStr
                    instancesUnitIdStr
                    pkgName
                    mbPkgVersion
                    deps

    -- finally install the dependecies
    forM_ depsExtracted $
        \ExtractedDar{..} -> installDar dbPath edConfFiles edDalfs edSrcs
  where
    -- generate interface files and install them in the package database
    generateAndInstallIfaceFiles ::
           LF.Package
        -> [(NormalizedFilePath, String)]
        -> Options
        -> FilePath
        -> FilePath
        -> FilePath
        -> String
        -> String
        -> String
        -> Maybe String
        -> [String]
        -> IO ()
    generateAndInstallIfaceFiles dalf src opts workDir dbPath projectPackageDatabase unitIdStr pkgIdStr pkgName mbPkgVersion deps = do
        loggerH <- getLogger opts "generate interface files"
        let src' = [ (toNormalizedFilePath $ workDir </> fromNormalizedFilePath nfp, str) | (nfp, str) <- src]
        mapM_ writeSrc src'
        opts' <-
            mkOptions $
            opts
                { optWriteInterface = False
                , optPackageDbs = projectPackageDatabase : optPackageDbs opts
                , optIfaceDir = Nothing
                , optIsGenerated = True
                , optDflagCheck = False
                , optMbPackageName = Just unitIdStr
                , optHideAllPkgs = True
                , optGhcCustomOpts = []
                , optPackageImports =
                      ("daml-prim", True, []) :
                      -- the following is for the edge case, when there is no standard library
                      -- dependency, but the dalf still uses builtins or builtin types like Party.
                      -- In this case, we use the current daml-stdlib as their origin.
                      [(damlStdlib, True, []) | not $ hasStdlibDep deps]  ++
                      [(takeBaseName dep, True, []) | dep <- deps]
                }

        _ <- withDamlIdeState opts' loggerH diagnosticsLogger $ \ide ->
            runAction ide $
            writeIfacesAndHie
                (toNormalizedFilePath "./")
                [fp | (fp, _content) <- src']
        -- write the conf file and refresh the package cache
        let (cfPath, cfBs) =
                mkConfFile
                    PackageConfigFields
                        { pName = pkgName
                        , pSrc = "" -- not used
                        , pExposedModules = Nothing
                        , pVersion = mbPkgVersion
                        , pDependencies = deps
                        , pDataDependencies = []
                        , pSdkVersion = "unknown"
                        , cliOpts = Nothing
                        }
                    (map T.unpack $ LF.packageModuleNames dalf)
                    pkgIdStr
        B.writeFile (dbPath </> "package.conf.d" </> cfPath) cfBs
        ghcPkgPath <- getGhcPkgPath
        callProcess
            (ghcPkgPath </> exe "ghc-pkg")
            [ "recache"
            -- ghc-pkg insists on using a global package db and will try
            -- to find one automatically if we don’t specify it here.
            , "--global-package-db=" ++ (dbPath </> "package.conf.d")
            , "--expand-pkgroot"
            ]

    -- generate a package containing template instances and install it in the package database
    generateAndInstallInstancesPkg ::
           [(NormalizedFilePath, String)]
        -> Options
        -> FilePath
        -> FilePath
        -> String
        -> String
        -> String
        -> Maybe String
        -> [String]
        -> IO ()
    generateAndInstallInstancesPkg templInstSrc opts dbPathAbs projectPackageDatabaseAbs unitIdStr instancesUnitIdStr pkgName mbPkgVersion deps =
        withTempDir $ \tempDir ->
            withCurrentDirectory tempDir $ do
                loggerH <- getLogger opts "generate instances package"
                mapM_ writeSrc templInstSrc
                let pkgConfig =
                        PackageConfigFields
                            { pName = "instances-" <> pkgName
                            , pSrc = "."
                            , pExposedModules = Nothing
                            , pVersion = mbPkgVersion
                            , pDependencies = (unitIdStr <.> "dalf") : deps
                            , pDataDependencies = []
                            , pSdkVersion = thisSdkVer
                            , cliOpts = Nothing
                            }
                opts' <-
                    mkOptions $
                    opts
                        { optWriteInterface = True
                        , optPackageDbs = projectPackageDatabaseAbs : optPackageDbs opts
                        , optIfaceDir = Just "./"
                        , optIsGenerated = True
                        , optDflagCheck = False
                        , optMbPackageName = Just instancesUnitIdStr
                        , optHideAllPkgs = True
                        , optPackageImports =
                              ("daml-prim", True, []) :
                              (unitIdStr, True, []) :
                              -- we need the standard library from the current sdk for the
                              -- definition of the template class.
                              [ ( damlStdlib
                                , False
                                , [("DA.Internal.Template", "Sdk.DA.Internal.Template") ])
                              ] ++
                              -- the following is for the edge case, when there is no standard
                              -- library dependency, but the dalf still uses builtins or builtin
                              -- types like Party.  In this case, we use the current daml-stdlib as
                              -- their origin.
                              [(damlStdlib, True, []) | not $ hasStdlibDep deps] ++
                              [(takeBaseName dep, True, []) | dep <- deps]
                        }
                mbDar <-
                    withDamlIdeState opts' loggerH diagnosticsLogger $ \ide ->
                        buildDar
                            ide
                            pkgConfig
                            (toNormalizedFilePath $
                             fromMaybe ifaceDir $ optIfaceDir opts')
                            (FromDalf False)
                dar <- mbErr "ERROR: Creation of instances DAR file failed." mbDar
              -- TODO (drsk) switch to different zip library so we don't have to write
              -- the dar.
                let darFp = instancesUnitIdStr <.> "dar"
                Zip.createArchive darFp dar
                ExtractedDar{..} <- extractDar darFp
                installDar dbPathAbs edConfFiles edDalfs edSrcs

    hasStdlibDep deps =
        any (\dep -> isJust $ stripPrefix "daml-stdlib" $ takeBaseName dep) deps


data ExtractedDar = ExtractedDar
    { edSdkVersions :: String
    , edMain :: [ZipArchive.Entry]
    , edConfFiles :: [ZipArchive.Entry]
    , edDalfs :: [ZipArchive.Entry]
    , edSrcs :: [ZipArchive.Entry]
    }


-- | Extract a dar archive
extractDar :: FilePath -> IO ExtractedDar
extractDar fp = do
    bs <- BSL.readFile fp
    let archive = ZipArchive.toArchive bs
    manifest <- getEntry manifestPath archive
    dalfManifest <- either fail pure $ readDalfManifest archive
    mainDalfEntry <- getEntry (mainDalfPath dalfManifest) archive
    sdkVersion <-
        case parseManifestFile $ BSL.toStrict $ ZipArchive.fromEntry manifest of
            Left err -> fail err
            Right manifest ->
                case lookup "Sdk-Version" manifest of
                    Nothing -> fail "No Sdk-Version entry in manifest"
                    Just version -> pure $! trim $ BSUTF8.toString version
    let confFiles =
            [ e
            | e <- ZipArchive.zEntries archive
            , ".conf" `isExtensionOf` ZipArchive.eRelativePath e
            ]
    let srcs =
            [ e
            | e <- ZipArchive.zEntries archive
            , takeExtension (ZipArchive.eRelativePath e) `elem`
                  [".daml", ".hie", ".hi"]
            ]
    dalfs <- forM (dalfPaths dalfManifest) $ \p -> getEntry p archive
    pure (ExtractedDar sdkVersion [mainDalfEntry] confFiles dalfs srcs)

-- | A helper to construct package ref to unit id maps.
getUnitId :: UnitId -> MS.Map LF.PackageId UnitId -> (LF.PackageRef -> UnitId)
getUnitId thisUnitId pkgMap =
    \case
        LF.PRSelf -> thisUnitId
        LF.PRImport pId ->
            fromMaybe
                (error $
                 "Unknown package id: " <> (T.unpack $ LF.unPackageId pId)) $
            MS.lookup pId pkgMap

-- Install a dar in the package database
installDar ::
       FilePath
    -> [ZipArchive.Entry]
    -> [ZipArchive.Entry]
    -> [ZipArchive.Entry]
    -> IO ()
installDar dbPath confFiles dalfs srcs = do
    forM_ dalfs $ \dalf -> do
        let path = dbPath </> ZipArchive.eRelativePath dalf
        createDirectoryIfMissing True (takeDirectory path)
        BSL.writeFile path (ZipArchive.fromEntry dalf)
    forM_ confFiles $ \conf ->
        BSL.writeFile
            (dbPath </> "package.conf.d" </>
             (takeFileName $ ZipArchive.eRelativePath conf))
            (ZipArchive.fromEntry conf)
    forM_ srcs $ \src -> do
        let path = dbPath </> ZipArchive.eRelativePath src
        write path (ZipArchive.fromEntry src)
    ghcPkgPath <- getGhcPkgPath
    callProcess
        (ghcPkgPath </> exe "ghc-pkg")
        [ "recache"
              -- ghc-pkg insists on using a global package db and will try
              -- to find one automatically if we don’t specify it here.
        , "--global-package-db=" ++ (dbPath </> "package.conf.d")
        , "--expand-pkgroot"
        ]
  where
    write fp bs =
        createDirectoryIfMissing True (takeDirectory fp) >> BSL.writeFile fp bs

-- | Write generated source files
writeSrc :: (NormalizedFilePath, String) -> IO ()
writeSrc (fp, content) = do
    let path = fromNormalizedFilePath fp
    createDirectoryIfMissing True $ takeDirectory path
    writeFileUTF8 path content

-- | Locate ghc-pkg
getGhcPkgPath :: IO FilePath
getGhcPkgPath =
    if isWindows
        then locateRunfiles "rules_haskell_ghc_windows_amd64/bin"
        else locateRunfiles "ghc_nix/lib/ghc-8.6.5/bin"

-- | Fail with an exit failure and errror message when Nothing is returned.
mbErr :: String -> Maybe a -> IO a
mbErr err = maybe (hPutStrLn stderr err >> exitFailure) pure

-- | Get an entry from a dar or fail.
getEntry :: FilePath -> ZipArchive.Archive -> IO ZipArchive.Entry
getEntry fp dar =
    maybe (fail $ "Package does not contain " <> fp) pure $
    ZipArchive.findEntryByPath fp dar

lfVersionString :: LF.Version -> String
lfVersionString = DA.Pretty.renderPretty
