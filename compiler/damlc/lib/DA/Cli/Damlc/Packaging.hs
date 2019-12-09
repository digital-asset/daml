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
import Data.Bifunctor
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.UTF8 as BSUTF8
import Data.Graph
import Data.List.Extra
import qualified Data.Map.Strict as MS
import Data.Maybe
import qualified Data.Text.Extended as T
import Development.IDE.Core.Service (runAction)
import Development.IDE.Types.Location
import "ghc-lib-parser" Module (UnitId, primUnitId, stringToUnitId, unitIdString,)
import qualified Module as GHC
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
import DA.Daml.Compiler.DataDependencies
import qualified DA.Daml.LF.Ast as LF
import DA.Daml.LF.Ast.Optics (packageRefs)
import qualified DA.Daml.LF.Proto3.Archive as Archive
import DA.Daml.LF.Reader
import DA.Daml.Options.Types
import DA.Daml.Project.Consts
import qualified DA.Pretty
import SdkVersion

-- | Create the project package database containing the given dar packages.
--
-- We differentiate between two kinds of dependencies.
--
-- deps (depencies in daml.yaml):
--   This is intended for packages that include interface files and can
--   be used directly.
--   These packages have to be built with the same SDK version.
--
-- data-deps (data-dependencies in daml.yaml):
--   This is intended for packages that have already been uploaded to the
--   ledger. Based on the DAML-LF we generate dummy interface files
--   and then remap references to those dummy packages to the original DAML-LF
--   package id.
createProjectPackageDb ::
       Options -> PackageSdkVersion -> [FilePath] -> [FilePath] -> IO ()
createProjectPackageDb opts thisSdkVer deps dataDeps = do
    let dbPath = projectPackageDatabase </> lfVersionString (optDamlLfVersion opts)
    -- Since we reinitialize the whole package db anyway,
    -- during `daml init`, we clear the package db before to avoid
    -- issues during SDk upgrades. Once we have a more clever mechanism than
    -- reinitializing everything, we probably want to change this.
    removePathForcibly dbPath
    createDirectoryIfMissing True $ dbPath </> "package.conf.d"

    deps <- expandSdkPackages (filter (`notElem` basePackages) deps)
    depsExtracted <- mapM extractDar deps
    let uniqSdkVersions = nubSort $ unPackageSdkVersion thisSdkVer : map edSdkVersions depsExtracted
    let depsSdkVersions = map edSdkVersions depsExtracted
    unless (all (== unPackageSdkVersion thisSdkVer) depsSdkVersions) $
           fail $
           "Package dependencies from different SDK versions: " ++
           intercalate ", " uniqSdkVersions

    -- deal with data imports first
    let (fpDars, fpDalfs) = partition ((== ".dar") . takeExtension) dataDeps
    dars <- mapM extractDar fpDars
    -- These are the dalfs that are in a DAR that has been passed in via data-dependencies.
    let dalfsFromDars =
            [ ( dropExtension $ takeFileName $ ZipArchive.eRelativePath e
              , BSL.toStrict $ ZipArchive.fromEntry e
              )
            | e <- concatMap edDalfs dars
            ]
    -- These are dalfs that have been passed in directly as DALFs via data-dependencies.
    dalfsFromFps <-
        forM fpDalfs $ \fp -> do
            bs <- BS.readFile fp
            pure (dropExtension $ takeFileName fp, bs)
    let allDalfs = dalfsFromDars ++ dalfsFromFps
    pkgs <- forM allDalfs $ \(name, dalf) -> do
        (pkgId, package) <-
            either (fail . DA.Pretty.renderPretty) pure $
            Archive.decodeArchive Archive.DecodeAsMain dalf
        pure (pkgId, package, dalf, stringToUnitId (parseUnitId name pkgId))

    dbPathAbs <- makeAbsolute dbPath
    projectPackageDatabaseAbs <- makeAbsolute projectPackageDatabase

    let (depGraph, vertexToNode) = buildLfPackageGraph pkgs
    -- Iterate over the dependency graph in topological order.
    -- We do a topological sort on the transposed graph which ensures that
    -- the packages with no dependencies come first and we
    -- never process a package without first having processed its dependencies.
    forM_ (topSort $ transposeG depGraph) $ \vertex -> do
        let (pkgNode, pkgId) = vertexToNode vertex
        let unitIdStr = unitIdString $ unitId pkgNode
        unless (unitIdString primUnitId `isPrefixOf` unitIdStr) $ do
            let instancesUnitIdStr = "instances-" <> unitIdStr
            let pkgIdStr = T.unpack $ LF.unPackageId pkgId
            let (pkgName, mbPkgVersion) =
                    fromMaybe (unitIdStr, Nothing) $ do
                        (uId, ver) <- stripInfixEnd "-" unitIdStr
                        guard $ all (`elem` '.' : ['0' .. '9']) ver
                        Just (uId, Just ver)
            let deps =
                    [ unitIdString (unitId depPkgNode) <.> "dalf"
                    | (depPkgNode, pId) <- map vertexToNode $ reachable depGraph vertex
                    , pkgId /= pId
                    ]
            let workDir = dbPath </> unitIdStr <> "-" <> pkgIdStr
            createDirectoryIfMissing True workDir
            -- write the dalf package
            BS.writeFile (workDir </> unitIdStr <.> "dalf") $ encodedDalf pkgNode
            generateAndInstallIfaceFiles
                (dalf pkgNode)
                (stubSources pkgNode)
                opts
                workDir
                dbPath
                projectPackageDatabase
                unitIdStr
                pkgIdStr
                pkgName
                mbPkgVersion
                deps

            unless (null $ templateInstanceSources pkgNode) $
                generateAndInstallInstancesPkg
                    thisSdkVer
                    (templateInstanceSources pkgNode)
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

-- Expand SDK package dependencies using the SDK root path.
-- E.g. `daml-trigger` --> `$DAML_SDK/daml-libs/daml-trigger.dar`
-- When invoked outside of the SDK, we will only error out
-- if there is actually an SDK package so that
-- When there is no SDK
expandSdkPackages :: [FilePath] -> IO [FilePath]
expandSdkPackages dars = do
    mbSdkPath <- handleIO (\_ -> pure Nothing) $ Just <$> getSdkPath
    mapM (expand mbSdkPath) dars
  where
    isSdkPackage fp = takeExtension fp `notElem` [".dar", ".dalf"]
    expand mbSdkPath fp
      | isSdkPackage fp = case mbSdkPath of
            Just sdkPath -> pure $ sdkPath </> "daml-libs" </> fp <.> "dar"
            Nothing -> fail $ "Cannot resolve SDK dependency '" ++ fp ++ "'. Use daml assistant."
      | otherwise = pure fp

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
                  baseImports ++
                  [ PackageImport (GHC.stringToUnitId $ takeBaseName dep) True []
                  | dep <- deps
                  , not $ unitIdString primUnitId `isPrefixOf` dep
                  ]
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
                    , pSdkVersion = PackageSdkVersion "unknown"
                    }
                (map T.unpack $ LF.packageModuleNames dalf)
                pkgIdStr
    BS.writeFile (dbPath </> "package.conf.d" </> cfPath) cfBs
    ghcPkgPath <- getGhcPkgPath
    callProcess
        (ghcPkgPath </> exe "ghc-pkg")
        [ "recache"
        -- ghc-pkg insists on using a global package db and will try
        -- to find one automatically if we don’t specify it here.
        , "--global-package-db=" ++ (dbPath </> "package.conf.d")
        , "--expand-pkgroot"
        ]

baseImports :: [PackageImport]
baseImports =
    [ PackageImport (GHC.stringToUnitId "daml-prim") True []
    -- we need the standard library from the current sdk for the
    -- definition of the template class.
    , PackageImport
       (GHC.stringToUnitId damlStdlib)
       False
       (map (bimap GHC.mkModuleName GHC.mkModuleName)
          [ ("DA.Internal.Template", "Sdk.DA.Internal.Template")
          , ("DA.Internal.LF", "Sdk.DA.Internal.LF")
          , ("DA.Internal.Prelude", "Sdk.DA.Internal.Prelude")
          ]
       )
    ]

-- generate a package containing template instances and install it in the package database
generateAndInstallInstancesPkg
    :: PackageSdkVersion
    -> [(NormalizedFilePath, String)]
    -> Options
    -> FilePath
    -> FilePath
    -> String
    -> String
    -> String
    -> Maybe String
    -> [String]
    -> IO ()
generateAndInstallInstancesPkg thisSdkVer templInstSrc opts dbPathAbs projectPackageDatabaseAbs unitIdStr instancesUnitIdStr pkgName mbPkgVersion deps =
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
                          PackageImport (stringToUnitId unitIdStr) True [] :
                          baseImports ++
                          -- the following is for the edge case, when there is no standard
                          -- library dependency, but the dalf still uses builtins or builtin
                          -- types like Party.  In this case, we use the current daml-stdlib as
                          -- their origin.
                          [PackageImport (stringToUnitId damlStdlib) True [] | not $ any isStdlib deps] ++
                          [ PackageImport (stringToUnitId $ takeBaseName dep) True []
                          | dep <- deps
                          , not $ unitIdString primUnitId `isPrefixOf` dep
                          ]
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

isStdlib :: FilePath -> Bool
isStdlib = isJust . stripPrefix "daml-stdlib" . takeBaseName

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
getUnitId :: UnitId -> MS.Map LF.PackageId UnitId -> LF.PackageRef -> UnitId
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


-- | The graph will have an edge from package A to package B if A depends on B.
buildLfPackageGraph
    :: [(LF.PackageId, LF.Package, BS.ByteString, UnitId)]
    -> ( Graph
       , Vertex -> (PackageNode, LF.PackageId)
       )
buildLfPackageGraph pkgs = (depGraph,  vertexToNode')
  where
    -- mapping from package id's to unit id's. if the same package is imported with
    -- different unit id's, we would loose a unit id here.
    pkgMap = MS.fromList [(pkgId, unitId) | (pkgId, _pkg, _bs, unitId) <- pkgs]
    -- order the packages in topological order
    (depGraph, vertexToNode, _keyToVertex) =
        graphFromEdges
            [ (PackageNode src templInstSrc unitId dalf bs, pkgId, pkgRefs)
            | (pkgId, dalf, bs, unitId) <- pkgs
            , let pkgRefs = [ pid | LF.PRImport pid <- toListOf packageRefs dalf ]
            , let getUid = getUnitId unitId pkgMap
            , let src = generateSrcPkgFromLf getUid (Just "Sdk") dalf
            , let templInstSrc = generateTemplateInstancesPkgFromLf getUid (Just "Sdk") pkgId dalf
            ]
    vertexToNode' v = case vertexToNode v of
        -- We don’t care about outgoing edges.
        (node, key, _keys) -> (node, key)

data PackageNode = PackageNode
  { stubSources :: [(NormalizedFilePath, String)]
  -- ^ Sources for the stub package containining data type definitions
  , templateInstanceSources :: [(NormalizedFilePath, String)]
  -- ^ Sources for the package containing instances for Template, Choice, …
  , unitId :: UnitId
  , dalf :: LF.Package
  , encodedDalf :: BS.ByteString
  }
