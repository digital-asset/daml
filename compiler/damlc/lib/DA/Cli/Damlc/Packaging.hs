-- Copyright (c) 2020 The DAML Authors. All rights reserved.
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
import Control.Exception.Extra
import Control.Lens (toListOf)
import Control.Monad.Extra
import Control.Monad.IO.Class
import Control.Monad.Trans.Maybe
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.UTF8 as BSUTF8
import Data.Graph
import Data.List.Extra
import qualified Data.Map.Strict as MS
import Data.Maybe
import qualified Data.NameMap as NM
import Data.Set (Set)
import qualified Data.Set as Set
import qualified Data.Text.Extended as T
import Development.IDE.Core.Rules (useE, useNoFileE)
import Development.IDE.Core.Service (runActionSync)
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
import DA.Daml.Compiler.DataDependencies as DataDeps
import qualified DA.Daml.LF.Ast as LF
import DA.Daml.LF.Ast.Optics (packageRefs)
import qualified DA.Daml.LF.Proto3.Archive as Archive
import DA.Daml.LF.Reader
import DA.Daml.Options.Types
import qualified DA.Pretty
import Development.IDE.Core.RuleTypes.Daml
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
createProjectPackageDb :: NormalizedFilePath -> Options -> PackageSdkVersion -> [FilePath] -> [FilePath] -> IO ()
createProjectPackageDb projectRoot opts thisSdkVer deps dataDeps
  | null dataDeps && all (`elem` basePackages) deps =
    -- Initializing the package db is expensive since it requires calling GenerateStablePackages and GeneratePackageMap.
    --Therefore we only do it if we actually have a dependency.
    clearPackageDb
  | otherwise = do
    clearPackageDb
    deps <- expandSdkPackages (filter (`notElem` basePackages) deps)
    depsExtracted <- mapM extractDar deps

    let uniqSdkVersions = nubSort $ unPackageSdkVersion thisSdkVer : map edSdkVersions depsExtracted
    let depsSdkVersions = map edSdkVersions depsExtracted
    unless (all (== unPackageSdkVersion thisSdkVer) depsSdkVersions) $
           fail $
           "Package dependencies from different SDK versions: " ++
           intercalate ", " uniqSdkVersions

    -- Register deps at the very beginning. This allows data-dependencies to
    -- depend on dependencies which is necessary so that we can reconstruct typeclass
    -- instances for a typeclass defined in a library.
    -- It does mean that we can’t have a dependency from a dependency on a
    -- data-dependency but that seems acceptable.
    -- See https://github.com/digital-asset/daml/issues/4218 for more details.
    -- TODO Enforce this with useful error messages
    forM_ depsExtracted $
        -- We only have the interface files for the main DALF in a `dependency` so we
        -- also only extract the main dalf.
        \ExtractedDar{..} -> installDar dbPath edConfFiles edMain edSrcs

    loggerH <- getLogger opts "generate package maps"
    mbRes <- withDamlIdeState opts loggerH diagnosticsLogger $ \ide -> runActionSync ide $ runMaybeT $
        (,) <$> useNoFileE GenerateStablePackages
            <*> useE GeneratePackageMap  projectRoot
    (stablePkgs, dependencies) <- maybe (fail "Failed to generate package info") pure mbRes
    let stablePkgIds :: Set LF.PackageId
        stablePkgIds = Set.fromList $ map LF.dalfPackageId $ MS.elems stablePkgs
    -- This includes both SDK dependencies like daml-prim and daml-stdlib but also DARs specified
    -- in the dependencies field.
    let dependencyPkgIdMap :: MS.Map UnitId LF.PackageId
        dependencyPkgIdMap = MS.map LF.dalfPackageId dependencies
    let dependencyPkgIds = Set.fromList $ MS.elems dependencyPkgIdMap

    -- Now handle data imports.
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
    pkgs <- flip mapMaybeM allDalfs $ \(name, dalf) -> runMaybeT $ do
        (pkgId, package) <-
            liftIO $
            either (fail . DA.Pretty.renderPretty) pure $
            Archive.decodeArchive Archive.DecodeAsDependency dalf
        -- daml-prim and daml-stdlib are somewhat special:
        --
        -- We always have daml-prim and daml-stdlib from the current SDK and we
        -- cannot control their unit id since that would require recompiling them.
        -- However, we might also have daml-prim and daml-stdlib in a different version
        -- in a DAR we are depending on. Luckily, we can control the unit id there.
        -- To avoid colliding unit ids which will confuse GHC (or rather hide
        -- one of them), we instead include the package hash in the unit id.
        --
        -- In principle, we can run into the same issue if you combine "dependencies"
        -- (which have precompiled interface files) and
        -- "data-dependencies". However, there you can get away with changing the
        -- package name and version to change the unit id which is not possible for
        -- daml-prim.
        --
        -- If the version of daml-prim/daml-stdlib in a data-dependency is the same
        -- as the one we are currently compiling against, we don’t need to apply this
        -- hack.
        let parsedUnitId = parseUnitId name pkgId
        let unitId = stringToUnitId $ case splitUnitId parsedUnitId of
              ("daml-prim", Nothing) | pkgId `Set.notMember` dependencyPkgIds -> "daml-prim-" <> T.unpack (LF.unPackageId pkgId)
              ("daml-stdlib", _) | pkgId `Set.notMember` dependencyPkgIds -> "daml-stdlib-" <> T.unpack (LF.unPackageId pkgId)
              _ -> parsedUnitId
        pure (pkgId, package, dalf, unitId)

    -- All transitive packages from DARs specified in  `dependencies`. This is only used for unit-id collision checks.
    transitiveDependencies <- fmap concat $ forM depsExtracted $ \ExtractedDar{..} -> forM edDalfs $ \zipEntry -> do
       let bytes = BSL.toStrict $ ZipArchive.fromEntry zipEntry
       pkgId <- liftIO $
            either (fail . DA.Pretty.renderPretty) pure $
            Archive.decodeArchivePackageId bytes
       let unitId = parseUnitId (takeBaseName $ ZipArchive.eRelativePath zipEntry) pkgId
       pure (pkgId, stringToUnitId unitId)

    let unitIdConflicts = MS.filter ((>=2) . Set.size) .  MS.fromListWith Set.union $ concat
            [ [ (unitId, Set.singleton pkgId)
              | (pkgId, _package, _dalf, unitId) <- pkgs ]
            , [ (unitId, Set.singleton (LF.dalfPackageId dalfPkg))
              | (unitId, dalfPkg) <- MS.toList dependencies ]
            , [ (unitId, Set.singleton pkgId)
              | (pkgId, unitId) <- transitiveDependencies
              ]
            ]
    when (not $ MS.null unitIdConflicts) $ do
        fail $ "Transitive dependencies with same unit id but conflicting package ids: "
            ++ intercalate ", "
                [ show k <> " [" <> intercalate "," (map show (Set.toList v)) <> "]"
                | (k,v) <- MS.toList unitIdConflicts ]

    let (depGraph, vertexToNode) = buildLfPackageGraph pkgs stablePkgs dependencies
    -- Iterate over the dependency graph in topological order.
    -- We do a topological sort on the transposed graph which ensures that
    -- the packages with no dependencies come first and we
    -- never process a package without first having processed its dependencies.
    forM_ (topSort $ transposeG depGraph) $ \vertex ->
      let (pkgNode, pkgId) = vertexToNode vertex in
      -- stable packages are mapped to the current version of daml-prim/daml-stdlib
      -- so we don’t need to generate interface files for them.
      unless (pkgId `Set.member` stablePkgIds || pkgId `Set.member` dependencyPkgIds) $ do
        let unitIdStr = unitIdString $ unitId pkgNode
        let _instancesUnitIdStr = "instances-" <> unitIdStr
        let pkgIdStr = T.unpack $ LF.unPackageId pkgId
        let (pkgName, mbPkgVersion) = splitUnitId unitIdStr
        let deps =
                [ unitIdString (unitId depPkgNode) <.> "dalf"
                | (depPkgNode, depPkgId) <- map vertexToNode $ reachable depGraph vertex
                , pkgId /= depPkgId
                , not (depPkgId `Set.member` stablePkgIds)
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
            dependencies
  where
    dbPath = projectPackageDatabase </> lfVersionString (optDamlLfVersion opts)
    clearPackageDb = do
        -- Since we reinitialize the whole package db during `daml init` anyway,
        -- we clear the package db before to avoid
        -- issues during SDk upgrades. Once we have a more clever mechanism than
        -- reinitializing everything, we probably want to change this.
        removePathForcibly dbPath
        createDirectoryIfMissing True $ dbPath </> "package.conf.d"

toGhcModuleName :: LF.ModuleName -> GHC.ModuleName
toGhcModuleName = GHC.mkModuleName . T.unpack . LF.moduleNameString

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
    -> MS.Map UnitId LF.DalfPackage
    -> IO ()
generateAndInstallIfaceFiles dalf src opts workDir dbPath projectPackageDatabase unitIdStr pkgIdStr pkgName mbPkgVersion deps dependencies = do
    loggerH <- getLogger opts "generate interface files"
    let src' = [ (toNormalizedFilePath $ workDir </> fromNormalizedFilePath nfp, str) | (nfp, str) <- src]
    mapM_ writeSrc src'
    -- We expose dependencies under a Pkg_$pkgId prefix so we can unambiguously refer to them
    -- while avoiding name collisions in package imports.
    -- TODO (MK)
    -- Use this scheme to refer to data-dependencies as well and replace the CurrentSdk prefix by this.
    let depImps =
            [ exposePackage unitId False
                  [ (toGhcModuleName modName, toGhcModuleName (prefixDependencyModule dalfPackageId modName))
                  | mod <- NM.toList $ LF.packageModules $ LF.extPackagePkg dalfPackagePkg
                  , let modName = LF.moduleName mod
                  ]
            | (unitId, LF.DalfPackage{..}) <- MS.toList dependencies]
    opts <-
        pure $ opts
            { optIfaceDir = Nothing
            -- We write ifaces below using writeIfacesAndHie so we don’t need to enable these options.
            , optPackageDbs = projectPackageDatabase : optPackageDbs opts
            , optIsGenerated = True
            , optDflagCheck = False
            , optMbPackageName = Just unitIdStr
            , optGhcCustomOpts = []
            , optPackageImports =
                  baseImports ++
                  depImps ++
                  [ exposePackage (GHC.stringToUnitId $ takeBaseName dep) True []
                  | dep <- deps
                  ]
            -- When compiling dummy interface files for a data-dependency,
            -- we know all package flags so we don’t need to infer anything.
            , optInferDependantPackages = InferDependantPackages False
            }

    res <- withDamlIdeState opts loggerH diagnosticsLogger $ \ide ->
        runActionSync ide $
        -- Setting ifDir to . means that the interface files will end up directly next to
        -- the source files which is what we want here.
        writeIfacesAndHie
            (toNormalizedFilePath ".")
            [fp | (fp, _content) <- src']
    when (isNothing res) $
      errorIO $ "Failed to compile interface for data-dependency: " <> unitIdStr
    -- write the conf file and refresh the package cache
    (cfPath, cfBs) <-
            mkConfFile
                PackageConfigFields
                    { pName = pkgName
                    , pSrc = error "src field was used for creation of pkg conf file"
                    , pExposedModules = Nothing
                    , pVersion = mbPkgVersion
                    , pDependencies = deps
                    , pDataDependencies = []
                    , pSdkVersion = error "sdk version field was used for creation of pkg conf file"
                    }
                (map T.unpack $ LF.packageModuleNames dalf)
                pkgIdStr
    BS.writeFile (dbPath </> "package.conf.d" </> cfPath) cfBs
    recachePkgDb dbPath

recachePkgDb :: FilePath -> IO ()
recachePkgDb dbPath = do
    ghcPkgPath <- getGhcPkgPath
    callProcess
        (ghcPkgPath </> exe "ghc-pkg")
        [ "recache"
        -- ghc-pkg insists on using a global package db and will try
        -- to find one automatically if we don’t specify it here.
        , "--global-package-db=" ++ (dbPath </> "package.conf.d")
        , "--expand-pkgroot"
        ]

-- TODO We should generate the list of stable packages automatically here.
baseImports :: [PackageFlag]
baseImports =
    [ exposePackage
        (GHC.stringToUnitId "daml-prim")
        False
        (map (\mod -> (GHC.mkModuleName mod, GHC.mkModuleName (currentSdkPrefix <> "." <> mod)))
           [ "GHC.Tuple"
           , "GHC.Types"
           , "DA.Types"
           , "DA.Internal.Erased"
           , "GHC.Err"
           , "Data.String"
           ]
        )
    -- We need the standard library from the current SDK, e.g., LF builtins like Optional are translated
    -- to types in the current standard library.
    , exposePackage
       (GHC.stringToUnitId damlStdlib)
       False
       (map (\mod -> (GHC.mkModuleName mod, GHC.mkModuleName (currentSdkPrefix <> "." <> mod)))
          [ "DA.Internal.Any"
          , "DA.Internal.Template"
          , "DA.Internal.Template.Functions"
          , "DA.Internal.LF"
          , "DA.Internal.Prelude"
          , "DA.Internal.Down"
          , "DA.NonEmpty.Types"
          , "DA.Semigroup.Types"
          , "DA.Monoid.Types"
          , "DA.Logic.Types"
          , "DA.Validation.Types"
          , "DA.Date.Types"
          , "DA.Time.Types"
          ]
       )
    ]

-- generate a package containing template instances and install it in the package database
-- See the comment on the call site above for why this is disabled.
_generateAndInstallInstancesPkg
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
_generateAndInstallInstancesPkg thisSdkVer templInstSrc opts dbPath projectPackageDatabase unitIdStr instancesUnitIdStr pkgName mbPkgVersion deps = do
    -- Given that the instances package generates actual code, we first build a DAR and then
    -- install that in the package db like any other DAR in `dependencies`.
    --
    -- Note that this will go away once we have finished the packaging rework
    -- since we will only generate dummy interfaces.

    -- We build in a temp dir to avoid cluttering a directory with the source files and the .daml folder
    -- created for the build. Therefore, we have to make dbPath and projectPackageDatabase absolute.
    dbPathAbs <- makeAbsolute dbPath
    projectPackageDatabaseAbs <- makeAbsolute projectPackageDatabase

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
            opts <-
                pure $
                opts
                    { optIfaceDir = Nothing
                    , optPackageDbs = projectPackageDatabaseAbs : optPackageDbs opts
                    , optIsGenerated = True
                    , optDflagCheck = False
                    , optMbPackageName = Just instancesUnitIdStr
                    , optPackageImports =
                          exposePackage (stringToUnitId unitIdStr) True [] :
                          baseImports ++
                          -- the following is for the edge case, when there is no standard
                          -- library dependency, but the dalf still uses builtins or builtin
                          -- types like Party.  In this case, we use the current daml-stdlib as
                          -- their origin.
                          [exposePackage (stringToUnitId damlStdlib) True [] | not $ any isStdlib deps] ++
                          [ exposePackage (stringToUnitId $ takeBaseName dep) True []
                          | dep <- deps
                          , not $ unitIdString primUnitId `isPrefixOf` dep
                          ]
                    }
            mbDar <-
                withDamlIdeState opts loggerH diagnosticsLogger $ \ide ->
                    buildDar
                        ide
                        pkgConfig
                        (toNormalizedFilePath $
                         fromMaybe ifaceDir $ optIfaceDir opts)
                        (FromDalf False)
            dar <- mbErr "ERROR: Creation of instances DAR file failed." mbDar
            -- We have to write the DAR using the `zip` library first so we can then read it using
            -- `zip-archive`. We should eventually get rid of `zip-archive` completely
            -- but this particular codepath will go away soon anyway.
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
    -> MS.Map (UnitId, LF.ModuleName) LF.DalfPackage
    -> MS.Map UnitId LF.DalfPackage
    -> ( Graph
       , Vertex -> (PackageNode, LF.PackageId)
       )
buildLfPackageGraph pkgs stablePkgs dependencyPkgs = (depGraph, vertexToNode')
  where
    -- mapping from package id's to unit id's. if the same package is imported with
    -- different unit id's, we would loose a unit id here.
    pkgMap =
        MS.fromList [(pkgId, unitId) | (pkgId, _pkg, _bs, unitId) <- pkgs] `MS.union`
        MS.fromList [(LF.dalfPackageId pkg, unitId) | (unitId, pkg) <- MS.toList dependencyPkgs]

    packages = MS.unions
      [ MS.fromList [(pkgId, pkg) | (pkgId, pkg, _, _) <- pkgs]
      , MS.fromList [(LF.dalfPackageId dalfPkg, LF.extPackagePkg $ LF.dalfPackagePkg dalfPkg) | dalfPkg <- MS.elems dependencyPkgs <> MS.elems stablePkgs]
      ]

    -- order the packages in topological order
    (depGraph, vertexToNode, _keyToVertex) =
        graphFromEdges
            [ (PackageNode src unitId dalf bs, pkgId, pkgRefs)
            | (pkgId, dalf, bs, unitId) <- pkgs
            , let pkgRefs = [ pid | LF.PRImport pid <- toListOf packageRefs dalf ]
            , let src = generateSrcPkgFromLf (config pkgId unitId) dalf
            ]
    vertexToNode' v = case vertexToNode v of
        -- We don’t care about outgoing edges.
        (node, key, _keys) -> (node, key)

    config pkgId unitId = DataDeps.Config
        { configPackages = packages
        , configGetUnitId = getUnitId unitId pkgMap
        , configSelfPkgId = pkgId
        , configStablePackages = Set.fromList $ map LF.dalfPackageId $ MS.elems stablePkgs
        , configDependencyPackages = Set.fromList $ map LF.dalfPackageId $ MS.elems dependencyPkgs
        , configSdkPrefix = [T.pack currentSdkPrefix]
        }

data PackageNode = PackageNode
  { stubSources :: [(NormalizedFilePath, String)]
  -- ^ Sources for the stub package containining data type definitions
  -- ^ Sources for the package containing instances for Template, Choice, …
  , unitId :: UnitId
  , dalf :: LF.Package
  , encodedDalf :: BS.ByteString
  }

currentSdkPrefix :: String
currentSdkPrefix = "CurrentSdk"

exposePackage :: GHC.UnitId -> Bool -> [(GHC.ModuleName, GHC.ModuleName)] -> PackageFlag
exposePackage unitId exposeImplicit mods = ExposePackage ("--package " <> show (showPackageFlag unitId exposeImplicit mods)) (UnitIdArg unitId) (ModRenaming exposeImplicit mods)

-- | This is only used in error messages.
showPackageFlag :: GHC.UnitId -> Bool -> [(GHC.ModuleName, GHC.ModuleName)] -> String
showPackageFlag unitId exposeImplicit mods = concat
    [ unitIdString unitId
    , if exposeImplicit then " with " else ""
    , if null mods then "" else "(" <> intercalate ", " (map showRenaming mods) <> ")"
    ]
  where showRenaming (a, b)
          | a == b = GHC.moduleNameString a
          | otherwise = GHC.moduleNameString a <> " as " <> GHC.moduleNameString b
