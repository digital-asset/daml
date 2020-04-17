-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Cli.Damlc.Packaging
  ( createProjectPackageDb
  , mbErr
  , getUnitId
  ) where

import qualified "zip-archive" Codec.Archive.Zip as ZipArchive
import Control.Lens (toListOf)
import Control.Monad.Extra
import Control.Monad.Trans.Maybe
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Data.Either.Combinators
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
import Development.IDE.GHC.Util (hscEnv)
import Development.IDE.Types.Location
import "ghc-lib-parser" DynFlags (DynFlags)
import "ghc-lib-parser" HscTypes as GHC
import "ghc-lib-parser" Module (UnitId, unitIdString)
import qualified Module as GHC
import qualified "ghc-lib-parser" Packages as GHC
import System.Directory.Extra
import System.Exit
import System.FilePath
import System.Info.Extra
import System.IO.Extra
import System.Process (callProcess)
import "ghc-lib-parser" UniqSet

import DA.Bazel.Runfiles
import DA.Cli.Damlc.Base
import DA.Daml.Compiler.Dar
import DA.Daml.Compiler.DataDependencies as DataDeps
import DA.Daml.Compiler.ExtractDar (extractDar,ExtractedDar(..))
import qualified DA.Daml.LF.Ast as LF
import DA.Daml.LF.Ast.Optics (packageRefs)
import qualified DA.Daml.LF.Proto3.Archive as Archive
import DA.Daml.Options.Packaging.Metadata
import DA.Daml.Options.Types
import DA.Daml.Package.Config
import qualified DA.Pretty
import Development.IDE.Core.IdeState.Daml
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
    deps <- expandSdkPackages (optDamlLfVersion opts) (filter (`notElem` basePackages) deps)
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
            <*> useE GeneratePackageMap projectRoot
    (stablePkgs, dependenciesInPkgDb) <- maybe (fail "Failed to generate package info") pure mbRes
    let stablePkgIds :: Set LF.PackageId
        stablePkgIds = Set.fromList $ map LF.dalfPackageId $ MS.elems stablePkgs
    let dependenciesInPkgDbIds =
            Set.fromList $ map LF.dalfPackageId $ MS.elems dependenciesInPkgDb

    -- Now handle data-dependencies.
    darsFromDataDependencies <- getDarsFromDataDependencies dependenciesInPkgDbIds dataDeps
    let dalfsFromDataDependencies = concatMap dalfs darsFromDataDependencies

    -- All transitive packages from DARs specified in  `dependencies`.
    -- This is only used for unit-id collision checks
    -- and dependencies on newer LF versions.
    darsFromDependencies <- getDarsFromDependencies dependenciesInPkgDbIds depsExtracted
    let dalfsFromDependencies = concatMap dalfs darsFromDependencies

    let dependencyInfo = DependencyInfo
            { dependenciesInPkgDb
            , dalfsFromDependencies
            , dalfsFromDataDependencies
            , mainUnitIds =
                  map (decodedUnitId . mainDalf)
                      (darsFromDataDependencies ++ darsFromDependencies)
            }

    -- We perform this check before checking for unit id collisions
    -- since it provides a more useful error message.
    whenLeft
        (checkForIncompatibleLfVersions (optDamlLfVersion opts) dependencyInfo)
        exitWithError
    whenLeft
        (checkForUnitIdConflicts dependencyInfo)
        exitWithError

    -- We run the checks for duplicate unit ids before
    -- to avoid blowing up GHC when setting up the GHC session.
    exposedModules <- getExposedModules opts projectRoot

    let (depGraph, vertexToNode) = buildLfPackageGraph dalfsFromDataDependencies stablePkgs dependenciesInPkgDb
    -- Iterate over the dependency graph in topological order.
    -- We do a topological sort on the transposed graph which ensures that
    -- the packages with no dependencies come first and we
    -- never process a package without first having processed its dependencies.
    forM_ (topSort $ transposeG depGraph) $ \vertex ->
      let (pkgNode, pkgId) = vertexToNode vertex in
      -- stable packages are mapped to the current version of daml-prim/daml-stdlib
      -- so we don’t need to generate interface files for them.
      unless (pkgId `Set.member` stablePkgIds || pkgId `Set.member` dependenciesInPkgDbIds) $ do
        let unitIdStr = unitIdString $ unitId pkgNode
        let pkgIdStr = T.unpack $ LF.unPackageId pkgId
        let (pkgName, mbPkgVersion) = LF.splitUnitId (unitId pkgNode)
        let deps =
                [ (unitId depPkgNode, dalfPackage depPkgNode)
                | (depPkgNode, depPkgId) <- map vertexToNode $ reachable depGraph vertex
                , pkgId /= depPkgId
                , not (depPkgId `Set.member` stablePkgIds)
                ]
        let workDir = dbPath </> unitIdStr <> "-" <> pkgIdStr
        createDirectoryIfMissing True workDir
        -- write the dalf package
        BS.writeFile (workDir </> unitIdStr <.> "dalf") $ LF.dalfPackageBytes (dalfPackage pkgNode)

        generateAndInstallIfaceFiles
            (LF.extPackagePkg $ LF.dalfPackagePkg $ dalfPackage pkgNode)
            (stubSources pkgNode)
            opts
            workDir
            dbPath
            projectPackageDatabase
            pkgId
            pkgName
            mbPkgVersion
            deps
            dependenciesInPkgDb
            exposedModules

    writeMetadata projectRoot (PackageDbMetadata (mainUnitIds dependencyInfo))
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
    -> LF.PackageId
    -> LF.PackageName
    -> Maybe LF.PackageVersion
    -> [(UnitId, LF.DalfPackage)] -- ^ List of packages referenced by this package.
    -> MS.Map UnitId LF.DalfPackage -- ^ Map of all packages in `dependencies`.
    -> MS.Map UnitId (UniqSet GHC.ModuleName)
    -> IO ()
generateAndInstallIfaceFiles dalf src opts workDir dbPath projectPackageDatabase pkgIdStr pkgName mbPkgVersion deps dependencies exposedModules = do
    loggerH <- getLogger opts "generate interface files"
    let src' = [ (toNormalizedFilePath' $ workDir </> fromNormalizedFilePath nfp, str) | (nfp, str) <- src]
    mapM_ writeSrc src'
    -- We expose dependencies under a Pkg_$pkgId prefix so we can unambiguously refer to them
    -- while avoiding name collisions in package imports. Note that we can only do this
    -- for exposed modules. GHC gets very unhappy if you try to remap modules that are not
    -- exposed.
    -- TODO (MK)
    -- Use this scheme to refer to data-dependencies as well and replace the CurrentSdk prefix by this.
    let depImps =
            [ exposePackage unitId False
                  [ (toGhcModuleName modName, toGhcModuleName (prefixDependencyModule dalfPackageId modName))
                  | mod <- NM.toList $ LF.packageModules $ LF.extPackagePkg dalfPackagePkg
                  , let modName = LF.moduleName mod
                  -- NOTE (MK) I am not sure if this lookup
                  -- can ever fail but for now, we keep exposing the module in that case.
                  , maybe True (toGhcModuleName modName `elementOfUniqSet`) mbExposed
                  ]
            | (unitId, LF.DalfPackage{..}) <- MS.toList dependencies <> deps
            , let mbExposed = MS.lookup unitId exposedModules
            ]
    opts <-
        pure $ opts
            { optIfaceDir = Nothing
            -- We write ifaces below using writeIfacesAndHie so we don’t need to enable these options.
            , optPackageDbs = projectPackageDatabase : optPackageDbs opts
            , optIsGenerated = True
            , optDflagCheck = False
            , optMbPackageName = Just pkgName
            , optMbPackageVersion = mbPkgVersion
            , optGhcCustomOpts = []
            , optPackageImports =
                  baseImports ++
                  depImps
            -- When compiling dummy interface files for a data-dependency,
            -- we know all package flags so we don’t need to infer anything.
            , optInferDependantPackages = InferDependantPackages False
            }

    res <- withDamlIdeState opts loggerH diagnosticsLogger $ \ide ->
        runActionSync ide $
        -- Setting ifDir to . means that the interface files will end up directly next to
        -- the source files which is what we want here.
        writeIfacesAndHie
            (toNormalizedFilePath' ".")
            [fp | (fp, _content) <- src']
    when (isNothing res) $
      exitWithError
          $ "Failed to compile interface for data-dependency: "
          <> unitIdString (pkgNameVersion pkgName mbPkgVersion)
    -- write the conf file and refresh the package cache
    let (cfPath, cfBs) = mkConfFile
            pkgName
            mbPkgVersion
            (map fst deps)
            Nothing
            (map (GHC.mkModuleName . T.unpack) $ LF.packageModuleNames dalf)
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
           , "DA.Internal.PromotedText"
           , "GHC.Err"
           , "Data.String"
           ]
        )
    -- We need the standard library from the current SDK, e.g., LF builtins like Optional are translated
    -- to types in the current standard library.
    , exposePackage
       damlStdlib
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
    -> ZipArchive.Entry
    -> [ZipArchive.Entry]
    -> IO ()
installDar dbPath confFiles dalf srcs = do
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

lfVersionString :: LF.Version -> String
lfVersionString = DA.Pretty.renderPretty


-- | The graph will have an edge from package A to package B if A depends on B.
buildLfPackageGraph
    :: [DecodedDalf]
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
        MS.fromList [ (LF.dalfPackageId pkg, unitId)
                    | (unitId, pkg) <-
                          MS.toList dependencyPkgs <>
                          map (\DecodedDalf{..} -> (decodedUnitId, decodedDalfPkg)) pkgs
                    ]

    packages =
        MS.fromList
            [ (LF.dalfPackageId dalfPkg, LF.extPackagePkg $ LF.dalfPackagePkg dalfPkg)
            | dalfPkg <- MS.elems dependencyPkgs <> MS.elems stablePkgs <> map decodedDalfPkg pkgs
            ]


    -- order the packages in topological order
    (depGraph, vertexToNode, _keyToVertex) =
        graphFromEdges
            [ (PackageNode src decodedUnitId decodedDalfPkg, LF.dalfPackageId decodedDalfPkg, pkgRefs)
            | DecodedDalf{decodedUnitId, decodedDalfPkg} <- pkgs
            , let pkg = LF.extPackagePkg (LF.dalfPackagePkg decodedDalfPkg)
            , let pkgRefs = [ pid | LF.PRImport pid <- toListOf packageRefs pkg ]
            , let src = generateSrcPkgFromLf (config (LF.dalfPackageId decodedDalfPkg) decodedUnitId) pkg
            ]
    vertexToNode' v = case vertexToNode v of
        -- We don’t care about outgoing edges.
        (node, key, _keys) -> (node, key)

    config pkgId unitId = DataDeps.Config
        { configPackages = packages
        , configGetUnitId = getUnitId unitId pkgMap
        , configSelfPkgId = pkgId
        , configStablePackages = MS.fromList [ (LF.dalfPackageId dalfPkg, unitId) | ((unitId, _), dalfPkg) <- MS.toList stablePkgs ]
        , configDependencyPackages = Set.fromList $ map LF.dalfPackageId $ MS.elems dependencyPkgs
        , configSdkPrefix = [T.pack currentSdkPrefix]
        }

data PackageNode = PackageNode
  { stubSources :: [(NormalizedFilePath, String)]
  -- ^ Sources for the stub package containining data type definitions
  -- ^ Sources for the package containing instances for Template, Choice, …
  , unitId :: UnitId
  , dalfPackage :: LF.DalfPackage
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

-- NOTE (MK) We used to call just errorIO here. However for reasons
-- that I do not understand this sometimes seemed to result in test failures
-- on Windows (never saw it anywhere else) where the executable failed
-- as expected but we got no output.
-- So now we are extra careful to make sure that the error message is actually
-- written somewhere.
exitWithError :: String -> IO a
exitWithError msg = do
    hPutStrLn stderr msg
    hFlush stderr
    exitFailure

data DependencyInfo = DependencyInfo
  { dependenciesInPkgDb :: MS.Map UnitId LF.DalfPackage
  -- ^ Dependencies picked up by the GeneratePackageMap rule.
  -- The rule is run after installing DALFs from `dependencies` so
  -- this includes dependencies in the builtin package db like daml-prim
  -- as well as the main DALFs of DARs specified in `dependencies`.
  , dalfsFromDependencies :: [DecodedDalf]
  -- ^ All dalfs (not just main DALFs) in DARs listed in `dependencies`.
  -- This does not include DALFs in the global package db like daml-prim.
  -- Note that a DAR does not include interface files for dependencies
  -- so to use a DAR as a `dependency` you also need to list the DARs of all
  -- of its dependencies.
  , dalfsFromDataDependencies :: [DecodedDalf]
  -- ^ All dalfs (not just main DALFs) from DARs and DALFs listed in `data-dependencies`.
  -- Note that for data-dependencies it is sufficient to list a DAR without
  -- listing all of its dependencies.
  , mainUnitIds :: [UnitId]
  -- ^ Unit id of the main DALFs specified in dependencies and
  -- data-dependencies. This will be used to generate the --package
  -- flags which define which packages are exposed by default.
  }

checkForIncompatibleLfVersions :: LF.Version -> DependencyInfo -> Either String ()
checkForIncompatibleLfVersions lfTarget DependencyInfo{dalfsFromDependencies, dalfsFromDataDependencies}
  | null incompatibleLfDeps = Right ()
  | otherwise = Left $ concat
        [ "Targeted LF version "
        , DA.Pretty.renderPretty lfTarget
        , " but dependencies have newer LF versions: "
        , intercalate ", "
              [ T.unpack (LF.unPackageId pkgId) <>
                " (" <> unitIdString unitId <> "): " <> DA.Pretty.renderPretty ver
              | ((pkgId, unitId), ver) <- incompatibleLfDeps
              ]
        ]
  where
    incompatibleLfDeps =
        filter (\(_, ver) -> ver > lfTarget) $
            [ ( (LF.dalfPackageId decodedDalfPkg, decodedUnitId)
              , (LF.packageLfVersion . LF.extPackagePkg . LF.dalfPackagePkg) decodedDalfPkg
              )
            | DecodedDalf{..} <- dalfsFromDataDependencies <> dalfsFromDependencies
            ]

checkForUnitIdConflicts :: DependencyInfo -> Either String ()
checkForUnitIdConflicts DependencyInfo{..}
  | MS.null unitIdConflicts = Right ()
  | otherwise = Left $ concat
        [ "Transitive dependencies with same unit id but conflicting package ids: "
        , intercalate ", "
              [ show k <>
                " [" <>
                intercalate "," (map (T.unpack . LF.unPackageId) (Set.toList v)) <>
                "]"
              | (k,v) <- MS.toList unitIdConflicts
              ]
        ]
  where
    unitIdConflicts = MS.filter ((>=2) . Set.size) .  MS.fromListWith Set.union $ concat
        [ [ (decodedUnitId, Set.singleton (LF.dalfPackageId decodedDalfPkg))
          | DecodedDalf{..} <- dalfsFromDataDependencies <> dalfsFromDependencies
          ]
        , [ (unitId, Set.singleton (LF.dalfPackageId dalfPkg))
          | (unitId, dalfPkg) <- MS.toList dependenciesInPkgDb
          ]
        ]

getExposedModules :: Options -> NormalizedFilePath -> IO (MS.Map UnitId (UniqSet GHC.ModuleName))
getExposedModules opts projectRoot = do
    logger <- getLogger opts "list exposed modules"
    -- We need to avoid inference of package flags. Otherwise, we will
    -- try to load package flags for data-dependencies that we have not generated
    -- yet. We only look for the packages in the package db so the --package flags
    -- do not matter and can be actively harmful since we might have picked up
    -- some from the daml.yaml if they are explicitly specified.
    opts <- pure opts
        { optInferDependantPackages = InferDependantPackages False
        , optPackageImports = []
        }
    hscEnv <-
        (maybe (exitWithError "Failed to list exposed modules") (pure . hscEnv) =<<) $
        withDamlIdeState opts logger diagnosticsLogger $ \ide ->
        runActionSync ide $ runMaybeT $ useE GhcSession projectRoot
    pure $! exposedModulesFromDynFlags $ hsc_dflags hscEnv
  where
    exposedModulesFromDynFlags :: DynFlags -> MS.Map UnitId (UniqSet GHC.ModuleName)
    exposedModulesFromDynFlags df =
        MS.fromList $
        map (\pkgConf -> (getUnitId pkgConf, mkUniqSet $ map fst $ GHC.exposedModules pkgConf)) $
        GHC.listPackageConfigMap df
    getUnitId = GHC.DefiniteUnitId . GHC.DefUnitId . GHC.unitId

-- | data-dependencies accept both DAR files as well as DALFs. The latter
-- is only used in tests and should probably go away at some point.
-- We treat a DALF as a DAR with only that DALF in the main DALF list.
getDarsFromDataDependencies :: Set LF.PackageId -> [FilePath] -> IO [DecodedDar]
getDarsFromDataDependencies dependenciesInPkgDb files = do
    dars <- forM fpDars $ \file -> do
        extractedDar <- extractDar file
        either fail pure (decodeDar dependenciesInPkgDb extractedDar)
    dalfs <-
        forM fpDalfs $ \fp -> do
            bs <- BS.readFile fp
            decodedDalf <- either fail pure $ decodeDalf dependenciesInPkgDb fp bs
            pure (DecodedDar decodedDalf [decodedDalf])
    pure (dars ++ dalfs)
    where (fpDars, fpDalfs) = partition ((== ".dar") . takeExtension) files

data DecodedDalf = DecodedDalf
    { decodedDalfPkg :: LF.DalfPackage
    , decodedUnitId :: UnitId
    }

data DecodedDar = DecodedDar
    { mainDalf :: DecodedDalf
    , dalfs :: [DecodedDalf]
    -- ^ Like in the MANIFEST.MF definition, this includes
    -- the main dalf.
    }

decodeDar :: Set LF.PackageId -> ExtractedDar -> Either String DecodedDar
decodeDar dependenciesInPkgDb ExtractedDar{..} = do
    mainDalf <-
        decodeDalf
            dependenciesInPkgDb
            (ZipArchive.eRelativePath edMain)
            (BSL.toStrict $ ZipArchive.fromEntry edMain)
    otherDalfs <-
        mapM decodeEntry $
            filter
                (\e -> ZipArchive.eRelativePath e /= ZipArchive.eRelativePath edMain)
                edDalfs
    let dalfs = mainDalf : otherDalfs
    pure DecodedDar{..}
  where
    decodeEntry entry =         decodeDalf
            dependenciesInPkgDb
            (ZipArchive.eRelativePath entry)
            (BSL.toStrict $ ZipArchive.fromEntry entry)

decodeDalf :: Set LF.PackageId -> FilePath -> BS.ByteString -> Either String DecodedDalf
decodeDalf dependenciesInPkgDb path bytes = do
    (pkgId, package) <-
        mapLeft DA.Pretty.renderPretty $
        Archive.decodeArchive Archive.DecodeAsDependency bytes
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
    let (name, mbVersion) = case LF.packageMetadataFromFile path package pkgId of
            (LF.PackageName "daml-prim", Nothing)
                | pkgId `Set.notMember` dependenciesInPkgDb ->
                  (LF.PackageName ("daml-prim-" <> LF.unPackageId pkgId), Nothing)
            (LF.PackageName "daml-stdlib", _)
                | pkgId `Set.notMember` dependenciesInPkgDb ->
                  (LF.PackageName ("daml-stdlib-" <> LF.unPackageId pkgId), Nothing)
            (name, mbVersion) -> (name, mbVersion)
    pure DecodedDalf
        { decodedDalfPkg = LF.DalfPackage pkgId (LF.ExternalPackage pkgId package) bytes
        , decodedUnitId = pkgNameVersion name mbVersion
        }

getDarsFromDependencies :: Set LF.PackageId -> [ExtractedDar] -> IO [DecodedDar]
getDarsFromDependencies dependenciesInPkgDb depsExtracted =
    either fail pure $ mapM (decodeDar dependenciesInPkgDb) depsExtracted
