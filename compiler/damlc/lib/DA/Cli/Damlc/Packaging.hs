-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Cli.Damlc.Packaging
  ( createProjectPackageDb
  , mbErr
  , getUnitId
  ) where

import Control.Exception.Safe (tryAny)
import Control.Lens (toListOf)
import Control.Monad.Extra
import Control.Monad.Trans.Maybe
import qualified Data.ByteString as BS
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
import GHC.Fingerprint
import "ghc-lib-parser" HscTypes as GHC
import "ghc-lib-parser" Module (UnitId, unitIdString)
import qualified Module as GHC
import qualified "ghc-lib-parser" Packages as GHC
import System.Directory.Extra
import System.Exit
import System.FilePath
import System.IO.Extra
import System.Info.Extra
import System.Process (callProcess)
import "ghc-lib-parser" UniqSet

import DA.Bazel.Runfiles
import DA.Cli.Damlc.Base
import DA.Daml.Compiler.Dar
import DA.Daml.Compiler.DataDependencies as DataDeps
import DA.Daml.Compiler.DecodeDar (DecodedDalf(..), decodeDalf)
import qualified DA.Daml.LF.Ast as LF
import DA.Daml.LF.Ast.Optics (packageRefs)
import DA.Daml.Options.Packaging.Metadata
import DA.Daml.Options.Types
import DA.Cli.Damlc.DependencyDb
import qualified DA.Pretty
import qualified DA.Service.Logger as Logger
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
createProjectPackageDb :: NormalizedFilePath -> Options -> MS.Map UnitId GHC.ModuleName -> IO ()
createProjectPackageDb projectRoot (disableScenarioService -> opts) modulePrefixes
  = do
    (needsReinitalization, depsFingerprint) <- dbNeedsReinitialization projectRoot depsDir
    when needsReinitalization $ do
      clearPackageDb


      -- Register deps at the very beginning. This allows data-dependencies to
      -- depend on dependencies which is necessary so that we can reconstruct typeclass
      -- instances for a typeclass defined in a library.
      -- It does mean that we can’t have a dependency from a dependency on a
      -- data-dependency but that seems acceptable.
      -- See https://github.com/digital-asset/daml/issues/4218 for more details.
      -- TODO Enforce this with useful error messages
      registerDepsInPkgDb depsDir dbPath

      loggerH <- getLogger opts "dependencies"
      mbRes <- withDamlIdeState opts loggerH diagnosticsLogger $ \ide -> runActionSync ide $ runMaybeT $
          (,) <$> useNoFileE GenerateStablePackages
              <*> (fst <$> useE GeneratePackageMap projectRoot)
      (stablePkgs, PackageMap dependenciesInPkgDb) <- maybe (fail "Failed to generate package info") pure mbRes
      let stablePkgIds :: Set LF.PackageId
          stablePkgIds = Set.fromList $ map LF.dalfPackageId $ MS.elems stablePkgs
      let dependenciesInPkgDbIds =
              Set.fromList $ map LF.dalfPackageId $ MS.elems dependenciesInPkgDb

      -- This is only used for unit-id collision checks and dependencies on newer LF versions.
      dalfsFromDependencyFps <- queryDalfs (Just [depMarker]) depsDir
      dalfsFromDependencies <- forM dalfsFromDependencyFps $ decodeDalf_ dependenciesInPkgDbIds
      dalfsFromDataDependencyFps <- queryDalfs (Just [dataDepMarker]) depsDir
      dalfsFromDataDependencies <- forM dalfsFromDataDependencyFps $ decodeDalf_ dependenciesInPkgDbIds
      mainDalfFps <- queryDalfs (Just [mainMarker]) depsDir
      mainDalfs <- forM mainDalfFps $ decodeDalf_ dependenciesInPkgDbIds

      let dependencyInfo = DependencyInfo
              { dependenciesInPkgDb
              , dalfsFromDependencies
              , dalfsFromDataDependencies
              , mainUnitIds = map decodedUnitId mainDalfs
              }

      -- We perform these check before checking for unit id collisions
      -- since it provides a more useful error message.
      whenLeft
          (checkForInconsistentLfVersions (optDamlLfVersion opts) dependencyInfo)
          exitWithError
      whenLeft
          (checkForIncompatibleLfVersions (optDamlLfVersion opts) dependencyInfo)
          exitWithError
      whenLeft
          (checkForUnitIdConflicts dependencyInfo)
          exitWithError

      -- We run the checks for duplicate unit ids before
      -- to avoid blowing up GHC when setting up the GHC session.
      exposedModules <- getExposedModules opts projectRoot

      Logger.logDebug loggerH "Building dependency package graph"

      let (depGraph, vertexToNode) = buildLfPackageGraph dalfsFromDataDependencies stablePkgs dependenciesInPkgDb


      validatedModulePrefixes <- either exitWithError pure (prefixModules modulePrefixes (dalfsFromDependencies <> dalfsFromDataDependencies))

      -- Iterate over the dependency graph in topological order.
      -- We do a topological sort on the transposed graph which ensures that
      -- the packages with no dependencies come first and we
      -- never process a package without first having processed its dependencies.

      Logger.logDebug loggerH "Registering dependency graph"

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
          let dalfDir = dbPath </> pkgIdStr
          createDirectoryIfMissing True workDir
          createDirectoryIfMissing True dalfDir
          BS.writeFile (dalfDir </> unitIdStr <.> "dalf") $ LF.dalfPackageBytes $ dalfPackage pkgNode

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

      writeMetadata
          projectRoot
          (PackageDbMetadata (mainUnitIds dependencyInfo) validatedModulePrefixes depsFingerprint)
  where
    dbPath = projectPackageDatabase </> lfVersionString (optDamlLfVersion opts)
    depsDir = dependenciesDir opts projectRoot
    decodeDalf_ pkgIds dalf = do
        bs <- BS.readFile dalf
        either fail pure $ decodeDalf pkgIds dalf bs
    clearPackageDb = do
        -- Since we reinitialize the whole package db during `daml init` anyway,
        -- we clear the package db before to avoid
        -- issues during SDk upgrades. Once we have a more clever mechanism than
        -- reinitializing everything, we probably want to change this.
        removePathForcibly dbPath
        createDirectoryIfMissing True $ dbPath </> "package.conf.d"

-- | Compute the hash over all dependencies and compare it to the one stored in the metadata file in
-- the package db to decide whether to run reinitialization or not.
dbNeedsReinitialization ::
       NormalizedFilePath -> FilePath -> IO (Bool, Fingerprint)
dbNeedsReinitialization projectRoot depsDir = do
    allDeps <- listFilesRecursive depsDir
    fileFingerprints <- mapM getFileHash allDeps
    let depsFingerprint = fingerprintFingerprints fileFingerprints
    -- Read the metadata of an already existing package database and see if wee need to reinitialize.
    errOrmetaData <- tryAny $ readMetadata projectRoot
    pure $
        case errOrmetaData of
            Left _err -> (True, depsFingerprint)
            Right metaData -> (fingerprintDependencies metaData /= depsFingerprint, depsFingerprint)

disableScenarioService :: Options -> Options
disableScenarioService opts = opts
  { optScenarioService = EnableScenarioService False
  }

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
    let pkgContext = T.pack (unitIdString (pkgNameVersion pkgName mbPkgVersion)) <> " (" <> LF.unPackageId pkgIdStr <> ")"
    loggerH <- getLogger opts $ "data-dependencies " <> pkgContext
    Logger.logDebug loggerH "Writing out dummy source files"
    let src' = [ (toNormalizedFilePath' $ workDir </> fromNormalizedFilePath nfp, str) | (nfp, str) <- src]
    mapM_ writeSrc src'
    Logger.logDebug loggerH "Compiling dummy interface files"
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
            -- we know all package flags so we don’t need to consult metadata.
            , optIgnorePackageMetadata = IgnorePackageMetadata True
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
    Logger.logDebug loggerH $ "Recaching package db for " <> pkgContext
    recachePkgDb dbPath

-- | Fake settings, we need those to make ghc-pkg happy.
--
-- As a long-term solution, it might make sense to clone ghc-pkg
-- and strip it down to only the functionality we need. A lot of this
-- is rather sketchy in the context of daml.
settings :: [(T.Text, T.Text)]
settings =
  [ ("target arch", "ArchUnknown")
  , ("target os", "OSUnknown")
  , ("target word size", "8")
  , ("Unregisterised", "YES")
  , ("target has GNU nonexec stack", "YES")
  , ("target has .ident directive", "YES")
  , ("target has subsections via symbols", "YES")
  , ("cross compiling", "NO")
  ]

-- Register a dar dependency in the package database
registerDepsInPkgDb :: FilePath -> FilePath -> IO ()
registerDepsInPkgDb depsPath dbPath = do
    mains <- queryDalfs (Just [mainMarker, depMarker]) depsPath
    let dirs = map takeDirectory mains
    forM_ dirs $ \dir -> do
      files <- listFilesRecursive dir
      copyFiles dir [f | f <- files, takeExtension f `elem` [".daml", ".hie", ".hi"] ] dbPath
      copyFiles dir [f | f <- files, "conf" `isExtensionOf` f] (dbPath </> "package.conf.d")
    copyFiles depsPath mains dbPath
    -- Note that we're not copying the `dalfs` directory, because we also only have interface files
    -- for the mains.
    recachePkgDb dbPath

copyFiles :: FilePath -> [FilePath] -> FilePath -> IO ()
copyFiles from srcs to = do
      forM_ srcs $ \src -> do
        let fp = to </> makeRelative from src
        createDirectoryIfMissing True (takeDirectory fp)
        copyFile src fp

recachePkgDb :: FilePath -> IO ()
recachePkgDb dbPath = do
    T.writeFileUtf8 (dbPath </> "settings") $ T.pack $ show settings
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
           , "DA.Internal.NatSyn"
           , "DA.Internal.PromotedText"
           , "DA.Exception.GeneralError"
           , "DA.Exception.ArithmeticError"
           , "DA.Exception.AssertionFailed"
           , "DA.Exception.PreconditionFailed"
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
          , "DA.Set.Types"
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
        else locateRunfiles "ghc_nix/lib/ghc-8.10.7/bin"

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
  -- ^ Unit id of the main DALFs specified in dependencies and the main DALFs
  -- of DARs specified in data-dependencies. This will be used to generate the
  -- --package flags which define which packages are exposed by default.
  }

showDeps :: [((LF.PackageId, UnitId), LF.Version)] -> String
showDeps deps =
    intercalate
        ", "
        [ T.unpack (LF.unPackageId pkgId) <> " (" <> unitIdString unitId <> "): " <>
        DA.Pretty.renderPretty ver
        | ((pkgId, unitId), ver) <- deps
        ]

checkForInconsistentLfVersions :: LF.Version -> DependencyInfo -> Either String ()
checkForInconsistentLfVersions lfTarget DependencyInfo{dalfsFromDependencies, mainUnitIds}
  | null inconsistentLfDeps = Right ()
  | otherwise = Left $ concat
        [ "Targeted LF version "
        , DA.Pretty.renderPretty lfTarget
        , " but dependencies have different LF versions: "
        , showDeps inconsistentLfDeps
        ]
  where
    inconsistentLfDeps =
        [ ((LF.dalfPackageId decodedDalfPkg, decodedUnitId), ver)
        | DecodedDalf {..} <- dalfsFromDependencies
        , let mainUnitIdsSet = Set.fromList mainUnitIds
        , decodedUnitId `Set.member` mainUnitIdsSet
        , let ver = LF.packageLfVersion $ LF.extPackagePkg $ LF.dalfPackagePkg decodedDalfPkg
        , ver /= lfTarget
        ]

checkForIncompatibleLfVersions :: LF.Version -> DependencyInfo -> Either String ()
checkForIncompatibleLfVersions lfTarget DependencyInfo{dalfsFromDependencies, dalfsFromDataDependencies}
  | null incompatibleLfDeps = Right ()
  | otherwise = Left $ concat
        [ "Targeted LF version "
        , DA.Pretty.renderPretty lfTarget
        , " but dependencies have newer LF versions: "
        , showDeps incompatibleLfDeps
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
        { optIgnorePackageMetadata = IgnorePackageMetadata True
        , optPackageImports = []
        }
    hscEnv <-
        (maybe (exitWithError "Failed to list exposed modules") (pure . hscEnv) =<<) $
        withDamlIdeState opts logger diagnosticsLogger $ \ide ->
        runActionSync ide $ runMaybeT $ fst <$> useE GhcSession projectRoot
    pure $! exposedModulesFromDynFlags $ hsc_dflags hscEnv
  where
    exposedModulesFromDynFlags :: DynFlags -> MS.Map UnitId (UniqSet GHC.ModuleName)
    exposedModulesFromDynFlags df =
        MS.fromList $
        map (\pkgConf -> (getUnitId pkgConf, mkUniqSet $ map fst $ GHC.exposedModules pkgConf)) $
        GHC.listPackageConfigMap df
    getUnitId = GHC.DefiniteUnitId . GHC.DefUnitId . GHC.unitId

-- | Given the prefixes declared in daml.yaml
-- and the list of decoded dalfs, validate that
-- the prefixes point to packages that exist
-- and associate them with all modules in the given package.
-- We run this after checking for unit id collisions so we assume
-- that the unit ids in the decoded dalfs are unique.
prefixModules
    :: MS.Map UnitId GHC.ModuleName
    -> [DecodedDalf]
    -> Either String (MS.Map UnitId (GHC.ModuleName, [LF.ModuleName]))
prefixModules prefixes dalfs = do
    MS.traverseWithKey f prefixes
  where unitIdMap = MS.fromList [(decodedUnitId, decodedDalfPkg) | DecodedDalf{..} <- dalfs]
        f unitId prefix = case MS.lookup unitId unitIdMap of
            Nothing -> Left ("Could not find package " <> unitIdString unitId)
            Just pkg -> Right
                ( prefix
                , NM.names . LF.packageModules . LF.extPackagePkg $ LF.dalfPackagePkg pkg
                )
