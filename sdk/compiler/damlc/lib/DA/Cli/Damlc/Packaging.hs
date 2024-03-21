-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DuplicateRecordFields #-}

module DA.Cli.Damlc.Packaging
  ( createProjectPackageDb
  , mbErr
  , getUnitId

    -- * Dependency graph construction
  , buildLfPackageGraph'
  , BuildLfPackageGraphArgs' (..)
  , BuildLfPackageGraphMetaArgs (..)
  ) where

import Control.Exception.Safe (tryAny)
import Control.Lens (none, toListOf)
import Control.Monad.Extra
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Maybe
import qualified Control.Monad.Trans.State.Strict as State
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
import Data.Tuple.Extra (fst3)
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
import DA.Daml.Compiler.Dar
import DA.Daml.Compiler.DataDependencies as DataDeps
import DA.Daml.Compiler.DecodeDar (DecodedDalf(..), decodeDalf)
import DA.Daml.Compiler.Output
import qualified DA.Daml.LF.Ast as LF
import DA.Daml.LF.Ast.Optics (packageRefs)
import qualified DA.Daml.LFConversion.MetadataEncoding as LFC
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
--   ledger. Based on the Daml-LF we generate dummy interface files
--   and then remap references to those dummy packages to the original Daml-LF
--   package id.
createProjectPackageDb :: NormalizedFilePath -> Options -> MS.Map UnitId GHC.ModuleName -> IO ()
createProjectPackageDb projectRoot (disableScenarioService -> opts) modulePrefixes
  = do
    (needsReinitalization, depsFingerprint) <- dbNeedsReinitialization projectRoot depsDir
    loggerH <- getLogger opts "package-db"
    when needsReinitalization $ do
      Logger.logDebug loggerH "package db is not up2date, reinitializing"
      clearPackageDb

      -- since we haven't registered any dependencies yet, builtinDependencies only contains daml-prim and daml-stdlib
      (stablePkgs, PackageMap builtinDependencies) <-
        fromMaybeM (fail "Failed to generate package info") $
          withDamlIdeState opts loggerH diagnosticsLogger $ \ide -> runActionSync ide $ runMaybeT $
            (,) <$> useNoFileE GenerateStablePackages
                <*> (fst <$> useE GeneratePackageMap projectRoot)

      let builtinDependenciesIds =
              Set.fromList $ map LF.dalfPackageId $ MS.elems builtinDependencies

      let decodeDalf_ dalf = do
            bs <- BS.readFile dalf
            (dalf,) <$> either fail pure (decodeDalf builtinDependenciesIds dalf bs)

      -- This is only used for unit-id collision checks and dependencies on newer LF versions.
      dalfsFromDependencyFps <- queryDalfs (Just [depMarker]) depsDir
      dalfsFromDataDependencyFps <- queryDalfs (Just [dataDepMarker]) depsDir
      mainDalfFps <- queryDalfs (Just [mainMarker]) depsDir

      dalfsFromDependenciesWithFps <- mapM decodeDalf_ (dalfsFromDependencyFps \\ dalfsFromDataDependencyFps)
      dalfsFromDataDependenciesWithFps <- mapM decodeDalf_ dalfsFromDataDependencyFps
      mainDalfsWithFps <- mapM decodeDalf_ mainDalfFps

      let
        dalfsFromDependencies = fmap snd dalfsFromDependenciesWithFps
        dalfsFromDataDependencies = fmap snd dalfsFromDataDependenciesWithFps
        dalfsFromAllDependencies = dalfsFromDependencies <> dalfsFromDataDependencies
        mainDalfs = fmap snd mainDalfsWithFps

        mainUnitIds = map decodedUnitId mainDalfs

      -- We perform these check before checking for unit id collisions
      -- since it provides a more useful error message.
      whenLeft
          (checkForInconsistentLfVersions (optDamlLfVersion opts) dalfsFromDependencies mainUnitIds)
          exitWithError
      whenLeft
          (checkForIncompatibleLfVersions (optDamlLfVersion opts) dalfsFromAllDependencies)
          exitWithError
      -- We run the check for duplicate unit ids here to avoid blowing up GHC when
      -- setting up the GHC session needed for getExposedModules in installDataDep further down.
      whenLeft
          (checkForUnitIdConflicts dalfsFromAllDependencies builtinDependencies)
          exitWithError

      Logger.logDebug loggerH "Building dependency package graph"

      let
        (depGraph, vertexToNode) = buildLfPackageGraph BuildLfPackageGraphArgs
          { builtinDeps = builtinDependenciesIds
          , stablePkgs = Set.fromList $ map LF.dalfPackageId $ MS.elems stablePkgs
          , deps = dalfsFromDependenciesWithFps
          , dataDeps = dalfsFromDataDependenciesWithFps
          }
        pkgs =
          [ dalfPkg
          | (node, _) <- vertexToNode <$> vertices depGraph
          , Just dalfPkg <- [packageNodeDecodedDalf node]
          ]

      validatedModulePrefixes <- either exitWithError pure (prefixModules modulePrefixes dalfsFromAllDependencies)

      -- Iterate over the dependency graph in topological order.
      -- We do a topological sort on the transposed graph which ensures that
      -- the packages with no dependencies come first and we
      -- never process a package without first having processed its dependencies.

      Logger.logDebug loggerH "Registering dependency graph"

      flip State.evalStateT builtinDependencies $ do
        let
          insert unitId dalfPackage = State.modify $ MS.insert unitId dalfPackage

        forM_ (topSort $ transposeG depGraph) $ \vertex -> do
          let (pkgNode, pkgId) = vertexToNode vertex
          case pkgNode of
            MkStableDependencyPackageNode ->
              -- stable packages are mapped to the current version of daml-prim/daml-stdlib
              -- so we don’t need to generate interface files for them.
              pure ()
            MkBuiltinDependencyPackageNode {} ->
              pure ()
            MkDependencyPackageNode DependencyPackageNode {dalf, unitId, dalfPackage} -> do
              liftIO $ registerDepInPkgDb dalf depsDir dbPath
              insert unitId dalfPackage
            MkDataDependencyPackageNode DataDependencyPackageNode {unitId, dalfPackage} -> do
              dependenciesSoFar <- State.get
              let
                depUnitIds :: [UnitId]
                depUnitIds =
                  [ unitId
                  | (depPkgNode, depPkgId) <- vertexToNode <$> reachable depGraph vertex
                  , pkgId /= depPkgId
                  , unitId <- case depPkgNode of
                      MkStableDependencyPackageNode -> []
                      MkBuiltinDependencyPackageNode BuiltinDependencyPackageNode {unitId} -> [unitId]
                      MkDependencyPackageNode DependencyPackageNode {unitId} -> [unitId]
                      MkDataDependencyPackageNode DataDependencyPackageNode {unitId} -> [unitId]
                  ]

              liftIO $ installDataDep InstallDataDepArgs
                { opts
                , projectRoot
                , dbPath
                , pkgs
                , stablePkgs
                , dependenciesSoFar
                , depUnitIds
                , pkgId
                , unitId
                , dalfPackage
                }

              insert unitId dalfPackage

      writeMetadata
          projectRoot
          (PackageDbMetadata mainUnitIds validatedModulePrefixes depsFingerprint)
  where
    dbPath = projectPackageDatabase </> lfVersionString (optDamlLfVersion opts)
    depsDir = dependenciesDir opts projectRoot
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


data InstallDataDepArgs = InstallDataDepArgs
  { opts :: Options
  , projectRoot :: NormalizedFilePath
  , dbPath :: FilePath
  , pkgs :: [DecodedDalf]
    -- ^ All the packages in the dependency graph of the package being built.
  , stablePkgs :: MS.Map (UnitId, LF.ModuleName) LF.DalfPackage
  , dependenciesSoFar :: MS.Map UnitId LF.DalfPackage
    -- ^ The dependencies and data-dependencies processed before this data-dependency.
  , depUnitIds :: [UnitId]
    -- ^ The UnitIds of the dependencies and data-dependencies of this data-dependency.
    -- The way we traverse the dependency graph ensures these have all been processed.
  , pkgId :: LF.PackageId
  , unitId :: UnitId
  , dalfPackage :: LF.DalfPackage
  }

installDataDep :: InstallDataDepArgs -> IO ()
installDataDep InstallDataDepArgs {..} = do
  exposedModules <- getExposedModules opts projectRoot

  let unitIdStr = unitIdString unitId
  let pkgIdStr = T.unpack $ LF.unPackageId pkgId
  let (pkgName, mbPkgVersion) = LF.splitUnitId unitId
  let workDir = dbPath </> unitIdStr <> "-" <> pkgIdStr
  let dalfDir = dbPath </> pkgIdStr
  createDirectoryIfMissing True workDir
  createDirectoryIfMissing True dalfDir
  BS.writeFile (dalfDir </> unitIdStr <.> "dalf") $ LF.dalfPackageBytes dalfPackage

  let

    -- mapping from package id's to unit id's. if the same package is imported with
    -- different unit id's, we would loose a unit id here.
    pkgMap =
        MS.fromList [ (LF.dalfPackageId pkg, unitId)
                    | (unitId, pkg) <-
                          MS.toList dependenciesSoFar <>
                          map (\DecodedDalf{..} -> (decodedUnitId, decodedDalfPkg)) pkgs
                    ]

    packages =
      MS.fromList
        [ (LF.dalfPackageId dalfPkg, LF.extPackagePkg $ LF.dalfPackagePkg dalfPkg)
        | dalfPkg <- MS.elems dependenciesSoFar <> MS.elems stablePkgs <> map decodedDalfPkg pkgs
        ]

    targetLfVersion = optDamlLfVersion opts

    dependencyInfo =
      buildDependencyInfo
        (map LF.dalfPackagePkg $ MS.elems dependenciesSoFar)
        (LF.initWorld (map (uncurry LF.ExternalPackage) (MS.toList packages)) targetLfVersion)

    config = DataDeps.Config
        { configPackages = packages
        , configGetUnitId = getUnitId unitId pkgMap
        , configSelfPkgId = pkgId
        , configStablePackages = MS.fromList [ (LF.dalfPackageId dalfPkg, unitId) | ((unitId, _), dalfPkg) <- MS.toList stablePkgs ]
        , configDependencyInfo = dependencyInfo
        , configSdkPrefix = [T.pack currentSdkPrefix]
        }

    pkg = LF.extPackagePkg (LF.dalfPackagePkg dalfPackage)

    -- Sources for the stub package containining data type definitions
    -- Sources for the package containing instances for Template, Choice, …
    stubSources = generateSrcPkgFromLf config pkg

  generateAndInstallIfaceFiles GenerateAndInstallIfaceFilesArgs
    { dalf = pkg
    , src = stubSources
    , opts
    , workDir
    , dbPath
    , pkgId
    , pkgName
    , mbPkgVersion
    , depUnitIds
    , dependenciesSoFar
    , exposedModules
    }

data GenerateAndInstallIfaceFilesArgs = GenerateAndInstallIfaceFilesArgs
  { dalf :: LF.Package
  , src :: [(NormalizedFilePath, String)]
  , opts :: Options
  , workDir :: FilePath
  , dbPath :: FilePath
  , pkgId :: LF.PackageId
  , pkgName :: LF.PackageName
  , mbPkgVersion :: Maybe LF.PackageVersion
  , depUnitIds :: [UnitId]
    -- ^ List of units referenced by this package.
  , dependenciesSoFar :: MS.Map UnitId LF.DalfPackage
    -- ^ The dependencies and data-dependencies processed before this data-dependency.
  , exposedModules :: MS.Map UnitId (UniqSet GHC.ModuleName)
  }

-- | Generate interface files and install them in the package database
generateAndInstallIfaceFiles :: GenerateAndInstallIfaceFilesArgs -> IO ()
generateAndInstallIfaceFiles GenerateAndInstallIfaceFilesArgs {..} = do
    let pkgContext = T.pack (unitIdString (pkgNameVersion pkgName mbPkgVersion)) <> " (" <> LF.unPackageId pkgId <> ")"
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
            | (unitId, LF.DalfPackage{..}) <- MS.toList dependenciesSoFar
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
            depUnitIds
            Nothing
            (map (GHC.mkModuleName . T.unpack) $ LF.packageModuleNames dalf)
            pkgId
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
  , ("target word big endian", "NO")
  , ("Unregisterised", "YES")
  , ("target has GNU nonexec stack", "YES")
  , ("target has .ident directive", "YES")
  , ("target has subsections via symbols", "YES")
  , ("cross compiling", "NO")
  , ("Leading underscore", "NO")
  , ("Tables next to code", "YES")
  ]

-- Register a single dar dependency in the package database
registerDepInPkgDb :: FilePath -> FilePath -> FilePath -> IO ()
registerDepInPkgDb dalfPath depsPath dbPath = do
  let dir = takeDirectory dalfPath
  files <- listFilesRecursive dir
  copyFiles dir [f | f <- files, takeExtension f `elem` [".daml", ".hie", ".hi"] ] dbPath
  copyFiles dir [f | f <- files, "conf" `isExtensionOf` f] (dbPath </> "package.conf.d")
  copyFiles depsPath [dalfPath] dbPath
  -- TODO: is it possible to register a package individually instead of recaching the entire ghc-pkg db?
  -- https://github.com/digital-asset/daml/issues/13320
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
          , "DA.Internal.Desugar"
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
        else locateRunfiles "ghc_nix/lib/ghc-9.0.2/bin"

-- | Fail with an exit failure and errror message when Nothing is returned.
mbErr :: String -> Maybe a -> IO a
mbErr err = maybe (hPutStrLn stderr err >> exitFailure) pure

lfVersionString :: LF.Version -> String
lfVersionString = DA.Pretty.renderPretty

data BuildLfPackageGraphArgs' decodedDalfWithPath = BuildLfPackageGraphArgs
  { builtinDeps :: Set LF.PackageId
  , stablePkgs :: Set LF.PackageId
  , deps :: [decodedDalfWithPath]
  , dataDeps :: [decodedDalfWithPath]
  }

type BuildLfPackageGraphArgs = BuildLfPackageGraphArgs' (FilePath, DecodedDalf)

-- | The graph will have an edge from package A to package B:
--   1. if A depends on B _OR_
--   2. if A is a data-dependency, B is a (regular) dependency, and B doesn't
--      depend (directly or transitively) on any data-dependencies.
--
--   The reason we want the second kind of edge is to ensure that the uses of
--   type classes (including definitions, instances, constraints and exports) in
--   data-dependencies can be rewritten to refer to the classes from (regular)
--   dependencies when they share the same module, name and structure (method
--   names and types). This only works if the dependency defining the class is
--   processed before the data-dependency that uses it, but since the
--   data-dependency does not explicitly depend on the dependency, we add the
--   second kind of edge to have this information in the dependency graph.
--
--   In particular, this means that instance rewriting is not guaranteed to
--   work for classes defined in packages that directly or transitively depend
--   on data-dependencies.
--
buildLfPackageGraph :: BuildLfPackageGraphArgs -> (Graph, Vertex -> (PackageNode, LF.PackageId))
buildLfPackageGraph =
  (\(graph, vertexToNode, _keyToVertex) -> (graph, vertexToNode)) .
    buildLfPackageGraph' BuildLfPackageGraphMetaArgs
      { getDecodedDalfPath = fst
      , getDecodedDalfUnitId = decodedUnitId . snd
      , getDecodedDalfPkg = decodedDalfPkg . snd
      , getDalfPkgId = LF.dalfPackageId
      , getDalfPkgRefs = dalfPackageRefs
      }
  where
    dalfPackageRefs :: LF.DalfPackage -> [LF.PackageId]
    dalfPackageRefs dalfPkg =
      let
        pkg = LF.extPackagePkg (LF.dalfPackagePkg dalfPkg)
        pid = LF.dalfPackageId dalfPkg
      in
        [ pid'
        | LF.PRImport pid' <-
            toListOf packageRefs pkg
              <>
              [ qualPackage
              | m <- NM.toList $ LF.packageModules pkg
              , Just LF.DefValue{dvalBinder=(_, ty)} <- [NM.lookup LFC.moduleImportsName (LF.moduleValues m)]
              , Just quals <- [LFC.decodeModuleImports ty]
              , LF.Qualified { LF.qualPackage } <- Set.toList quals
              ]
        , pid' /= pid
        ]

data BuildLfPackageGraphMetaArgs decodedDalfWithPath dalfPackage = BuildLfPackageGraphMetaArgs
  { getDecodedDalfPath :: decodedDalfWithPath -> FilePath
  , getDecodedDalfUnitId :: decodedDalfWithPath -> UnitId
  , getDecodedDalfPkg :: decodedDalfWithPath -> dalfPackage
  , getDalfPkgId :: dalfPackage -> LF.PackageId
  , getDalfPkgRefs :: dalfPackage -> [LF.PackageId]
  }

buildLfPackageGraph' ::
     BuildLfPackageGraphMetaArgs decodedDalfWithPath dalfPackage
  -> BuildLfPackageGraphArgs' decodedDalfWithPath
  -> (Graph, Vertex -> (PackageNode' dalfPackage, LF.PackageId), LF.PackageId -> Maybe Vertex)
buildLfPackageGraph' BuildLfPackageGraphMetaArgs {..} BuildLfPackageGraphArgs {..} = (depGraph, vertexToNode', keyToVertex)
  where
    (depGraph, vertexToNode, keyToVertex) =
        graphFromEdges
          [ (node, key, deps <> etc)
          | v <- vertices depGraph0
          , let (node, key, deps) = vertexToNode0 v
          , let etc = case node of
                  MkDataDependencyPackageNode {} -> depsWithoutDataDeps
                  _ -> []
          ]

    depsWithoutDataDeps =
      [ key
      | v <- vertices depGraph0
      , (node, key, _) <- [vertexToNode0 v]
      , isDependencyPackageNode node
      , none (isDataDependencyPackageNode  . fst3 . vertexToNode0) (reachable depGraph0 v)
      ]

    -- order the packages in topological order
    (depGraph0, vertexToNode0, _keyToVertex0) =
        graphFromEdges $
          -- We might have multiple copies of the same package if, for example,
          -- the project we're building has multiple data-dependencies from older
          -- SDKs, each of which bring a copy of daml-prim and daml-stdlib.
          nubSortOn (\(_,pid,_) -> pid) $
            [ (node, pid, pkgRefs)
            | (isDataDep, decodedDalfWithPath) <- fmap (False,) deps <> fmap (True,) dataDeps
            , let
                dalf = getDecodedDalfPath decodedDalfWithPath
                unitId = getDecodedDalfUnitId decodedDalfWithPath
                dalfPackage = getDecodedDalfPkg decodedDalfWithPath
                pid = getDalfPkgId dalfPackage
                pkgRefs = getDalfPkgRefs dalfPackage
                node
                  | pid `elem` builtinDeps = MkBuiltinDependencyPackageNode BuiltinDependencyPackageNode {..}
                  | pid `elem` stablePkgs = MkStableDependencyPackageNode
                  | isDataDep = MkDataDependencyPackageNode DataDependencyPackageNode {..}
                  | otherwise = MkDependencyPackageNode DependencyPackageNode {..}
            ]

    vertexToNode' v = case vertexToNode v of
        -- We don’t care about outgoing edges.
        (node, key, _keys) -> (node, key)

data PackageNode' dalfPackage
  = MkDependencyPackageNode (DependencyPackageNode' dalfPackage)
  | MkDataDependencyPackageNode (DataDependencyPackageNode' dalfPackage)
  | MkBuiltinDependencyPackageNode BuiltinDependencyPackageNode
  | MkStableDependencyPackageNode

type PackageNode = PackageNode' LF.DalfPackage

isDependencyPackageNode :: PackageNode' dalfPackage -> Bool
isDependencyPackageNode = \case
  MkDependencyPackageNode {} -> True
  _ -> False

isDataDependencyPackageNode :: PackageNode' dalfPackage -> Bool
isDataDependencyPackageNode = \case
  MkDataDependencyPackageNode {} -> True
  _ -> False

packageNodeDecodedDalf :: PackageNode -> Maybe DecodedDalf
packageNodeDecodedDalf = \case
  MkDependencyPackageNode DependencyPackageNode {unitId, dalfPackage} ->
    Just $ DecodedDalf dalfPackage unitId
  MkDataDependencyPackageNode DataDependencyPackageNode {unitId, dalfPackage} ->
    Just $ DecodedDalf dalfPackage unitId
  MkBuiltinDependencyPackageNode BuiltinDependencyPackageNode {} ->
    Nothing
  MkStableDependencyPackageNode ->
    Nothing

data DependencyPackageNode' dalfPackage = DependencyPackageNode
  { dalf :: FilePath
  , unitId :: UnitId
  , dalfPackage :: dalfPackage
  }

data DataDependencyPackageNode' dalfPackage = DataDependencyPackageNode
  { unitId :: UnitId
  , dalfPackage :: dalfPackage
  }

data BuiltinDependencyPackageNode = BuiltinDependencyPackageNode
  { unitId :: UnitId
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

showDeps :: [((LF.PackageId, UnitId), LF.Version)] -> String
showDeps deps =
    intercalate
        ", "
        [ T.unpack (LF.unPackageId pkgId) <> " (" <> unitIdString unitId <> "): " <>
        DA.Pretty.renderPretty ver
        | ((pkgId, unitId), ver) <- deps
        ]

checkForInconsistentLfVersions ::
     LF.Version
  -- ^ Target LF version.
  -> [DecodedDalf]
  -- ^ All dalfs (not just main DALFs) in DARs listed in `dependencies`.
  -- This does not include DALFs in the global package db like daml-prim.
  -- Note that a DAR does not include interface files for dependencies
  -- so to use a DAR as a `dependency` you also need to list the DARs of all
  -- of its dependencies.
  -> [UnitId]
  -- ^ Unit id of the main DALFs specified in dependencies and the main DALFs
  -- of DARs specified in data-dependencies. This will be used to generate the
  -- --package flags which define which packages are exposed by default.
  -> Either String ()
checkForInconsistentLfVersions lfTarget dalfsFromDependencies mainUnitIds
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

checkForIncompatibleLfVersions ::
     LF.Version
  -- ^ Target LF version.
  -> [DecodedDalf]
  -- ^ All dalfs (not just main DALFs) in DARs listed in `dependencies` and
  -- all dalfs (not just main DALFs) from DARs and DALFs listed in `data-dependencies`.
  -- This does not include DALFs in the global package db like daml-prim.
  -- Note that a DAR does not include interface files for dependencies
  -- so to use a DAR as a `dependency` you also need to list the DARs of all
  -- of its dependencies.
  -- Note that for data-dependencies it is sufficient to list a DAR without
  -- listing all of its dependencies.
  -> Either String ()
checkForIncompatibleLfVersions lfTarget dalfs
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
            | DecodedDalf{..} <- dalfs
            ]

checkForUnitIdConflicts ::
     [DecodedDalf]
  -- ^ All dalfs (not just main DALFs) in DARs listed in `dependencies` and
  -- all dalfs (not just main DALFs) from DARs and DALFs listed in `data-dependencies`.
  -- This does not include DALFs in the global package db like daml-prim.
  -- Note that a DAR does not include interface files for dependencies
  -- so to use a DAR as a `dependency` you also need to list the DARs of all
  -- of its dependencies.
  -- Note that for data-dependencies it is sufficient to list a DAR without
  -- listing all of its dependencies.
  -> MS.Map UnitId LF.DalfPackage
  -- ^ Dependencies picked up by the GeneratePackageMap rule.
  -- The rule is run before installing DALFs from `dependencies` so
  -- this only includes dependencies in the builtin package db
  -- like daml-prim and daml-stdlib
  -> Either String ()
checkForUnitIdConflicts dalfs builtinDependencies
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
          | DecodedDalf{..} <- dalfs
          ]
        , [ (unitId, Set.singleton (LF.dalfPackageId dalfPkg))
          | (unitId, dalfPkg) <- MS.toList builtinDependencies
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
