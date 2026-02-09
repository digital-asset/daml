-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DuplicateRecordFields #-}

module DA.Cli.Damlc.Packaging
  ( createPackageDb
  , setupPackageDb
  , setupPackageDbFromPackageConfig
  , mbErr
  , getUnitId

    -- * Dependency graph construction
  , buildLfPackageGraph'
  , BuildLfPackageGraphArgs' (..)
  , BuildLfPackageGraphMetaArgs (..)
  ) where

import Control.Exception.Safe (tryAny)
import Control.Lens (none, toListOf)
import Control.Monad.Extra (forM_, fromMaybeM, when)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Maybe (MaybeT (..), runMaybeT)
import qualified Control.Monad.Trans.State.Strict as State
import qualified Data.ByteString as BS
import Data.Either.Combinators (whenLeft)
import Data.Graph (Graph, Vertex, graphFromEdges, reachable, topSort, transposeG, vertices)
import Data.List.Extra ((\\), intercalate, nubOrd, nubSortOn)
import qualified Data.Map.Strict as MS
import Data.Maybe (fromMaybe, mapMaybe)
import qualified Data.NameMap as NM
import Data.Set (Set)
import qualified Data.Set as Set
import qualified Data.Text.Extended as T
import Data.Tuple.Extra (fst3, snd3)
import Development.IDE.Core.Rules (transitivePkgDeps, useE, usesE, useNoFileE)
import Development.IDE.Core.Service (runActionSync)
import Development.IDE.GHC.Util (hscEnv)
import Development.IDE.Types.Location (NormalizedFilePath, fromNormalizedFilePath, toNormalizedFilePath')
import "ghc-lib-parser" DynFlags (DynFlags)
import GHC.Fingerprint (Fingerprint, fingerprintFingerprints, getFileHash)
import "ghc-lib-parser" HscTypes as GHC
import "ghc-lib-parser" Module (UnitId, unitIdString)
import qualified Module as GHC
import qualified "ghc-lib-parser" Packages as GHC
import System.Directory.Extra (copyFile, createDirectoryIfMissing, listFilesRecursive, removePathForcibly)
import System.Exit
import System.FileLock
import System.FilePath
import System.IO.Extra (hFlush, hPutStrLn, stderr, writeFileUTF8)
import System.Info.Extra
import System.Process (callProcess)
import "ghc-lib-parser" UniqSet

import DA.Bazel.Runfiles
import DA.Cli.Damlc.DependencyDb
import DA.Daml.Assistant.Env (getDamlEnv, getDamlPath, envUseCache)
import DA.Daml.Assistant.Types (LookForPackagePath (..))
import DA.Daml.Assistant.Util (wrapErr)
import DA.Daml.Assistant.Version (resolveReleaseVersionUnsafe)
import DA.Daml.Compiler.Dar
import DA.Daml.Compiler.DataDependencies as DataDeps
import DA.Daml.Compiler.DecodeDar (DecodedDalf(..), decodeDalf)
import DA.Daml.Compiler.Output
import DA.Daml.LF.Ast.Optics (packageRefs)
import DA.Daml.Options.Packaging.Metadata
import DA.Daml.Options.Types
import DA.Daml.Package.Config (PackageConfigFields (..))
import DA.Daml.Project.Consts (damlAssistantIsSet)
import DA.Daml.Project.Types (ReleaseVersion, unsafeResolveReleaseVersion)
import Development.IDE.Core.IdeState.Daml
import Development.IDE.Core.RuleTypes.Daml
import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LFConversion.MetadataEncoding as LFC
import qualified DA.Pretty
import qualified DA.Service.Logger as Logger
import SdkVersion.Class (SdkVersioned, damlStdlib, unresolvedBuiltinSdkVersion)

-- | Create the package database containing the given dar packages.
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
createPackageDb :: SdkVersioned => NormalizedFilePath -> Options -> MS.Map UnitId GHC.ModuleName -> IO ()
createPackageDb packageRoot (disableScriptService -> opts) modulePrefixes
  = do
    (needsReinitalization, depsFingerprint) <- dbNeedsReinitialization packageRoot depsDir modulePrefixes
    loggerH <- getLogger opts "package-db"
    when needsReinitalization $ do
      Logger.logDebug loggerH "package db is not up2date, reinitializing"
      clearPackageDb

      -- since we haven't registered any dependencies yet, builtinDependencies only contains daml-prim and daml-stdlib
      (stablePkgs, PackageMap builtinDependencies) <-
        fromMaybeM (fail "Failed to generate package info") $
          withDamlIdeState opts loggerH diagnosticsLogger $ \ide -> runActionSync ide $ runMaybeT $
            (,) <$> useNoFileE GenerateStablePackages
                <*> (fst <$> useE GeneratePackageMap packageRoot)

      let builtinDependenciesIds =
              Set.fromList $ map LF.dalfPackageId $ MS.elems builtinDependencies

      let decodeDalf_ dalf = do
            bs <- BS.readFile dalf
            (dalf,) <$> either fail pure (decodeDalf builtinDependenciesIds bs)

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
          pkgUnitIdMap = MS.fromList
            [ (pkgId, unitId)
            | (node, pkgId) <- vertexToNode <$> vertices depGraph
            , unitId <- case node of
                MkStableDependencyPackageNode -> []
                MkBuiltinDependencyPackageNode BuiltinDependencyPackageNode {unitId} -> [unitId]
                MkDependencyPackageNode DependencyPackageNode {unitId} -> [unitId]
                MkDataDependencyPackageNode DataDependencyPackageNode {unitId} -> [unitId]
            ]

        forM_ (topSort $ transposeG depGraph) $ \vertex -> do
          let (pkgNode, pkgId) = vertexToNode vertex
          case pkgNode of
            MkStableDependencyPackageNode -> do
              -- stable packages are mapped to the current version of daml-prim/daml-stdlib
              -- so we don’t need to generate interface files for them.
              pure ()
            MkBuiltinDependencyPackageNode {} -> do
              pure ()
            MkDependencyPackageNode DependencyPackageNode {dalf, unitId, dalfPackage} -> do
              liftIO $ registerDepInPkgDb dalf depsDir dbPath
              insert unitId dalfPackage
            MkDataDependencyPackageNode DataDependencyPackageNode {unitId, dalfPackage, referencesPkgs} -> do
              dependenciesSoFar <- State.get
              let depUnitIds :: [UnitId]
                  depUnitIds = mapMaybe (`MS.lookup` pkgUnitIdMap) referencesPkgs

              liftIO $ installDataDep InstallDataDepArgs
                { opts
                , packageRoot
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
          packageRoot
          (PackageDbMetadata mainUnitIds validatedModulePrefixes depsFingerprint)
  where
    dbPath = packageDatabasePath </> lfVersionString (optDamlLfVersion opts)
    depsDir = dependenciesDir opts packageRoot
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
       NormalizedFilePath -> FilePath -> MS.Map UnitId GHC.ModuleName -> IO (Bool, Fingerprint)
dbNeedsReinitialization packageRoot depsDir modulePrefixes = do
    allDeps <- listFilesRecursive depsDir
    fileFingerprints <- mapM getFileHash allDeps
    let depsFingerprint = fingerprintFingerprints fileFingerprints
    -- Read the metadata of an already existing package database and see if wee need to reinitialize.
    errOrmetaData <- tryAny $ readMetadata packageRoot
    pure $
        case errOrmetaData of
            Left _err -> (True, depsFingerprint)
            Right metaData ->
              let fingerprintChanged = fingerprintDependencies metaData /= depsFingerprint
                  -- Use `fst` to throw away the contained module list, as the daml.yaml doesn't contain this information
                  modulePrefixesChanged = (fst <$> moduleRenamings metaData) /= modulePrefixes
               in (fingerprintChanged || modulePrefixesChanged, depsFingerprint)

disableScriptService :: Options -> Options
disableScriptService opts = opts
  { optScriptService = EnableScriptService False
  }

toGhcModuleName :: LF.ModuleName -> GHC.ModuleName
toGhcModuleName = GHC.mkModuleName . T.unpack . LF.moduleNameString


data InstallDataDepArgs = InstallDataDepArgs
  { opts :: Options
  , packageRoot :: NormalizedFilePath
  , dbPath :: FilePath
  , pkgs :: [DecodedDalf]
    -- ^ All the packages in the dependency graph of the package being built.
  , stablePkgs :: MS.Map (UnitId, LF.ModuleName) LF.DalfPackage
  , dependenciesSoFar :: MS.Map UnitId LF.DalfPackage
    -- ^ The dependencies and data-dependencies processed before this data-dependency.
  , depUnitIds :: [UnitId]
    -- ^ The UnitIds of the dependencies and data-dependencies of this data-dependency.
    -- The way we traverse the dependency graph ensures these have all been processed.
    -- This will not include dependencies added to the graph for instance rewriting
  , pkgId :: LF.PackageId
  , unitId :: UnitId
  , dalfPackage :: LF.DalfPackage
  }

installDataDep :: SdkVersioned => InstallDataDepArgs -> IO ()
installDataDep InstallDataDepArgs {..} = do
  exposedModules <- getExposedModules opts packageRoot

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
        , configIgnoreExplicitExports = getIgnoreDataDepVisibility $ optIgnoreDataDepVisibility opts
        , configExplicitSerializable = optExplicitSerializable opts
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
    -- ^ List of units directly referenced by this package.
  , dependenciesSoFar :: MS.Map UnitId LF.DalfPackage
    -- ^ The dependencies and data-dependencies processed before this data-dependency.
  , exposedModules :: MS.Map UnitId (UniqSet GHC.ModuleName)
  }

-- | Generate interface files and install them in the package database
generateAndInstallIfaceFiles :: SdkVersioned => GenerateAndInstallIfaceFilesArgs -> IO ()
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
            , optPackageDbs = packageDatabasePath : optPackageDbs opts
            , optIsGenerated = True
            , optDflagCheck = False
            , optMbPackageName = Just pkgName
            , optMbPackageVersion = mbPkgVersion
            -- This is essentially a new package options, cannot inherit optHideUnitId = True from IDE
            -- or the dependency will be installed as "main"
            , optHideUnitId = False
            , optGhcCustomOpts = []
            , optPackageImports =
                  baseImports ++
                  depImps
            -- When compiling dummy interface files for a data-dependency,
            -- we know all package flags so we don’t need to consult metadata.
            , optIgnorePackageMetadata = IgnorePackageMetadata True
            }

    mDepUnitIds <- withDamlIdeState opts loggerH diagnosticsLogger $ \ide ->
        runActionSync ide $ runMaybeT $ do
            -- Setting ifDir to . means that the interface files will end up directly next to
            -- the source files which is what we want here.
            let files = [fp | (fp, _content) <- src']
            _ <- MaybeT $ writeIfacesAndHie (toNormalizedFilePath' ".") files
            -- We cannot directly use the dependencies in the generated graph here, as for
            -- data-dependencies, they include all regular dependencies, so they are loaded first
            -- and can be used in instance rewriting in the generated data dep code.
            -- Instead, depUnitIds contains only the original dependencies,
            -- and we rerun GetDependencies to capture any new dependencies added by
            -- instance rewriting
            usedInstalledUnitIds <- concatMap (transitivePkgDeps . fst) <$> usesE GetDependencies files
            let usedUnitIds = GHC.DefiniteUnitId . GHC.DefUnitId <$> usedInstalledUnitIds
            pure $ nubOrd $ usedUnitIds <> depUnitIds

    realDepUnitIds <- case mDepUnitIds of
        Nothing ->
            exitWithError
                $ "Failed to compile interface for data-dependency: "
                <> unitIdString (pkgNameVersion pkgName mbPkgVersion)
        Just realDepUnitIds -> pure realDepUnitIds
    -- write the conf file and refresh the package cache
    let (cfPath, cfBs) = mkConfFile
            pkgName
            mbPkgVersion
            realDepUnitIds
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
    ghcPkgExe <- getGhcPkgExe
    callProcess
        ghcPkgExe
        [ "recache"
        -- ghc-pkg insists on using a global package db and will try
        -- to find one automatically if we don’t specify it here.
        , "--global-package-db=" ++ (dbPath </> "package.conf.d")
        , "--expand-pkgroot"
        ]

-- TODO We should generate the list of stable packages automatically here.
baseImports :: SdkVersioned => [PackageFlag]
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
           , "DA.Internal.Serializable"
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
          , "DA.Internal.Interface.AnyView.Types"
          , "DA.Action.State.Type"
          , "DA.Random.Types"
          , "DA.Stack.Types"
          , "DA.Internal.Fail"
          , "DA.Internal.Fail.Types"
          ]
       )
    ]

-- | A helper to construct package ref to unit id maps.
getUnitId :: UnitId -> MS.Map LF.PackageId UnitId -> LF.SelfOrImportedPackageId -> UnitId
getUnitId thisUnitId pkgMap =
    \case
        LF.SelfPackageId -> thisUnitId
        LF.ImportedPackageId pId ->
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
getGhcPkgExe :: IO FilePath
getGhcPkgExe = locateResource Resource
  { resourcesPath = exe "ghc-pkg"
    -- //compiler/damlc:ghc-pkg-dist
    -- In a packaged application, the executable is stored directly underneath
    -- the resources directory because the target produces a tarball which has
    -- the executable directly under the top directory.
    -- See @bazel_tools/packaging/packaging.bzl@.
  , runfilesPathPrefix =
      -- when running as a bazel target, the executable has the same name
      -- but comes from the a different target depending on the OS, so the
      -- prefix used changes.
      -- see @compiler/damlc/util.bzl@
      if isWindows then
        -- @rules_haskell_ghc_windows_amd64//:bin/ghc-pkg.exe
        "rules_haskell_ghc_windows_amd64" </> "bin"
      else
        -- @ghc_nix//:lib/ghc-9.0.2/bin/ghc-pkg
        "ghc_nix" </> "lib" </> "ghc-9.0.2" </> "bin"
  }

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
      , getDalfPkgRefs = nubOrd . dalfPackageRefs
      }
  where
    dalfPackageRefs :: LF.DalfPackage -> [LF.PackageId]
    dalfPackageRefs dalfPkg =
      let
        pkg = LF.extPackagePkg (LF.dalfPackagePkg dalfPkg)
        pid = LF.dalfPackageId dalfPkg
      in
        [ pid'
        | LF.ImportedPackageId pid' <-
            toListOf packageRefs pkg
              <>
              [ qualPackage
              | m <- NM.toList $ LF.packageModules pkg
              , Just LF.DefValue{dvalBinder=(_, ty)} <- [NM.lookup LFC.moduleImportsName (LF.moduleValues m)]
              , Just quals <- [LFC.decodeModuleImports ty]
              , LF.Qualified { LF.qualPackage } <- Set.toList quals
              ]
              -- Pull out all package ids from re-exports as well
              <>
              [ qualPackage
              | m <- NM.toList $ LF.packageModules pkg
              , LF.DefValue {dvalBinder=(name, ty)} <- NM.toList $ LF.moduleValues m
              , Just _ <- [LFC.unReExportName name]
              , Just export <- [LFC.decodeExportInfo ty]
              , LFC.QualName (LF.Qualified { LF.qualPackage }) <-
                  [ case export of 
                      LFC.ExportInfoVal name -> name
                      LFC.ExportInfoTC name _ _ -> name
                  ]
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
    -- Extend the dependency graph with edges for data dependencies to
    -- regular deps (that do not themselves have data deps)
    -- See case 2. in comment for buildLfPackageGraph
    (depGraph, vertexToNode, keyToVertex) =
        graphFromEdges
          [ (node', key, deps')
          | v <- vertices depGraph0
          , let (node, key, deps) = vertexToNode0 v
          , let (node', deps') = case node of
                  MkDataDependencyPackageNode node ->
                    ( MkDataDependencyPackageNode $ node {referencesPkgs = snd3 . vertexToNode0 <$> filter (/= v) (reachable depGraph0 v)}
                    , deps <> depsWithoutDataDeps
                    )
                  _ -> (node, deps)
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
          -- the package we're building has multiple data-dependencies from older
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
                referencesPkgs = []
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
  , referencesPkgs :: [LF.PackageId]
    -- ^ Since data-dependency deps are extended to handle instance rewriting
    -- we keep the original deps in the node for populating the database
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
        , " but dependencies have incompatible LF versions: "
        , showDeps incompatibleLfDeps
        ]
  where
    incompatibleLfDeps =
        filter (\(_, ver) -> not (lfTarget `LF.canDependOn` ver)) $
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

getExposedModules :: SdkVersioned => Options -> NormalizedFilePath -> IO (MS.Map UnitId (UniqSet GHC.ModuleName))
getExposedModules opts packageRoot = do
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
        runActionSync ide $ runMaybeT $ fst <$> useE GhcSession packageRoot
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

unsafeSetupPackageDb
    :: SdkVersioned
    => NormalizedFilePath
    -> Options
    -> ReleaseVersion
    -> [String] -- Package dependencies. Can be base-packages, sdk-packages or filepath.
    -> [FilePath] -- Data Dependencies. Can be filepath to dars/dalfs.
    -> MS.Map UnitId GHC.ModuleName
    -> IO ()
unsafeSetupPackageDb packageRoot opts releaseVersion pDependencies pDataDependencies pModulePrefixes = do
    installDependencies
        packageRoot
        opts
        releaseVersion
        pDependencies
        pDataDependencies
    createPackageDb packageRoot opts pModulePrefixes

withPkgDbLock :: NormalizedFilePath -> IO a -> IO a
withPkgDbLock packageRoot act = do
    let packageDbLockFile = packageDbLockPath packageRoot
    createDirectoryIfMissing True $ takeDirectory packageDbLockFile
    withFileLock packageDbLockFile Exclusive $ const act

-- Installs dependencies and creates package Db ready to be used
setupPackageDb
    :: SdkVersioned
    => NormalizedFilePath
    -> Options
    -> ReleaseVersion
    -> [String] -- Package dependencies. Can be base-packages, sdk-packages or filepath.
    -> [FilePath] -- Data Dependencies. Can be filepath to dars/dalfs.
    -> MS.Map UnitId GHC.ModuleName
    -> IO ()
setupPackageDb packageRoot opts releaseVersion pDependencies pDataDependencies pModulePrefixes =
    withPkgDbLock packageRoot $ unsafeSetupPackageDb packageRoot opts releaseVersion pDependencies pDataDependencies pModulePrefixes

setupPackageDbFromPackageConfig
    :: SdkVersioned
    => NormalizedFilePath
    -> Options
    -> PackageConfigFields
    -> IO ()
setupPackageDbFromPackageConfig packageRoot opts PackageConfigFields {..} =
    withPkgDbLock packageRoot $ do
        let sdkVersion = fromMaybe unresolvedBuiltinSdkVersion pSdkVersion
        damlAssistantIsSet <- damlAssistantIsSet
        releaseVersion <- if damlAssistantIsSet
            then do
              damlPath <- getDamlPath
              damlEnv <- getDamlEnv damlPath (LookForPackagePath False)
              wrapErr "installing dependencies and initializing package database" $
                resolveReleaseVersionUnsafe (envUseCache damlEnv) sdkVersion
            else pure (unsafeResolveReleaseVersion sdkVersion)
        unsafeSetupPackageDb packageRoot opts releaseVersion pDependencies pDataDependencies pModulePrefixes
