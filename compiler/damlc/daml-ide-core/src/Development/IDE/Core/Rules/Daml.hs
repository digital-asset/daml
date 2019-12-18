-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module Development.IDE.Core.Rules.Daml
    ( module Development.IDE.Core.Rules
    , module Development.IDE.Core.Rules.Daml
    ) where

import Outputable (showSDoc)
import TcIface (typecheckIface)
import LoadIface (readIface)
import TidyPgm
import DynFlags
import qualified GHC
import qualified Module as GHC
import GhcMonad
import Data.IORef
import qualified Proto3.Suite             as Proto
import DA.Daml.LF.Proto3.DecodeV1
import DA.Daml.LF.Proto3.EncodeV1
import HscTypes
import MkIface
import Maybes (MaybeErr(..))
import TcRnMonad (initIfaceLoad)

import System.IO

import Control.Concurrent.Extra
import Control.Exception
import Control.Monad.Except
import Control.Monad.Extra
import Control.Monad.Trans.Maybe
import Development.IDE.Core.Compile
import Development.IDE.GHC.Error
import Development.IDE.GHC.Warnings
import Development.IDE.Core.OfInterest
import Development.IDE.GHC.Util
import Development.IDE.Types.Logger hiding (Priority)
import DA.Daml.Options.Types
import qualified Text.PrettyPrint.Annotated.HughesPJClass as HughesPJPretty
import Development.IDE.Types.Location as Base
import Data.Aeson hiding (Options)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.UTF8 as BS
import Data.Either.Extra
import Data.Foldable
import Data.List.Extra
import qualified Data.List.NonEmpty as NonEmpty
import qualified Data.IntMap.Strict as IntMap
import qualified Data.Map.Strict as Map
import Data.Maybe
import qualified Data.NameMap as NM
import Data.Set (Set)
import qualified Data.Set as Set
import qualified Data.Text.Encoding as T
import qualified Data.Text.Extended as T
import qualified Data.Text.Lazy as TL
import Data.Tuple.Extra
import Development.Shake hiding (Diagnostic, Env, doesFileExist)
import "ghc-lib" GHC hiding (typecheckModule, Succeeded)
import "ghc-lib-parser" Module (stringToUnitId, UnitId(..), DefUnitId(..))
import Safe
import System.IO.Error
import System.Directory.Extra
import System.FilePath

import qualified Network.HTTP.Types as HTTP.Types
import qualified Network.URI as URI

import Development.IDE.Import.DependencyInformation
import Development.IDE.Core.Rules hiding (mainRule)
import qualified Development.IDE.Core.Rules as IDE
import Development.IDE.Core.Service.Daml
import Development.IDE.Core.Shake
import Development.IDE.Types.Diagnostics
import qualified Language.Haskell.LSP.Messages as LSP
import qualified Language.Haskell.LSP.Types as LSP

import Development.IDE.Core.RuleTypes.Daml

import DA.Daml.DocTest
import DA.Daml.LFConversion (convertModule, sourceLocToRange)
import DA.Daml.LFConversion.UtilLF
import DA.Daml.LF.Reader (stripPkgId)
import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.InferSerializability as Serializability
import qualified DA.Daml.LF.PrettyScenario as LF
import qualified DA.Daml.LF.Proto3.Archive as Archive
import qualified DA.Daml.LF.ScenarioServiceClient as SS
import qualified DA.Daml.LF.Simplifier as LF
import qualified DA.Daml.LF.TypeChecker as LF
import qualified DA.Pretty as Pretty
import SdkVersion (damlStdlib)

import qualified Language.Haskell.Exts.SrcLoc as HSE
import Language.Haskell.HLint4

-- | Get thr URI that corresponds to a virtual resource. The VS Code has a
-- document provider that will handle our special documents.
-- The Uri looks like this:
-- daml://[command]/[client data]?[server]=[key]&[key]=[value]
--
-- The command tells the server if it should do scenario interpretation or
-- core translation.
-- The client data is here to transmit data from the client to the client.
-- The server ignores this part and is even allowed to change it.
-- The server data is here to send data to the server, like what file we
-- want to translate.
--
-- The client uses a combination of the command and server data
-- to generate a caching key.
virtualResourceToUri
    :: VirtualResource
    -> T.Text
virtualResourceToUri vr = case vr of
    VRScenario filePath topLevelDeclName ->
        T.pack $ "daml://compiler?" <> keyValueToQueryString
            [ ("file", fromNormalizedFilePath filePath)
            , ("top-level-decl", T.unpack topLevelDeclName)
            ]
  where
    urlEncode :: String -> String
    urlEncode = URI.escapeURIString URI.isUnreserved

    keyValueToQueryString :: [(String, String)] -> String
    keyValueToQueryString kvs =
        intercalate "&"
      $ map (\(k, v) -> k ++ "=" ++ urlEncode v) kvs

uriToVirtualResource
    :: URI.URI
    -> Maybe VirtualResource
uriToVirtualResource uri = do
    guard $ URI.uriScheme uri == "daml:"
    case URI.uriRegName <$> URI.uriAuthority uri of
        Just "compiler" -> do
            let decoded = queryString uri
            file <- Map.lookup "file" decoded
            topLevelDecl <- Map.lookup "top-level-decl" decoded
            pure $ VRScenario (toNormalizedFilePath file) (T.pack topLevelDecl)
        _ -> Nothing

  where
    queryString :: URI.URI -> Map.Map String String
    queryString u0 = fromMaybe Map.empty $ case tailMay $ URI.uriQuery u0 of
        Nothing -> Nothing
        Just u ->
            Just
          $ Map.fromList
          $ map (\(k, v) -> (BS.toString k, BS.toString v))
          $ HTTP.Types.parseSimpleQuery
          $ BS.fromString
          $ URI.unEscapeString u

sendFileDiagnostics :: [FileDiagnostic] -> Action ()
sendFileDiagnostics diags =
    mapM_ (uncurry sendDiagnostics) (groupSort diags)

-- TODO: Move this to ghcide, perhaps.
sendDiagnostics :: NormalizedFilePath -> [Diagnostic] -> Action ()
sendDiagnostics fp diags = do
    let uri = filePathToUri (fromNormalizedFilePath fp)
        event = LSP.NotPublishDiagnostics $
            LSP.NotificationMessage "2.0" LSP.TextDocumentPublishDiagnostics $
            LSP.PublishDiagnosticsParams uri (List diags)
            -- ^ This is just 'publishDiagnosticsNotification' from ghcide.
    sendEvent event

-- | Get an unvalidated DALF package.
-- This must only be used for debugging/testing.
getRawDalf :: NormalizedFilePath -> Action (Maybe LF.Package)
getRawDalf absFile = fmap getWhnfPackage <$> use GenerateRawPackage absFile

-- | Get a validated DALF package.
getDalf :: NormalizedFilePath -> Action (Maybe LF.Package)
getDalf file = fmap getWhnfPackage <$> use GeneratePackage file

-- | A dependency on a compiled library.
data DalfDependency = DalfDependency
  { ddName         :: !T.Text
    -- ^ The name of the dependency.
  , ddDalfFile     :: !FilePath
    -- ^ The absolute path to the dalf file.
  }

getDlintIdeas :: NormalizedFilePath -> Action (Maybe ())
getDlintIdeas f = runMaybeT $ useE GetDlintDiagnostics f

ideErrorPretty :: Pretty.Pretty e => NormalizedFilePath -> e -> FileDiagnostic
ideErrorPretty fp = ideErrorText fp . T.pack . HughesPJPretty.prettyShow

finalPackageCheck :: NormalizedFilePath -> LF.Package -> Action (Maybe ())
finalPackageCheck fp pkg = do
    case LF.nameCheckPackage pkg of
        Left e -> do
            sendFileDiagnostics [ideErrorPretty fp e]
            pure Nothing

        Right () ->
            pure $ Just ()

getDalfDependencies :: [NormalizedFilePath] -> MaybeT Action (Map.Map UnitId LF.DalfPackage)
getDalfDependencies files = do
    unitIds <- concatMap transitivePkgDeps <$> usesE GetDependencies files
    pkgMap <- useNoFileE GeneratePackageMap
    let actualDeps = Map.restrictKeys pkgMap (Set.fromList $ map (DefiniteUnitId . DefUnitId) unitIds)
    -- For now, we unconditionally include all stable packages.
    -- Given that they are quite small and it is pretty much impossible to not depend on them
    -- this is fine. We might want to try being more clever here in the future.
    stablePackages <-
        fmap (Map.mapKeys stableUnitId) $
        useNoFileE GenerateStablePackages
    pure $ stablePackages `Map.union` actualDeps
  where stableUnitId (unitId, modName) = stringToUnitId $ GHC.unitIdString unitId <> "-" <> T.unpack (T.intercalate "-" $ LF.unModuleName modName)

runScenarios :: NormalizedFilePath -> Action (Maybe [(VirtualResource, Either SS.Error SS.ScenarioResult)])
runScenarios file = use RunScenarios file

-- | Get a list of the scenarios in a given file
getScenarioNames :: NormalizedFilePath -> Action (Maybe [VirtualResource])
getScenarioNames file = fmap f <$> use GenerateRawDalf file
    where f = map (VRScenario file . LF.unExprValName . LF.qualObject . fst) . scenariosInModule

priorityGenerateDalf :: Priority
priorityGenerateDalf = priorityGenerateCore

-- Generates the DALF for a module without adding serializability information
-- or type checking it.
generateRawDalfRule :: Rules ()
generateRawDalfRule =
    define $ \GenerateRawDalf file -> do
        lfVersion <- getDamlLfVersion
        (coreDiags, mbCore) <- generateCore file
        fmap (first (coreDiags ++)) $
            case mbCore of
                Nothing -> return ([], Nothing)
                Just (safeMode, cgGuts, details) -> do
                    let core = cgGutsToCoreModule safeMode cgGuts details
                    setPriority priorityGenerateDalf
                    -- Generate the map from package names to package hashes
                    pkgMap <- useNoFile_ GeneratePackageMap
                    stablePkgs <- useNoFile_ GenerateStablePackages
                    DamlEnv{envIsGenerated} <- getDamlServiceEnv
                    -- GHC Core to DAML LF
                    case convertModule lfVersion pkgMap (Map.map LF.dalfPackageId stablePkgs) envIsGenerated file core of
                        Left e -> return ([e], Nothing)
                        Right v -> return ([], Just $ LF.simplifyModule v)

getExternalPackages :: Action [LF.ExternalPackage]
getExternalPackages = do
    pkgMap <- useNoFile_ GeneratePackageMap
    stablePackages <- useNoFile_ GenerateStablePackages
    -- We need to dedup here to make sure that each package only appears once.
    pure $
        Map.elems $ Map.fromList $ map (\e@(LF.ExternalPackage pkgId _) -> (pkgId, e)) $
        map LF.dalfPackagePkg (Map.elems pkgMap) <> map LF.dalfPackagePkg (Map.elems stablePackages)

-- Generates and type checks the DALF for a module.
generateDalfRule :: Rules ()
generateDalfRule =
    define $ \GenerateDalf file -> do
        lfVersion <- getDamlLfVersion
        WhnfPackage pkg <- use_ GeneratePackageDeps file
        pkgs <- getExternalPackages
        let world = LF.initWorldSelf pkgs pkg
        rawDalf <- use_ GenerateRawDalf file
        setPriority priorityGenerateDalf
        pure $ toIdeResult $ do
            let liftError e = [ideErrorPretty file e]
            dalf <- mapLeft liftError $
                Serializability.inferModule world lfVersion rawDalf
            mapLeft liftError $ LF.checkModule world lfVersion dalf
            pure dalf

-- TODO Share code with typecheckModule in ghcide. The environment needs to be setup
-- slightly differently but we can probably factor out shared code here.
ondiskTypeCheck :: HscEnv -> [(ModSummary, ModIface)] -> ParsedModule -> IO ([FileDiagnostic], Maybe TcModuleResult)
ondiskTypeCheck hsc deps pm = do
    fmap (either (, Nothing) (second Just)) $
      runGhcEnv hsc $
      catchSrcErrors "typecheck" $ do
        let mss = map fst deps
        session <- getSession
        setSession session { hsc_mod_graph = mkModuleGraph mss }
        let installedModules  = map (GHC.InstalledModule (thisInstalledUnitId $ hsc_dflags session) . moduleName . ms_mod) mss
            installedFindResults = zipWith (\ms im -> InstalledFound (ms_location ms) im) mss installedModules
        -- We have to create a new IORef here instead of modifying the existing IORef as
        -- it is shared between concurrent compilations.
        prevFinderCache <- liftIO $ readIORef $ hsc_FC session
        let newFinderCache =
                foldl'
                    (\fc (im, ifr) -> GHC.extendInstalledModuleEnv fc im ifr) prevFinderCache
                    $ zip installedModules installedFindResults
        newFinderCacheVar <- liftIO $ newIORef $! newFinderCache
        modifySession $ \s -> s { hsc_FC = newFinderCacheVar }
        -- Currently GetDependencies returns things in topological order so A comes before B if A imports B.
        -- We need to reverse this as GHC gets very unhappy otherwise and complains about broken interfaces.
        -- Long-term we might just want to change the order returned by GetDependencies
        mapM_ (uncurry loadDepModule) (reverse deps)
        (warnings, tcm) <- withWarnings "typecheck" $ \tweak ->
            GHC.typecheckModule pm { pm_mod_summary = tweak (pm_mod_summary pm) }
        tcm <- mkTcModuleResult tcm
        pure (map snd warnings, tcm)

loadDepModule :: GhcMonad m => ModSummary -> ModIface -> m ()
loadDepModule ms iface = do
    hsc <- getSession
    -- The fixIO here is crucial and matches what GHC does. Otherwise GHC will fail
    -- to find identifiers in the interface and explode.
    -- For more details, look at hscIncrementalCompile and Note [Knot-tying typecheckIface] in GHC.
    details <- liftIO $ fixIO $ \details -> do
        let hsc' = hsc { hsc_HPT = addToHpt (hsc_HPT hsc) (moduleName mod) (HomeModInfo iface details Nothing) }
        initIfaceLoad hsc' (typecheckIface iface)
    let mod_info = HomeModInfo iface details Nothing
    modifySession $ \e ->
        e { hsc_HPT = addToHpt (hsc_HPT e) (moduleName mod) mod_info }
    where mod = ms_mod ms

-- TODO Share code with compileModule in ghcide. Given that this is fairly mechanical, this is not critical
-- but still worth doing in the long-term.
ondiskDesugar :: HscEnv -> TypecheckedModule -> IO ([FileDiagnostic], Maybe CoreModule)
ondiskDesugar hsc tm =
    fmap (either (, Nothing) (second Just)) $
    runGhcEnv hsc $
        catchSrcErrors "compile" $ do
            session <- getSession
            (warnings, desugar) <- withWarnings "compile" $ \tweak -> do
                let pm = tm_parsed_module tm
                let pm' = pm{pm_mod_summary = tweak $ pm_mod_summary pm}
                let tm' = tm{tm_parsed_module  = pm'}
                GHC.dm_core_module <$> GHC.desugarModule tm'
            -- give variables unique OccNames
            (tidy, details) <- liftIO $ tidyProgram session desugar

            let core = CoreModule
                         (cg_module tidy)
                         (md_types details)
                         (cg_binds tidy)
                         (mg_safe_haskell desugar)

            return (map snd warnings, core)

-- This rule is for on-disk incremental builds. We cannot use the fine-grained rules that we have for
-- in-memory builds since we need to be able to serialize intermediate results. GHC doesn’t provide a way to serialize
-- TypeCheckedModules or CoreModules. In addition to that, making this too fine-grained would probably also incur a performance penalty.
-- Therefore we have a single rule that performs the steps parsed module -> typechecked module -> core module -> DAML-LF module.
-- This rule writes both the .dalf and the .hi files.
-- We use the ABI hash of the .hi files to detect if we need to recompile dependent files. Note that this is more aggressive
-- than just looking at the file hash. E.g., consider module A depending on module B. If B changes but its ABI hash stays the same
-- we do not need to recompile A.
generateSerializedDalfRule :: Options -> Rules ()
generateSerializedDalfRule options =
    defineOnDisk $ \GenerateSerializedDalf file ->
      OnDiskRule
        { getHash = do
              exists <- liftIO $ doesFileExist (fromNormalizedFilePath $ hiFileName file)
              if exists
                  then do
                    hsc <- hscEnv <$> use_ GhcSession file
                    pm <- use_ GetParsedModule file
                    iface <- liftIO $ loadIfaceFromFile hsc pm (hiFileName file)
                    pure $ fingerprintToBS $ mi_mod_hash iface
                  else pure ""
        , runRule = do
            lfVersion <- getDamlLfVersion
            -- build dependencies
            files <- discardInternalModules (optMbPackageName options) . transitiveModuleDeps =<< use_ GetDependencies file
            dalfDeps <- uses_ ReadSerializedDalf files
            -- type checking
            pm <- use_ GetParsedModule file
            deps <- uses_ ReadInterface files
            hsc <- hscEnv <$> use_ GhcSession file
            (diags, mbRes) <- liftIO $ ondiskTypeCheck hsc deps pm
            case mbRes of
                Nothing -> pure (diags, Nothing)
                Just tm -> fmap (first (diags ++)) $ do
                    liftIO $ writeIfaceFile
                      (hsc_dflags hsc)
                      (fromNormalizedFilePath $ hiFileName file)
                      (hm_iface $ tmrModInfo tm)
                    -- compile to core
                    (diags, mbRes) <- liftIO $ ondiskDesugar hsc (tmrModule tm)
                    case mbRes of
                        Nothing -> pure (diags, Nothing)
                        Just core -> fmap (first (diags ++)) $ do
                            -- lf conversion
                            pkgMap <- useNoFile_ GeneratePackageMap
                            stablePkgs <- useNoFile_ GenerateStablePackages
                            DamlEnv{envIsGenerated} <- getDamlServiceEnv
                            case convertModule lfVersion pkgMap (Map.map LF.dalfPackageId stablePkgs) envIsGenerated file core of
                                Left e -> pure ([e], Nothing)
                                Right rawDalf -> do
                                    -- LF postprocessing
                                    rawDalf <- pure $ LF.simplifyModule rawDalf
                                    pkgs <- getExternalPackages
                                    let world = LF.initWorldSelf pkgs (buildPackage (optMbPackageName options) lfVersion dalfDeps)
                                    let liftError e = [ideErrorPretty file e]
                                    let dalfOrErr = do
                                            dalf <- mapLeft liftError $
                                                Serializability.inferModule world lfVersion rawDalf
                                            mapLeft liftError $ LF.checkModule world lfVersion dalf
                                            pure dalf
                                    case dalfOrErr of
                                        Left diags -> pure (diags, Nothing)
                                        Right dalf -> do
                                            writeDalfFile (dalfFileName file) dalf
                                            pure ([], Just $ fingerprintToBS $ mi_mod_hash $ hm_iface $ tmrModInfo tm)
        }

readSerializedDalfRule :: Rules ()
readSerializedDalfRule =
    defineEarlyCutoff $ \ReadSerializedDalf file -> do
      let dalfFile = dalfFileName file
      needOnDisk GenerateSerializedDalf file
      dalf <- readDalfFromFile dalfFile
      (_, iface) <- use_ ReadInterface file
      pure (Just $ fingerprintToBS $ mi_mod_hash iface, ([], Just dalf))

readInterfaceRule :: Rules ()
readInterfaceRule =
    defineEarlyCutoff $ \ReadInterface file -> do
      hsc <- hscEnv <$> use_ GhcSession file
      needOnDisk GenerateSerializedDalf file
      pm <- use_ GetParsedModule file
      iface <- liftIO $ loadIfaceFromFile hsc pm (hiFileName file)
      pure (Just $ fingerprintToBS $ mi_mod_hash iface, ([], Just (pm_mod_summary pm, iface)))

loadIfaceFromFile :: HscEnv -> ParsedModule -> NormalizedFilePath -> IO ModIface
loadIfaceFromFile hsc pm hiFile = initIfaceLoad hsc $ do
    let mod = ms_mod $ pm_mod_summary pm
    r <- readIface mod (fromNormalizedFilePath hiFile)
    case r of
        Succeeded iface -> pure iface
        Maybes.Failed err -> fail (showSDoc (hsc_dflags hsc) err)

-- | Generate a doctest module based on the doc tests in the given module.
generateDocTestModuleRule :: Rules ()
generateDocTestModuleRule =
    define $ \GenerateDocTestModule file -> do
        pm <- use_ GetParsedModule file
        pure ([], Just $ getDocTestModule pm)

-- | Load all the packages that are available in the package database directories. We expect the
-- filename to match the package name.
-- TODO (drsk): We might want to change this to load only needed packages in the future.
generatePackageMap ::
     [FilePath] -> IO ([FileDiagnostic], Map.Map UnitId LF.DalfPackage)
generatePackageMap fps = do
  (diags, pkgs) <-
    fmap (partitionEithers . concat) $
    forM fps $ \fp -> do
      allFiles <- listFilesRecursive fp
      let dalfs = filter ((== ".dalf") . takeExtension) allFiles
      forM dalfs $ \dalf -> do
          dalfPkgOrErr <- readDalfPackage dalf
          let baseName = takeBaseName dalf
          let getUnitId pkg =
                  ( stringToUnitId $
                    fromMaybe baseName $
                    stripPkgId
                        baseName
                        (T.unpack $ LF.unPackageId $ LF.dalfPackageId pkg)
                  , pkg)
          pure (fmap getUnitId dalfPkgOrErr)
  return (diags, Map.fromList pkgs)

readDalfPackage :: FilePath -> IO (Either FileDiagnostic LF.DalfPackage)
readDalfPackage dalf = do
    bs <- BS.readFile dalf
    pure $ do
        (pkgId, package) <-
            mapLeft (ideErrorPretty $ toNormalizedFilePath dalf) $ Archive.decodeArchive Archive.DecodeAsDependency bs
        Right (LF.DalfPackage pkgId (LF.ExternalPackage pkgId package) bs)

generatePackageMapRule :: Options -> Rules ()
generatePackageMapRule opts =
    defineEarlyCutoff $ \GeneratePackageMap _file -> assert (null $ fromNormalizedFilePath _file) $ do
        (errs, res) <-
            liftIO $ generatePackageMap (optPackageDbs opts)
        when (errs /= []) $ do
            logger <- actionLogger
            liftIO $ logError logger $ T.pack $
                "Rule GeneratePackageMap generated errors\n" ++
                "Options: " ++ show (optPackageDbs opts) ++ "\n" ++
                "Errors:\n" ++ unlines (map show errs)
        let hash = BS.concat $ map (T.encodeUtf8 . LF.unPackageId . LF.dalfPackageId) $ Map.elems res
        return (Just hash, ([], Just res))

generateStablePackages :: LF.Version -> FilePath -> IO ([FileDiagnostic], Map.Map (UnitId, LF.ModuleName) LF.DalfPackage)
generateStablePackages lfVersion fp = do
    (diags, pkgs) <- fmap partitionEithers $ do
        allFiles <- listFilesRecursive fp
        let dalfs = filter ((== ".dalf") . takeExtension) allFiles
        forM dalfs $ \dalf -> do
            let packagePath = takeFileName $ takeDirectory dalf
            let unitId = stringToUnitId $ if packagePath == "daml-stdlib"
                    then damlStdlib -- We patch this to add the version number
                    else packagePath
            let moduleName = LF.ModuleName (NonEmpty.toList $ T.splitOn "-" $ T.pack $ dropExtension $ takeFileName dalf)
            dalfPkgOrErr <- readDalfPackage dalf
            pure (fmap ((unitId, moduleName),) dalfPkgOrErr)
    -- We filter out stable packages for newer LF versions, e.g., the stable packages for wrappers around Any.
    -- It might seem tempting to make stable packages per LF version but this makes no sense at all.
    -- Packages should remain stable as we move to newer LF versions.
    pure (diags, Map.fromList $ filter (\(_, pkg) -> lfVersion >= LF.packageLfVersion (LF.extPackagePkg $ LF.dalfPackagePkg pkg)) pkgs)


generateStablePackagesRule :: Options -> Rules ()
generateStablePackagesRule opts =
    defineEarlyCutoff $ \GenerateStablePackages _file -> assert (null $ fromNormalizedFilePath _file) $ do
        lfVersion <- getDamlLfVersion
        (errs, res) <- liftIO $ maybe (pure ([], Map.empty)) (generateStablePackages lfVersion) (optStablePackages opts)
        when (errs /= []) $ do
            logger <- actionLogger
            liftIO $ logError logger $ T.pack $
                "Rule GenerateStablePackages generated errors\n" ++
                "Options: " ++ show (optStablePackages opts) ++ "\n" ++
                "Errors:\n" ++ unlines (map show errs)
        let hash = BS.concat $ map (T.encodeUtf8 . LF.unPackageId . LF.dalfPackageId) $ Map.elems res
        return (Just hash, ([], Just res))


generatePackageRule :: Rules ()
generatePackageRule =
    define $ \GeneratePackage file -> do
        WhnfPackage deps <- use_ GeneratePackageDeps file
        dalf <- use_ GenerateDalf file
        return ([], Just $ WhnfPackage $ deps{LF.packageModules = NM.insert dalf (LF.packageModules deps)})

-- We don’t really gain anything by turning this into a rule since we only call it once
-- and having it be a function makes the merging a bit easier.
generateSerializedPackage :: String -> [NormalizedFilePath] -> MaybeT Action LF.Package
generateSerializedPackage pkgName rootFiles = do
    fileDeps <- usesE GetDependencies rootFiles
    let allFiles = nubSort $ rootFiles <> concatMap transitiveModuleDeps fileDeps
    files <- lift $ discardInternalModules (Just pkgName) allFiles
    dalfs <- usesE ReadSerializedDalf files
    lfVersion <- lift getDamlLfVersion
    pure $ buildPackage (Just pkgName) lfVersion dalfs

-- | Artifact directory for incremental builds.
buildDir :: FilePath
buildDir = ".daml/build"

-- | Path to the dalf file used in incremental builds.
dalfFileName :: NormalizedFilePath -> NormalizedFilePath
dalfFileName file =
    toNormalizedFilePath $ buildDir </> fromNormalizedFilePath file -<.> "dalf"

-- | Path to the interface file used in incremental builds.
hiFileName :: NormalizedFilePath -> NormalizedFilePath
hiFileName file =
    toNormalizedFilePath $ buildDir </> fromNormalizedFilePath file -<.> "hi"

readDalfFromFile :: NormalizedFilePath -> Action LF.Module
readDalfFromFile dalfFile = do
    lfVersion <- getDamlLfVersion
    liftIO $ do
            bytes <- BS.readFile $ fromNormalizedFilePath dalfFile
            protoPkg <- case Proto.fromByteString bytes of
                Left err -> fail (show err)
                Right a -> pure a
            case decodeScenarioModule (TL.pack $ LF.renderMinorVersion $ LF.versionMinor lfVersion) protoPkg of
                Left err -> fail (show err)
                Right mod -> pure mod

writeDalfFile :: NormalizedFilePath -> LF.Module -> Action ()
writeDalfFile dalfFile mod = do
    lfVersion <- getDamlLfVersion
    liftIO $ createDirectoryIfMissing True (takeDirectory $ fromNormalizedFilePath dalfFile)
    liftIO $ BSL.writeFile (fromNormalizedFilePath dalfFile) $ Proto.toLazyByteString $ encodeScenarioModule lfVersion mod

-- Generates a DAML-LF archive without adding serializability information
-- or type checking it. This must only be used for debugging/testing.
generateRawPackageRule :: Options -> Rules ()
generateRawPackageRule options =
    define $ \GenerateRawPackage file -> do
        lfVersion <- getDamlLfVersion
        fs <- transitiveModuleDeps <$> use_ GetDependencies file
        files <- discardInternalModules (optMbPackageName options) (fs ++ [file])
        dalfs <- uses_ GenerateRawDalf files
        -- build package
        let pkg = buildPackage (optMbPackageName options) lfVersion dalfs
        return ([], Just $ WhnfPackage pkg)

generatePackageDepsRule :: Options -> Rules ()
generatePackageDepsRule options =
    define $ \GeneratePackageDeps file -> do
        lfVersion <- getDamlLfVersion
        fs <- transitiveModuleDeps <$> use_ GetDependencies file
        files <- discardInternalModules (optMbPackageName options) fs
        dalfs <- uses_ GenerateDalf files

        -- build package
        return ([], Just $ WhnfPackage $ buildPackage (optMbPackageName options) lfVersion dalfs)

contextForFile :: NormalizedFilePath -> Action SS.Context
contextForFile file = do
    lfVersion <- getDamlLfVersion
    WhnfPackage pkg <- use_ GeneratePackage file
    pkgMap <- useNoFile_ GeneratePackageMap
    stablePackages <- useNoFile_ GenerateStablePackages
    encodedModules <-
        mapM (\m -> fmap (\(hash, bs) -> (hash, (LF.moduleName m, bs))) (encodeModule lfVersion m)) $
        NM.toList $ LF.packageModules pkg
    DamlEnv{..} <- getDamlServiceEnv
    pure SS.Context
        { ctxModules = Map.fromList encodedModules
        , ctxPackages = [(LF.dalfPackageId pkg, LF.dalfPackageBytes pkg) | pkg <- Map.elems pkgMap ++ Map.elems stablePackages]
        , ctxDamlLfVersion = lfVersion
        , ctxSkipValidation = SS.SkipValidation (getSkipScenarioValidation envSkipScenarioValidation)
        }

worldForFile :: NormalizedFilePath -> Action LF.World
worldForFile file = do
    WhnfPackage pkg <- use_ GeneratePackage file
    pkgs <- getExternalPackages
    pure $ LF.initWorldSelf pkgs pkg

data ScenarioBackendException = ScenarioBackendException
    { scenarioNote :: String
    -- ^ A note to add more context to the error
    , scenarioBackendError :: SS.BackendError
    } deriving Show

instance Exception ScenarioBackendException

createScenarioContextRule :: Rules ()
createScenarioContextRule =
    define $ \CreateScenarioContext file -> do
        ctx <- contextForFile file
        Just scenarioService <- envScenarioService <$> getDamlServiceEnv
        ctxIdOrErr <- liftIO $ SS.getNewCtx scenarioService ctx
        ctxId <-
            liftIO $ either
                (throwIO . ScenarioBackendException "Failed to create scenario context")
                pure
                ctxIdOrErr
        scenarioContextsVar <- envScenarioContexts <$> getDamlServiceEnv
        liftIO $ modifyVar_ scenarioContextsVar $ pure . Map.insert file ctxId
        pure ([], Just ctxId)

-- | This helper should be used instead of GenerateDalf/GenerateRawDalf
-- for generating modules that are sent to the scenario service.
-- It switches between GenerateRawDalf and GenerateDalf depending
-- on whether we only do light or full validation.
dalfForScenario :: NormalizedFilePath -> Action LF.Module
dalfForScenario file = do
    DamlEnv{..} <- getDamlServiceEnv
    if getSkipScenarioValidation envSkipScenarioValidation then
        use_ GenerateRawDalf file
    else
        use_ GenerateDalf file

runScenariosRule :: Rules ()
runScenariosRule =
    define $ \RunScenarios file -> do
      m <- dalfForScenario file
      world <- worldForFile file
      let scenarios = map fst $ scenariosInModule m
          toDiagnostic :: LF.ValueRef -> Either SS.Error SS.ScenarioResult -> Maybe FileDiagnostic
          toDiagnostic scenario (Left err) =
              Just $ (file,) $ Diagnostic
              { _range = maybe noRange sourceLocToRange mbLoc
              , _severity = Just DsError
              , _source = Just "Scenario"
              , _message = Pretty.renderPlain $ formatScenarioError world err
              , _code = Nothing
              , _relatedInformation = Nothing
              }
            where scenarioName = LF.qualObject scenario
                  mbLoc = NM.lookup scenarioName (LF.moduleValues m) >>= LF.dvalLocation
          toDiagnostic _ (Right _) = Nothing
      Just scenarioService <- envScenarioService <$> getDamlServiceEnv
      ctxRoot <- use_ GetScenarioRoot file
      ctxId <- use_ CreateScenarioContext ctxRoot
      scenarioResults <-
          liftIO $ forM scenarios $ \scenario -> do
              (vr, res) <- runScenario scenarioService file ctxId scenario
              pure (toDiagnostic scenario res, (vr, res))
      let (diags, results) = unzip scenarioResults
      pure (catMaybes diags, Just results)

encodeModule :: LF.Version -> LF.Module -> Action (SS.Hash, BS.ByteString)
encodeModule lfVersion m =
    case LF.moduleSource m of
      Just file
        | isAbsolute file -> use_ EncodeModule $ toNormalizedFilePath file
      _ -> pure $ SS.encodeModule lfVersion m

getScenarioRootsRule :: Rules ()
getScenarioRootsRule =
    defineNoFile $ \GetScenarioRoots -> do
        filesOfInterest <- getFilesOfInterest
        openVRs <- useNoFile_ GetOpenVirtualResources
        let files = Set.toList (filesOfInterest `Set.union` Set.map vrScenarioFile openVRs)
        deps <- forP files $ \file -> do
            transitiveDeps <- maybe [] transitiveModuleDeps <$> use GetDependencies file
            pure $ Map.fromList [ (f, file) | f <- transitiveDeps ]
        -- We want to ensure that files of interest always map to themselves even if there are dependencies
        -- between files of interest so we union them separately. (`Map.union` is left-biased.)
        pure $ Map.fromList (map dupe files) `Map.union` Map.unions deps

getScenarioRootRule :: Rules ()
getScenarioRootRule =
    defineEarlyCutoff $ \GetScenarioRoot file -> do
        ctxRoots <- useNoFile_ GetScenarioRoots
        case Map.lookup file ctxRoots of
            Nothing -> liftIO $
                fail $ "No scenario root for file " <> show (fromNormalizedFilePath file) <> "."
            Just root -> pure (Just $ BS.fromString $ fromNormalizedFilePath root, ([], Just root))


-- | Virtual resource changed notification
-- This notification is sent by the server to the client when
-- an open virtual resource changes.
virtualResourceChangedNotification :: T.Text
virtualResourceChangedNotification = "daml/virtualResource/didChange"

-- | Parameters for the virtual resource changed notification
data VirtualResourceChangedParams = VirtualResourceChangedParams
    { _vrcpUri      :: !T.Text
      -- ^ The uri of the virtual resource.
    , _vrcpContents :: !T.Text
      -- ^ The new contents of the virtual resource.
    } deriving Show

instance ToJSON VirtualResourceChangedParams where
    toJSON VirtualResourceChangedParams{..} =
        object ["uri" .= _vrcpUri, "contents" .= _vrcpContents ]

instance FromJSON VirtualResourceChangedParams where
    parseJSON = withObject "VirtualResourceChangedParams" $ \o ->
        VirtualResourceChangedParams <$> o .: "uri" <*> o .: "contents"

vrChangedNotification :: VirtualResource -> T.Text -> LSP.FromServerMessage
vrChangedNotification vr doc =
    LSP.NotCustomServer $
    LSP.NotificationMessage "2.0" (LSP.CustomServerMethod virtualResourceChangedNotification) $
    toJSON $ VirtualResourceChangedParams (virtualResourceToUri vr) doc

-- | Virtual resource note set notification
-- This notification is sent by the server to the client when
-- an open virtual resource note is set.
virtualResourceNoteSetNotification :: T.Text
virtualResourceNoteSetNotification = "daml/virtualResource/note"

-- | Parameters for the virtual resource changed notification
data VirtualResourceNoteSetParams = VirtualResourceNoteSetParams
    { _vrcpNoteUri      :: !T.Text
      -- ^ The uri of the virtual resource.
    , _vrcpNoteContent :: !T.Text
      -- ^ The new contents of the virtual resource.
    } deriving Show

instance ToJSON VirtualResourceNoteSetParams where
    toJSON VirtualResourceNoteSetParams{..} =
        object ["uri" .= _vrcpNoteUri, "note" .= _vrcpNoteContent ]

instance FromJSON VirtualResourceNoteSetParams where
    parseJSON = withObject "VirtualResourceNoteSetParams" $ \o ->
        VirtualResourceNoteSetParams <$> o .: "uri" <*> o .: "note"

vrNoteSetNotification :: VirtualResource -> T.Text -> LSP.FromServerMessage
vrNoteSetNotification vr note =
    LSP.NotCustomServer $
    LSP.NotificationMessage "2.0" (LSP.CustomServerMethod virtualResourceNoteSetNotification) $
    toJSON $ VirtualResourceNoteSetParams (virtualResourceToUri vr) note

-- A rule that builds the files-of-interest and notifies via the
-- callback of any errors. NOTE: results may contain errors for any
-- dependent module.
-- TODO (MK): We should have a non-DAML version of this rule
ofInterestRule :: Options -> Rules ()
ofInterestRule opts = do
    -- go through a rule (not just an action), so it shows up in the profile
    action $ useNoFile OfInterest
    defineNoFile $ \OfInterest -> do
        setPriority priorityFilesOfInterest
        DamlEnv{..} <- getDamlServiceEnv
        -- query for files of interest
        files   <- getFilesOfInterest
        openVRs <- useNoFile_ GetOpenVirtualResources
        let vrFiles = Set.map vrScenarioFile openVRs
        -- We run scenarios for all files of interest to get diagnostics
        -- and for the files for which we have open VRs so that they get
        -- updated.
        let scenarioFiles = files `Set.union` vrFiles
        gc scenarioFiles
        let openVRsByFile = Map.fromList (map (\vr -> (vrScenarioFile vr, vr)) $ Set.toList openVRs)
        -- compile and notify any errors
        let runScenarios file = do
                world <- worldForFile file
                mbVrs <- use RunScenarios file
                forM_ (fromMaybe [] mbVrs) $ \(vr, res) -> do
                    let doc = formatScenarioResult world res
                    when (vr `Set.member` openVRs) $
                        sendEvent $ vrChangedNotification vr doc
                let vrScenarioNames = Set.fromList $ fmap (vrScenarioName . fst) (concat $ maybeToList mbVrs)
                forM_ (Map.lookup file openVRsByFile) $ \ovr -> do
                    when (not $ vrScenarioName ovr `Set.member` vrScenarioNames) $
                        sendEvent $ vrNoteSetNotification ovr $ LF.scenarioNotInFileNote $
                        T.pack $ fromNormalizedFilePath file

        -- We don’t always have a scenario service (e.g., damlc compile)
        -- so only run scenarios if we have one.
        let shouldRunScenarios = isJust envScenarioService

        let notifyOpenVrsOnGetDalfError file = do
            mbDalf <- getDalf file
            when (isNothing mbDalf) $ do
                forM_ (Map.lookup file openVRsByFile) $ \ovr ->
                    sendEvent $ vrNoteSetNotification ovr $ LF.fileWScenarioNoLongerCompilesNote $ T.pack $
                        fromNormalizedFilePath file

        let dlintEnabled = case optDlintUsage opts of
              DlintEnabled _ _ -> True
              DlintDisabled -> False
        let files = Set.toList scenarioFiles
        let dalfActions = [notifyOpenVrsOnGetDalfError f | f <- files]
        let dlintActions = [use_ GetDlintDiagnostics f | dlintEnabled, f <- files]
        let runScenarioActions = [runScenarios f | shouldRunScenarios, f <- files]
        _ <- parallel $ dalfActions <> dlintActions <> runScenarioActions
        return ()
  where
      gc :: Set NormalizedFilePath -> Action ()
      gc roots = do
        depInfoOrErr <- sequence <$> uses GetDependencyInformation (Set.toList roots)
        -- We only clear results if there are no errors in the
        -- dependency information (in particular, no parse errors).
        -- This prevents us from clearing the results for files that are
        -- only temporarily unreachable due to a parse error.
        whenJust depInfoOrErr $ \depInfos -> do
          let noErrors = all (IntMap.null . depErrorNodes) depInfos
          when noErrors $ do
            -- We insert the empty file path since we use this for rules that do not depend
            -- on the given file.
            let reachableFiles =
                    -- To guard against buggy dependency info, we add
                    -- the roots even though they should be included.
                    roots `Set.union`
                    (Set.insert "" $ Set.fromList $ concatMap reachableModules depInfos)
            garbageCollect (`Set.member` reachableFiles)
          DamlEnv{..} <- getDamlServiceEnv
          liftIO $ whenJust envScenarioService $ \scenarioService -> do
              ctxRoots <- modifyVar envScenarioContexts $ \ctxs -> do
                  let gcdCtxs = Map.restrictKeys ctxs roots
                  pure (gcdCtxs, Map.elems gcdCtxs)
              prevCtxRoots <- modifyVar envPreviousScenarioContexts $ \prevCtxs -> pure (ctxRoots, prevCtxs)
              when (prevCtxRoots /= ctxRoots) $ void $ SS.gcCtxs scenarioService ctxRoots

getOpenVirtualResourcesRule :: Rules ()
getOpenVirtualResourcesRule = do
    defineEarlyCutoff $ \GetOpenVirtualResources _file -> assert (null $ fromNormalizedFilePath _file) $ do
        alwaysRerun
        DamlEnv{..} <- getDamlServiceEnv
        openVRs <- liftIO $ readVar envOpenVirtualResources
        pure (Just $ BS.fromString $ show openVRs, ([], Just openVRs))

formatScenarioError :: LF.World -> SS.Error -> Pretty.Doc Pretty.SyntaxClass
formatScenarioError world  err = case err of
    SS.BackendError err -> Pretty.pretty $ "Scenario service backend error: " <> show err
    SS.ScenarioError err -> LF.prettyScenarioError world err
    SS.ExceptionError err -> Pretty.pretty $ "Exception during scenario execution: " <> show err

formatScenarioResult :: LF.World -> Either SS.Error SS.ScenarioResult -> T.Text
formatScenarioResult world errOrRes =
    case errOrRes of
        Left err ->
            Pretty.renderHtmlDocumentText 128 (formatScenarioError world err)
        Right res ->
            LF.renderScenarioResult world res

runScenario :: SS.Handle -> NormalizedFilePath -> SS.ContextId -> LF.ValueRef -> IO (VirtualResource, Either SS.Error SS.ScenarioResult)
runScenario scenarioService file ctxId scenario = do
    res <- SS.runScenario scenarioService ctxId scenario
    let scenarioName = LF.qualObject scenario
    let vr = VRScenario file (LF.unExprValName scenarioName)
    pure (vr, res)

encodeModuleRule :: Options -> Rules ()
encodeModuleRule options =
    define $ \EncodeModule file -> do
        lfVersion <- getDamlLfVersion
        fs <- transitiveModuleDeps <$> use_ GetDependencies file
        files <- discardInternalModules (optMbPackageName options) fs
        encodedDeps <- uses_ EncodeModule files
        m <- dalfForScenario file
        let (hash, bs) = SS.encodeModule lfVersion m
        return ([], Just (mconcat $ hash : map fst encodedDeps, bs))

-- dlint

dlintSettings :: FilePath -> Bool -> IO ([Classify], Hint)
dlintSettings dlintDataDir enableOverrides = do
    curdir <- getCurrentDirectory
    home <- ((:[]) <$> getHomeDirectory) `catchIOError` (const $ return [])
    dlintYaml <- if enableOverrides
        then
          findM System.Directory.Extra.doesFileExist $
          map (</> ".dlint.yaml") (ancestors curdir ++ home)
      else
        return Nothing
    (_, cs, hs) <- foldMapM parseSettings $
      (dlintDataDir </> "dlint.yaml") : maybeToList dlintYaml
    return (cs, hs)
    where
      ancestors = init . map joinPath . reverse . inits . splitPath
      -- `findSettings` calls `readFilesConfig` which in turn calls
      -- `readFileConfigYaml` which finally calls `decodeFileEither` from
      -- the `yaml` library.  Annoyingly that function catches async
      -- exceptions and in particular, it ends up catching
      -- `ThreadKilled`. So, we have to mask to stop it from doing that.
      parseSettings f = mask $ \unmask ->
           findSettings (unmask . const (return (f, Nothing))) (Just f)
      foldMapM f = foldlM (\acc a -> do w <- f a; return $! mappend acc w) mempty



getDlintSettingsRule :: DlintUsage -> Rules ()
getDlintSettingsRule usage =
    defineNoFile $ \GetDlintSettings ->
      liftIO $ case usage of
          DlintEnabled dir enableOverrides -> dlintSettings dir enableOverrides
          DlintDisabled -> fail "linter configuration unspecified"

getDlintDiagnosticsRule :: Rules ()
getDlintDiagnosticsRule =
    define $ \GetDlintDiagnostics file -> do
        pm <- use_ GetParsedModule file
        let anns = pm_annotations pm
        let modu = pm_parsed_source pm
        (classify, hint) <- useNoFile_ GetDlintSettings
        let ideas = applyHints classify hint [createModuleEx anns modu]
        return ([diagnostic file i | i <- ideas, ideaSeverity i /= Ignore], Just ())
    where
      srcSpanToRange :: HSE.SrcSpan -> LSP.Range
      srcSpanToRange span = Range {
          _start = LSP.Position {
                _line = HSE.srcSpanStartLine span - 1
              , _character  = HSE.srcSpanStartColumn span - 1}
        , _end   = LSP.Position {
                _line = HSE.srcSpanEndLine span - 1
             , _character = HSE.srcSpanEndColumn span - 1}
        }
      diagnostic :: NormalizedFilePath -> Idea -> FileDiagnostic
      diagnostic file i =
        (file, LSP.Diagnostic {
              _range = srcSpanToRange $ ideaSpan i
            , _severity = Just LSP.DsInfo
            , _code = Nothing
            , _source = Just "linter"
            , _message = T.pack $ show i
            , _relatedInformation = Nothing
      })

--
scenariosInModule :: LF.Module -> [(LF.ValueRef, Maybe LF.SourceLoc)]
scenariosInModule m =
    [ (LF.Qualified LF.PRSelf (LF.moduleName m) (LF.dvalName val), LF.dvalLocation val)
    | val <- NM.toList (LF.moduleValues m), LF.getIsTest (LF.dvalIsTest val)]

getDamlLfVersion :: Action LF.Version
getDamlLfVersion = envDamlLfVersion <$> getDamlServiceEnv

-- | This operates on file paths rather than module names so that we avoid introducing a dependency on GetParsedModule.
discardInternalModules :: Maybe String -> [NormalizedFilePath] -> Action [NormalizedFilePath]
discardInternalModules mbPackageName files = do
    stablePackages <- useNoFile_ GenerateStablePackages
    pure $ filter (shouldKeep stablePackages) files
  where shouldKeep stablePackages f =
            not (any (`isSuffixOf` fromNormalizedFilePath f) internalModules) &&
            not (any (\(unitId, modName) ->
                          mbPackageName == Just (GHC.unitIdString unitId) &&
                          moduleNameFile modName `isSuffixOf` fromNormalizedFilePath f)
                     $ Map.keys stablePackages)
        moduleNameFile (LF.ModuleName segments) = joinPath (map T.unpack segments) <.> "daml"

internalModules :: [FilePath]
internalModules = map normalise
  [ "Data/String.daml"
  , "GHC/CString.daml"
  , "GHC/Integer/Type.daml"
  , "GHC/Natural.daml"
  , "GHC/Real.daml"
  , "GHC/Types.daml"
  ]

-- | Checks if a given module is internal, i.e. gets removed in the Core->LF
-- translation. TODO where should this live?
modIsInternal :: Module -> Bool
modIsInternal m = moduleNameString (moduleName m) `elem` internalModules
  -- TODO should we consider DA.Internal.* internal? Difference to GHC.*
  -- modules is that these do not disappear in the LF conversion.


damlRule :: Options -> Rules ()
damlRule opts = do
    generateRawDalfRule
    generateDalfRule
    generateSerializedDalfRule opts
    readSerializedDalfRule
    readInterfaceRule
    generateDocTestModuleRule
    generatePackageMapRule opts
    generateStablePackagesRule opts
    generatePackageRule
    generateRawPackageRule opts
    generatePackageDepsRule opts
    runScenariosRule
    getScenarioRootsRule
    getScenarioRootRule
    getDlintDiagnosticsRule
    ofInterestRule opts
    encodeModuleRule opts
    createScenarioContextRule
    getOpenVirtualResourcesRule
    getDlintSettingsRule (optDlintUsage opts)

mainRule :: Options -> Rules ()
mainRule options = do
    IDE.mainRule
    damlRule options
