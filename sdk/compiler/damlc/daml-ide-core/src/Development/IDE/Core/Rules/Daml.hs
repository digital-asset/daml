-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module Development.IDE.Core.Rules.Daml
    ( module Development.IDE.Core.Rules
    , module Development.IDE.Core.Rules.Daml
    , module Development.IDE.Core.Rules.Daml.SpanInfo
    ) where

import Outputable (showSDoc)
import TcIface (typecheckIface)
import LoadIface (readIface)
import TidyPgm
import DynFlags
import SrcLoc
import qualified GHC
import qualified Module as GHC
import GhcMonad
import Data.IORef
import qualified Proto3.Suite             as Proto
import qualified DA.Daml.LF.Proto3.Decode as Decode
import qualified DA.Daml.LF.Proto3.Encode as Encode
import HscTypes
import MkIface
import Maybes (MaybeErr(..), rightToMaybe)
import TcRnMonad (initIfaceLoad)
import qualified "zip-archive" Codec.Archive.Zip as ZipArchive
import Control.Concurrent.Extra
import Control.DeepSeq (NFData())
import Control.Exception
import Control.Monad.Except
import Control.Monad.Extra
import Control.Monad.Trans.Maybe
import DA.Daml.Compiler.ExtractDar (extractDar, ExtractedDar(..), edDeps)
import DA.Daml.LF.Ast.Version ( Version(versionMajor), renderMajorVersion, supports, featurePackageImports )
import DA.Daml.Options
import DA.Daml.Options.Packaging.Metadata
import DA.Daml.Options.Types
import DA.Daml.Project.Consts (packageConfigName)
import DA.Daml.Project.Types (PackagePath (..))
import Data.Aeson hiding (Options)
import Data.Bifunctor (bimap)
import Data.Binary (Binary())
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.UTF8 as BS
import Data.Either.Extra
import Data.Foldable
import Data.Function (on)
import qualified Data.HashMap.Strict as HashMap
import qualified Data.HashSet as HashSet
import Data.Hashable (Hashable())
import qualified Data.IntMap.Strict as IntMap
import Data.List.Extra
import qualified Data.List.NonEmpty as NonEmpty
import qualified Data.Map.Strict as Map
import Data.Maybe
import qualified Data.NameMap as NM
import qualified Data.Set as Set
import qualified Data.Text.Encoding as T
import qualified Data.Text.Extended as T
import Data.Tuple.Extra
import Data.Typeable (Typeable())
import qualified Data.Vector as V
import Development.IDE.Core.Compile
import Development.IDE.Core.OfInterest
import Development.IDE.GHC.Error
import Development.IDE.GHC.Util
import Development.IDE.GHC.Warnings
import Development.IDE.Types.Location as Base
import Development.IDE.Types.Logger hiding (Priority)

import Development.Shake hiding (Diagnostic, Env, doesFileExist)
import "ghc-lib" GHC hiding (Succeeded, typecheckModule)
import "ghc-lib-parser" Module (DefUnitId(..), UnitId(..), stringToUnitId)
import Safe
import System.Directory.Extra as Dir
import System.FilePath
import qualified System.FilePath.Posix as FPP
import System.IO
import System.IO.Error
import qualified Text.PrettyPrint.Annotated.HughesPJClass as HughesPJPretty
import GHC.Word

import qualified Network.HTTP.Types as HTTP.Types
import qualified Network.URI as URI

import Development.IDE.Import.DependencyInformation
import Development.IDE.Core.Rules hiding (mainRule)
import qualified Development.IDE.Core.Rules as IDE
import Development.IDE.Core.Service.Daml
import Development.IDE.Core.Shake
import Development.IDE.Types.Diagnostics
import qualified Language.LSP.Types as LSP

import Development.IDE.Core.RuleTypes.Daml

import DA.Bazel.Runfiles
import DA.Daml.DocTest
import DA.Daml.LFConversion (convertModule)
import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.InferSerializability as Serializability
import qualified DA.Daml.LF.PrettyScript as LF
import qualified DA.Daml.LF.Proto3.Archive as Archive
import qualified DA.Daml.LF.ScriptServiceClient as SS
import qualified DA.Daml.LF.Simplifier as LF
import qualified DA.Daml.LF.TypeChecker as LF
import qualified DA.Daml.LF.TypeChecker.Upgrade as Upgrade
import DA.Daml.UtilLF
import qualified DA.Pretty as Pretty
import DA.Pretty (PrettyLevel)
import SdkVersion.Class (SdkVersioned, damlStdlib)

import Language.Haskell.HLint4

import Development.IDE.Core.Rules.Daml.SpanInfo

-- | Get thr URI that corresponds to a virtual resource. The VS Code has a
-- document provider that will handle our special documents.
-- The Uri looks like this:
-- daml://[command]/[client data]?[server]=[key]&[key]=[value]
--
-- The command tells the server if it should do script interpretation or
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
    VRScript filePath (ScriptName topLevelDeclName) ->
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
            pure $ VRScript (toNormalizedFilePath' file) (ScriptName $ T.pack topLevelDecl)
        _ -> Nothing

  where
    queryString :: URI.URI -> Map.Map String String
    queryString u0 = fromMaybe Map.empty $ case tailMay $ URI.uriQuery u0 of
        Nothing -> Nothing
        Just u ->
            Just
          $ Map.fromList
          $ map (bimap BS.toString BS.toString)
          $ HTTP.Types.parseSimpleQuery
          $ BS.fromString
          $ URI.unEscapeString u

-- | Enhance parse error messages with more helpful, context-specific information
enhanceParseErrorDiagnostics :: [FileDiagnostic] -> [FileDiagnostic]
enhanceParseErrorDiagnostics = map enhanceSingleDiagnostic
  where
    enhanceSingleDiagnostic :: FileDiagnostic -> FileDiagnostic
    enhanceSingleDiagnostic fd@(file, showDiag, diag@Diagnostic{..}) =
      case _severity of
        Just DsError | isParseError _message ->
          let enhancedMessage = enhanceParseErrorMessage _message
          in (file, showDiag, diag { _message = enhancedMessage })
        _ -> fd

    isParseError :: T.Text -> Bool
    isParseError msg = "parse error" `T.isInfixOf` msg

    enhanceParseErrorMessage :: T.Text -> T.Text
    enhanceParseErrorMessage msg
      -- Issue #22354: Improve error for badly indented 'choice'
      | "parse error on input 'choice'" `T.isInfixOf` msg =
          msg <> "\n\nHint: This often occurs when 'choice' is not properly indented.\n" <>
                 "Make sure 'choice' is indented at the same level as other template clauses like 'signatory' and 'observer'.\n" <>
                 "Example:\n" <>
                 "  template MyTemplate\n" <>
                 "    with ...\n" <>
                 "    where\n" <>
                 "      signatory ...\n" <>
                 "      choice MyChoice : ..."

      -- Issue #22361: Improve generic parse error with template-related hints
      -- The missing key type signature produces a very generic error, so we provide
      -- general hints that cover common template parsing issues
      | "parse error (possibly incorrect indentation or mismatched brackets)" `T.isInfixOf` msg =
          msg <> "\n\nHint: Common causes of this error in templates include:\n" <>
                 "  - Missing type signature on 'key' (unlike 'signatory', 'key' requires a type)\n" <>
                 "    Example: key owner : Party\n" <>
                 "  - Incorrect indentation of template clauses\n" <>
                 "  - Mismatched brackets or parentheses"

      | otherwise = msg

sendFileDiagnostics :: [FileDiagnostic] -> Action ()
sendFileDiagnostics diags =
    let enhancedDiags = enhanceParseErrorDiagnostics diags
    in mapM_ (uncurry sendDiagnostics) (groupSort $ map (\(file, _showDiag, diag) -> (file, diag)) enhancedDiags)

sendDiagnostics :: NormalizedFilePath -> [Diagnostic] -> Action ()
sendDiagnostics fp diags = do
    ShakeExtras {lspEnv} <- getShakeExtras
    let uri = fromNormalizedUri (filePathToUri' fp)
    liftIO $
        sendNotification lspEnv LSP.STextDocumentPublishDiagnostics $
        LSP.PublishDiagnosticsParams uri Nothing (List diags)

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
getDlintIdeas f = runMaybeT $ fst <$> useE GetDlintDiagnostics f

ideErrorPretty :: Pretty.Pretty e => NormalizedFilePath -> e -> FileDiagnostic
ideErrorPretty fp = ideErrorText fp . T.pack . HughesPJPretty.prettyShow

finalPackageCheck :: NormalizedFilePath -> LF.Package -> Action (Maybe ())
finalPackageCheck fp pkg =
    runDiagnosticCheck $ diagsToIdeResult fp (LF.nameCheckPackage pkg)

runDiagnosticCheck :: IdeResult a -> Action (Maybe a)
runDiagnosticCheck (diags, r) = do
    sendFileDiagnostics diags
    pure r

diagsToIdeResult :: NormalizedFilePath -> [Diagnostic] -> IdeResult ()
diagsToIdeResult fp diags = (map (fp, ShowDiag,) diags, r)
    where r = if any ((Just DsError ==) . _severity) diags then Nothing else Just ()

-- | Dependencies on other packages excluding stable DALFs.
getUnstableDalfDependencies :: [NormalizedFilePath] -> MaybeT Action (Map.Map UnitId LF.DalfPackage)
getUnstableDalfDependencies files = do
    unitIds <- concatMap transitivePkgDeps <$> usesE' GetDependencies files
    pkgMap <- Map.unions . map getPackageMap <$> usesE' GeneratePackageMap files
    pure $ Map.restrictKeys pkgMap (Set.fromList $ map (DefiniteUnitId . DefUnitId) unitIds)

getDalfDependencies :: SdkVersioned => [NormalizedFilePath] -> MaybeT Action (Map.Map UnitId LF.DalfPackage)
getDalfDependencies files = do
    actualDeps <- getUnstableDalfDependencies files
    -- For now, we unconditionally include all stable packages.
    -- Given that they are quite small and it is pretty much impossible to not depend on them
    -- this is fine. We might want to try being more clever here in the future.
    stablePackages <-
        fmap (Map.mapKeys stableUnitId) $
        useNoFileE GenerateStablePackages
    pure $ stablePackages `Map.union` actualDeps
  where stableUnitId (unitId, modName) = stringToUnitId $ GHC.unitIdString (stripStdlibVersion unitId) <> "-" <> T.unpack (T.intercalate "-" $ LF.unModuleName modName)
        stripStdlibVersion unitId
          | unitId == damlStdlib = stringToUnitId "daml-stdlib"
          | otherwise = unitId


runScripts :: NormalizedFilePath -> Action (Maybe [(ScriptName, Either SS.Error SS.ScriptResult)])
runScripts file = use RunScripts file

priorityGenerateDalf :: Priority
priorityGenerateDalf = priorityGenerateCore

-- Generates the DALF for a module without adding serializability information
-- or type checking it.
generateRawDalfRule :: SdkVersioned => Options -> Rules ()
generateRawDalfRule opts =
    define $ \GenerateRawDalf file -> do
        lfVersion <- getDamlLfVersion
        (coreDiags, mbCore) <- generateCore (RunSimplifier False) file
        fmap (first (coreDiags ++)) $
            case mbCore of
                Nothing -> return ([], Nothing)
                Just (safeMode, cgGuts, details) -> do
                    let core = cgGutsToCoreModule safeMode cgGuts details
                    setPriority priorityGenerateDalf
                    -- Generate the map from package names to package hashes
                    PackageMap pkgMap <- use_ GeneratePackageMap file
                    stablePkgs <- useNoFile_ GenerateStablePackages
                    DamlEnv{envEnableInterfaces} <- getDamlServiceEnv
                    modIface <- hm_iface . tmrModInfo <$> use_ TypeCheck file
                    -- GHC Core to Daml-LF
                    case convertModule lfVersion envEnableInterfaces (optLfConversionWarningFlags opts) pkgMap (Map.map LF.dalfPackageId stablePkgs) file core modIface details of
                        Left e -> return ([e], Nothing)
                        Right (v, conversionWarnings) -> do
                            WhnfPackage pkg <- use_ GeneratePackageDeps file
                            pkgs <- getExternalPackages file
                            let world = LF.initWorldSelf pkgs pkg
                                simplified = LF.simplifyModule world lfVersion v
                                serializabilityOptions = Serializability.SerializabilityOptions
                                  { soForceUtilityPackage = getForceUtilityPackage $ optForceUtilityPackage opts
                                  , soExplicitSerializable = getExplicitSerializable $ optExplicitSerializable opts
                                  }
                            pure $! case Serializability.inferModule world serializabilityOptions simplified of
                              Left err -> ([ideErrorPretty file err], Nothing)
                              Right dalf -> (conversionWarnings, Just dalf)

getExternalPackages :: NormalizedFilePath -> Action [LF.ExternalPackage]
getExternalPackages file = do
    PackageMap pkgMap <- use_ GeneratePackageMap file
    stablePackages <- useNoFile_ GenerateStablePackages
    -- We need to dedup here to make sure that each package only appears once.
    pure $
        Map.elems $ Map.fromList $ map (\e@(LF.ExternalPackage pkgId _) -> (pkgId, e)) $
        map LF.dalfPackagePkg (Map.elems pkgMap) <> map LF.dalfPackagePkg (Map.elems stablePackages)

-- Generates and type checks the DALF for a module.
generateDalfRule :: SdkVersioned => Options -> Rules ()
generateDalfRule opts =
    define $ \GenerateDalf file -> do
        lfVersion <- getDamlLfVersion
        mbDalfDependencies <- runMaybeT (getDalfDependencies [file])
        WhnfPackage pkg <- use_ GeneratePackageDeps file
        pkgs <- getExternalPackages file
        let world = LF.initWorldSelf pkgs pkg
        rawDalf <- use_ GenerateRawDalf file
        upgradedPackage <- join <$> useNoFile ExtractUpgradedPackage
        setPriority priorityGenerateDalf
        let lfDiags = LF.checkModule world lfVersion rawDalf
            upgradeDiags = Upgrade.checkModule world rawDalf (map (Upgrade.dalfPackageToUpgradedPkg . snd) (foldMap Map.toList mbDalfDependencies)) lfVersion (optUpgradeInfo opts) (optTypecheckerWarningFlags opts) upgradedPackage
        pure $! second (rawDalf <$) (diagsToIdeResult file (lfDiags ++ upgradeDiags))

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

packageMetadataFromOptions :: Options -> LF.PackageMetadata
packageMetadataFromOptions options = LF.PackageMetadata
    { packageName = fromMaybe (LF.PackageName "unknown") (optMbPackageName options)
    , packageVersion = fromMaybe (LF.PackageVersion "0.0.0") (optMbPackageVersion options)
    , upgradedPackageId = Nothing -- set by daml build
    }

extractImports :: [LF.ModuleWithImports] -> ([LF.Module], LF.ImportedPackages)
extractImports = foldr (\(mod, imp) (mods, imps) -> (mod:mods, imp `merge` imps)) ([], Right Set.empty)
  where
    merge = LF.mergeImportedPackages


-- This rule is for on-disk incremental builds. We cannot use the fine-grained rules that we have for
-- in-memory builds since we need to be able to serialize intermediate results. GHC doesn’t provide a way to serialize
-- TypeCheckedModules or CoreModules. In addition to that, making this too fine-grained would probably also incur a performance penalty.
-- Therefore we have a single rule that performs the steps parsed module -> typechecked module -> core module -> Daml-LF module.
-- This rule writes both the .dalf and the .hi files.
-- We use the ABI hash of the .hi files to detect if we need to recompile dependent files. Note that this is more aggressive
-- than just looking at the file hash. E.g., consider module A depending on module B. If B changes but its ABI hash stays the same
-- we do not need to recompile A.
generateSerializedDalfRule :: SdkVersioned => Options -> Rules ()
generateSerializedDalfRule options =
    defineOnDisk $ \GenerateSerializedDalf file ->
      OnDiskRule
        { getHash = do
              exists <- liftIO $ Dir.doesFileExist (fromNormalizedFilePath $ hiFileName file)
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
            files <- discardInternalModules (optUnitId options) . transitiveModuleDeps =<< use_ GetDependencies file
            dalfDeps <- map fst <$> uses_ ReadSerializedDalf files
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
                            PackageMap pkgMap <- use_ GeneratePackageMap file
                            stablePkgs <- useNoFile_ GenerateStablePackages
                            imports <- use_ GeneratePackageImports file
                            DamlEnv{envEnableInterfaces} <- getDamlServiceEnv
                            let modInfo = tmrModInfo tm
                                details = hm_details modInfo
                                modIface = hm_iface modInfo
                            case convertModule lfVersion envEnableInterfaces (optLfConversionWarningFlags options) pkgMap (Map.map LF.dalfPackageId stablePkgs) file core modIface details of
                                Left e -> pure ([e], Nothing)
                                Right (rawDalf, conversionWarnings) -> do
                                    -- LF postprocessing
                                    pkgs <- getExternalPackages file
                                    let selfPkg = buildPackage
                                                    (packageMetadataFromOptions options)
                                                    lfVersion
                                                    dalfDeps
                                                    imports
                                        world = LF.initWorldSelf pkgs selfPkg
                                        simplified = LF.simplifyModule (LF.initWorld [] lfVersion) lfVersion rawDalf
                                        -- NOTE (SF): We pass a dummy LF.World to the simplifier because we don't want inlining
                                        -- across modules when doing incremental builds. The reason is that our Shake rules
                                        -- use ABI changes to determine whether to rebuild the module, so if an implementaion
                                        -- changes without a corresponding ABI change, we would end up with an outdated
                                        -- implementation.
                                        serializabilityOptions = Serializability.SerializabilityOptions
                                            { soForceUtilityPackage = getForceUtilityPackage $ optForceUtilityPackage options
                                            , soExplicitSerializable = getExplicitSerializable $ optExplicitSerializable options
                                            }

                                    case Serializability.inferModule world serializabilityOptions simplified of
                                        Left err -> pure (conversionWarnings ++ [ideErrorPretty file err], Nothing)
                                        Right dalf -> do
                                            let (diags, checkResult) = diagsToIdeResult file $ LF.checkModule world lfVersion dalf
                                            fmap (conversionWarnings ++ diags,) $ case checkResult of
                                                Nothing -> pure Nothing
                                                Just () -> do
                                                    writeDalfFile (dalfFileName file) (dalf, imports)
                                                    pure (Just $ fingerprintToBS $ mi_mod_hash $ hm_iface $ tmrModInfo tm)
        }

readSerializedDalfRule :: Rules ()
readSerializedDalfRule =
    defineEarlyCutoff $ \ReadSerializedDalf file -> do
      let dalfFile = dalfFileName file
      needOnDisk GenerateSerializedDalf file
      dalf <- readDalfFromFile dalfFile
      (_, iface) <- use_ ReadInterface file
      --TODO[RB] ask Remy if we should do some fingerprintToBS magic
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
generatePackageMap :: LF.Version -> Maybe NormalizedFilePath -> [FilePath] -> IO ([FileDiagnostic], Map.Map UnitId LF.DalfPackage)
generatePackageMap version mbProjRoot userPkgDbs = do
    versionedPackageDbs <- getPackageDbs version mbProjRoot userPkgDbs
    (diags, pkgs) <-
        fmap (partitionEithers . concat) $
        forM versionedPackageDbs $ \db -> do
            allFiles <- listFilesRecursive db
            let dalfs = filter ((== ".dalf") . takeExtension) allFiles
            forM dalfs $ \dalf -> do
                dalfPkgOrErr <- readDalfPackage dalf
                pure (fmap (\dalfPkg -> (getUnitId dalf dalfPkg, dalfPkg)) dalfPkgOrErr)

    let unitIdConflicts = Map.filter ((>=2) . Set.size) . Map.fromListWith Set.union $
            [ (unitId, Set.singleton (LF.dalfPackageId dalfPkg))
            | (unitId, dalfPkg) <- pkgs ]
    when (not $ Map.null unitIdConflicts) $ do
        fail $ "Transitive dependencies with same unit id but conflicting package ids: "
            ++ intercalate ", "
                [ show k <> " [" <> intercalate "," (map show (Set.toList v)) <> "]"
                | (k,v) <- Map.toList unitIdConflicts ]

    return (diags, Map.fromList pkgs)
  where
    -- If we use data-dependencies we can end up with multiple DALFs for daml-prim/daml-stdlib
    -- one per version. The one shipped with the SDK is called daml-prim.dalf and daml-stdlib-$VERSION.dalf
    -- and have the same unit ids, so we do not need to strip package ids.
    -- The one coming from daml-prim will be called daml-prim-$PKGID.dalf daml-stdlib-$PKGID.dalf
    -- To avoid collisions, we include this hash in the unit id so we also don’t want to strip
    -- package ids here.
    getUnitId :: FilePath -> LF.DalfPackage -> UnitId
    getUnitId dalf pkg
      | "daml-prim" `T.isPrefixOf` name = stringToUnitId (takeBaseName dalf)
      | "daml-stdlib" `T.isPrefixOf` name = stringToUnitId (takeBaseName dalf)
      | otherwise = pkgNameVersion (LF.PackageName name) mbVersion
      where (LF.PackageName name, mbVersion)
               = LF.safePackageMetadata (LF.extPackagePkg $ LF.dalfPackagePkg pkg)

getUpgradedPackageErrs :: Options -> LSP.NormalizedFilePath -> LF.Package -> [FileDiagnostic]
getUpgradedPackageErrs opts file mainPkg
  | not (uiTypecheckUpgrades (optUpgradeInfo opts)) = [] -- If user turns off typecheck upgrades, then even metadata warnings are disabled
  | otherwise = catMaybes
  [ if optDamlLfVersion opts `lfVersionMajorNe` LF.packageLfVersion mainPkg
      then
        Just $
          ideErrorPretty file $ mconcat
            [ mainPackage <> " LF Version ("
            , T.pack $ LF.renderVersion $ LF.packageLfVersion mainPkg
            , ") must have the same major LF version as " <> upgradedPackage <> " LF Version ("
            , T.pack $ LF.renderVersion $ optDamlLfVersion opts
            , ")"
            ]
      else
        justIf (optDamlLfVersion opts `lfVersionMinorLt` LF.packageLfVersion mainPkg) $
          ideErrorPretty file $ mconcat
            [ mainPackage <> " LF Version ("
            , T.pack $ LF.renderVersion $ optDamlLfVersion opts
            , ") cannot be lower than the " <> upgradedPackage <> " LF Version ("
            , T.pack $ LF.renderVersion $ LF.packageLfVersion mainPkg
            , ")"
            ]
  , justIf (optMbPackageName opts /= Just (LF.packageName $ LF.packageMetadata mainPkg)) $
      ideErrorPretty file $ mconcat
        [ "Main package must have the same package name as upgraded package."
        , "\n" <> mainPackage <> " name: "
        , maybe "<unknown>" LF.unPackageName (optMbPackageName opts)

        , "\n" <> upgradedPackage <> " name: "
        , LF.unPackageName (LF.packageName $ LF.packageMetadata mainPkg)
        ]
  , justIf (optMbPackageVersion opts == Just (LF.packageVersion $ LF.packageMetadata mainPkg)) $
      ideErrorPretty file $
        mainPackage <> " cannot have the same package version as " <> upgradedPackage
  , justIf (maybe False (`packageVersionLt` LF.packageVersion (LF.packageMetadata mainPkg)) $ optMbPackageVersion opts) $
      ideErrorPretty file $
        upgradedPackage <> " cannot have a higher package version than " <> mainPackage
  ]
  where
    justIf :: Bool -> a -> Maybe a
    justIf cond val = guard cond >> Just val

    -- package versions have been checked at this point
    packageVersionLt :: LF.PackageVersion -> LF.PackageVersion -> Bool
    packageVersionLt = (<) `on` fromRight (error "Impossible invalid package version") . LF.splitPackageVersion id

    lfVersionMinorLt :: LF.Version -> LF.Version -> Bool
    lfVersionMinorLt = (<) `on` LF.versionMinor

    lfVersionMajorNe :: LF.Version -> LF.Version -> Bool
    lfVersionMajorNe = (/=) `on` LF.versionMajor

    -- Renders "v1.0.0" if the version exists, "no version" else
    renderMPackageVersion :: Maybe LF.PackageVersion -> T.Text
    renderMPackageVersion = maybe "no version" $ \v -> "v" <> LF.unPackageVersion v

    mainPackage :: T.Text
    mainPackage = "Main package (" <> renderMPackageVersion (optMbPackageVersion opts) <> ")"

    upgradedPackage :: T.Text
    upgradedPackage = "Upgraded package (" <> renderMPackageVersion (Just $ LF.packageVersion $ LF.packageMetadata mainPkg) <> ")"

extractUpgradedPackageRule :: Options -> Rules ()
extractUpgradedPackageRule opts = do
  defineNoFile $ \ExtractUpgradedPackage ->
    forM (uiUpgradedPackagePath $ optUpgradeInfo opts) $
      use_ ExtractUpgradedPackageFile . toNormalizedFilePath'
  define $ \ExtractUpgradedPackageFile file -> do
    extractedDar <- liftIO $ extractDar (fromNormalizedFilePath file)
    let decodeEntryWithUnitId decodeAs entry = do
          let bs = BSL.toStrict $ ZipArchive.fromEntry entry
          (pkgId, pkg) <- Archive.decodeArchive decodeAs bs
          pure $ Upgrade.mkUpgradedPkgWithNameAndVersion pkgId pkg
    let mainAndDeps ::
          Either Archive.ArchiveError
            (Upgrade.UpgradedPkgWithNameAndVersion,
             [Upgrade.UpgradedPkgWithNameAndVersion])
        mainAndDeps = do
           main <- decodeEntryWithUnitId Archive.DecodeAsMain (edMain extractedDar)
           deps <- decodeEntryWithUnitId Archive.DecodeAsDependency `traverse` edDeps extractedDar
           pure (main, deps)
        packageConfigFilePath = maybe file (LSP.toNormalizedFilePath . (</> packageConfigName) . unwrapPackagePath) $ optMbPackageConfigPath opts
        diags = case mainAndDeps of
          Left _ -> [ideErrorPretty packageConfigFilePath ("Could not decode file as a DAR." :: T.Text)]
          Right (mainPkg, _) ->
            getUpgradedPackageErrs opts packageConfigFilePath (Upgrade.upwnavPkg mainPkg)
    extras <- getShakeExtras
    updateFileDiagnostics packageConfigFilePath ExtractUpgradedPackageFile extras $ map (\(_,y,z) -> (y,z)) diags
    pure ([], guard (null diags) >> rightToMaybe mainAndDeps)

readDalfPackage :: FilePath -> IO (Either FileDiagnostic LF.DalfPackage)
readDalfPackage dalf = do
    bs <- BS.readFile dalf
    pure $ do
        (pkgId, package) <-
            mapLeft (ideErrorPretty $ toNormalizedFilePath' dalf) $ Archive.decodeArchive Archive.DecodeAsDependency bs
        Right (LF.DalfPackage pkgId (LF.ExternalPackage pkgId package) bs)

generatePackageMapRule :: Options -> Rules ()
generatePackageMapRule opts = do
    defineNoFile $ \GeneratePackageMapIO -> do
        f <- liftIO $ do
            findPackageRoot <- memoIO findPackageRoot
            generatePackageMap <- memoIO $ \mbRoot -> generatePackageMap (optDamlLfVersion opts) mbRoot (optPackageDbs opts)
            pure $ \file -> do
                mPackageRoot <- liftIO (findPackageRoot file)
                liftIO $ generatePackageMap (LSP.toNormalizedFilePath <$> mPackageRoot)
        pure (GeneratePackageMapFun f)
    defineEarlyCutoff $ \GeneratePackageMap file -> do
        GeneratePackageMapFun fun <- useNoFile_ GeneratePackageMapIO
        (errs, res) <- fun $ fromNormalizedFilePath file
        when (errs /= []) $ do
            logger <- actionLogger
            liftIO $ logError logger $ T.pack $
                "Rule GeneratePackageMap generated errors\n" ++
                "Options: " ++ show (optPackageDbs opts) ++ "\n" ++
                "Errors:\n" ++ unlines (map show errs)
        let hash = BS.concat $ map (T.encodeUtf8 . LF.unPackageId . LF.dalfPackageId) $ Map.elems res
        return (Just hash, ([], Just (PackageMap res)))

damlGhcSessionRule :: SdkVersioned => Options -> Rules ()
damlGhcSessionRule opts@Options{..} = do
    -- The file path here is optional so we go for defineNoFile
    -- (or the equivalent thereof for rules with cut off).
    defineEarlyCutoff $ \(DamlGhcSession mPackageRoot) _file -> assert (null $ fromNormalizedFilePath _file) $ do
        let base = mkBaseUnits (optUnitId opts)
        extraPkgFlags <- liftIO $ case mPackageRoot of
            Just packageRoot | not (getIgnorePackageMetadata optIgnorePackageMetadata) ->
                -- We catch doesNotExistError which could happen if the
                -- package db has never been initialized. In that case, we
                -- return no extra package flags.
                handleJust
                    (guard . isDoesNotExistError)
                    (const $ pure []) $ do
                    PackageDbMetadata{..} <- readMetadata packageRoot
                    let mainPkgs = map mkPackageFlag directDependencies
                    let renamings =
                            map (\(unitId, (prefix, modules)) -> renamingToFlag unitId prefix modules)
                                (Map.toList moduleRenamings)
                    pure (mainPkgs ++ renamings)
            _ -> pure []
        optPackageImports <- pure $ map mkPackageFlag base ++ extraPkgFlags ++ optPackageImports
        env <- liftIO $ runGhcFast $ do
            setupDamlGHC mPackageRoot opts
            GHC.getSession
        pkg <- liftIO $ generatePackageState optDamlLfVersion mPackageRoot optPackageDbs optPackageImports
        dflags <- liftIO $ checkDFlags opts $ setPackageDynFlags pkg $ hsc_dflags env
        hscEnv <- liftIO $ newHscEnvEq env{hsc_dflags = dflags}
        -- In the IDE we do not care about the cache value here but for
        -- incremental builds we need an early cutoff.
        pure (Just "", ([], Just hscEnv))

generateStablePackages :: SdkVersioned => LF.Version -> FilePath -> IO ([FileDiagnostic], Map.Map (UnitId, LF.ModuleName) LF.DalfPackage)
generateStablePackages lfVersion fp = do
    (diags, pkgs) <- fmap partitionEithers $ do
        let prefix = fp </> ("lf-v" <> renderMajorVersion (versionMajor lfVersion))
        -- It is very tempting to just use a listFilesRecursive here.
        -- However, that has broken CI several times on Windows due to the lack of
        -- sandboxing which resulted in newly added files being picked up from other PRs.
        -- Given that this list doesn’t change too often and you will get a compile error
        -- if you forget to update it, we hardcode it here.
            dalfs = map (prefix </>) $ concat
                [ map ("daml-prim" </>)
                    [ "DA-Internal-Erased.dalf"
                    , "DA-Internal-NatSyn.dalf"
                    , "DA-Internal-PromotedText.dalf"
                    , "DA-Exception-GeneralError.dalf"
                    , "DA-Exception-ArithmeticError.dalf"
                    , "DA-Exception-AssertionFailed.dalf"
                    , "DA-Exception-PreconditionFailed.dalf"
                    , "DA-Types.dalf"
                    , "GHC-Prim.dalf"
                    , "GHC-Tuple.dalf"
                    , "GHC-Types.dalf"]
                , map ("daml-stdlib" </>)
                    [ "DA-Internal-Any.dalf"
                    , "DA-Internal-Template.dalf"
                    , "DA-Date-Types.dalf"
                    , "DA-NonEmpty-Types.dalf"
                    , "DA-Time-Types.dalf"
                    , "DA-Semigroup-Types.dalf"
                    , "DA-Set-Types.dalf"
                    , "DA-Monoid-Types.dalf"
                    , "DA-Validation-Types.dalf"
                    , "DA-Logic-Types.dalf"
                    , "DA-Internal-Down.dalf"
                    , "DA-Internal-Interface-AnyView-Types.dalf"
                    , "DA-Action-State-Type.dalf"
                    , "DA-Random-Types.dalf"
                    , "DA-Stack-Types.dalf"
                    , "DA-Internal-Fail-Types.dalf"
                    ]
                ]
        forM dalfs $ \dalf -> do
            let packagePath = takeFileName $ takeDirectory dalf
            let unitId = if packagePath == "daml-stdlib"
                    then damlStdlib -- We patch this to add the version number
                    else stringToUnitId packagePath
            let moduleName = LF.ModuleName (NonEmpty.toList $ T.splitOn "-" $ T.pack $ dropExtension $ takeFileName dalf)
            dalfPkgOrErr <- readDalfPackage dalf
            pure (fmap ((unitId, moduleName),) dalfPkgOrErr)
    -- We filter out stable packages for newer LF versions, e.g., the stable packages for wrappers around Any.
    -- It might seem tempting to make stable packages per LF version but this makes no sense at all.
    -- Packages should remain stable as we move to newer LF versions. Changing the LF version would change the hash.
    pure
        ( diags
        , Map.fromList $ filter (pkgCompatibleWith lfVersion . snd) pkgs
        )
  where
    pkgCompatibleWith lfVersion pkg = lfVersion `LF.canDependOn` dalfPackageVersion pkg
    dalfPackageVersion pkg = LF.packageLfVersion (LF.extPackagePkg $ LF.dalfPackagePkg pkg)


-- | Find the directory containing the stable packages if it exists.
locateStablePackages :: IO FilePath
locateStablePackages = locateResource Resource
  -- //compiler/damlc/stable-packages
  { resourcesPath = "stable-packages"
    -- In a packaged application, the directory "stable-packages" is preserved
    -- underneath the resources directory because the bazel target includes
    -- multiple files.
    -- See @bazel_tools/packaging/packaging.bzl@.
  , runfilesPathPrefix = mainWorkspace </> "compiler" </> "damlc"
  }

generateStablePackagesRule :: SdkVersioned => Options -> Rules ()
generateStablePackagesRule opts =
    defineEarlyCutoff $ \GenerateStablePackages _file -> assert (null $ fromNormalizedFilePath _file) $ do
        lfVersion <- getDamlLfVersion
        stablePackagesDir <- liftIO locateStablePackages
        (errs, res) <- liftIO $ generateStablePackages lfVersion stablePackagesDir
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
generateSerializedPackage :: LF.PackageName -> Maybe LF.PackageVersion -> LF.PackageMetadata -> [NormalizedFilePath] -> MaybeT Action LF.Package
generateSerializedPackage pkgName pkgVersion meta rootFiles = do
    fileDeps <- usesE' GetDependencies rootFiles
    let allFiles = nubSort $ rootFiles <> concatMap transitiveModuleDeps fileDeps
    files <- lift $ discardInternalModules (Just $ pkgNameVersion pkgName pkgVersion) allFiles
    (dalfs, imports) <- extractImports <$> usesE' ReadSerializedDalf files
    lfVersion <- lift getDamlLfVersion
    pure $ buildPackage meta lfVersion dalfs imports

-- | Artifact directory for incremental builds.
buildDir :: FilePath
buildDir = ".daml/build"

-- | Path to the dalf file used in incremental builds.
dalfFileName :: NormalizedFilePath -> NormalizedFilePath
dalfFileName file =
    toNormalizedFilePath' $ buildDir </> fromNormalizedFilePath file -<.> "dalf"

-- | Path to the interface file used in incremental builds.
hiFileName :: NormalizedFilePath -> NormalizedFilePath
hiFileName file =
    toNormalizedFilePath' $ buildDir </> fromNormalizedFilePath file -<.> "hi"

readDalfFromFile :: NormalizedFilePath -> Action LF.ModuleWithImports
readDalfFromFile dalfFile = do
    lfVersion <- getDamlLfVersion
    liftIO $
        case LF.versionMajor lfVersion of
            LF.V2 -> decode Decode.decodeSinglePackageModule lfVersion
  where
    decode decodeSinglePackageModule lfVersion = do
        bytes <- BS.readFile $ fromNormalizedFilePath dalfFile
        protoPkg <- case Proto.fromByteString bytes of
            Left err -> fail (show err)
            Right a -> pure a
        case decodeSinglePackageModule lfVersion protoPkg of
            Left err -> fail (show err)
            Right mod -> pure mod

writeDalfFile :: NormalizedFilePath -> LF.ModuleWithImports -> Action ()
writeDalfFile dalfFile mod = do
    lfVersion <- getDamlLfVersion
    liftIO $
        case LF.versionMajor lfVersion of
            LF.V2 -> encode Encode.encodeSinglePackageModule lfVersion
  where
    encode encodeSinglePackageModule lfVersion = do
        liftIO $
            createDirectoryIfMissing
                True
                (takeDirectory $ fromNormalizedFilePath dalfFile)
        liftIO $
            BSL.writeFile (fromNormalizedFilePath dalfFile) $
                Proto.toLazyByteString $
                    encodeSinglePackageModule lfVersion mod

convertUnitId :: Map.Map GHC.UnitId LF.DalfPackage -> GHC.InstalledUnitId -> LF.PackageId
convertUnitId pkgMap id =
  let LF.DalfPackage { dalfPackageId } = fromJust (Map.lookup (DefiniteUnitId (DefUnitId id)) pkgMap)
  in dalfPackageId

depsToIds :: Map.Map GHC.UnitId LF.DalfPackage -> IntMap.IntMap (Set.Set GHC.InstalledUnitId) -> LF.PackageIds
depsToIds pkgMap unitMap = Set.map (convertUnitId pkgMap) $ mconcat $ IntMap.elems unitMap

generatePackageImports :: Rules ()
generatePackageImports =
  --TODO[RB]: probably see if we need to guard this on the version
  defineEarlyCutoff $ \GeneratePackageImports file -> do
    lfVersion <- getDamlLfVersion
    imports <- if lfVersion `supports` featurePackageImports
      then do
        PackageMap pkgMap <- use_ GeneratePackageMap file
        deps <- depPkgDeps <$> use_ GetDependencyInformation file
        return $ Right $ depsToIds pkgMap deps
      else
        return $ Left LF.noPkgImportsReasonLfDoesNotSupportPkgImports
    let hash :: BS.ByteString
        hash = foldMap (BS.fromString . T.unpack . LF.unPackageId) (toList $ fromRight mempty imports)
    return (Just hash, ([], Just imports))

-- Generates a Daml-LF archive without adding serializability information
-- or type checking it. This must only be used for debugging/testing.
generateRawPackageRule :: Options -> Rules ()
generateRawPackageRule options =
    define $ \GenerateRawPackage file -> do
        lfVersion <- getDamlLfVersion
        imports <- use_ GeneratePackageImports file
        fs <- transitiveModuleDeps <$> use_ GetDependencies file
        files <- discardInternalModules (optUnitId options) (fs ++ [file])
        dalfs <- uses_ GenerateRawDalf files
        -- build package
        let pkg = buildPackage (packageMetadataFromOptions options) lfVersion dalfs imports
        return ([], Just $ WhnfPackage pkg)

generatePackageDepsRule :: Options -> Rules ()
generatePackageDepsRule options =
    define $ \GeneratePackageDeps file -> do
        lfVersion <- getDamlLfVersion
        imports <- use_ GeneratePackageImports file
        fs <- transitiveModuleDeps <$> use_ GetDependencies file
        files <- discardInternalModules (optUnitId options) fs
        dalfs <- uses_ GenerateDalf files

        -- build package
        return ([], Just $ WhnfPackage $ buildPackage (packageMetadataFromOptions options) lfVersion dalfs imports)

contextForModule :: NormalizedFilePath -> Action SS.Context
contextForModule modFile = do
    lfVersion <- getDamlLfVersion
    WhnfPackage pkg <- use_ GeneratePackage modFile
    PackageMap pkgMap <- use_ GeneratePackageMap modFile
    stablePackages <- useNoFile_ GenerateStablePackages
    encodedModules <-
        mapM (\m -> fmap (\(hash, bs) -> (hash, (LF.moduleName m, bs))) (encodeModule lfVersion m)) $
        NM.toList $ LF.packageModules pkg
    DamlEnv{..} <- getDamlServiceEnv
    pure SS.Context
        { ctxModules = Map.fromList encodedModules
        , ctxPackages = [(LF.dalfPackageId pkg, LF.dalfPackageBytes pkg) | pkg <- Map.elems pkgMap ++ Map.elems stablePackages]
        , ctxSkipValidation = SS.SkipValidation (getSkipScriptValidation envSkipScriptValidation)
        , ctxPackageMetadata = LF.packageMetadata pkg
        }

contextForExtPkg :: NormalizedFilePath -> LF.ExternalPackage -> Action SS.Context
contextForExtPkg damlFile extPkg = do
    PackageMap pkgMap <- use_ GeneratePackageMap damlFile
    stablePackages <- useNoFile_ GenerateStablePackages
    pure
        SS.Context
            { ctxModules = Map.empty -- modules are loaded as a package
            , ctxPackages =
                  [ (LF.dalfPackageId pkg, LF.dalfPackageBytes pkg)
                  | pkg <- Map.elems pkgMap ++ Map.elems stablePackages
                  ]
            , ctxSkipValidation = SS.SkipValidation True -- no validation for external packages
            , ctxPackageMetadata = LF.packageMetadata $ LF.extPackagePkg extPkg
            }

worldForFile :: NormalizedFilePath -> Action LF.World
worldForFile file = do
    WhnfPackage pkg <- use_ GeneratePackage file
    pkgs <- getExternalPackages file
    pure $ LF.initWorldSelf pkgs pkg

data ScriptBackendException = ScriptBackendException
    { scriptNote :: String
    -- ^ A note to add more context to the error
    , scriptBackendError :: SS.BackendError
    } deriving Show

instance Exception ScriptBackendException

createScriptContextRule :: Rules ()
createScriptContextRule =
    define $ \CreateScriptContext file -> do
        ctx <- contextForModule file
        Just scriptService <- envScriptService <$> getDamlServiceEnv
        scriptContextsVar <- envScriptContexts <$> getDamlServiceEnv
        -- We need to keep the lock while creating the context not just while
        -- updating the variable. That avoids the following race:
        -- 1. getNewCtx creates a new context A
        -- 2. Before scriptContextsVar is updated, gcCtxs kicks in and ends up GCing A.
        -- 3. Now we update the var and insert A (which has been GCd).
        -- 4. We return A from the rule and run a script on A which
        --    now fails due to a missing context.
        ctxId <- liftIO $ modifyMVar scriptContextsVar $ \prevCtxs -> do
          ctxIdOrErr <- SS.getNewCtx scriptService ctx
          ctxId <-
              either
                  (throwIO . ScriptBackendException "Failed to create script context")
                  pure
                  ctxIdOrErr
          pure (HashMap.insert file ctxId prevCtxs, ctxId)
        pure ([], Just ctxId)

-- | This helper should be used instead of GenerateDalf/GenerateRawDalf
-- for generating modules that are sent to the script service.
-- It switches between GenerateRawDalf and GenerateDalf depending
-- on whether we only do light or full validation.
moduleForScript :: NormalizedFilePath -> Action LF.Module
moduleForScript file = do
    DamlEnv{..} <- getDamlServiceEnv
    if getSkipScriptValidation envSkipScriptValidation then
        use_ GenerateRawDalf file
    else
        use_ GenerateDalf file

runScriptsRule :: Rules ()
runScriptsRule =
    define $ \RunScripts file -> do
      scripts <- use_ GetScripts file
      results <-
          forM scripts $ \script ->
              use_ (RunSingleScript script) file
      pure ([], Just (concat results))

getScriptsRule :: Rules ()
getScriptsRule =
    define $ \GetScripts file -> do
      m <- moduleForScript file
      testFilter <- envTestFilter <$> getDamlServiceEnv
      let scripts =
              [ ScriptName name
              | (sc, _scLoc) <- scriptsInModule m
              , let name = LF.unExprValName sc
              , testFilter name]
      pure ([], Just scripts)

getScripts :: NormalizedFilePath -> Action [ScriptName]
getScripts file = use_ GetScripts file

runSingleScriptRule :: Rules ()
runSingleScriptRule =
    define $ \(RunSingleScript (ScriptName targetScriptName)) file -> do
      m <- moduleForScript file
      world <- worldForFile file
      Just scriptService <- envScriptService <$> getDamlServiceEnv

      ctxRoot <- use_ GetScriptRoot file
      ctxId <- use_ CreateScriptContext ctxRoot

      let scripts =
            [ (sc, loc)
            | (sc, loc) <- scriptsInModule m
            , targetScriptName == LF.unExprValName sc]

      lvl <- getDetailLevel
      scriptResults <-
          forM scripts $ \(script, loc) -> do
              (vr, res) <- runScript scriptService (Just file) ctxId (LF.moduleName m) script
              let range = maybe noRange sourceLocToRange loc
              pure (toDiagnostics lvl world file range res, (vr, res))
      let (diags, results) = unzip scriptResults
      pure (concat diags, Just results)

runScriptsPkg :: NormalizedFilePath -> LF.ExternalPackage -> Action (Maybe [(ScriptName, Either SS.Error SS.ScriptResult)])
runScriptsPkg damlFile extPkg = do
    Just scriptService <- envScriptService <$> getDamlServiceEnv
    ctx <- contextForExtPkg damlFile extPkg
    ctxIdOrErr <- liftIO $ SS.getNewCtx scriptService ctx
    ctxId <-
        liftIO $
        either
            (throwIO . ScriptBackendException "Failed to create script context")
            pure
            ctxIdOrErr
    scriptContextsVar <- envScriptContexts <$> getDamlServiceEnv
    liftIO $ modifyMVar_ scriptContextsVar $ pure . HashMap.insert damlFile ctxId
    results <- forM scripts $ \(modName, script) ->
        runScript scriptService Nothing ctxId modName script
    -- modify result to map back to PackageId
    pure $ Just results
  where
    pkg = LF.extPackagePkg extPkg
    scripts =
        [ (modName, sc)
        | mod <- NM.elems $ LF.packageModules pkg
        , let modName = LF.moduleName mod
        , not $ ["Daml", "Script"] `isPrefixOf` LF.unModuleName modName
        , (sc, _scLoc) <- scriptsInModule mod
        ]

toDiagnostics ::
       PrettyLevel
    -> LF.World
    -> NormalizedFilePath
    -> Range
    -> Either SS.Error SS.ScriptResult
    -> [FileDiagnostic]
toDiagnostics lvl world scriptFile scriptRange = \case
    Left err -> pure $ mkDiagnostic DsError (scriptFile, scriptRange) $
        formatScriptError lvl world err
    Right SS.ScriptResult{..} ->
        [ mkDiagnostic DsWarning fileRange (LF.prettyWarningMessage warning)
        | warning <- V.toList scriptResultWarnings
        , let fileRange = fileRangeFromMaybeLocation $
                SS.warningMessageCommitLocation warning
        ]
  where
    mkDiagnostic severity (file, range) pretty = (file, ShowDiag, ) $ Diagnostic
        { _range = range
        , _severity = Just severity
        , _source = Just "Script"
        , _message = Pretty.renderPlain pretty
        , _code = Nothing
        , _tags = Nothing
        , _relatedInformation = Nothing
        }

    fileRangeFromMaybeLocation :: Maybe SS.Location -> (NormalizedFilePath, Range)
    fileRangeFromMaybeLocation mbLocation =
        fromMaybe (scriptFile, scriptRange) $ do
            location <- mbLocation
            lfModule <- LF.lookupLocationModule world location
            filePath <- LF.moduleSource lfModule
            Just (toNormalizedFilePath' filePath, rangeFromLocation location)

    rangeFromLocation :: SS.Location -> LSP.Range
    rangeFromLocation SS.Location{..} = Range
        { _start = LSP.Position
            { _line = fromIntegral locationStartLine
            , _character = fromIntegral locationStartCol
            }
        , _end = LSP.Position
            { _line = fromIntegral locationEndLine
            , _character = fromIntegral locationEndCol
            }
        }

encodeModule :: LF.Version -> LF.Module -> Action (SS.Hash, BS.ByteString)
encodeModule lfVersion m = do
    case LF.moduleSource m of
      Just file -> use_ EncodeModule $ toNormalizedFilePath' file
      _ -> pure $ SS.encodeModule lfVersion m

getScriptRootsRule :: Rules ()
getScriptRootsRule =
    defineNoFile $ \GetScriptRoots -> do
        filesOfInterest <- getFilesOfInterest
        openVRs <- useNoFile_ GetOpenVirtualResources
        let files = HashSet.toList (filesOfInterest `HashSet.union` HashSet.map vrScriptFile openVRs)
        deps <- forP files $ \file -> do
            transitiveDeps <- maybe [] transitiveModuleDeps <$> use GetDependencies file
            pure $ Map.fromList [ (f, file) | f <- transitiveDeps ]
        -- We want to ensure that files of interest always map to themselves even if there are dependencies
        -- between files of interest so we union them separately. (`Map.union` is left-biased.)
        pure $ Map.fromList (map dupe files) `Map.union` Map.unions deps

getScriptRootRule :: Rules ()
getScriptRootRule =
    defineEarlyCutoff $ \GetScriptRoot file -> do
        ctxRoots <- useNoFile_ GetScriptRoots
        case Map.lookup file ctxRoots of
            Nothing -> liftIO $
                fail $ "No script root for file " <> show (fromNormalizedFilePath file) <> "."
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

vrChangedNotification :: VirtualResource -> T.Text -> Action ()
vrChangedNotification vr doc = do
    ShakeExtras { lspEnv } <- getShakeExtras
    liftIO $
        sendNotification lspEnv (LSP.SCustomMethod virtualResourceChangedNotification) $
        toJSON $ VirtualResourceChangedParams (virtualResourceToUri vr) doc

virtualResourceProgressNotification :: T.Text
virtualResourceProgressNotification = "daml/virtualResource/didProgress"

-- | Parameters for the virtual resource progress notification
data VirtualResourceProgressParams = VirtualResourceProgressParams
    { _vrppUri      :: !T.Text
      -- ^ The uri of the virtual resource.
    , _vrppMillisecondsPassed :: !Word64
      -- ^ The progress status of the virtual resource
    , _vrppStartedAt :: !Word64
      -- ^ When this status info started
    } deriving Show

instance ToJSON VirtualResourceProgressParams where
    toJSON VirtualResourceProgressParams{..} =
        object ["uri" .= _vrppUri, "millisecondsPassed" .= _vrppMillisecondsPassed, "startedAt" .= _vrppStartedAt ]

instance FromJSON VirtualResourceProgressParams where
    parseJSON = withObject "VirtualResourceProgressParams" $ \o ->
        VirtualResourceProgressParams <$> o .: "uri" <*> o .: "millisecondsPassed" <*> o .: "startedAt"

vrProgressNotification :: ShakeLspEnv -> VirtualResource -> SS.ScriptStatus -> IO ()
vrProgressNotification lspEnv vr status = do
    sendNotification lspEnv (LSP.SCustomMethod virtualResourceProgressNotification) $
        toJSON $
            VirtualResourceProgressParams
                (virtualResourceToUri vr)
                (SS.scriptStatusMillisecondsPassed status)
                (SS.scriptStatusStartedAt status)

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

vrNoteSetNotification :: VirtualResource -> T.Text -> Action ()
vrNoteSetNotification vr note = do
    ShakeExtras { lspEnv } <- getShakeExtras
    liftIO $
        sendNotification lspEnv (LSP.SCustomMethod virtualResourceNoteSetNotification) $
        toJSON $ VirtualResourceNoteSetParams (virtualResourceToUri vr) note

-- A rule that builds the files-of-interest and notifies via the
-- callback of any errors. NOTE: results may contain errors for any
-- dependent module.
-- TODO (MK): We should have a non-Daml version of this rule
ofInterestRule :: Rules ()
ofInterestRule = do
    -- go through a rule (not just an action), so it shows up in the profile
    action $ useNoFile OfInterest
    defineNoFile $ \OfInterest -> do
        setPriority priorityFilesOfInterest
        DamlEnv{..} <- getDamlServiceEnv

        -- query for files of interest & open scripts
        files <- getFilesOfInterest
        openVRs <- useNoFile_ GetOpenVirtualResources
        let vrFiles =
                HashMap.fromListWith (<>)
                    (map (\vr -> (vrScriptFile vr, [vr])) $ HashSet.toList openVRs)

        -- determine all files
        let allFiles = files `HashSet.union` HashMap.keysSet vrFiles
        gc allFiles

        -- Check files that can't be compiled
        let checkUncompilableFiles = flip map (HashSet.toList allFiles) $ \file -> do
            mbDalf <- getDalf file
            when (isNothing mbDalf) $ do
                forM_ (HashMap.lookupDefault [] file vrFiles) $ \ovr ->
                    vrNoteSetNotification ovr $ LF.fileWScriptNoLongerCompilesNote $ T.pack $
                        fromNormalizedFilePath file

        -- Check diagnostics from Dlint
        let dlintActions = map (use_ GetDlintDiagnostics) (HashSet.toList allFiles)

        -- Run any open scripts to report their results
        let runScriptActions =
                if isJust envScriptService -- only run Scripts when we have a service
                    then if getStudioAutorunAllScripts envStudioAutorunAllScripts
                            then map runWholeFile (HashSet.toList allFiles)
                            else map (\(VRScript file name) -> runScript file name) (HashSet.toList openVRs)
                    else []

        -- Run all in parallel
        _ <- parallel $ checkUncompilableFiles <> dlintActions <> runScriptActions
        return ()
  where
      -- Run all scripts in a file, used when StudioAutorunAllScripts flag is set
      runWholeFile file = do
          scripts <- use GetScripts file
          mapM_ (runScript file) (fromMaybe [] scripts)

      -- Run a single script
      runScript file scriptName = do
          -- Extract file with world info
          world <- worldForFile file

          -- Run either the script or the script in the appropriate file
          mbScriptResults <- use (RunSingleScript scriptName) file
          let scriptResults = fromMaybe [] mbScriptResults

          lvl <- getDetailLevel

          -- Should be a singleton list, send results via LSP
          forM_ scriptResults $ \(scriptName, res) -> do
              let doc = formatScriptResult lvl world res
              vrChangedNotification (VRScript file scriptName) doc

          -- If the script name is not in the results, the script no
          -- longer exists on this file - Notify the client via LSP
          when (scriptName `notElem` map fst scriptResults) $
              vrNoteSetNotification (VRScript file scriptName) $ LF.scriptNotInFileNote $
              T.pack $ fromNormalizedFilePath file

      gc :: HashSet.HashSet NormalizedFilePath -> Action ()
      gc roots = do
        depInfoOrErr <- sequence <$> uses GetDependencyInformation (HashSet.toList roots)
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
                    roots `HashSet.union`
                    (HashSet.insert emptyFilePath $ HashSet.fromList $ concatMap reachableModules depInfos)
            garbageCollect (`HashSet.member` reachableFiles)
          DamlEnv{..} <- getDamlServiceEnv
          liftIO $ whenJust envScriptService $ \scriptService -> do
              mask $ \restore -> do
                  ctxs <- takeMVar envScriptContexts
                  -- Filter down to contexts of files of interest.
                  let gcdCtxsMap :: HashMap.HashMap NormalizedFilePath SS.ContextId
                      gcdCtxsMap = HashMap.filterWithKey (\k _ -> k `HashSet.member` roots) ctxs
                      gcdCtxs = HashMap.elems gcdCtxsMap
                  -- Note (MK) We don’t want to keep sending GC grpc requests if nothing
                  -- changed. We used to keep track of the last GC request and GC if that was
                  -- different. However, that causes an issue in the folllowing script.
                  -- This script is exactly what we hit in the integration tests.
                  --
                  -- 1. A is the only file of interest.
                  -- 2. We run GC, no script context has been allocated.
                  --    No script contexts will be garbage collected.
                  -- 3. Now the script context is allocated.
                  -- 4. B is set to the only file of interest.
                  -- 5. We run GC, the script context for B has not been allocated yet.
                  --    A is not a file of interest so gcdCtxs is still empty.
                  --    Therefore the old and current contexts are identical.
                  --
                  -- We now GC under the following condition:
                  --
                  -- > gcdCtxs is different from ctxs or the last GC was different from gcdCtxs
                  --
                  -- The former covers the above script, the latter covers the case where
                  -- a script context changed but the files of interest did not.
                  prevCtxRoots <- takeMVar envPreviousScriptContexts
                  when (gcdCtxs /= HashMap.elems ctxs || prevCtxRoots /= gcdCtxs) $
                      -- We want to avoid updating the maps if gcCtxs throws an exception
                      -- so we do some custom masking. We could still end up GC’ing on the
                      -- server and getting an exception afterwards. This is fine, at worst
                      -- we will just GC again.
                      restore (void $ SS.gcCtxs scriptService gcdCtxs) `onException`
                          (putMVar envPreviousScriptContexts prevCtxRoots >>
                           putMVar envScriptContexts ctxs)
                  -- We are masked so this is atomic.
                  putMVar envPreviousScriptContexts gcdCtxs
                  putMVar envScriptContexts gcdCtxsMap

getOpenVirtualResourcesRule :: Rules ()
getOpenVirtualResourcesRule = do
    defineEarlyCutoff $ \GetOpenVirtualResources _file -> assert (null $ fromNormalizedFilePath _file) $ do
        alwaysRerun
        DamlEnv{..} <- getDamlServiceEnv
        openVRs <- liftIO $ readVar envOpenVirtualResources
        pure (Just $ BS.fromString $ show openVRs, ([], Just openVRs))

formatHtmlScriptError :: PrettyLevel -> LF.World -> SS.Error -> T.Text
formatHtmlScriptError lvl world  err = case err of
    SS.BackendError err ->
        Pretty.renderHtmlDocumentText 128 $ Pretty.pretty $ "Script service backend error: " <> show err
    SS.ScriptError err -> LF.renderScriptError lvl world err
    SS.ExceptionError err ->
        Pretty.renderHtmlDocumentText 128 $ Pretty.pretty $ "Exception during script execution: " <> show err

formatScriptError :: PrettyLevel -> LF.World -> SS.Error -> Pretty.Doc Pretty.SyntaxClass
formatScriptError lvl world  err = case err of
    SS.BackendError err -> Pretty.pretty $ "Script service backend error: " <> show err
    SS.ScriptError err -> LF.prettyScriptError lvl world err
    SS.ExceptionError err -> Pretty.pretty $ "Exception during script execution: " <> show err

formatScriptResult :: PrettyLevel -> LF.World -> Either SS.Error SS.ScriptResult -> T.Text
formatScriptResult lvl world errOrRes =
    case errOrRes of
        Left err ->
            formatHtmlScriptError lvl world err
        Right res ->
            LF.renderScriptResult lvl world res

runScript :: SS.Handle -> Maybe NormalizedFilePath -> SS.ContextId -> LF.ModuleName -> LF.ExprValName -> Action (ScriptName, Either SS.Error SS.ScriptResult)
runScript scriptService mbFile ctxId moduleName exprName = do
    ShakeExtras {lspEnv} <- getShakeExtras
    let scriptName = ScriptName $ LF.unExprValName exprName
    let liveHandler = case mbFile of
            Just file -> vrProgressNotification lspEnv $ VRScript file scriptName
            Nothing -> const $ pure ()
    logger <- actionLogger
    res <- liftIO $ SS.runLiveScript scriptService ctxId logger moduleName exprName liveHandler
    pure (scriptName, res)

encodeModuleRule :: Options -> Rules ()
encodeModuleRule options =
    define $ \EncodeModule file -> do
        lfVersion <- getDamlLfVersion
        fs <- transitiveModuleDeps <$> use_ GetDependencies file
        imports <- use_ GeneratePackageImports file
        files <- discardInternalModules (optUnitId options) fs
        encodedDeps <- uses_ EncodeModule files
        m <- moduleForScript file
        let (hash, bs) = SS.encodeModuleWithImports lfVersion (m, imports)
        return ([], Just (mconcat $ hash : map fst encodedDeps, bs))

-- dlint

dlintSettings :: DlintUsage -> IO ([Classify], Hint)
dlintSettings DlintDisabled = pure ([], mempty @Hint)
dlintSettings (DlintEnabled DlintOptions {..}) = do
    dlintRulesFile <- getDlintRulesFile dlintRulesFile
    hintFiles <- getHintFiles dlintHintFiles
    (_, cs, hs) <- foldMapM parseSettings $
      dlintRulesFile : hintFiles
    return (cs, hs)
    where
      getDlintRulesFile :: DlintRulesFile -> IO FilePath
      getDlintRulesFile = \case
        DefaultDlintRulesFile -> locateResource Resource
          -- //compiler/damlc/daml-ide-core:dlint.yaml
          { resourcesPath = "dlint.yaml"
            -- In a packaged application, this is stored directly underneath
            -- the resources directory because it's a single file.
            -- See @bazel_tools/packaging/packaging.bzl@.
          , runfilesPathPrefix = mainWorkspace </> "compiler" </> "damlc" </> "daml-ide-core"
          }
        ExplicitDlintRulesFile path -> pure path

      getHintFiles :: DlintHintFiles -> IO [FilePath]
      getHintFiles = \case
        ImplicitDlintHintFile -> do
          curdir <- getCurrentDirectory
          home <- ((:[]) <$> getHomeDirectory) `catchIOError` (const $ return [])
          fmap maybeToList $ findM Dir.doesFileExist $
            map (</> ".dlint.yaml") (ancestors curdir ++ home)
        ExplicitDlintHintFiles files -> pure files

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
      liftIO $ dlintSettings usage

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
      srcSpanToRange :: SrcSpan -> LSP.Range
      srcSpanToRange (RealSrcSpan span) = Range {
          _start = LSP.Position {
                _line = fromIntegral (srcSpanStartLine span) - 1
              , _character  = fromIntegral (srcSpanStartCol span) - 1}
        , _end   = LSP.Position {
                _line = fromIntegral (srcSpanEndLine span) - 1
             , _character = fromIntegral (srcSpanEndCol span) - 1}
        }
      srcSpanToRange (UnhelpfulSpan _) = Range {
          _start = LSP.Position {
                _line = -1
              , _character  = -1}
        , _end   = LSP.Position {
                _line = -1
             , _character = -1}
        }
      diagnostic :: NormalizedFilePath -> Idea -> FileDiagnostic
      diagnostic file i =
        (file, ShowDiag, LSP.Diagnostic {
              _range = srcSpanToRange $ ideaSpan i
            , _severity = Just LSP.DsInfo
            , _code = Nothing
            , _source = Just "linter"
            , _message = T.pack $ show i
            , _relatedInformation = Nothing
            , _tags = Nothing
      })

isDamlScriptModule :: LF.ModuleName -> Bool
isDamlScriptModule (LF.ModuleName ["Daml", "Script"]) = True
isDamlScriptModule (LF.ModuleName ["Daml", "Script", "Internal", "LowLevel"]) = True
isDamlScriptModule _ = False

scriptsInModule :: LF.Module -> [(LF.ExprValName, Maybe LF.SourceLoc)]
scriptsInModule m =
    [ (LF.dvalName val, LF.dvalLocation val)
    | val <- NM.toList (LF.moduleValues m)
    , T.head (LF.unExprValName (LF.dvalName val)) /= '$'
    , LF.TConApp (LF.Qualified _ damlScriptModule (LF.TypeConName ["Script"])) _ <-  [LF.dvalType val]
    , isDamlScriptModule damlScriptModule
    ]

getDamlLfVersion :: Action LF.Version
getDamlLfVersion = envDamlLfVersion <$> getDamlServiceEnv

getDetailLevel :: Action PrettyLevel
getDetailLevel = envDetailLevel <$> getDamlServiceEnv

-- | This operates on file paths rather than module names so that we avoid introducing a dependency on GetParsedModule.
discardInternalModules :: Maybe UnitId -> [NormalizedFilePath] -> Action [NormalizedFilePath]
discardInternalModules mbPackageName files = do
    stablePackages <- useNoFile_ GenerateStablePackages
    pure $ filter (shouldKeep stablePackages) files
  where shouldKeep stablePackages f =
            not (any (`isSuffixOf` fromNormalizedFilePath f) internalModules) &&
            not (any (\(unitId, modName) ->
                          mbPackageName == Just unitId &&
                          moduleNameFile modName `isSuffixOf` fromNormalizedFilePath f)
                     $ Map.keys stablePackages)
        moduleNameFile (LF.ModuleName segments) = FPP.joinPath (map T.unpack segments) <.> "daml"

usesE' ::
       ( Eq k
       , Hashable k
       , Binary k
       , Show k
       , Show (RuleResult k)
       , Typeable k
       , Typeable (RuleResult k)
       , NFData k
       , NFData (RuleResult k)
       )
    => k
    -> [NormalizedFilePath]
    -> MaybeT Action [RuleResult k]
usesE' k = fmap (map fst) . usesE k


internalModules :: [FilePath]
internalModules = map FPP.normalise
  [ "Data/String.daml"
  , "GHC/CString.daml"
  , "GHC/Integer/Type.daml"
  , "GHC/Natural.daml"
  , "GHC/Real.daml"
  , "GHC/Types.daml"
  ]


damlRule :: SdkVersioned => Options -> Rules ()
damlRule opts = do
    generateRawDalfRule opts
    generateDalfRule opts
    generatePackageImports
    generateSerializedDalfRule opts
    readSerializedDalfRule
    readInterfaceRule
    generateDocTestModuleRule
    generatePackageMapRule opts
    generateStablePackagesRule opts
    generatePackageRule
    generateRawPackageRule opts
    generatePackageDepsRule opts
    runScriptsRule
    runSingleScriptRule
    getScriptsRule
    getScriptRootsRule
    getScriptRootRule
    getDlintDiagnosticsRule
    encodeModuleRule opts
    createScriptContextRule
    getOpenVirtualResourcesRule
    getDlintSettingsRule (optDlintUsage opts)
    damlGhcSessionRule opts
    extractUpgradedPackageRule opts
    when (optEnableOfInterestRule opts) ofInterestRule

mainRule :: SdkVersioned => Options -> Rules ()
mainRule options = do
    IDE.mainRule
    damlRule options
