-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE OverloadedStrings #-}
module Development.IDE.State.Rules.Daml
    ( module Development.IDE.State.Rules
    , module Development.IDE.State.Rules.Daml
    ) where

import Control.Concurrent.Extra
import Control.Exception
import Control.Monad.Except
import Control.Monad.Extra
import qualified Data.ByteString as BS
import Data.Either.Extra
import qualified Data.Map.Strict as Map
import Data.Maybe
import qualified Data.NameMap as NM
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Tagged
import qualified Data.Text as T
import Development.Shake hiding (Diagnostic, Env)
import "ghc-lib" GHC
import "ghc-lib-parser" Module (UnitId, stringToUnitId)
import System.Directory.Extra (listFilesRecursive)
import System.FilePath

import Development.IDE.Functions.Compile (CompileOpts(..))
import Development.IDE.Functions.DependencyInformation
import Development.IDE.State.Rules hiding (mainRule)
import qualified Development.IDE.State.Rules as IDE
import Development.IDE.State.Service.Daml
import Development.IDE.State.Shake
import Development.IDE.Types.Diagnostics
import Development.IDE.Types.LSP
import Development.IDE.UtilGHC

import Development.IDE.State.RuleTypes.Daml

import DA.Daml.GHC.Compiler.Convert (convertModule, sourceLocToRange)
import DA.Daml.GHC.Compiler.UtilLF
import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.InferSerializability as Serializability
import qualified DA.Daml.LF.PrettyScenario as LF
import qualified DA.Daml.LF.Proto3.Archive as Archive
import qualified DA.Daml.LF.ScenarioServiceClient as SS
import qualified DA.Daml.LF.Simplifier as LF
import qualified DA.Daml.LF.TypeChecker as LF
import qualified DA.Pretty as Pretty

-- | Get an unvalidated DALF package.
-- This must only be used for debugging/testing.
getRawDalf :: FilePath -> Action (Maybe LF.Package)
getRawDalf absFile = use GenerateRawPackage absFile

-- | Get a validated DALF package.
getDalf :: FilePath -> Action (Maybe LF.Package)
getDalf file = eitherToMaybe <$>
    (runExceptT $ useE GeneratePackage file)

runScenarios :: FilePath -> Action (Maybe [(VirtualResource, Either SS.Error SS.ScenarioResult)])
runScenarios file = use RunScenarios file

-- | Get a list of the scenarios in a given file
getScenarios :: FilePath -> Action [VirtualResource]
getScenarios file = do
    m <- use_ GenerateRawDalf file
    pure [ VRScenario file (unTagged $ LF.qualObject ref)
         | ref <- scenariosInModule m
         ]

-- Generates the DALF for a module without adding serializability information
-- or type checking it.
generateRawDalfRule :: Rules ()
generateRawDalfRule =
    define $ \GenerateRawDalf file -> do
        lfVersion <- getDamlLfVersion
        core <- use_ GenerateCore file
        setPriority PriorityGenerateDalf
        -- Generate the map from package names to package hashes
        pkgMap <- use_ GeneratePackageMap ""
        let pkgMap0 = Map.map (\(pId, _pkg, _bs, _fp) -> unTagged pId) pkgMap
        -- GHC Core to DAML LF
        case convertModule lfVersion pkgMap0 core of
            Left e -> return ([e], Nothing)
            Right v -> return ([], Just v)

-- Generates and type checks the DALF for a module.
generateDalfRule :: Rules ()
generateDalfRule =
    define $ \GenerateDalf file -> fmap toIdeResultNew $ runExceptT $ do
        lfVersion <- lift getDamlLfVersion
        pkg <- useE GeneratePackageDeps file
        -- The file argument isn’t used in the rule, so we leave it empty to increase caching.
        pkgMap <- useE GeneratePackageMap ""
        let pkgs = [(pId, pkg) | (pId, pkg, _bs, _fp) <- Map.elems pkgMap]
        let world = (LF.initWorld pkgs lfVersion) {LF.worldSelf = pkg}
        unsimplifiedRawDalf <- useE GenerateRawDalf file
        let rawDalf = LF.simplifyModule unsimplifiedRawDalf
        lift $ setPriority PriorityGenerateDalf

        dalf <- withExceptT (pure . ideErrorPretty file)
          $ liftEither
          $ Serializability.inferModule world rawDalf

        withExceptT (pure . ideErrorPretty file)
          $ liftEither
          $ LF.checkModule world lfVersion dalf

        pure dalf

-- | Load all the packages that are available in the package database directories. We expect the
-- filename to match the package name.
-- TODO (drsk): We might want to change this to load only needed packages in the future.
generatePackageMap ::
     [FilePath] -> IO ([Diagnostic], Map.Map UnitId (LF.PackageId, LF.Package, BS.ByteString, FilePath))
generatePackageMap fps = do
  (diags, pkgs) <-
    fmap (partitionEithers . concat) $
    forM fps $ \fp -> do
      allFiles <- listFilesRecursive fp
      let dalfs = filter ((== ".dalf") . takeExtension) allFiles
      forM dalfs $ \dalf -> do
        dalfBS <- BS.readFile dalf
        return $ do
          (pkgId, package) <-
            mapLeft (ideErrorPretty dalf) $
            Archive.decodeArchive dalfBS
          let unitId = stringToUnitId $ dropExtension $ takeFileName dalf
          Right (unitId, (pkgId, package, dalfBS, dalf))
  return (diags, Map.fromList pkgs)

generatePackageMapRule :: Rules ()
generatePackageMapRule =
    defineNoFile $ \GeneratePackageMap -> do
        env <- getServiceEnv
        (errs, res) <-
            liftIO $ generatePackageMap (optPackageDbs $ envOptions env)
        when (errs /= []) $
            reportSeriousError $
            "Rule GeneratePackageMap generated errors " ++ show errs
        return res

generatePackageRule :: Rules ()
generatePackageRule =
    define $ \GeneratePackage file -> do
        deps <- use_ GeneratePackageDeps file
        dalf <- use_ GenerateDalf file
        return ([], Just deps{LF.packageModules = NM.insert dalf (LF.packageModules deps)})

-- Generates a DAML-LF archive without adding serializability information
-- or type checking it. This must only be used for debugging/testing.
generateRawPackageRule :: Rules ()
generateRawPackageRule =
    define $ \GenerateRawPackage file -> do
        lfVersion <- getDamlLfVersion
        fs <- transitiveModuleDeps <$> use_ GetDependencies file
        files <- discardInternalModules (fs ++ [file])
        dalfs <- uses_ GenerateRawDalf files

        -- build package
        env <- getServiceEnv
        let pkg = buildPackage (optMbPackageName (envOptions env)) lfVersion dalfs
        return ([], Just pkg)

generatePackageDepsRule :: Rules ()
generatePackageDepsRule =
    define $ \GeneratePackageDeps file -> do
        lfVersion <- getDamlLfVersion
        fs <- transitiveModuleDeps <$> use_ GetDependencies file
        files <- discardInternalModules fs
        dalfs <- uses_ GenerateDalf files

        -- build package
        env <- getServiceEnv
        return ([], Just $ buildPackage (optMbPackageName (envOptions env)) lfVersion dalfs)

contextForFile :: FilePath -> Action SS.Context
contextForFile file = do
    lfVersion <- getDamlLfVersion
    pkg <- use_ GeneratePackage file
    pkgMap <- use_ GeneratePackageMap ""
    encodedModules <-
        mapM (\m -> fmap (\(hash, bs) -> (hash, (LF.moduleName m, bs))) (encodeModule lfVersion m)) $
        NM.toList $ LF.packageModules pkg
    pure SS.Context
        { ctxModules = Map.fromList encodedModules
        , ctxPackages = map (\(pId, _, p, _) -> (pId, p)) (Map.elems pkgMap)
        , ctxDamlLfVersion = lfVersion
        }

worldForFile :: FilePath -> Action LF.World
worldForFile file = do
    lfVersion <- getDamlLfVersion
    pkg <- use_ GeneratePackage file
    pkgMap <- use_ GeneratePackageMap ""
    let pkgs = [ (pId, pkg) | (pId, pkg, _, _) <- Map.elems pkgMap ]
    pure (LF.initWorld pkgs lfVersion) { LF.worldSelf = pkg }

data ScenarioBackendException = ScenarioBackendException
    { scenarioNote :: String
    -- ^ A note to add more context to the error
    , scenarioBackendError :: SS.BackendError
    } deriving Show

instance Exception ScenarioBackendException

-- | We didn’t find a context root for the given file.
-- This probably means that you haven’t set the files of interest or open VRs
-- such that the given file is included in the transitive dependencies.
data ScenarioRootException = ScenarioRootException
    { missingRootFor :: FilePath
    } deriving Show

instance Exception ScenarioRootException

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

runScenariosRule :: Rules ()
runScenariosRule =
    define $ \RunScenarios file -> do
      m <- use_ GenerateRawDalf file
      world <- worldForFile file
      let scenarios = scenariosInModule m
          toDiagnostic :: LF.ValueRef -> Either SS.Error SS.ScenarioResult -> Maybe Diagnostic
          toDiagnostic scenario (Left err) =
              Just $ addFilePath file $ Diagnostic
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
      ctxRoots <- liftIO . readVar . envScenarioContextRoots =<< getDamlServiceEnv
      ctxRoot <- liftIO $ maybe (throwIO $ ScenarioRootException file) pure $ Map.lookup file ctxRoots
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
        | isAbsolute file -> use_ EncodeModule file
      _ -> pure $ SS.encodeModule lfVersion m


-- A rule that builds the files-of-interest and notifies via the
-- callback of any errors. NOTE: results may contain errors for any
-- dependent module.
-- TODO (MK): We should have a non-DAML version of this rule
ofInterestRule :: Rules ()
ofInterestRule = do
    -- go through a rule (not just an action), so it shows up in the profile
    action $ use OfInterest ""
    defineNoFile $ \OfInterest -> do
        setPriority PriorityFilesOfInterest
        alwaysRerun
        Env{..} <- getServiceEnv
        DamlEnv{..} <- getDamlServiceEnv
        -- query for files of interest
        files   <- liftIO $ readVar envOfInterestVar
        openVRs <- liftIO $ readVar envOpenVirtualResources
        let vrFiles = Set.map vrScenarioFile openVRs
        -- We run scenarios for all files of interest to get diagnostics
        -- and for the files for which we have open VRs so that they get
        -- updated.
        let scenarioFiles = files `Set.union` vrFiles
        gc scenarioFiles
        -- compile and notify any errors
        let runScenarios file = void $ runExceptT $ do
                world <- lift $ worldForFile file
                vrs <- useE RunScenarios file
                lift $ forM vrs $ \(vr, res) -> do
                    let doc = formatScenarioResult world res
                    when (vr `Set.member` openVRs) $
                        sendEvent $ EventVirtualResourceChanged vr doc
        -- We don’t always have a scenario service (e.g., damlc compile)
        -- so only run scenarios if we have one.
        let shouldRunScenarios = isJust envScenarioService
        when shouldRunScenarios $ do
            deps <- forP (Set.toList scenarioFiles) $ \file -> do
                transitiveDeps <- maybe [] transitiveModuleDeps <$> use GetDependencies file
                pure $ Map.fromList [ (f, file) | f <- transitiveDeps ]
            -- We want to ensure that files of interest always map to themselves even if there are dependencies
            -- between files of interest so we union them separately.
            liftIO $ writeVar envScenarioContextRoots $
                Map.fromList [(f,f) | f <- Set.toList scenarioFiles] `Map.union` Map.unions deps
        _ <- parallel $
            map (void . getDalf) (Set.toList scenarioFiles) <>
            [runScenarios file | shouldRunScenarios, file <- Set.toList scenarioFiles]
        return ()
  where
      gc :: Set FilePath -> Action ()
      gc roots = do
        depInfoOrErr <- sequence <$> uses GetDependencyInformation (Set.toList roots)
        -- We only clear results if there are no errors in the
        -- dependency information (in particular, no parse errors).
        -- This prevents us from clearing the results for files that are
        -- only temporarily unreachable due to a parse error.
        whenJust depInfoOrErr $ \depInfo -> do
          let noErrors = all (Map.null . depErrorNodes) depInfo
          when noErrors $ do
            -- We insert the empty file path since we use this for rules that do not depend
            -- on the given file.
            let reachableFiles =
                    -- To guard against buggy dependency info, we add
                    -- the roots even though they should be included.
                    roots `Set.union`
                    (Set.insert "" $ foldMap (Map.keysSet . depModuleDeps) depInfo)
            garbageCollect (`Set.member` reachableFiles)
          DamlEnv{..} <- getDamlServiceEnv
          liftIO $ whenJust envScenarioService $ \scenarioService -> do
              ctxRoots <- modifyVar envScenarioContexts $ \ctxs -> do
                  let gcdCtxs = Map.restrictKeys ctxs roots
                  pure (gcdCtxs, Map.elems gcdCtxs)
              prevCtxRoots <- modifyVar envPreviousScenarioContexts $ \prevCtxs -> pure (ctxRoots, prevCtxs)
              when (prevCtxRoots /= ctxRoots) $ void $ SS.gcCtxs scenarioService ctxRoots

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

runScenario :: SS.Handle -> FilePath -> SS.ContextId -> LF.ValueRef -> IO (VirtualResource, Either SS.Error SS.ScenarioResult)
runScenario scenarioService file ctxId scenario = do
    res <- SS.runScenario scenarioService ctxId scenario
    let scenarioName = LF.qualObject scenario
    let vr = VRScenario file (unTagged scenarioName)
    pure (vr, res)

encodeModuleRule :: Rules ()
encodeModuleRule =
    define $ \EncodeModule file -> do
        lfVersion <- getDamlLfVersion
        fs <- transitiveModuleDeps <$> use_ GetDependencies file
        files <- discardInternalModules fs
        encodedDeps <- uses_ EncodeModule files
        m <- use_ GenerateRawDalf file
        let (hash, bs) = SS.encodeModule lfVersion m
        return ([], Just (mconcat $ hash : map fst encodedDeps, bs))

scenariosInModule :: LF.Module -> [LF.ValueRef]
scenariosInModule m =
    [ LF.Qualified LF.PRSelf (LF.moduleName m) (LF.dvalName val)
    | val <- NM.toList (LF.moduleValues m), LF.getIsTest (LF.dvalIsTest val)]

getDamlLfVersion:: Action LF.Version
getDamlLfVersion = envDamlLfVersion <$> getDamlServiceEnv

discardInternalModules :: [FilePath] -> Action [FilePath]
discardInternalModules files =
    mapM (liftIO . fileFromParsedModule) .
    filter (not . modIsInternal . ms_mod . pm_mod_summary) =<<
    uses_ GetParsedModule files

damlRule :: Rules ()
damlRule = do
    generateRawDalfRule
    generateDalfRule
    generatePackageMapRule
    generatePackageRule
    generateRawPackageRule
    generatePackageDepsRule
    runScenariosRule
    ofInterestRule
    encodeModuleRule
    createScenarioContextRule

mainRule :: Rules ()
mainRule = do
    IDE.mainRule
    damlRule
