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
import DA.Daml.GHC.Compiler.Options(Options(..))
import qualified Data.ByteString as BS
import qualified Data.ByteString.UTF8 as BS
import Data.Either.Extra
import qualified Data.Map.Strict as Map
import Data.Maybe
import qualified Data.NameMap as NM
import Data.Set (Set)
import qualified Data.Set as Set
import qualified Data.Text as T
import Data.Tuple.Extra
import Development.Shake hiding (Diagnostic, Env)
import "ghc-lib" GHC
import "ghc-lib-parser" Module (UnitId, stringToUnitId)
import System.Directory.Extra (listFilesRecursive)
import System.FilePath

import Development.IDE.Functions.DependencyInformation
import Development.IDE.State.Rules hiding (mainRule)
import qualified Development.IDE.State.Rules as IDE
import Development.IDE.State.Service.Daml
import Development.IDE.State.Shake
import Development.IDE.Types.Diagnostics
import Development.IDE.Types.LSP

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
getScenarioNames :: FilePath -> Action (Maybe [VirtualResource])
getScenarioNames file = fmap f <$> use GenerateRawDalf file
    where f = map (VRScenario file . LF.unExprValName . LF.qualObject) . scenariosInModule

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
        let pkgMap0 = Map.map (\(pId, _pkg, _bs, _fp) -> LF.unPackageId pId) pkgMap
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
        let world = LF.initWorldSelf pkgs lfVersion pkg
        unsimplifiedRawDalf <- useE GenerateRawDalf file
        let rawDalf = LF.simplifyModule unsimplifiedRawDalf
        lift $ setPriority PriorityGenerateDalf

        dalf <- withExceptT (pure . ideErrorPretty file)
          $ liftEither
          $ Serializability.inferModule world lfVersion rawDalf

        withExceptT (pure . ideErrorPretty file)
          $ liftEither
          $ LF.checkModule world lfVersion dalf

        pure dalf

-- | Load all the packages that are available in the package database directories. We expect the
-- filename to match the package name.
-- TODO (drsk): We might want to change this to load only needed packages in the future.
generatePackageMap ::
     [FilePath] -> IO ([FileDiagnostic], Map.Map UnitId (LF.PackageId, LF.Package, BS.ByteString, FilePath))
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

generatePackageMapRule :: Options -> Rules ()
generatePackageMapRule opts =
    defineNoFile $ \GeneratePackageMap -> do
        (errs, res) <-
            liftIO $ generatePackageMap (optPackageDbs opts)
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
generateRawPackageRule :: Options -> Rules ()
generateRawPackageRule options =
    define $ \GenerateRawPackage file -> do
        lfVersion <- getDamlLfVersion
        fs <- transitiveModuleDeps <$> use_ GetDependencies file
        files <- discardInternalModules (fs ++ [file])
        dalfs <- uses_ GenerateRawDalf files

        -- build package
        let pkg = buildPackage (optMbPackageName options) lfVersion dalfs
        return ([], Just pkg)

generatePackageDepsRule :: Options -> Rules ()
generatePackageDepsRule options =
    define $ \GeneratePackageDeps file -> do
        lfVersion <- getDamlLfVersion
        fs <- transitiveModuleDeps <$> use_ GetDependencies file
        files <- discardInternalModules fs
        dalfs <- uses_ GenerateDalf files

        -- build package
        return ([], Just $ buildPackage (optMbPackageName options) lfVersion dalfs)

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
    pure $ LF.initWorldSelf pkgs lfVersion pkg

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

runScenariosRule :: Rules ()
runScenariosRule =
    define $ \RunScenarios file -> do
      m <- use_ GenerateRawDalf file
      world <- worldForFile file
      let scenarios = scenariosInModule m
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
        | isAbsolute file -> use_ EncodeModule file
      _ -> pure $ SS.encodeModule lfVersion m

getScenarioRootsRule :: Rules ()
getScenarioRootsRule =
    defineNoFile $ \GetScenarioRoots -> do
        filesOfInterest <- use_ GetFilesOfInterest ""
        openVRs <- use_ GetOpenVirtualResources ""
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
        ctxRoots <- use_ GetScenarioRoots ""
        case Map.lookup file ctxRoots of
            Nothing -> liftIO $
                fail $ "No scenario root for file " <> show file <> "."
            Just root -> pure (Just $ BS.fromString root, ([], Just root))

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
        Env{..} <- getServiceEnv
        DamlEnv{..} <- getDamlServiceEnv
        -- query for files of interest
        files   <- use_ GetFilesOfInterest ""
        openVRs <- use_ GetOpenVirtualResources ""
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

getFilesOfInterestRule :: Rules ()
getFilesOfInterestRule = do
    defineEarlyCutoff $ \GetFilesOfInterest _file -> assert (null _file) $ do
        alwaysRerun
        Env{..} <- getServiceEnv
        filesOfInterest <- liftIO $ readVar envOfInterestVar
        pure (Just $ BS.fromString $ show filesOfInterest, ([], Just filesOfInterest))

getOpenVirtualResourcesRule :: Rules ()
getOpenVirtualResourcesRule = do
    defineEarlyCutoff $ \GetOpenVirtualResources _file -> assert (null _file) $ do
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

runScenario :: SS.Handle -> FilePath -> SS.ContextId -> LF.ValueRef -> IO (VirtualResource, Either SS.Error SS.ScenarioResult)
runScenario scenarioService file ctxId scenario = do
    res <- SS.runScenario scenarioService ctxId scenario
    let scenarioName = LF.qualObject scenario
    let vr = VRScenario file (LF.unExprValName scenarioName)
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

internalModules :: [String]
internalModules =
  [ "Data.String"
  , "GHC.CString"
  , "GHC.Integer.Type"
  , "GHC.Natural"
  , "GHC.Real"
  , "GHC.Types"
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
    generatePackageMapRule opts
    generatePackageRule
    generateRawPackageRule opts
    generatePackageDepsRule opts
    runScenariosRule
    getScenarioRootsRule
    getScenarioRootRule
    ofInterestRule
    encodeModuleRule
    createScenarioContextRule
    getFilesOfInterestRule
    getOpenVirtualResourcesRule

mainRule :: Options -> Rules ()
mainRule options = do
    IDE.mainRule
    damlRule options
