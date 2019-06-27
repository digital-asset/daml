-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE DuplicateRecordFields #-}

-- | A Shake implementation of the compiler service, built
--   using the "Shaker" abstraction layer for in-memory use.
--
module Development.IDE.Core.Rules(
    Shake.IdeState,
    RuleT.GetDependencies(..),
    RuleT.GetParsedModule(..),
    Dep.TransitiveDependencies(..),
    Priority(..),
    runAction, runActions, useE, usesE,
    toIdeResult, defineNoFile,
    mainRule,
    getGhcCore,
    getAtPoint,
    getDefinition,
    getDependencies,
    getParsedModule,
    fileFromParsedModule
    ) where

import Control.Monad.Except (ExceptT(ExceptT), forM, lift, runExceptT, join)
import Control.Monad.Trans.Maybe (MaybeT(MaybeT), runMaybeT)
import qualified Development.IDE.Core.Compile as Compile
import qualified Development.IDE.Types.Options as Compile
import qualified Development.IDE.Import.DependencyInformation as Dep
import qualified Development.IDE.Import.FindImports as FindImports
import Development.IDE.Core.FileStore (getFileContents, getFileExists)
import Development.IDE.Types.Diagnostics as Base
import qualified Development.IDE.Types.Location as Location
import Data.Bifunctor (second)
import Data.Either.Extra (lefts, eitherToMaybe, partitionEithers)
import Data.Maybe (mapMaybe)
import Data.Foldable (toList)
import qualified Data.Map.Strict as Map
import qualified Data.Set as Set
import qualified Data.Text as T
import Development.IDE.GHC.Error (srcSpanToLocation, srcSpanToFilename)
import Development.Shake (Action, Rules, liftIO)
import qualified Development.IDE.Core.RuleTypes as RuleT

import qualified GHC as GHC
import Development.IDE.GHC.Compat (hie_file_result, readHieFile)
import UniqSupply (mkSplitUniqSupply)
import NameCache (initNameCache)

import qualified Development.IDE.Spans.AtPoint as AtPoint
import Development.IDE.Core.Service (getIdeOptions, runAction, runActions)
import qualified Development.IDE.Core.Shake as Shake

-- | This is useful for rules to convert rules that can only produce errors or
-- a result into the more general IdeResult type that supports producing
-- warnings while also producing a result.
toIdeResult :: Either [FileDiagnostic] v -> Shake.IdeResult v
toIdeResult = either (, Nothing) (([],) . Just)

-- | useE is useful to implement functions that aren’t rules but need shortcircuiting
-- e.g. getDefinition.
useE :: Shake.IdeRule k v => k -> Location.NormalizedFilePath -> MaybeT Action v
useE k = MaybeT . Shake.use k

usesE :: Shake.IdeRule k v => k -> [Location.NormalizedFilePath] -> MaybeT Action [v]
usesE k = MaybeT . fmap sequence . Shake.uses k

defineNoFile :: Shake.IdeRule k v => (k -> Action v) -> Rules ()
defineNoFile f = Shake.define $ \k file -> do
    if file == "" then do res <- f k; return ([], Just res) else
        fail $ "Rule " ++ show k ++ " should always be called with the empty string for a file"


------------------------------------------------------------
-- Exposed API


-- | Generate the GHC Core for the supplied file and its dependencies.
getGhcCore :: Location.NormalizedFilePath -> Action (Maybe [GHC.CoreModule])
getGhcCore file = runMaybeT $ do
    files <- Dep.transitiveModuleDeps <$> useE RuleT.GetDependencies file
    pms   <- usesE RuleT.GetParsedModule $ files ++ [file]
    usesE RuleT.GenerateCore $ map fileFromParsedModule pms



-- | Get all transitive file dependencies of a given module.
-- Does not include the file itself.
getDependencies :: Location.NormalizedFilePath -> Action (Maybe [Location.NormalizedFilePath])
getDependencies file = fmap Dep.transitiveModuleDeps <$> Shake.use RuleT.GetDependencies file

-- | Try to get hover text for the name under point.
getAtPoint :: Location.NormalizedFilePath -> Location.Position -> Action (Maybe (Maybe Location.Range, [T.Text]))
getAtPoint file pos = fmap join $ runMaybeT $ do
  opts <- lift getIdeOptions
  files <- Dep.transitiveModuleDeps <$> useE RuleT.GetDependencies file
  tms   <- usesE RuleT.TypeCheck (file : files)
  spans <- useE RuleT.GetSpanInfo file
  return $ AtPoint.atPoint opts (map Compile.tmrModule tms) spans pos

-- | Goto Definition.
getDefinition :: Location.NormalizedFilePath -> Location.Position -> Action (Maybe Location.Location)
getDefinition file pos = fmap join $ runMaybeT $ do
    spans <- useE RuleT.GetSpanInfo file
    pkgState <- useE RuleT.GhcSession ""
    opts <- lift getIdeOptions
    let getHieFile x = Shake.use (RuleT.GetHieFile x) ""
    lift $ AtPoint.gotoDefinition getHieFile opts pkgState spans pos

-- | Parse the contents of a daml file.
getParsedModule :: Location.NormalizedFilePath -> Action (Maybe GHC.ParsedModule)
getParsedModule file = Shake.use RuleT.GetParsedModule file


------------------------------------------------------------
-- Rules
-- These typically go from key to value and are oracles.

-- TODO (MK) This should be independent of DAML or move out of hie-core.
-- | We build artefacts based on the following high-to-low priority order.
data Priority
    = PriorityTypeCheck
    | PriorityGenerateDalf
    | PriorityFilesOfInterest
  deriving (Eq, Ord, Show, Enum)


getParsedModuleRule :: Rules ()
getParsedModuleRule =
    Shake.define $ \RuleT.GetParsedModule file -> do
        (_, contents) <- getFileContents file
        packageState <- Shake.use_ RuleT.GhcSession ""
        opt <- getIdeOptions
        liftIO $ Compile.parseModule opt packageState (Location.fromNormalizedFilePath file) contents

getLocatedImportsRule :: Rules ()
getLocatedImportsRule =
    Shake.define $ \RuleT.GetLocatedImports file -> do
        pm <- Shake.use_ RuleT.GetParsedModule file
        let ms = GHC.pm_mod_summary pm
        let imports = GHC.ms_textual_imps ms
        packageState <- Shake.use_ RuleT.GhcSession ""
        dflags <- liftIO $ Compile.getGhcDynFlags pm packageState
        opt <- getIdeOptions
        xs <- forM imports $ \(mbPkgName, modName) ->
            (modName, ) <$> FindImports.locateModule dflags (Compile.optExtensions opt) getFileExists modName mbPkgName
        return (concat $ lefts $ map snd xs, Just $ map (second eitherToMaybe) xs)


-- | Given a target file path, construct the raw dependency results by following
-- imports recursively.
rawDependencyInformation :: Location.NormalizedFilePath -> ExceptT [FileDiagnostic] Action Dep.RawDependencyInformation
rawDependencyInformation f = go (Set.singleton f) Map.empty Map.empty
  where go fs !modGraph !pkgs =
          case Set.minView fs of
            Nothing -> pure (Dep.RawDependencyInformation modGraph pkgs)
            Just (f, fs) -> do
              importsOrErr <- lift $ Shake.use RuleT.GetLocatedImports f
              case importsOrErr of
                Nothing ->
                  let modGraph' = Map.insert f (Left Dep.ModuleParseError) modGraph
                  in go fs modGraph' pkgs
                Just imports -> do
                  packageState <- lift $ Shake.use_ RuleT.GhcSession ""
                  modOrPkgImports <- forM imports $ \imp -> do
                    case imp of
                      (_modName, Just (FindImports.PackageImport pkg)) -> do
                          pkgs <- ExceptT $ liftIO $ Compile.computePackageDeps packageState pkg
                          pure $ Right $ pkg:pkgs
                      (modName, Just (FindImports.FileImport absFile)) -> pure $ Left (modName, Just absFile)
                      (modName, Nothing) -> pure $ Left (modName, Nothing)
                  let (modImports, pkgImports) = partitionEithers modOrPkgImports
                  let newFiles = Set.fromList (mapMaybe snd modImports) Set.\\ Map.keysSet modGraph
                      modGraph' = Map.insert f (Right modImports) modGraph
                      pkgs' = Map.insert f (Set.fromList $ concat pkgImports) pkgs
                  go (fs `Set.union` newFiles) modGraph' pkgs'

getDependencyInformationRule :: Rules ()
getDependencyInformationRule =
    Shake.define $ \RuleT.GetDependencyInformation file -> fmap toIdeResult $ runExceptT $ do
       rawDepInfo <- rawDependencyInformation file
       pure $ Dep.processDependencyInformation rawDepInfo

reportImportCyclesRule :: Rules ()
reportImportCyclesRule =
    Shake.define $ \RuleT.ReportImportCycles file -> fmap (\errs -> if null errs then ([], Just ()) else (errs, Nothing)) $ do
        Dep.DependencyInformation{..} <- Shake.use_ RuleT.GetDependencyInformation file
        case Map.lookup file depErrorNodes of
            Nothing -> pure []
            Just errs -> do
                let cycles = mapMaybe (cycleErrorInFile file) (toList errs)
                -- Convert cycles of files into cycles of module names
                forM cycles $ \(imp, files) -> do
                    modNames <- mapM getModuleName files
                    pure $ toDiag imp modNames
    where cycleErrorInFile f (Dep.PartOfCycle imp fs)
            | f `elem` fs = Just (imp, fs)
          cycleErrorInFile _ _ = Nothing
          toDiag imp mods = (fp ,) $ Diagnostic
            { _range = (Location._range :: Location.Location -> Location.Range) loc
            , _severity = Just DsError
            , _source = Just "Import cycle detection"
            , _message = "Cyclic module dependency between " <> showCycle mods
            , _code = Nothing
            , _relatedInformation = Nothing
            }
            where loc = srcSpanToLocation (GHC.getLoc imp)
                  fp = Location.toNormalizedFilePath $ srcSpanToFilename (GHC.getLoc imp)
          getModuleName file = do
           pm <- Shake.use_ RuleT.GetParsedModule file
           pure (GHC.moduleNameString . GHC.moduleName . GHC.ms_mod $ GHC.pm_mod_summary pm)
          showCycle mods  = T.intercalate ", " (map T.pack mods)

-- returns all transitive dependencies in topological order.
-- NOTE: result does not include the argument file.
getDependenciesRule :: Rules ()
getDependenciesRule =
    Shake.define $ \RuleT.GetDependencies file -> do
        depInfo@Dep.DependencyInformation{..} <- Shake.use_ RuleT.GetDependencyInformation file
        let allFiles = Map.keys depModuleDeps <> Map.keys depErrorNodes
        _ <- Shake.uses_ RuleT.ReportImportCycles allFiles
        return ([], Dep.transitiveDeps depInfo file)

-- Source SpanInfo is used by AtPoint and Goto Definition.
getSpanInfoRule :: Rules ()
getSpanInfoRule =
    Shake.define $ \RuleT.GetSpanInfo file -> do
        pm <- Shake.use_ RuleT.GetParsedModule file
        tc <- Shake.use_ RuleT.TypeCheck file
        imports <- Shake.use_ RuleT.GetLocatedImports file
        packageState <- Shake.use_ RuleT.GhcSession ""
        x <- liftIO $ Compile.getSrcSpanInfos pm packageState (fileImports imports) tc
        return ([], Just x)

-- Typechecks a module.
typeCheckRule :: Rules ()
typeCheckRule =
    Shake.define $ \RuleT.TypeCheck file -> do
        pm <- Shake.use_ RuleT.GetParsedModule file
        deps <- Shake.use_ RuleT.GetDependencies file
        tms <- Shake.uses_ RuleT.TypeCheck (Dep.transitiveModuleDeps deps)
        Shake.setPriority PriorityTypeCheck
        packageState <- Shake.use_ RuleT.GhcSession ""
        opt <- getIdeOptions
        liftIO $ Compile.typecheckModule opt packageState tms pm


generateCoreRule :: Rules ()
generateCoreRule =
    Shake.define $ \RuleT.GenerateCore file -> do
        deps <- Shake.use_ RuleT.GetDependencies file
        (tm:tms) <- Shake.uses_ RuleT.TypeCheck (file:Dep.transitiveModuleDeps deps)
        let pm = GHC.tm_parsed_module . Compile.tmrModule $ tm
        Shake.setPriority PriorityGenerateDalf
        packageState <- Shake.use_ RuleT.GhcSession ""
        liftIO $ Compile.compileModule pm packageState tms tm

loadGhcSession :: Rules ()
loadGhcSession =
    defineNoFile $ \RuleT.GhcSession -> do
        opts <- getIdeOptions
        Compile.optGhcSession opts


getHieFileRule :: Rules ()
getHieFileRule =
    defineNoFile $ \(RuleT.GetHieFile f) -> do
        u <- liftIO $ mkSplitUniqSupply 'a'
        let nameCache = initNameCache u []
        liftIO $ fmap (hie_file_result . fst) $ readHieFile nameCache f

-- | A rule that wires per-file rules together
mainRule :: Rules ()
mainRule = do
    getParsedModuleRule
    getLocatedImportsRule
    getDependencyInformationRule
    reportImportCyclesRule
    getDependenciesRule
    typeCheckRule
    getSpanInfoRule
    generateCoreRule
    loadGhcSession
    getHieFileRule

------------------------------------------------------------

fileFromParsedModule :: GHC.ParsedModule -> Location.NormalizedFilePath
fileFromParsedModule = Location.toNormalizedFilePath . GHC.ms_hspp_file . GHC.pm_mod_summary

fileImports ::
     [(GHC.Located GHC.ModuleName, Maybe FindImports.Import)]
  -> [(GHC.Located GHC.ModuleName, Maybe Location.NormalizedFilePath)]
fileImports = mapMaybe $ \case
    (modName, Nothing) -> Just (modName, Nothing)
    (modName, Just (FindImports.FileImport absFile)) -> Just (modName, Just absFile)
    (_modName, Just (FindImports.PackageImport _pkg)) -> Nothing
