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
    IdeState, GetDependencies(..), GetParsedModule(..), TransitiveDependencies(..),
    Priority(..),
    priorityTypeCheck,
    priorityGenerateCore,
    priorityFilesOfInterest,
    runAction, useE, useNoFileE, usesE,
    toIdeResult, defineNoFile,
    mainRule,
    getGhcCore,
    getAtPoint,
    getDefinition,
    getDependencies,
    getParsedModule,
    fileFromParsedModule
    ) where

import           Control.Monad.Except
import Control.Monad.Trans.Maybe
import qualified Development.IDE.Core.Compile             as Compile
import qualified Development.IDE.Types.Options as Compile
import qualified Development.IDE.Spans.Calculate as Compile
import Development.IDE.Import.DependencyInformation
import Development.IDE.Import.FindImports
import           Development.IDE.Core.FileStore
import           Development.IDE.Types.Diagnostics as Base
import Development.IDE.Types.Location
import Data.Bifunctor
import Data.Either.Extra
import Data.Maybe
import           Data.Foldable
import qualified Data.Map.Strict                          as Map
import qualified Data.Set                                 as Set
import qualified Data.Text                                as T
import           Development.IDE.GHC.Error
import           Development.Shake                        hiding (Diagnostic, Env, newCache)
import Development.IDE.Core.RuleTypes

import           GHC
import Development.IDE.GHC.Compat
import           UniqSupply
import NameCache
import HscTypes

import qualified Development.IDE.Spans.AtPoint as AtPoint
import Development.IDE.Core.Service
import Development.IDE.Core.Shake

-- | This is useful for rules to convert rules that can only produce errors or
-- a result into the more general IdeResult type that supports producing
-- warnings while also producing a result.
toIdeResult :: Either [FileDiagnostic] v -> IdeResult v
toIdeResult = either (, Nothing) (([],) . Just)

-- | useE is useful to implement functions that aren’t rules but need shortcircuiting
-- e.g. getDefinition.
useE :: IdeRule k v => k -> NormalizedFilePath -> MaybeT Action v
useE k = MaybeT . use k

useNoFileE :: IdeRule k v => k -> MaybeT Action v
useNoFileE k = useE k ""

usesE :: IdeRule k v => k -> [NormalizedFilePath] -> MaybeT Action [v]
usesE k = MaybeT . fmap sequence . uses k

defineNoFile :: IdeRule k v => (k -> Action v) -> Rules ()
defineNoFile f = define $ \k file -> do
    if file == "" then do res <- f k; return ([], Just res) else
        fail $ "Rule " ++ show k ++ " should always be called with the empty string for a file"


------------------------------------------------------------
-- Exposed API


-- | Generate the GHC Core for the supplied file and its dependencies.
getGhcCore :: NormalizedFilePath -> Action (Maybe [CoreModule])
getGhcCore file = runMaybeT $ do
    files <- transitiveModuleDeps <$> useE GetDependencies file
    pms   <- usesE GetParsedModule $ files ++ [file]
    usesE GenerateCore $ map fileFromParsedModule pms



-- | Get all transitive file dependencies of a given module.
-- Does not include the file itself.
getDependencies :: NormalizedFilePath -> Action (Maybe [NormalizedFilePath])
getDependencies file = fmap transitiveModuleDeps <$> use GetDependencies file

-- | Try to get hover text for the name under point.
getAtPoint :: NormalizedFilePath -> Position -> Action (Maybe (Maybe Range, [T.Text]))
getAtPoint file pos = fmap join $ runMaybeT $ do
  opts <- lift getIdeOptions
  files <- transitiveModuleDeps <$> useE GetDependencies file
  tms   <- usesE TypeCheck (file : files)
  spans <- useE GetSpanInfo file
  return $ AtPoint.atPoint opts (map Compile.tmrModule tms) spans pos

-- | Goto Definition.
getDefinition :: NormalizedFilePath -> Position -> Action (Maybe Location)
getDefinition file pos = fmap join $ runMaybeT $ do
    spans <- useE GetSpanInfo file
    pkgState <- useNoFileE GhcSession
    opts <- lift getIdeOptions
    let getHieFile x = useNoFile (GetHieFile x)
    lift $ AtPoint.gotoDefinition getHieFile opts pkgState spans pos

-- | Parse the contents of a daml file.
getParsedModule :: NormalizedFilePath -> Action (Maybe ParsedModule)
getParsedModule file = use GetParsedModule file


------------------------------------------------------------
-- Rules
-- These typically go from key to value and are oracles.

priorityTypeCheck :: Priority
priorityTypeCheck = Priority 0

priorityGenerateCore :: Priority
priorityGenerateCore = Priority (-1)

priorityFilesOfInterest :: Priority
priorityFilesOfInterest = Priority (-2)

getParsedModuleRule :: Rules ()
getParsedModuleRule =
    define $ \GetParsedModule file -> do
        (_, contents) <- getFileContents file
        packageState <- useNoFile_ GhcSession
        opt <- getIdeOptions
        liftIO $ Compile.parseModule opt packageState (fromNormalizedFilePath file) contents

getLocatedImportsRule :: Rules ()
getLocatedImportsRule =
    define $ \GetLocatedImports file -> do
        pm <- use_ GetParsedModule file
        let ms = pm_mod_summary pm
        let imports = ms_textual_imps ms
        env <- useNoFile_ GhcSession
        let dflags = Compile.addRelativeImport pm $ hsc_dflags env
        opt <- getIdeOptions
        xs <- forM imports $ \(mbPkgName, modName) ->
            (modName, ) <$> locateModule dflags (Compile.optExtensions opt) getFileExists modName mbPkgName
        return (concat $ lefts $ map snd xs, Just $ map (second eitherToMaybe) xs)


-- | Given a target file path, construct the raw dependency results by following
-- imports recursively.
rawDependencyInformation :: NormalizedFilePath -> ExceptT [FileDiagnostic] Action RawDependencyInformation
rawDependencyInformation f = go (Set.singleton f) Map.empty Map.empty
  where go fs !modGraph !pkgs =
          case Set.minView fs of
            Nothing -> pure (RawDependencyInformation modGraph pkgs)
            Just (f, fs) -> do
              importsOrErr <- lift $ use GetLocatedImports f
              case importsOrErr of
                Nothing ->
                  let modGraph' = Map.insert f (Left ModuleParseError) modGraph
                  in go fs modGraph' pkgs
                Just imports -> do
                  packageState <- lift $ useNoFile_ GhcSession
                  modOrPkgImports <- forM imports $ \imp -> do
                    case imp of
                      (_modName, Just (PackageImport pkg)) -> do
                          pkgs <- ExceptT $ liftIO $ Compile.computePackageDeps packageState pkg
                          pure $ Right $ pkg:pkgs
                      (modName, Just (FileImport absFile)) -> pure $ Left (modName, Just absFile)
                      (modName, Nothing) -> pure $ Left (modName, Nothing)
                  let (modImports, pkgImports) = partitionEithers modOrPkgImports
                  let newFiles = Set.fromList (mapMaybe snd modImports) Set.\\ Map.keysSet modGraph
                      modGraph' = Map.insert f (Right modImports) modGraph
                      pkgs' = Map.insert f (Set.fromList $ concat pkgImports) pkgs
                  go (fs `Set.union` newFiles) modGraph' pkgs'

getDependencyInformationRule :: Rules ()
getDependencyInformationRule =
    define $ \GetDependencyInformation file -> fmap toIdeResult $ runExceptT $ do
       rawDepInfo <- rawDependencyInformation file
       pure $ processDependencyInformation rawDepInfo

reportImportCyclesRule :: Rules ()
reportImportCyclesRule =
    define $ \ReportImportCycles file -> fmap (\errs -> if null errs then ([], Just ()) else (errs, Nothing)) $ do
        DependencyInformation{..} <- use_ GetDependencyInformation file
        case Map.lookup file depErrorNodes of
            Nothing -> pure []
            Just errs -> do
                let cycles = mapMaybe (cycleErrorInFile file) (toList errs)
                -- Convert cycles of files into cycles of module names
                forM cycles $ \(imp, files) -> do
                    modNames <- mapM getModuleName files
                    pure $ toDiag imp modNames
    where cycleErrorInFile f (PartOfCycle imp fs)
            | f `elem` fs = Just (imp, fs)
          cycleErrorInFile _ _ = Nothing
          toDiag imp mods = (fp ,) $ Diagnostic
            { _range = (_range :: Location -> Range) loc
            , _severity = Just DsError
            , _source = Just "Import cycle detection"
            , _message = "Cyclic module dependency between " <> showCycle mods
            , _code = Nothing
            , _relatedInformation = Nothing
            }
            where loc = srcSpanToLocation (getLoc imp)
                  fp = toNormalizedFilePath $ srcSpanToFilename (getLoc imp)
          getModuleName file = do
           pm <- use_ GetParsedModule file
           pure (moduleNameString . moduleName . ms_mod $ pm_mod_summary pm)
          showCycle mods  = T.intercalate ", " (map T.pack mods)

-- returns all transitive dependencies in topological order.
-- NOTE: result does not include the argument file.
getDependenciesRule :: Rules ()
getDependenciesRule =
    define $ \GetDependencies file -> do
        depInfo@DependencyInformation{..} <- use_ GetDependencyInformation file
        let allFiles = Map.keys depModuleDeps <> Map.keys depErrorNodes
        _ <- uses_ ReportImportCycles allFiles
        return ([], transitiveDeps depInfo file)

-- Source SpanInfo is used by AtPoint and Goto Definition.
getSpanInfoRule :: Rules ()
getSpanInfoRule =
    define $ \GetSpanInfo file -> do
        tc <- use_ TypeCheck file
        imports <- use_ GetLocatedImports file
        packageState <- useNoFile_ GhcSession
        x <- liftIO $ Compile.getSrcSpanInfos packageState (fileImports imports) tc
        return ([], Just x)

-- Typechecks a module.
typeCheckRule :: Rules ()
typeCheckRule =
    define $ \TypeCheck file -> do
        pm <- use_ GetParsedModule file
        deps <- use_ GetDependencies file
        tms <- uses_ TypeCheck (transitiveModuleDeps deps)
        setPriority priorityTypeCheck
        packageState <- useNoFile_ GhcSession
        opt <- getIdeOptions
        liftIO $ Compile.typecheckModule opt packageState tms pm


generateCoreRule :: Rules ()
generateCoreRule =
    define $ \GenerateCore file -> do
        deps <- use_ GetDependencies file
        (tm:tms) <- uses_ TypeCheck (file:transitiveModuleDeps deps)
        setPriority priorityGenerateCore
        packageState <- useNoFile_ GhcSession
        liftIO $ Compile.compileModule packageState tms tm

loadGhcSession :: Rules ()
loadGhcSession =
    defineNoFile $ \GhcSession -> do
        opts <- getIdeOptions
        Compile.optGhcSession opts


getHieFileRule :: Rules ()
getHieFileRule =
    defineNoFile $ \(GetHieFile f) -> do
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

fileFromParsedModule :: ParsedModule -> NormalizedFilePath
fileFromParsedModule = toNormalizedFilePath . ms_hspp_file . pm_mod_summary

fileImports ::
     [(Located ModuleName, Maybe Import)]
  -> [(Located ModuleName, Maybe NormalizedFilePath)]
fileImports = mapMaybe $ \case
    (modName, Nothing) -> Just (modName, Nothing)
    (modName, Just (FileImport absFile)) -> Just (modName, Just absFile)
    (_modName, Just (PackageImport _pkg)) -> Nothing
