-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE OverloadedStrings #-}
module Development.IDE.Core.Rules.Daml
    ( module Development.IDE.Core.Rules
    , module Development.IDE.Core.Rules.Daml
    ) where

import Control.Concurrent.Extra
import Control.Exception
import Control.Monad.Except
import Control.Monad.Extra
import Control.Monad.Trans.Maybe
import DA.Daml.GHC.Compiler.Options
import Development.IDE.Types.Location as Base
import Data.Aeson hiding (Options)
import qualified Data.ByteString as BS
import qualified Data.ByteString.UTF8 as BS
import Data.Either.Extra
import Data.List
import qualified Data.Map.Strict as Map
import Data.Maybe
import qualified Data.NameMap as NM
import Data.Set (Set)
import qualified Data.Set as Set
import qualified Data.Text as T
import Data.Tuple.Extra
import Development.Shake hiding (Diagnostic, Env)
import "ghc-lib" GHC
import "ghc-lib-parser" Module (UnitId, stringToUnitId, unitIdString, UnitId(..), DefUnitId(..))
import Safe
import System.Directory.Extra (listFilesRecursive)
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

-- | Get an unvalidated DALF package.
-- This must only be used for debugging/testing.
getRawDalf :: NormalizedFilePath -> Action (Maybe LF.Package)
getRawDalf absFile = use GenerateRawPackage absFile

-- | Get a validated DALF package.
getDalf :: NormalizedFilePath -> Action (Maybe LF.Package)
getDalf file = use GeneratePackage file

getDalfModule :: NormalizedFilePath -> Action (Maybe LF.Module)
getDalfModule file = use GenerateDalf file

newtype GlobalPkgMap = GlobalPkgMap (Map.Map UnitId (LF.PackageId, LF.Package, BS.ByteString, FilePath))
instance IsIdeGlobal GlobalPkgMap

-- | A dependency on a compiled library.
data DalfDependency = DalfDependency
  { ddName         :: !T.Text
    -- ^ The name of the dependency.
  , ddDalfFile     :: !FilePath
    -- ^ The absolute path to the dalf file.
  }

getDalfDependencies :: NormalizedFilePath -> MaybeT Action [DalfDependency]
getDalfDependencies file = do
    unitIds <- transitivePkgDeps <$> useE GetDependencies file
    GlobalPkgMap pkgMap <- lift getIdeGlobalAction
    pure
        [ DalfDependency (T.pack $ unitIdString uid) fp
        | (uid, (_, _, _, fp)) <-
          Map.toList $
          Map.restrictKeys pkgMap (Set.fromList $ map (DefiniteUnitId . DefUnitId) unitIds)
        ]

runScenarios :: NormalizedFilePath -> Action (Maybe [(VirtualResource, Either SS.Error SS.ScenarioResult)])
runScenarios file = use RunScenarios file

-- | Get a list of the scenarios in a given file
getScenarioNames :: NormalizedFilePath -> Action (Maybe [VirtualResource])
getScenarioNames file = fmap f <$> use GenerateRawDalf file
    where f = map (VRScenario file . LF.unExprValName . LF.qualObject . fst) . scenariosInModule

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
        case convertModule lfVersion pkgMap0 file core of
            Left e -> return ([e], Nothing)
            Right v -> return ([], Just v)

-- Generates and type checks the DALF for a module.
generateDalfRule :: Rules ()
generateDalfRule =
    define $ \GenerateDalf file -> do
        lfVersion <- getDamlLfVersion
        pkg <- use_ GeneratePackageDeps file
        -- The file argument isn’t used in the rule, so we leave it empty to increase caching.
        pkgMap <- use_ GeneratePackageMap ""
        let pkgs = [(pId, pkg) | (pId, pkg, _bs, _fp) <- Map.elems pkgMap]
        let world = LF.initWorldSelf pkgs pkg
        unsimplifiedRawDalf <- use_ GenerateRawDalf file
        let rawDalf = LF.simplifyModule unsimplifiedRawDalf
        setPriority PriorityGenerateDalf
        pure $ toIdeResult $ do
            let liftError e = [ideErrorPretty file e]
            dalf <- mapLeft liftError $
                Serializability.inferModule world lfVersion rawDalf
            mapLeft liftError $ LF.checkModule world lfVersion dalf
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
            mapLeft (ideErrorPretty $ toNormalizedFilePath dalf) $
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

contextForFile :: NormalizedFilePath -> Action SS.Context
contextForFile file = do
    lfVersion <- getDamlLfVersion
    pkg <- use_ GeneratePackage file
    pkgMap <- use_ GeneratePackageMap ""
    encodedModules <-
        mapM (\m -> fmap (\(hash, bs) -> (hash, (LF.moduleName m, bs))) (encodeModule lfVersion m)) $
        NM.toList $ LF.packageModules pkg
    DamlEnv{..} <- getDamlServiceEnv
    pure SS.Context
        { ctxModules = Map.fromList encodedModules
        , ctxPackages = map (\(pId, _, p, _) -> (pId, p)) (Map.elems pkgMap)
        , ctxDamlLfVersion = lfVersion
        , ctxLightValidation = case envScenarioValidation of
              ScenarioValidationFull -> SS.LightValidation False
              ScenarioValidationLight -> SS.LightValidation True
        }

worldForFile :: NormalizedFilePath -> Action LF.World
worldForFile file = do
    pkg <- use_ GeneratePackage file
    pkgMap <- use_ GeneratePackageMap ""
    let pkgs = [ (pId, pkg) | (pId, pkg, _, _) <- Map.elems pkgMap ]
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
    case envScenarioValidation of
        ScenarioValidationLight -> use_ GenerateRawDalf file
        ScenarioValidationFull -> use_ GenerateDalf file

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
        let runScenarios file = do
                world <- worldForFile file
                mbVrs <- use RunScenarios file
                forM_ (fromMaybe [] mbVrs) $ \(vr, res) -> do
                    let doc = formatScenarioResult world res
                    when (vr `Set.member` openVRs) $
                        sendEvent $ vrChangedNotification vr doc
        -- We don’t always have a scenario service (e.g., damlc compile)
        -- so only run scenarios if we have one.
        let shouldRunScenarios = isJust envScenarioService
        _ <- parallel $
            map (void . getDalf) (Set.toList scenarioFiles) <>
            [runScenarios file | shouldRunScenarios, file <- Set.toList scenarioFiles]
        return ()
  where
      gc :: Set NormalizedFilePath -> Action ()
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

encodeModuleRule :: Rules ()
encodeModuleRule =
    define $ \EncodeModule file -> do
        lfVersion <- getDamlLfVersion
        fs <- transitiveModuleDeps <$> use_ GetDependencies file
        files <- discardInternalModules fs
        encodedDeps <- uses_ EncodeModule files
        m <- dalfForScenario file
        let (hash, bs) = SS.encodeModule lfVersion m
        return ([], Just (mconcat $ hash : map fst encodedDeps, bs))

scenariosInModule :: LF.Module -> [(LF.ValueRef, Maybe LF.SourceLoc)]
scenariosInModule m =
    [ (LF.Qualified LF.PRSelf (LF.moduleName m) (LF.dvalName val), LF.dvalLocation val)
    | val <- NM.toList (LF.moduleValues m), LF.getIsTest (LF.dvalIsTest val)]

getDamlLfVersion:: Action LF.Version
getDamlLfVersion = envDamlLfVersion <$> getDamlServiceEnv

discardInternalModules :: [NormalizedFilePath] -> Action [NormalizedFilePath]
discardInternalModules files = do
    mods <- uses_ GetParsedModule files
    pure $ map fileFromParsedModule $
        filter (not . modIsInternal . ms_mod . pm_mod_summary) mods

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
    getOpenVirtualResourcesRule

mainRule :: Options -> Rules ()
mainRule options = do
    IDE.mainRule
    damlRule options
