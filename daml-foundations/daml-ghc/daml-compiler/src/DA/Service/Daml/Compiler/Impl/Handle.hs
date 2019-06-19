-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DA.Service.Daml.Compiler.Impl.Handle
  (-- * compilation handle providers
    IdeState
  , getIdeState
  , withIdeState
  , CompilerService.setFilesOfInterest
  , CompilerService.modifyFilesOfInterest
  , CompilerService.setOpenVirtualResources
  , CompilerService.modifyOpenVirtualResources
  , getAssociatedVirtualResources
  , compileFile
  , toIdeLogger
  , parseFile
  , UseDalf(..)
  , buildDar
  , getDalfDependencies

  , DalfDependency(..)

  , CompilerService.virtualResourceToUri
  , CompilerService.uriToVirtualResource

  -- * Compilation options (re-exported)
  , defaultOptionsIO
  , mkOptions
  , Options(..)
  , getBaseDir
  ) where

-- HS/DAML to LF compiler (daml-ghc)
import DA.Daml.GHC.Compiler.Convert (sourceLocToRange)
import DA.Daml.GHC.Compiler.Options
import qualified DA.Service.Daml.Compiler.Impl.Scenario as Scenario
import "ghc-lib-parser" Module (unitIdString, DefUnitId(..), UnitId(..))
import "ghc-lib" GHC (ParsedModule)

-- DAML compiler and infrastructure
import qualified DA.Daml.LF.Ast                             as LF
import           DA.Daml.LF.Proto3.Archive                  (encodeArchiveLazy)
import qualified DA.Service.Logger                          as Logger
import qualified DA.Service.Daml.Compiler.Impl.Dar          as Dar

import           Control.Monad.Trans.Except              as Ex
import           Control.Monad.Except              as Ex
import           Control.Monad.IO.Class                     (liftIO)
import qualified Data.ByteString                            as BS
import qualified Data.ByteString.Lazy                       as BSL
import qualified Data.Map.Strict                            as Map
import qualified Data.Set                                   as S
import qualified Data.Text                                  as T
import Data.Maybe

import qualified Development.IDE.Logger as IdeLogger
import           Development.IDE.State.API               (IdeState)
import qualified Development.IDE.State.API               as CompilerService
import Development.IDE.State.FileStore
import qualified Development.IDE.State.Rules.Daml as CompilerService
import qualified Development.IDE.State.Shake as Shake
import           Development.IDE.Types.Diagnostics as Base
import           Development.IDE.Types.LSP
import qualified Language.Haskell.LSP.Messages as LSP
import           System.FilePath

------------------------------------------------------------------------
-- Types for dependency information on DARs
------------------------------------------------------------------------
--

-- | A dependency on a compiled library.
data DalfDependency = DalfDependency
  { ddName         :: !T.Text
    -- ^ The name of the dependency.
  , ddDalfFile     :: !FilePath
    -- ^ The absolute path to the dalf file.
  }

newtype GlobalPkgMap = GlobalPkgMap (Map.Map UnitId (LF.PackageId, LF.Package, BS.ByteString, FilePath))
instance Shake.IsIdeGlobal GlobalPkgMap

getIdeState
    :: Options
    -> Maybe Scenario.Handle
    -> Logger.Handle IO
    -> (LSP.FromServerMessage -> IO ())
    -> VFSHandle
    -> IO IdeState
getIdeState compilerOpts mbScenarioService loggerH eventHandler vfs = do
    -- Load the packages from the package database for the scenario service. We swallow errors here
    -- but shake will report them when typechecking anything.
    (_diags, pkgMap) <- CompilerService.generatePackageMap (optPackageDbs compilerOpts)
    let rule = do
            CompilerService.mainRule compilerOpts
            Shake.addIdeGlobal $ GlobalPkgMap pkgMap
    CompilerService.initialise rule eventHandler (toIdeLogger loggerH) compilerOpts vfs mbScenarioService

-- Wrapper for the common case where the scenario service will be started automatically (if enabled)
-- and we use the builtin VFSHandle.
withIdeState
    :: Options
    -> Logger.Handle IO
    -> (LSP.FromServerMessage -> IO ())
    -> (IdeState -> IO a)
    -> IO a
withIdeState compilerOpts loggerH eventHandler f =
    Scenario.withScenarioService' (optScenarioService compilerOpts) loggerH $ \mbScenarioService -> do
        vfs <- makeVFSHandle
        ideState <- getIdeState compilerOpts mbScenarioService loggerH eventHandler vfs
        f ideState

-- | Adapter to the IDE logger module.
toIdeLogger :: Logger.Handle IO -> IdeLogger.Handle
toIdeLogger h = IdeLogger.Handle {
       logSeriousError = Logger.logError h
     , logInfo = Logger.logInfo h
     , logDebug = Logger.logDebug h
     , logWarning = Logger.logWarning h
     }

------------------------------------------------------------------------------

getAssociatedVirtualResources
  :: IdeState
  -> NormalizedFilePath
  -> IO [(Base.Range, T.Text, VirtualResource)]
getAssociatedVirtualResources service filePath = do
    mod0 <- CompilerService.runAction service (CompilerService.getDalfModule filePath)
    case mod0 of
        Nothing ->
            -- This can happen if there is a parse or a type error.
            -- The diagnostics for that will be reported somewhere else.
            return []
        Just mod0 -> pure
            [ (sourceLocToRange loc, "Scenario: " <> name, vr)
            | (valRef, Just loc) <- CompilerService.scenariosInModule mod0
            , let name = LF.unExprValName (LF.qualObject valRef)
            , let vr = VRScenario filePath name
            ]


-- | Compile the supplied file using the Compiler Service into a DAML LF Package.
-- TODO options and warnings
compileFile
    :: IdeState
    -- -> Options
    -- -> Bool -- ^ collect and display warnings
    -> NormalizedFilePath
    -> ExceptT [FileDiagnostic] IO LF.Package
compileFile service fp = do
    -- We need to mark the file we are compiling as a file of interest.
    -- Otherwise all diagnostics produced during compilation will be garbage
    -- collected afterwards.
    liftIO $ CompilerService.setFilesOfInterest service (S.singleton fp)
    liftIO $ CompilerService.logDebug service $ "Compiling: " <> T.pack (fromNormalizedFilePath fp)
    res <- liftIO $ CompilerService.runAction service (CompilerService.getDalf fp)
    case res of
        Nothing -> do
            diag <- liftIO $ CompilerService.getDiagnostics service
            throwE diag
        Just v -> return v

-- | Parse the supplied file to a ghc ParsedModule.
parseFile
    :: IdeState
    -> NormalizedFilePath
    -> ExceptT [FileDiagnostic] IO ParsedModule
parseFile service fp = do
    liftIO $ CompilerService.setFilesOfInterest service (S.singleton fp)
    liftIO $ CompilerService.logDebug service $ "Parsing: " <> T.pack (fromNormalizedFilePath fp)
    res <- liftIO $ CompilerService.runAction service (CompilerService.getParsedModule fp)
    case res of
        Nothing -> do
            diag <- liftIO $ CompilerService.getDiagnostics service
            throwE diag
        Just pm -> return pm

newtype UseDalf = UseDalf{unUseDalf :: Bool}

buildDar ::
     IdeState
  -> NormalizedFilePath
  -> Maybe [String]
  -> String
  -> String
  -> (LF.Package -> [(String, BS.ByteString)])
  -- We allow datafiles to depend on the package being produces to
  -- allow inference of things like exposedModules.
  -- Once we kill the old "package" command we could instead just
  -- pass "PackageConfigFields" to this function and construct the data
  -- files in here.
  -> UseDalf
  -> ExceptT [FileDiagnostic] IO BS.ByteString
buildDar service file mbExposedModules pkgName sdkVersion buildDataFiles dalfInput = do
  let file' = fromNormalizedFilePath file
  liftIO $
    CompilerService.logDebug service $
    "Creating dar: " <> T.pack file'
  if unUseDalf dalfInput
    then liftIO $ do
      bytes <- BSL.readFile file'
      Dar.buildDar
        bytes
        (toNormalizedFilePath $ takeDirectory file')
        []
        []
        []
        pkgName
        sdkVersion
    else do
      pkg <- compileFile service file
      let pkgModuleNames = S.fromList $ map T.unpack $ LF.packageModuleNames pkg
      let missingExposed = S.fromList (fromMaybe [] mbExposedModules) S.\\ pkgModuleNames
      unless (S.null missingExposed) $ do
          liftIO $ CompilerService.logSeriousError service $
              "The following modules are declared in exposed-modules but are not part of the DALF: " <>
              T.pack (show $ S.toList missingExposed)
          throwE []
      let dalf = encodeArchiveLazy pkg
      -- get all dalf dependencies.
      deps <- getDalfDependencies service file
      dalfDependencies<- forM deps $ \(DalfDependency depName fp) -> do
        pkgDalf <- liftIO $ BS.readFile fp
        return (depName, pkgDalf)
      -- get all file dependencies
      mbFileDependencies <-
        liftIO $
        CompilerService.runAction service (CompilerService.getDependencies file)
      case mbFileDependencies of
        Nothing -> do
          diag <- liftIO $ CompilerService.getDiagnostics service
          throwE diag
        Just fileDependencies -> do
          liftIO $
            Dar.buildDar
              dalf
              (toNormalizedFilePath $ takeDirectory file')
              dalfDependencies
              (file:fileDependencies)
              (buildDataFiles pkg)
              pkgName
              sdkVersion

-- | Get the transitive package dependencies on other dalfs.
getDalfDependencies ::
       IdeState -> NormalizedFilePath -> ExceptT [FileDiagnostic] IO [DalfDependency]
getDalfDependencies service afp = do
    res <-
        liftIO $
        CompilerService.runAction
            service
            (CompilerService.getDalfDependencies afp)
    GlobalPkgMap pkgMap <- liftIO $ Shake.getIdeGlobalState service
    case res of
        Nothing -> do
            diag <- liftIO $ CompilerService.getDiagnostics service
            throwE diag
        Just uids ->
            return
                [ DalfDependency (T.pack $ unitIdString uid) fp
                | (uid, (_, _, _, fp)) <-
                      Map.toList $
                      Map.restrictKeys
                          pkgMap
                          (S.fromList $ map (DefiniteUnitId . DefUnitId) uids)
                ]
