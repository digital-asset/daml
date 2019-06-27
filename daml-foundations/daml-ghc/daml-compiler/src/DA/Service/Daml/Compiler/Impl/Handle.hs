-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DA.Service.Daml.Compiler.Impl.Handle
  (-- * compilation handle providers
    Rules.Daml.IdeState
  , getIdeState
  , withIdeState
  , IDE.State.setFilesOfInterest
  , IDE.State.modifyFilesOfInterest
  , IDE.State.setOpenVirtualResources
  , IDE.State.modifyOpenVirtualResources
  , getAssociatedVirtualResources
  , compileFile
  , toIdeLogger
  , parseFile
  , UseDalf(..)
  , buildDar
  , Rules.Daml.getDalfDependencies

  , Rules.Daml.DalfDependency(..)

  , Rules.Daml.virtualResourceToUri
  , Rules.Daml.uriToVirtualResource

  -- * Compilation options (re-exported)
  , COpts.defaultOptionsIO
  , COpts.mkOptions
  , COpts.Options(..)
  , COpts.getBaseDir
  ) where

-- HS/DAML to LF compiler (daml-ghc)
import DA.Daml.GHC.Compiler.Convert (sourceLocToRange)
import qualified DA.Daml.GHC.Compiler.Options as COpts
import Development.IDE.Core.Service.Daml (VirtualResource(VRScenario))
import qualified DA.Service.Daml.Compiler.Impl.Scenario as Scenario
import "ghc-lib" GHC (ParsedModule)

-- DAML compiler and infrastructure
import qualified DA.Daml.LF.Ast                             as LF
import           DA.Daml.LF.Proto3.Archive                  (encodeArchiveLazy)
import qualified DA.Service.Logger                          as Logger
import qualified DA.Service.Daml.Compiler.Impl.Dar          as Dar

import           Control.Monad.Trans.Except              as Ex
import Control.Monad.Trans.Maybe (MaybeT(MaybeT), runMaybeT)
import           Control.Monad.Except              as Ex
import           Control.Monad.IO.Class                     (liftIO)
import qualified Data.ByteString                            as BS
import qualified Data.ByteString.Lazy                       as BSL
import qualified Data.Set                                   as S
import qualified Data.Text                                  as T
import Data.Maybe (fromMaybe)

import qualified Development.IDE.Types.Logger as IdeLogger
import qualified Development.IDE.State.API as IDE.State
import qualified Development.IDE.Core.Rules.Daml as Rules.Daml
import Development.IDE.Core.RuleTypes.Daml (GeneratePackage(GeneratePackage))
import qualified Development.IDE.Core.Shake as Shake
import           Development.IDE.Types.Diagnostics as Base
import qualified Language.Haskell.LSP.Messages as LSP
import Development.IDE.Types.Location as Base
import           System.FilePath (takeDirectory)

------------------------------------------------------------------------
-- Types for dependency information on DARs
------------------------------------------------------------------------
--

getIdeState
    :: COpts.Options
    -> Maybe Scenario.Handle
    -> Logger.Handle IO
    -> (LSP.FromServerMessage -> IO ())
    -> IDE.State.VFSHandle
    -> IO Rules.Daml.IdeState
getIdeState compilerOpts mbScenarioService loggerH eventHandler vfs = do
    -- Load the packages from the package database for the scenario service. We swallow errors here
    -- but shake will report them when typechecking anything.
    (_diags, pkgMap) <- Rules.Daml.generatePackageMap (COpts.optPackageDbs compilerOpts)
    let rule = do
            Rules.Daml.mainRule compilerOpts
            Shake.addIdeGlobal $ Rules.Daml.GlobalPkgMap pkgMap
    IDE.State.initialise rule eventHandler (toIdeLogger loggerH) compilerOpts vfs mbScenarioService

-- Wrapper for the common case where the scenario service will be started automatically (if enabled)
-- and we use the builtin VFSHandle.
withIdeState
    :: COpts.Options
    -> Logger.Handle IO
    -> (LSP.FromServerMessage -> IO ())
    -> (Rules.Daml.IdeState -> IO a)
    -> IO a
withIdeState compilerOpts loggerH eventHandler f =
    Scenario.withScenarioService' (COpts.optScenarioService compilerOpts) loggerH $ \mbScenarioService -> do
        vfs <- IDE.State.makeVFSHandle
        ideState <- getIdeState compilerOpts mbScenarioService loggerH eventHandler vfs
        f ideState

-- | Adapter to the IDE logger module.
toIdeLogger :: Logger.Handle IO -> IdeLogger.Logger
toIdeLogger h = IdeLogger.Logger {
       logSeriousError = Logger.logError h
     , logInfo = Logger.logInfo h
     , logDebug = Logger.logDebug h
     , logWarning = Logger.logWarning h
     }

------------------------------------------------------------------------------

getAssociatedVirtualResources
  :: Rules.Daml.IdeState
  -> NormalizedFilePath
  -> IO [(Base.Range, T.Text, VirtualResource)]
getAssociatedVirtualResources service filePath = do
    mod0 <- Rules.Daml.runAction service (Rules.Daml.getDalfModule filePath)
    case mod0 of
        Nothing ->
            -- This can happen if there is a parse or a type error.
            -- The diagnostics for that will be reported somewhere else.
            return []
        Just mod0 -> pure
            [ (sourceLocToRange loc, "Scenario: " <> name, vr)
            | (valRef, Just loc) <- Rules.Daml.scenariosInModule mod0
            , let name = LF.unExprValName (LF.qualObject valRef)
            , let vr = VRScenario filePath name
            ]


-- | Compile the supplied file using the Compiler Service into a DAML LF Package.
-- TODO options and warnings
compileFile
    :: Rules.Daml.IdeState
    -- -> Options
    -- -> Bool -- ^ collect and display warnings
    -> NormalizedFilePath
    -> ExceptT [FileDiagnostic] IO LF.Package
compileFile service fp = do
    -- We need to mark the file we are compiling as a file of interest.
    -- Otherwise all diagnostics produced during compilation will be garbage
    -- collected afterwards.
    liftIO $ IDE.State.setFilesOfInterest service (S.singleton fp)
    liftIO $ IdeLogger.logDebug (IDE.State.ideLogger service) $ "Compiling: " <> T.pack (fromNormalizedFilePath fp)
    actionToExceptT service (Rules.Daml.getDalf fp)

-- | Parse the supplied file to a ghc ParsedModule.
parseFile
    :: Rules.Daml.IdeState
    -> NormalizedFilePath
    -> ExceptT [FileDiagnostic] IO ParsedModule
parseFile service fp = do
    liftIO $ IDE.State.setFilesOfInterest service (S.singleton fp)
    liftIO $ IdeLogger.logDebug (IDE.State.ideLogger service) $ "Parsing: " <> T.pack (fromNormalizedFilePath fp)
    actionToExceptT service (Rules.Daml.getParsedModule fp)

newtype UseDalf = UseDalf{unUseDalf :: Bool}

buildDar ::
     Rules.Daml.IdeState
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
    IdeLogger.logDebug (IDE.State.ideLogger service) $
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
    else actionToExceptT service $ runMaybeT $ do
      pkg <- Rules.Daml.useE GeneratePackage file
      let pkgModuleNames = S.fromList $ map T.unpack $ LF.packageModuleNames pkg
      let missingExposed = S.fromList (fromMaybe [] mbExposedModules) S.\\ pkgModuleNames
      unless (S.null missingExposed) $ do
          liftIO $ IdeLogger.logSeriousError (IDE.State.ideLogger service) $
              "The following modules are declared in exposed-modules but are not part of the DALF: " <>
              T.pack (show $ S.toList missingExposed)
          fail ""
      let dalf = encodeArchiveLazy pkg
      -- get all dalf dependencies.
      deps <- Rules.Daml.getDalfDependencies file
      dalfDependencies<- forM deps $ \(Rules.Daml.DalfDependency depName fp) -> do
        pkgDalf <- liftIO $ BS.readFile fp
        return (depName, pkgDalf)
      -- get all file dependencies
      fileDependencies <- MaybeT $ Rules.Daml.getDependencies file
      liftIO $
        Dar.buildDar
          dalf
          (toNormalizedFilePath $ takeDirectory file')
          dalfDependencies
          (file:fileDependencies)
          (buildDataFiles pkg)
          pkgName
          sdkVersion

-- | Run an action with the given state and lift the result into an ExceptT
-- using the diagnostics from `getDiagnostics` in the `Nothing` case.
actionToExceptT :: IDE.State.IdeState -> IDE.State.Action (Maybe a) -> ExceptT [FileDiagnostic] IO a
actionToExceptT service act = do
    mbA <- lift (Rules.Daml.runAction service act)
    case mbA of
        Nothing -> do
            diag <- liftIO $ IDE.State.getDiagnostics service
            throwE diag
        Just a -> pure a
