-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DA.Service.Daml.Compiler.Impl.Handle
  (-- * compilation handle providers
    IdeState
  , getIdeState
  , withIdeState
  , setFilesOfInterest
  , modifyFilesOfInterest
  , setOpenVirtualResources
  , modifyOpenVirtualResources
  , getAssociatedVirtualResources
  , toIdeLogger
  , UseDalf(..)
  , buildDar
  , getDalfDependencies

  , DalfDependency(..)

  , virtualResourceToUri
  , uriToVirtualResource

  -- * Compilation options (re-exported)
  , defaultOptionsIO
  , mkOptions
  , Options(..)
  , getBaseDir
  ) where

-- HS/DAML to LF compiler (daml-ghc)
import DA.Daml.GHC.Compiler.Convert (sourceLocToRange)
import DA.Daml.GHC.Compiler.Options
import Development.IDE.Core.Service.Daml
import qualified DA.Service.Daml.Compiler.Impl.Scenario as Scenario

-- DAML compiler and infrastructure
import qualified DA.Daml.LF.Ast                             as LF
import           DA.Daml.LF.Proto3.Archive                  (encodeArchiveLazy)
import qualified DA.Service.Logger                          as Logger
import qualified DA.Service.Daml.Compiler.Impl.Dar          as Dar

import Control.Monad.Trans.Maybe
import           Control.Monad.Except              as Ex
import           Control.Monad.IO.Class                     (liftIO)
import qualified Data.ByteString                            as BS
import qualified Data.ByteString.Lazy                       as BSL
import qualified Data.Set                                   as S
import qualified Data.Text                                  as T
import Data.Maybe

import qualified Development.IDE.Types.Logger as IdeLogger
import Development.IDE.State.API
import Development.IDE.Core.Rules.Daml
import Development.IDE.Core.RuleTypes.Daml
import qualified Development.IDE.Core.Shake as Shake
import qualified Language.Haskell.LSP.Messages as LSP
import Development.IDE.Types.Location as Base
import           System.FilePath

------------------------------------------------------------------------
-- Types for dependency information on DARs
------------------------------------------------------------------------
--

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
    (_diags, pkgMap) <- generatePackageMap (optPackageDbs compilerOpts)
    let rule = do
            mainRule compilerOpts
            Shake.addIdeGlobal $ GlobalPkgMap pkgMap
    initialise rule eventHandler (toIdeLogger loggerH) compilerOpts vfs mbScenarioService

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
toIdeLogger :: Logger.Handle IO -> IdeLogger.Logger
toIdeLogger h = IdeLogger.Logger {
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
    mod0 <- runAction service (getDalfModule filePath)
    case mod0 of
        Nothing ->
            -- This can happen if there is a parse or a type error.
            -- The diagnostics for that will be reported somewhere else.
            return []
        Just mod0 -> pure
            [ (sourceLocToRange loc, "Scenario: " <> name, vr)
            | (valRef, Just loc) <- scenariosInModule mod0
            , let name = LF.unExprValName (LF.qualObject valRef)
            , let vr = VRScenario filePath name
            ]


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
  -> IO (Maybe BS.ByteString)
buildDar service file mbExposedModules pkgName sdkVersion buildDataFiles dalfInput = do
  let file' = fromNormalizedFilePath file
  liftIO $
    IdeLogger.logDebug (ideLogger service) $
    "Creating dar: " <> T.pack file'
  if unUseDalf dalfInput
    then liftIO $ Just <$> do
      bytes <- BSL.readFile file'
      Dar.buildDar
        bytes
        (toNormalizedFilePath $ takeDirectory file')
        []
        []
        []
        pkgName
        sdkVersion
    else runAction service $ runMaybeT $ do
      pkg <- useE GeneratePackage file
      let pkgModuleNames = S.fromList $ map T.unpack $ LF.packageModuleNames pkg
      let missingExposed = S.fromList (fromMaybe [] mbExposedModules) S.\\ pkgModuleNames
      unless (S.null missingExposed) $ do
          liftIO $ IdeLogger.logSeriousError (ideLogger service) $
              "The following modules are declared in exposed-modules but are not part of the DALF: " <>
              T.pack (show $ S.toList missingExposed)
          fail ""
      let dalf = encodeArchiveLazy pkg
      -- get all dalf dependencies.
      deps <- getDalfDependencies file
      dalfDependencies<- forM deps $ \(DalfDependency depName fp) -> do
        pkgDalf <- liftIO $ BS.readFile fp
        return (depName, pkgDalf)
      -- get all file dependencies
      fileDependencies <- MaybeT $ getDependencies file
      liftIO $
        Dar.buildDar
          dalf
          (toNormalizedFilePath $ takeDirectory file')
          dalfDependencies
          (file:fileDependencies)
          (buildDataFiles pkg)
          pkgName
          sdkVersion
