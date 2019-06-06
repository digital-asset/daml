-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DA.Service.Daml.Compiler.Impl.Handle
  (-- * compilation handle providers
    IdeState
  , getIdeState
  , withIdeState
  , setFilesOfInterest
  , onFileModified
  , setOpenVirtualResources
  , getAssociatedVirtualResources
  , gotoDefinition
  , atPoint
  , compileFile
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
import qualified DA.Service.Daml.Compiler.Impl.Scenario as Scenario
import "ghc-lib-parser" Module (unitIdString, DefUnitId(..), UnitId(..))

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
import qualified Data.ByteString.Char8 as BS8
import qualified Data.Map.Strict                            as Map
import qualified Data.NameMap as NM
import qualified Data.Set                                   as S
import qualified Data.Text                                  as T
import Data.List
import Data.Maybe
import Safe

import qualified Development.IDE.Logger as IdeLogger
import           Development.IDE.State.API               (IdeState)
import qualified Development.IDE.State.API               as CompilerService
import Development.IDE.State.FileStore
import qualified Development.IDE.State.Rules.Daml as CompilerService
import qualified Development.IDE.State.Shake as Shake
import           Development.IDE.Types.Diagnostics as Base
import           Development.IDE.Types.LSP
import           System.FilePath

import qualified Network.HTTP.Types as HTTP.Types
import qualified Network.URI as URI

-- Virtual resource Uri
-----------------------

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
            [ ("file", filePath)
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
            pure $ VRScenario file (T.pack topLevelDecl)
        _ -> Nothing

  where
    queryString :: URI.URI -> Map.Map String String
    queryString u0 = fromMaybe Map.empty $ case tailMay $ URI.uriQuery u0 of
        Nothing -> Nothing
        Just u ->
            Just
          $ Map.fromList
          $ map (\(k, v) -> (BS8.unpack k, BS8.unpack v))
          $ HTTP.Types.parseSimpleQuery
          $ BS8.pack
          $ URI.unEscapeString u

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
    -> (Event -> IO ())
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
    -> (Event -> IO ())
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
     , logDebug = Logger.logDebug h
     }

------------------------------------------------------------------------------

-- | Update the files-of-interest, which we recieve asynchronous notifications for.
setFilesOfInterest
    :: IdeState
    -> [FilePath]
    -> IO ()
setFilesOfInterest service files = do
    CompilerService.logDebug service $ "Setting files of interest to: " <> T.pack (show files)
    CompilerService.setFilesOfInterest service (S.fromList files)

setOpenVirtualResources
    :: IdeState
    -> [VirtualResource]
    -> IO ()
setOpenVirtualResources service vrs = do
    CompilerService.logDebug service $ "Setting vrs of interest to: " <> T.pack (show vrs)
    CompilerService.setOpenVirtualResources service (S.fromList vrs)

getAssociatedVirtualResources
  :: IdeState
  -> FilePath
  -> IO [(Base.Range, T.Text, VirtualResource)]
getAssociatedVirtualResources service filePath = do
    mod0 <- runExceptT $ do
        mods <- NM.toList . LF.packageModules <$> compileFile service filePath
        case lastMay mods of
            Nothing -> throwError [errorDiag filePath "Get associated virtual resources"
                "No modules returned by compiler."]
            Just mod0 -> return mod0
    case mod0 of
        Left err -> do
            CompilerService.logSeriousError service $ T.unlines ["ERROR in GetAssociatedVirtualResources:", T.pack (show err)]
            return []
        Right mod0 -> pure
            [ (sourceLocToRange loc, "Scenario: " <> name, vr)
            | value@LF.DefValue{dvalLocation = Just loc} <- NM.toList (LF.moduleValues mod0)
            , LF.getIsTest (LF.dvalIsTest value)
            , let name = LF.unExprValName (LF.dvalName value)
            , let vr = VRScenario filePath name
            ]


gotoDefinition
    :: IdeState
    -> FilePath
    -> Base.Position
    -> IO (Maybe Base.Location)
gotoDefinition service afp pos = do
    CompilerService.logDebug service $ "Goto definition: " <> T.pack (show afp)
    CompilerService.runAction service (CompilerService.getDefinition afp pos)

atPoint
    :: IdeState
    -> FilePath
    -> Base.Position
    -> IO (Maybe (Maybe Base.Range, [HoverText]))
atPoint service afp pos = do
    CompilerService.logDebug service $ "AtPoint: " <> T.pack (show afp)
    CompilerService.runAction service (CompilerService.getAtPoint afp pos)

-- | Compile the supplied file using the Compiler Service into a DAML LF Package.
-- TODO options and warnings
compileFile
    :: IdeState
    -- -> Options
    -- -> Bool -- ^ collect and display warnings
    -> FilePath
    -> ExceptT [FileDiagnostic] IO LF.Package
compileFile service fp = do
    -- We need to mark the file we are compiling as a file of interest.
    -- Otherwise all diagnostics produced during compilation will be garbage
    -- collected afterwards.
    liftIO $ setFilesOfInterest service [fp]
    liftIO $ CompilerService.logDebug service $ "Compiling: " <> T.pack fp
    res <- liftIO $ CompilerService.runAction service (CompilerService.getDalf fp)
    case res of
        Nothing -> do
            diag <- liftIO $ CompilerService.getDiagnostics service
            throwE diag
        Just v -> return v

-- | Manages the file store (caching compilation results and unsaved content).
onFileModified
    :: IdeState
    -> FilePath
    -> Maybe T.Text
    -> IO ()
onFileModified service fp mbContents = do
    CompilerService.logDebug service $ "File modified " <> T.pack (show fp)
    CompilerService.setBufferModified service fp mbContents

newtype UseDalf = UseDalf{unUseDalf :: Bool}

buildDar ::
     IdeState
  -> FilePath
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
  liftIO $
    CompilerService.logDebug service $
    "Creating dar: " <> T.pack file
  if unUseDalf dalfInput
    then liftIO $ do
      bytes <- BSL.readFile file
      Dar.buildDar
        bytes
        (takeDirectory file)
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
              (takeDirectory file)
              dalfDependencies
              (file:fileDependencies)
              (buildDataFiles pkg)
              pkgName
              sdkVersion

-- | Get the transitive package dependencies on other dalfs.
getDalfDependencies ::
       IdeState -> FilePath -> ExceptT [FileDiagnostic] IO [DalfDependency]
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
