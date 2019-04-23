-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DA.Service.Daml.Compiler.Impl.Handle
  (-- * compilation handle providers
    IdeState
  , newIdeState
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

import           DA.Prelude

-- HS/DAML to LF compiler (daml-ghc)
import DA.Daml.GHC.Compiler.Convert (sourceLocToRange)
import DA.Daml.GHC.Compiler.Options
import qualified DA.Service.Daml.Compiler.Impl.Scenario as Scenario
import "ghc-lib-parser" Module                                     (unitIdString, UnitId(..), DefUnitId(..))

-- DAML compiler and infrastructure
import qualified DA.Daml.LF.Ast                             as LF
import           DA.Daml.LF.Proto3.Archive                  (encodeArchiveLazy)
import qualified DA.Service.Logger                          as Logger
import qualified DA.Service.Daml.Compiler.Impl.Dar          as Dar

import           Control.Concurrent.STM
import           Control.Monad.Except.Extended              as Ex
import           Control.Monad.IO.Class                     (liftIO)
import           Control.Monad.Managed.Extended
import qualified Data.ByteString                            as BS
import qualified Data.ByteString.Lazy                       as BSL
import qualified Data.ByteString.Char8 as BS8
import qualified Data.Map.Strict                            as Map
import qualified Data.NameMap as NM
import qualified Data.Set                                   as S
import qualified Data.Text                                  as T

import           Data.Time.Clock
import           Data.Traversable                           (for)

import qualified Development.IDE.Logger as IdeLogger
import           Development.IDE.State.API               (IdeState)
import qualified Development.IDE.State.API               as CompilerService
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
        _ -> empty

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

newIdeState :: Options
            -> Maybe (Event -> STM ())
            -> Logger.Handle IO
            -> Managed IdeState
newIdeState compilerOpts mbEventHandler loggerH = do
  mbScenarioService <- for mbEventHandler $ \eventHandler -> Scenario.startScenarioService eventHandler loggerH

  -- Load the packages from the package database for the scenario service. We swallow errors here
  -- but shake will report them when typechecking anything.
  (_diags, pkgMap) <- liftIO $ CompilerService.generatePackageMap (optPackageDbs compilerOpts)
  let rule = do
        CompilerService.mainRule
        Shake.addIdeGlobal $ GlobalPkgMap pkgMap
  liftIO $ CompilerService.initialise rule mbEventHandler (toIdeLogger loggerH) compilerOpts mbScenarioService

-- | Adapter to the IDE logger module.
toIdeLogger :: Logger.Handle m -> IdeLogger.Handle m
toIdeLogger h = IdeLogger.Handle {
       logError = Logger.logError h
     , logWarning = Logger.logWarning h
     , logInfo = Logger.logInfo h
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
getAssociatedVirtualResources service filePath =
  runDefaultExceptT [] $ logExceptT "GetAssociatedVirtualResources" $ do
    mods <- NM.toList . LF.packageModules <$> compileFile service filePath
    when (null mods) $
      throwError $ toStore [errorDiag filePath "Get associated virtual resources"
        "No modules returned by compiler."]
    -- NOTE(MH): 'compile' returns the modules in topologically sorted order.
    -- Thus, the module we care about is the last in the list.
    let mod0 = last mods
    pure
      [ (sourceLocToRange loc, "Scenario: " <> name, vr)
      | value@LF.DefValue{dvalLocation = Just loc} <- NM.toList (LF.moduleValues mod0)
      , LF.getIsTest (LF.dvalIsTest value)
      , let name = unTagged (LF.dvalName value)
      , let vr = VRScenario filePath name
      ]
  where
    logExceptT src act = act `catchError` \err -> do
      lift $ CompilerService.logError service $ T.unlines ["ERROR in " <> src <> ":", T.pack (show err)]
      throwError err

    toStore :: [Diagnostic] -> DiagnosticStore
    toStore ds = addDiagnostics filePath ds Map.empty

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
    -> ExceptT DiagnosticStore IO LF.Package
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
onFileModified service fp mcontents = do
    CompilerService.logDebug service $ "File modified " <> T.pack (show fp)
    time <- liftIO getCurrentTime
    CompilerService.setBufferModified service fp (mcontents, time)

newtype UseDalf = UseDalf{unUseDalf :: Bool}

buildDar ::
     IdeState
  -> FilePath
  -> String
  -> [(String, BS.ByteString)]
  -> UseDalf
  -> ExceptT DiagnosticStore IO BS.ByteString
buildDar service file pkgName dataFiles dalfInput = do
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
        dataFiles
        pkgName
    else do
      dalf <- encodeArchiveLazy <$> compileFile service file
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
              dataFiles
              pkgName

-- | Get the transitive package dependencies on other dalfs.
getDalfDependencies ::
       IdeState -> FilePath -> ExceptT DiagnosticStore IO [DalfDependency]
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
