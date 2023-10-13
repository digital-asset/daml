-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE TypeFamilies #-}

-- | Custom methods for interupting the usual goto definition flow for multi-ide deferring
module DA.Daml.LanguageServer.SplitGotoDefinition
    ( GotoDefinitionByNameParams (..)
    , GotoDefinitionByNameResult
    , TryGetDefinitionName (..)
    , TryGetDefinitionNameSpace (..)
    , TryGetDefinitionParams (..)
    , TryGetDefinitionResult (..)
    , fromTryGetDefinitionNameSpace
    , plugin
    , toTryGetDefinitionNameSpace
    ) where

import Control.Lens ((^.))
import Control.Monad.IO.Class
import Control.Monad.Trans.Maybe (MaybeT (..), runMaybeT)
import Control.Monad.Trans.Except (ExceptT (..), runExceptT, throwE, except)
import DA.Daml.Compiler.Dar (getDamlFiles)
import DA.Daml.Package.Config (PackageConfigFields (pSrc), parseProjectConfig)
import DA.Daml.Project.Config (readProjectConfig)
import DA.Daml.Project.Types (ProjectPath (..))
import qualified Data.Aeson as Aeson
import Data.Aeson.TH
import qualified Data.Aeson.Types as Aeson
import Data.Bifunctor (first)
import Data.List (find, isSuffixOf, sortOn)
import Data.Maybe (listToMaybe)
import qualified Data.Text as T
import Development.IDE.Core.Rules.Daml
import Development.IDE.Core.RuleTypes
import Development.IDE.Core.Service.Daml
import Development.IDE.Core.Shake (IdeRule, use)
import Development.IDE.GHC.Error (srcSpanToLocation)
import Development.IDE.Plugin
import Development.IDE.Spans.Type (getNameM, spaninfoSource, spansExprs)
import Development.IDE.Types.Location
import Development.Shake (Action)
import qualified Language.LSP.Server as LSP
import Language.LSP.Types
import qualified Language.LSP.Types as LSP
import qualified Language.LSP.Types.Lens as LSP
import qualified Language.LSP.Types.Utils as LSP
import System.FilePath ((</>))
import "ghc-lib" GhcPlugins (
  Module,
  isGoodSrcSpan,
  moduleName,
  moduleNameString,
  moduleUnitId,
  occNameString,
  unitIdString,
 )
import "ghc-lib-parser" Name (
  Name,
  NameSpace,
  dataName,
  isDataConNameSpace,
  isExternalName,
  isTcClsNameSpace,
  isTvNameSpace,
  mkOccName,
  nameModule_maybe,
  nameOccName,
  nameSrcSpan,
  occNameSpace,
  tcClsName,
  tvName,
  varName,
 )
  
data TryGetDefinitionParams = TryGetDefinitionParams
  { tgdpTextDocument :: TextDocumentIdentifier
  , tgdpPosition :: Position
  }
  deriving Show
deriveJSON LSP.lspOptions ''TryGetDefinitionParams

data TryGetDefinitionNameSpace
  = VariableName 
  | DataName 
  | TypeVariableName 
  | TypeCnstrOrClassName 
  deriving Show
deriveJSON LSP.lspOptions ''TryGetDefinitionNameSpace

fromTryGetDefinitionNameSpace :: TryGetDefinitionNameSpace -> NameSpace
fromTryGetDefinitionNameSpace = \case
  VariableName -> varName
  DataName -> dataName
  TypeVariableName -> tvName
  TypeCnstrOrClassName -> tcClsName

toTryGetDefinitionNameSpace :: NameSpace -> TryGetDefinitionNameSpace
toTryGetDefinitionNameSpace ns = if
  | isDataConNameSpace ns -> DataName
  | isTcClsNameSpace ns -> TypeCnstrOrClassName
  | isTvNameSpace ns -> TypeVariableName
  | otherwise -> VariableName

data TryGetDefinitionName = TryGetDefinitionName
  { tgdnModuleName :: String
  , tgdnPackageUnitId :: String
  , tgdnIdentifierName :: String
  , tgdnIdentifierNameSpace :: TryGetDefinitionNameSpace
  }
  deriving Show
deriveJSON LSP.lspOptions ''TryGetDefinitionName

data TryGetDefinitionResult = TryGetDefinitionResult
  { tgdrLocation :: Location
  , tgdrName :: Maybe TryGetDefinitionName
  }
  deriving Show
deriveJSON LSP.lspOptions ''TryGetDefinitionResult

data GotoDefinitionByNameParams = GotoDefinitionByNameParams
  { gdnpBackupLocation :: Location
  , gdnpName :: TryGetDefinitionName
  }
  deriving Show
deriveJSON LSP.lspOptions ''GotoDefinitionByNameParams

type GotoDefinitionByNameResult = Location

{- 
2 methods:
tryGotoDefinition :: position -> location + Maybe (name + package)
  -- locationsAtPoint but if its an unhelpful name, we provide that too
gotoDefinitionByName :: name -> location
  -- try to lookup the name (by its module) in own modules, give back the source


flow
call tryGotoDefinition on package of given location
  if no (name + package), or we don't have an ide/source for that package, return the location immediately
  else
    call gotoDefinitionByName on returned package
-}

plugin :: Plugin c
plugin = Plugin
    { pluginCommands = mempty
    , pluginRules = mempty
    , pluginHandlers = 
        customMethodHandler "tryGetDefinition" tryGetDefinition
          <> customMethodHandler "gotoDefinitionByName" gotoDefinitionByName
    , pluginNotificationHandlers = mempty
    }

customMethodHandler
  :: forall req res c
  .  (Aeson.FromJSON req, Aeson.ToJSON res)
  => T.Text
  -> (IdeState -> req -> LSP.LspM c (Either LSP.ResponseError res))
  -> PluginHandlers c
customMethodHandler name f = pluginHandler (SCustomMethod $ "daml/" <> name) $ \ideState value ->
  let (!params :: req) = 
        either 
          (\err -> error $ "Failed to parse message of daml/" <> T.unpack name <> ": " <> err) id 
            $ Aeson.parseEither Aeson.parseJSON value
   in fmap Aeson.toJSON <$> f ideState params

nameSortExternalModule :: Name -> Maybe Module
nameSortExternalModule m | isExternalName m = nameModule_maybe m
nameSortExternalModule _ = Nothing

-- daml/tryGetDefinition :: TryGetDefinitionParams -> Maybe TryGetDefinitionResult
tryGetDefinition :: IdeState -> TryGetDefinitionParams -> LSP.LspM c (Either ResponseError (Maybe TryGetDefinitionResult))
tryGetDefinition ideState params = Right <$>
  case uriToFilePath' $ tgdpTextDocument params ^. LSP.uri of
    Nothing -> pure Nothing
    Just (toNormalizedFilePath' -> file) ->
      liftIO $ runActionSync ideState $ runMaybeT $ do
        (loc, mName) <- MaybeT $ getDefinitionWithName file $ tgdpPosition params
        let tgdName = do
              name <- mName
              m <- nameSortExternalModule name
              pure $ TryGetDefinitionName
                (moduleNameString $ moduleName m)
                (unitIdString $ moduleUnitId m)
                (occNameString $ nameOccName name)
                (toTryGetDefinitionNameSpace $ occNameSpace $ nameOccName name)
        pure $ TryGetDefinitionResult loc tgdName

replaceChar :: Char -> Char -> String -> String
replaceChar val replacement = fmap (\c -> if c == val then replacement else c)

-- daml/gotoDefinitionByName :: GotoDefinitionByNameParams -> GotoDefinitionByNameResult
gotoDefinitionByName :: IdeState -> GotoDefinitionByNameParams -> LSP.LspM c (Either ResponseError GotoDefinitionByNameResult)
gotoDefinitionByName ideState params = do
  mRoot <- LSP.getRootPath
  liftIO $ runActionSync ideState $ exceptTToResult $ do
    -- Working out the file by getting the IDE root and pSrc from daml.yaml, getting all the source files for it, then searching for our module
    --   We search rather than explicitly building the path to account for pSrc being a daml file, whereby file discovery logic is via dependencies
    -- I tried to do this better, by looking up the module name in IDE state, but it seems the IDE doesn't load
    -- modules until you open the file, so it doesn't hold any context about "all" modules.
    -- (trust me I tried so hard to make this work)
    root <- hoistMaybe (Just "Failed to get IDE root") mRoot
    projectConfig <- liftIO $ readProjectConfig (ProjectPath root)
    config <- except $ first (Just . show) $ parseProjectConfig projectConfig

    srcFiles <- maybeTToExceptT "Failed to get source files" $ getDamlFiles $ root </> pSrc config
    -- Must be sorted shorted to longest, since we always want the shortest path that matches our suffix
    -- to avoid accidentally picking Main.A.B.C if we're just looking for A.B.C
    -- We also prefix all paths with "/" and search for our suffix starting with "/"
    -- This is avoid incorrectly picking MA.B.C if we were looking for A.B.C and it didnt exist
    let sortedSrcFiles = sortOn (Prelude.length . fromNormalizedFilePath) srcFiles
        moduleSuffix = "/" <> replaceChar '.' '/' (tgdnModuleName $ gdnpName params) <> ".daml"
    file <-
      hoistMaybe (Just "Failed to find module") $
        find (isSuffixOf moduleSuffix . ("/" <> ) . fromNormalizedFilePath) sortedSrcFiles
    
    -- It might be better to get the typechecked module and look for the identifier in there?
    spans <- useOrThrow "Failed to get span info" GetSpanInfo file
    let expectedOccName = mkOccName (fromTryGetDefinitionNameSpace $ tgdnIdentifierNameSpace $ gdnpName params) (tgdnIdentifierName $ gdnpName params)
        locations = 
          [ srcSpanToLocation $ nameSrcSpan name
          | Just name <- getNameM . spaninfoSource <$> spansExprs spans
          , expectedOccName == nameOccName name && isGoodSrcSpan (nameSrcSpan name)
          ]
    
    hoistMaybe Nothing $ listToMaybe locations
  where
    -- A Nothing error means no location, a string error means a response error
    exceptTToResult :: ExceptT (Maybe String) Action GotoDefinitionByNameResult -> Action (Either ResponseError GotoDefinitionByNameResult)
    exceptTToResult t = fmap (either (maybe (Right $ gdnpBackupLocation params) (\msg -> Left $ ResponseError ParseError (T.pack msg) Nothing)) Right) $ runExceptT t
    useOrThrow :: IdeRule k v => String -> k -> NormalizedFilePath -> ExceptT (Maybe String) Action v
    useOrThrow msg k path = ExceptT $ maybe (Left $ Just msg) Right <$> use k path
    hoistMaybe :: Monad m => x -> Maybe a -> ExceptT x m a
    hoistMaybe err = maybe (throwE err) pure
    maybeTToExceptT :: String -> MaybeT Action a -> ExceptT (Maybe String) Action a
    maybeTToExceptT err m = ExceptT $ maybe (Left $ Just err) Right <$> runMaybeT m
