-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE TemplateHaskell #-}

module DA.Daml.LF.Ast.World(
    World,
    DalfPackage(..),
    getWorldSelfPkgLfVersion,
    getWorldSelfPkgModules,
    getWorldImported,
    initWorld,
    initWorldSelf,
    extendWorldSelf,
    ExternalPackage(..),
    LookupError(..),
    lookupTemplate,
    lookupException,
    lookupTypeSyn,
    lookupDataType,
    lookupChoice,
    lookupInterfaceChoice,
    lookupInterfaceMethod,
    lookupValue,
    lookupModule,
    lookupInterface,
    lookupTemplateOrInterface,
    lookupInterfaceInstance,
    ) where

import DA.Pretty

import Control.DeepSeq
import Control.Lens
import qualified Data.ByteString as BS
import qualified Data.HashMap.Strict as HMS
import Data.List
import qualified Data.NameMap as NM
import GHC.Generics
import Data.Either.Extra (maybeToEither)

import DA.Daml.LF.Ast.Base
import DA.Daml.LF.Ast.Pretty ()
import DA.Daml.LF.Ast.Version
import DA.Daml.LF.TemplateOrInterface (TemplateOrInterface)
import qualified DA.Daml.LF.TemplateOrInterface as TemplateOrInterface

-- | The 'World' contains all imported packages together with (a subset of)
-- the modules of the current package. The latter shall always be closed under
-- module dependencies but we don't enforce this here.
data World = World
  { _worldImported :: HMS.HashMap PackageId Package
  , _worldSelfPkgLfVersion :: Version
  , _worldSelfPkgModules :: NM.NameMap Module
  }


getWorldSelfPkgLfVersion :: World -> Version
getWorldSelfPkgLfVersion = _worldSelfPkgLfVersion

getWorldSelfPkgModules :: World -> NM.NameMap Module
getWorldSelfPkgModules = _worldSelfPkgModules

getWorldImported :: World -> [ExternalPackage]
getWorldImported world = map (uncurry ExternalPackage) $ HMS.toList (_worldImported world)

-- | A package where all references to `SelfPackageId` have been rewritten
-- to `ImportedPackageId`.
data ExternalPackage = ExternalPackage
  { extPackageId :: PackageId
  , extPackagePkg :: Package
  } deriving (Show, Eq, Generic)

instance NFData ExternalPackage

makeLensesFor [("_worldSelfPkgModules","worldSelfPkgModules")] ''World

data DalfPackage = DalfPackage
    { dalfPackageId :: PackageId
    , dalfPackagePkg :: ExternalPackage
    , dalfPackageBytes :: BS.ByteString
    } deriving (Show, Eq, Generic)

instance NFData DalfPackage

-- | Construct the 'World' from only the imported packages.
-- TODO (drsk) : hurraybit: please check that the duplicate package id check here is not needed.
initWorld :: [ExternalPackage] -> Version -> World
initWorld importedPkgs version =
  World
    (foldl' insertPkg HMS.empty importedPkgs)
    version
    NM.empty
  where
    insertPkg hms (ExternalPackage pkgId pkg) = HMS.insert pkgId pkg hms

-- | Create a World with an initial self package
initWorldSelf :: [ExternalPackage] -> Package -> World
initWorldSelf importedPkgs pkg =
    (initWorld importedPkgs $ packageLfVersion pkg)
        { _worldSelfPkgLfVersion = packageLfVersion pkg
        , _worldSelfPkgModules = packageModules pkg
        }

-- | Extend the 'World' by a module in the current package.
extendWorldSelf :: Module -> World -> World
extendWorldSelf = over worldSelfPkgModules . NM.insert

data LookupError
  = LEPackage !PackageId
  | LEModule !SelfOrImportedPackageId !ModuleName
  | LETypeSyn !(Qualified TypeSynName)
  | LEDataType !(Qualified TypeConName)
  | LEValue !(Qualified ExprValName)
  | LETemplate !(Qualified TypeConName)
  | LEException !(Qualified TypeConName)
  | LEChoice !(Qualified TypeConName) !ChoiceName
  | LEInterface !(Qualified TypeConName)
  | LEInterfaceMethod !(Qualified TypeConName) !MethodName
  | LETemplateOrInterface !(Qualified TypeConName)
  | LEUnknownInterfaceInstance !InterfaceInstanceHead
  | LEAmbiguousInterfaceInstance !InterfaceInstanceHead
  deriving (Eq, Ord, Show)

lookupModule :: Qualified a -> World -> Either LookupError Module
lookupModule (Qualified pkgRef modName _) (World importedPkgs _ selfPkgMods) = do
  mods <- case pkgRef of
    SelfPackageId -> pure selfPkgMods
    ImportedPackageId pkgId ->
      case HMS.lookup pkgId importedPkgs of
        Nothing -> Left (LEPackage pkgId)
        Just pkg -> pure (packageModules pkg)
  case NM.lookup modName mods of
    Nothing -> Left (LEModule pkgRef modName)
    Just mod0 -> Right mod0

lookupDefinition
  :: (NM.Named a)
  => (Module -> NM.NameMap a)
  -> (Qualified (NM.Name a) -> LookupError)
  -> Qualified (NM.Name a)
  -> World
  -> Either LookupError a
lookupDefinition selDefs mkError ref world = do
  mod0 <- lookupModule ref world
  case NM.lookup (qualObject ref) (selDefs mod0) of
    Nothing -> Left (mkError ref)
    Just def -> pure def

lookupTypeSyn :: Qualified TypeSynName -> World -> Either LookupError DefTypeSyn
lookupTypeSyn = lookupDefinition moduleSynonyms LETypeSyn

lookupDataType :: Qualified TypeConName -> World -> Either LookupError DefDataType
lookupDataType = lookupDefinition moduleDataTypes LEDataType

lookupValue :: Qualified ExprValName -> World -> Either LookupError DefValue
lookupValue = lookupDefinition moduleValues LEValue

lookupTemplate :: Qualified TypeConName -> World -> Either LookupError Template
lookupTemplate = lookupDefinition moduleTemplates LETemplate

lookupInterface :: Qualified TypeConName -> World -> Either LookupError DefInterface
lookupInterface = lookupDefinition moduleInterfaces LEInterface

lookupTemplateOrInterface ::
  Qualified TypeConName -> World -> Either LookupError (TemplateOrInterface Template DefInterface)
lookupTemplateOrInterface tyCon world =
  case lookupTemplate tyCon world of
    Right template -> Right (TemplateOrInterface.Template template)
    Left _ ->
      case lookupInterface tyCon world of
        Right interface -> Right (TemplateOrInterface.Interface interface)
        Left _ -> Left (LETemplateOrInterface tyCon)

lookupException :: Qualified TypeConName -> World -> Either LookupError DefException
lookupException = lookupDefinition moduleExceptions LEException

lookupChoice :: (Qualified TypeConName, ChoiceName) -> World -> Either LookupError TemplateChoice
lookupChoice (tplRef, chName) world = do
  tpl <- lookupTemplate tplRef world
  case NM.lookup chName (tplChoices tpl) of
    Nothing -> Left (LEChoice tplRef chName)
    Just choice -> Right choice

lookupInterfaceChoice :: (Qualified TypeConName, ChoiceName) -> World ->
  Either LookupError TemplateChoice
lookupInterfaceChoice (ifaceRef, chName) world = do
  DefInterface{..} <- lookupInterface ifaceRef world
  maybeToEither (LEChoice ifaceRef chName) $
    NM.lookup chName intChoices

lookupInterfaceMethod :: (Qualified TypeConName, MethodName) -> World -> Either LookupError InterfaceMethod
lookupInterfaceMethod (ifaceRef, methodName) world = do
  iface <- lookupInterface ifaceRef world
  maybeToEither (LEInterfaceMethod ifaceRef methodName) $
      NM.lookup methodName (intMethods iface)

lookupInterfaceInstance ::
  InterfaceInstanceHead -> World -> Either LookupError ()
lookupInterfaceInstance iiHead@InterfaceInstanceHead {..} world = do
  _ <- lookupInterface iiInterface world
  template <- lookupTemplate iiTemplate world
  case NM.lookup iiInterface (tplImplements template) of
    Nothing -> Left (LEUnknownInterfaceInstance iiHead)
    Just _ -> Right ()

instance Pretty LookupError where
  pPrint = \case
    LEPackage pkgId -> "unknown package:" <-> pretty pkgId
    LEModule SelfPackageId modName -> "unknown module:" <-> pretty modName
    LEModule (ImportedPackageId pkgId) modName -> "unknown module:" <-> pretty pkgId <> ":" <> pretty modName
    LETypeSyn synRef -> "unknown type synonym:" <-> pretty synRef
    LEDataType datRef -> "unknown data type:" <-> pretty datRef
    LEValue valRef-> "unknown value:" <-> pretty valRef
    LETemplate tplRef -> "unknown template:" <-> pretty tplRef
    LEException exnRef -> "unknown exception:" <-> pretty exnRef
    LEChoice tplRef chName -> "unknown choice:" <-> pretty tplRef <> ":" <> pretty chName
    LEInterface ifaceRef -> "unknown interface:" <-> pretty ifaceRef
    LEInterfaceMethod ifaceRef methodName -> "unknown interface method:" <-> pretty ifaceRef <> "." <> pretty methodName
    LETemplateOrInterface tyRef -> "unknown template or interface:" <-> pretty tyRef
    LEUnknownInterfaceInstance iiHead -> "unknown" <-> quotes (pretty iiHead)
    LEAmbiguousInterfaceInstance iiHead ->
      hsep
        [ "ambiguous"
        , quotes (pretty iiHead) <> ","
        , "both the interface and the template define this interface instance."
        ]
