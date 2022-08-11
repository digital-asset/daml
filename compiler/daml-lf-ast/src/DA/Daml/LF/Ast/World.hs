-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE TemplateHaskell #-}

module DA.Daml.LF.Ast.World(
    World,
    DalfPackage(..),
    getWorldSelf,
    getWorldImported,
    initWorld,
    initWorldSelf,
    extendWorldSelf,
    ExternalPackage(..),
    LookupError,
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
    InterfaceInstanceInfo(..),
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
import DA.Daml.LF.TemplateOrInterface (TemplateOrInterface')
import qualified DA.Daml.LF.TemplateOrInterface as TemplateOrInterface

-- | The 'World' contains all imported packages together with (a subset of)
-- the modules of the current package. The latter shall always be closed under
-- module dependencies but we don't enforce this here.
data World = World
  { _worldImported :: HMS.HashMap PackageId Package
  , _worldSelf :: Package
  }


getWorldSelf :: World -> Package
getWorldSelf = _worldSelf

getWorldImported :: World -> [ExternalPackage]
getWorldImported world = map (uncurry ExternalPackage) $ HMS.toList (_worldImported world)

-- | A package where all references to `PRSelf` have been rewritten
-- to `PRImport`.
data ExternalPackage = ExternalPackage
  { extPackageId :: PackageId
  , extPackagePkg :: Package
  } deriving (Show, Eq, Generic)

instance NFData ExternalPackage

makeLensesFor [("_worldSelf","worldSelf")] ''World

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
    (Package version NM.empty Nothing)
  where
    insertPkg hms (ExternalPackage pkgId pkg) = HMS.insert pkgId pkg hms

-- | Create a World with an initial self package
initWorldSelf :: [ExternalPackage] -> Package -> World
initWorldSelf importedPkgs pkg = (initWorld importedPkgs $ packageLfVersion pkg){_worldSelf = pkg}

-- | Extend the 'World' by a module in the current package.
extendWorldSelf :: Module -> World -> World
extendWorldSelf = over (worldSelf . _packageModules) . NM.insert

data LookupError
  = LEPackage !PackageId
  | LEModule !PackageRef !ModuleName
  | LETypeSyn !(Qualified TypeSynName)
  | LEDataType !(Qualified TypeConName)
  | LEValue !(Qualified ExprValName)
  | LETemplate !(Qualified TypeConName)
  | LEException !(Qualified TypeConName)
  | LEChoice !(Qualified TypeConName) !ChoiceName
  | LEInterface !(Qualified TypeConName)
  | LEInterfaceMethod !(Qualified TypeConName) !MethodName
  | LEUnknownInterfaceInstance !InterfaceInstanceKey
  | LEAmbiguousInterfaceInstance !InterfaceInstanceKey
  deriving (Eq, Ord, Show)

lookupModule :: Qualified a -> World -> Either LookupError Module
lookupModule (Qualified pkgRef modName _) (World importedPkgs selfPkg) = do
  Package { packageModules = mods } <- case pkgRef of
    PRSelf -> pure selfPkg
    PRImport pkgId ->
      case HMS.lookup pkgId importedPkgs of
        Nothing -> Left (LEPackage pkgId)
        Just mods -> pure mods
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

data InterfaceInstanceInfo = InterfaceInstanceInfo
  { defInterface :: DefInterface
  , parent :: TemplateOrInterface' (Qualified TypeConName)
  }

lookupInterfaceInstance ::
  InterfaceInstanceKey -> World -> Either LookupError InterfaceInstanceInfo
lookupInterfaceInstance iiKey@InterfaceInstanceKey {..} world = do
  interface <- lookupInterface iiInterface world
  template <- lookupTemplate iiTemplate world
  let
    onInterface = NM.lookup iiTemplate (intCoImplements interface)
    onTemplate = NM.lookup iiInterface (tplImplements template)
    ok = Right . InterfaceInstanceInfo interface
    err mkErr = Left (mkErr iiKey)
  case (onInterface, onTemplate) of
    (Nothing, Nothing) -> err LEUnknownInterfaceInstance
    (Just _, Nothing) -> ok (TemplateOrInterface.Interface iiInterface)
    (Nothing, Just _) -> ok (TemplateOrInterface.Template iiTemplate)
    (Just _, Just _) -> err LEAmbiguousInterfaceInstance

instance Pretty LookupError where
  pPrint = \case
    LEPackage pkgId -> "unknown package:" <-> pretty pkgId
    LEModule PRSelf modName -> "unknown module:" <-> pretty modName
    LEModule (PRImport pkgId) modName -> "unknown module:" <-> pretty pkgId <> ":" <> pretty modName
    LETypeSyn synRef -> "unknown type synonym:" <-> pretty synRef
    LEDataType datRef -> "unknown data type:" <-> pretty datRef
    LEValue valRef-> "unknown value:" <-> pretty valRef
    LETemplate tplRef -> "unknown template:" <-> pretty tplRef
    LEException exnRef -> "unknown exception:" <-> pretty exnRef
    LEChoice tplRef chName -> "unknown choice:" <-> pretty tplRef <> ":" <> pretty chName
    LEInterface ifaceRef -> "unknown interface:" <-> pretty ifaceRef
    LEInterfaceMethod ifaceRef methodName -> "unknown interface method:" <-> pretty ifaceRef <> "." <> pretty methodName
    LEUnknownInterfaceInstance iiKey -> "unknown" <-> quotes (pretty iiKey)
    LEAmbiguousInterfaceInstance iiKey ->
      hsep
        [ "ambiguous"
        , quotes (pretty iiKey) <> ","
        , "both the interface and the template define this interface instance."
        ]
