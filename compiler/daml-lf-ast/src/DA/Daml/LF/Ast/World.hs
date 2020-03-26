-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE TemplateHaskell #-}

module DA.Daml.LF.Ast.World(
    World,
    DalfPackage(..),
    getWorldSelf,
    initWorld,
    initWorldSelf,
    extendWorldSelf,
    ExternalPackage(..),
    LookupError,
    lookupTemplate,
    lookupTypeSyn,
    lookupDataType,
    lookupChoice,
    lookupValue,
    lookupModule
    ) where

import DA.Pretty

import Control.DeepSeq
import Control.Lens
import qualified Data.ByteString as BS
import qualified Data.HashMap.Strict as HMS
import Data.List
import qualified Data.NameMap as NM
import GHC.Generics

import DA.Daml.LF.Ast.Base
import DA.Daml.LF.Ast.Pretty ()
import DA.Daml.LF.Ast.Version

-- | The 'World' contains all imported packages together with (a subset of)
-- the modules of the current package. The latter shall always be closed under
-- module dependencies but we don't enforce this here.
data World = World
  { _worldImported :: HMS.HashMap PackageId Package
  , _worldSelf :: Package
  }

getWorldSelf :: World -> Package
getWorldSelf = _worldSelf

makeLensesFor [("_worldSelf","worldSelf")] ''World

-- | A package where all references to `PRSelf` have been rewritten
-- to `PRImport`.
data ExternalPackage = ExternalPackage
  { extPackageId :: PackageId
  , extPackagePkg :: Package
  } deriving (Show, Eq, Generic)

instance NFData ExternalPackage

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
  | LEChoice !(Qualified TypeConName) !ChoiceName
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

lookupChoice :: (Qualified TypeConName, ChoiceName) -> World -> Either LookupError TemplateChoice
lookupChoice (tplRef, chName) world = do
  tpl <- lookupTemplate tplRef world
  case NM.lookup chName (tplChoices tpl) of
    Nothing -> Left (LEChoice tplRef chName)
    Just choice -> Right choice

instance Pretty LookupError where
  pPrint = \case
    LEPackage pkgId -> "unknown package:" <-> pretty pkgId
    LEModule PRSelf modName -> "unknown module:" <-> pretty modName
    LEModule (PRImport pkgId) modName -> "unknown module:" <-> pretty pkgId <> ":" <> pretty modName
    LETypeSyn synRef -> "unknown type synonym:" <-> pretty synRef
    LEDataType datRef -> "unknown data type:" <-> pretty datRef
    LEValue valRef-> "unknown value:" <-> pretty valRef
    LETemplate tplRef -> "unknown template:" <-> pretty tplRef
    LEChoice tplRef chName -> "unknown choice:" <-> pretty tplRef <> ":" <> pretty chName
