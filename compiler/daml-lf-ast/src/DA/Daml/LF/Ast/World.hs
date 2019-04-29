-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
module DA.Daml.LF.Ast.World where

import DA.Prelude
import DA.Pretty

import           Control.Lens
import qualified Data.HashMap.Strict as HMS
import qualified Data.NameMap as NM

import DA.Daml.LF.Ast.Base
import DA.Daml.LF.Ast.Optics (moduleModuleRef)
import DA.Daml.LF.Ast.Pretty
import DA.Daml.LF.Ast.Version

-- | The 'World' contains all imported packages together with (a subset of)
-- the modules of the current package. The latter shall always be closed under
-- module dependencies but we don't enforce this here.
data World = World
  { worldImported :: HMS.HashMap PackageId Package
  , worldSelf :: Package
  }

makeUnderscoreLenses ''World

emptyWorld :: Version -> World
emptyWorld = initWorld []

-- | Construct the 'World' from only the imported packages.
initWorld :: [(PackageId, Package)] -> Version -> World
initWorld importedPkgs version =
  World
    (HMS.mapWithKey rewritePRSelf (foldl insertPkg HMS.empty importedPkgs))
    (Package version NM.empty)
  where
    insertPkg hms (pkgId, pkg)
      | pkgId `HMS.member` hms =
          error $  "World.initWorld: duplicate package id " ++ show pkgId
      | otherwise = HMS.insert pkgId pkg hms
    rewritePRSelf :: PackageId -> Package -> Package
    rewritePRSelf pkgId = over (_packageModules . NM.traverse . moduleModuleRef . _1) $ \case
      PRSelf -> PRImport pkgId
      ref@PRImport{} -> ref

singlePackageWorld :: Package -> World
singlePackageWorld = World HMS.empty

-- | Extend the 'World' by a module in the current package.
extendWorld :: Module -> World -> World
extendWorld = over (_worldSelf . _packageModules) . NM.insert

data LookupError
  = LEPackage !PackageId
  | LEModule !PackageRef !ModuleName
  | LEDataType !(Qualified TypeConName)
  | LEValue !(Qualified ExprValName)
  | LETemplate !(Qualified TypeConName)
  | LEChoice !(Qualified TypeConName) !ChoiceName
  deriving (Eq, Ord, Show)

lookupModule :: Qualified a -> World -> Either LookupError Module
lookupModule (Qualified pkgRef modName _) (World importedPkgs selfPkg) = do
  Package _version mods <- case pkgRef of
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
    LEPackage pkgId -> "unknown package:" <-> prettyName pkgId
    LEModule PRSelf modName -> "unknown module:" <-> prettyDottedName modName
    LEModule (PRImport pkgId) modName -> "unknown module:" <-> prettyName pkgId <> ":" <> prettyDottedName modName
    LEDataType datRef -> "unknown data type:" <-> prettyQualified prettyDottedName datRef
    LEValue valRef-> "unknown value:" <-> prettyQualified prettyName valRef
    LETemplate tplRef -> "unknown template:" <-> prettyQualified prettyDottedName tplRef
    LEChoice tplRef chName -> "unknown choice:" <-> prettyQualified prettyDottedName tplRef <> ":" <> prettyName chName
