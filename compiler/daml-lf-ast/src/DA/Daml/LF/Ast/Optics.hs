-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeOperators #-}
module DA.Daml.LF.Ast.Optics(
    moduleModuleRef,
    unlocate,
    moduleExpr,
    dataConsType,
    _PRSelfModule,
    exprPartyLiteral,
    exprValueRef,
    packageRefs,
    templateExpr
    ) where

import Control.Lens
import Control.Lens.Ast
import Control.Lens.MonoTraversal
import Data.Fixed (Fixed)
import Data.Functor.Foldable (cata, embed)
import Data.Int
import Data.Text (Text)
import qualified Data.NameMap as NM
import GHC.Generics hiding (from, to)
import GHC.Generics.Lens

import DA.Daml.LF.Ast.Base
import DA.Daml.LF.Ast.Recursive
import DA.Daml.LF.Ast.Version (Version)

-- | WARNING: The result is not a proper prism.
-- The intended use case is something along the lines of
--
-- > e ^. rightSpine (unlocate p)
--
-- which does basically the same as
--
-- > e ^. rightSpine p
--
-- but looks through location annotations.
unlocate :: Prism' Expr a -> Prism' Expr a
unlocate p = prism inj proj
  where
    inj x = p # x
    proj e = case matching p (e ^. rightSpine _ELocation . _2) of
      Left _ -> Left e
      Right x -> Right x

-- | Prism that matches on a 'Qualified' whenever it references the given module
-- in the same package.
_PRSelfModule :: ModuleName -> Prism' (Qualified a) a
_PRSelfModule modName = prism (Qualified PRSelf modName) $ \case
  Qualified PRSelf modName' x | modName' == modName -> Right x
  q -> Left q

templateChoiceExpr :: Traversal' TemplateChoice Expr
templateChoiceExpr f (TemplateChoice loc name consuming actor selfBinder argBinder typ update) =
  TemplateChoice loc name consuming
  <$> f actor
  <*> pure selfBinder
  <*> pure argBinder
  <*> pure typ
  <*> f update

templateExpr :: Traversal' Template Expr
templateExpr f (Template loc tpl param precond signatories observers agreement choices key) =
  Template loc tpl param
  <$> f precond
  <*> f signatories
  <*> f observers
  <*> f agreement
  <*> (NM.traverse . templateChoiceExpr) f choices
  <*> (traverse . templateKeyExpr) f key

templateKeyExpr :: Traversal' TemplateKey Expr
templateKeyExpr f (TemplateKey typ body maintainers) = TemplateKey
  <$> pure typ
  <*> f body
  <*> f maintainers

moduleExpr :: Traversal' Module Expr
moduleExpr f (Module name path flags dataTypes values templates) =
  Module name path flags dataTypes
  <$> (NM.traverse . _dvalBody) f values
  <*> (NM.traverse . templateExpr) f templates

dataConsType :: Traversal' DataCons Type
dataConsType f = \case
  DataRecord  fs -> DataRecord  <$> (traverse . _2) f fs
  DataVariant cs -> DataVariant <$> (traverse . _2) f cs
  DataEnum cs -> pure $ DataEnum cs

type ModuleRef = (PackageRef, ModuleName)

-- | Traverse all the module references contained in 'Qualified's in a 'Package'.
moduleModuleRef :: Traversal' Module ModuleRef
moduleModuleRef = monoTraverse

instance MonoTraversable ModuleRef (Qualified a) where
  monoTraverse f (Qualified pkg0 mod0 x) =
    (\(pkg1, mod1) -> Qualified pkg1 mod1 x) <$> f (pkg0, mod0)

instance MonoTraversable ModuleRef ChoiceName where monoTraverse _ = pure
instance MonoTraversable ModuleRef ExprValName where monoTraverse _ = pure
instance MonoTraversable ModuleRef ExprVarName where monoTraverse _ = pure
instance MonoTraversable ModuleRef FieldName where monoTraverse _ = pure
instance MonoTraversable ModuleRef ModuleName where monoTraverse _ = pure
instance MonoTraversable ModuleRef TypeConName where monoTraverse _ = pure
instance MonoTraversable ModuleRef TypeVarName where monoTraverse _ = pure
instance MonoTraversable ModuleRef VariantConName where monoTraverse _ = pure

-- NOTE(MH): This is an optimization to avoid running into a dead end.
instance {-# OVERLAPPING #-} MonoTraversable ModuleRef FilePath where monoTraverse _ = pure

-- NOTE(MH): Builtins are not supposed to contain references to other modules.
instance MonoTraversable ModuleRef Kind where monoTraverse _ = pure
instance MonoTraversable ModuleRef BuiltinType where monoTraverse _ = pure
instance MonoTraversable ModuleRef BuiltinExpr where monoTraverse _ = pure
instance MonoTraversable ModuleRef SourceLoc where monoTraverse _ = pure

instance MonoTraversable ModuleRef TypeConApp
instance MonoTraversable ModuleRef Type

instance MonoTraversable ModuleRef Binding
instance MonoTraversable ModuleRef CasePattern
instance MonoTraversable ModuleRef CaseAlternative
instance MonoTraversable ModuleRef RetrieveByKey
instance MonoTraversable ModuleRef Update
instance MonoTraversable ModuleRef Scenario
instance MonoTraversable ModuleRef Expr

instance MonoTraversable ModuleRef IsSerializable
instance MonoTraversable ModuleRef DataCons
instance MonoTraversable ModuleRef DefDataType

instance MonoTraversable ModuleRef HasNoPartyLiterals
instance MonoTraversable ModuleRef IsTest
instance MonoTraversable ModuleRef DefValue

instance MonoTraversable ModuleRef Bool where monoTraverse _ = pure
-- NOTE(MH): Stakeholder signatures /currently/ don't contain references to
-- other modules. This might change in the future.
instance MonoTraversable ModuleRef TemplateChoice
instance MonoTraversable ModuleRef TemplateKey
instance MonoTraversable ModuleRef Template

instance MonoTraversable ModuleRef FeatureFlags
instance MonoTraversable ModuleRef Module

exprPartyLiteral
  :: forall f. Applicative f
  => (PartyLiteral -> f PartyLiteral) -> (Expr -> f Expr)
exprPartyLiteral f = cata go
  where
    go :: ExprF (f Expr) -> f Expr
    go = \case
      EBuiltinF (BEParty pty) -> EBuiltin . BEParty <$> f pty
      e -> embed <$> sequenceA e

-- | Traverse over all references to top-level values in an expression.
exprValueRef
  :: forall f. Applicative f
  => (ValueRef -> f ValueRef) -> (Expr -> f Expr)
exprValueRef f = cata go
  where
    go :: ExprF (f Expr) -> f Expr
    go = \case
      EValF val -> EVal <$> f val
      e -> embed <$> sequenceA e

class HasPackageRefs' a where
  packageRefs' :: Traversal' (a p) PackageRef

instance HasPackageRefs' V1 where
  packageRefs' = _V1

instance HasPackageRefs' U1 where
  packageRefs' = ignored

instance (HasPackageRefs' f, HasPackageRefs' g) => HasPackageRefs' (f :+: g) where
  packageRefs' f (L1 v) = L1 <$> packageRefs' f v
  packageRefs' f (R1 v) = R1 <$> packageRefs' f v

instance (HasPackageRefs' f, HasPackageRefs' g) => HasPackageRefs' (f :*: g) where
  packageRefs' f (a :*: b) = (:*:) <$> packageRefs' f a <*> packageRefs' f b

instance HasPackageRefs a => HasPackageRefs' (K1 i a) where
  packageRefs' = _K1 . packageRefs

instance HasPackageRefs' x => HasPackageRefs' (M1 i a x) where
  packageRefs' = _M1 . packageRefs'

class HasPackageRefs a where
  packageRefs :: Traversal' a PackageRef
  default packageRefs :: (Generic a, HasPackageRefs' (Rep a)) => Traversal' a PackageRef
  packageRefs = generic . packageRefs'

instance HasPackageRefs a => HasPackageRefs (Maybe a) where
  packageRefs = traverse . packageRefs

instance HasPackageRefs a => HasPackageRefs [a] where
  packageRefs = traverse . packageRefs

instance (HasPackageRefs a, HasPackageRefs b) => HasPackageRefs (a, b) where
  packageRefs = beside packageRefs packageRefs

instance HasPackageRefs Int where
  packageRefs = ignored
instance HasPackageRefs Int32 where
  packageRefs = ignored
instance HasPackageRefs Int64 where
  packageRefs = ignored
instance HasPackageRefs Bool where
  packageRefs = ignored
instance HasPackageRefs Char where
  packageRefs = ignored
instance HasPackageRefs Version where
  packageRefs = ignored
instance HasPackageRefs Text where
  packageRefs = ignored
instance HasPackageRefs (Fixed r) where
  packageRefs = ignored

instance (HasPackageRefs a, NM.Named a) => HasPackageRefs (NM.NameMap a) where
  packageRefs = NM.traverse . packageRefs

instance HasPackageRefs PackageRef where
  packageRefs = id

instance HasPackageRefs a => HasPackageRefs (Qualified a)
instance HasPackageRefs Binding
instance HasPackageRefs BuiltinExpr
instance HasPackageRefs BuiltinType
instance HasPackageRefs CaseAlternative
instance HasPackageRefs CasePattern
instance HasPackageRefs ChoiceName
instance HasPackageRefs DataCons
instance HasPackageRefs DefDataType
instance HasPackageRefs DefValue
instance HasPackageRefs Expr
instance HasPackageRefs ExprValName
instance HasPackageRefs ExprVarName
instance HasPackageRefs FeatureFlags
instance HasPackageRefs FieldName
instance HasPackageRefs HasNoPartyLiterals
instance HasPackageRefs IsSerializable
instance HasPackageRefs IsTest
instance HasPackageRefs Kind
instance HasPackageRefs Module
instance HasPackageRefs ModuleName
instance HasPackageRefs Package
instance HasPackageRefs PartyLiteral
instance HasPackageRefs RetrieveByKey
instance HasPackageRefs Scenario
instance HasPackageRefs SourceLoc
instance HasPackageRefs Template
instance HasPackageRefs TemplateChoice
instance HasPackageRefs TemplateKey
instance HasPackageRefs Type
instance HasPackageRefs TypeConApp
instance HasPackageRefs TypeConName
instance HasPackageRefs TypeVarName
instance HasPackageRefs Update
instance HasPackageRefs VariantConName
