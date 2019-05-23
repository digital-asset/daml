-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes #-}
module DA.Daml.LF.Ast.Optics(
    moduleModuleRef,
    unlocate,
    moduleExpr,
    dataConsType,
    _PRSelfModule,
    exprPartyLiteral,
    exprValueRef,
    templateExpr
    ) where

import Control.Lens
import Control.Lens.Ast
import Control.Lens.MonoTraversal
import Data.Functor.Foldable (cata, embed)
import qualified Data.NameMap as NM

import DA.Daml.LF.Ast.Base
import DA.Daml.LF.Ast.Recursive

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
instance MonoTraversable ModuleRef EnumCon where monoTraverse _ = pure
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
