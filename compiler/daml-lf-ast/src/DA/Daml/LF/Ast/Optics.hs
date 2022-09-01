-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}

module DA.Daml.LF.Ast.Optics(
    ModuleRef,
    moduleModuleRef,
    typeModuleRef,
    unlocate,
    moduleExpr,
    dataConsType,
    _PRSelfModule,
    exprValueRef,
    packageRefs,
    templateExpr,
    builtinType
    ) where

import Control.Lens
import Control.Lens.Ast
import Control.Lens.MonoTraversal
import Data.Functor.Foldable (cata, embed)
import qualified Data.NameMap as NM
import qualified Data.Text as T
import qualified Data.Set as S

import DA.Daml.LF.Ast.Base
import DA.Daml.LF.Ast.TypeLevelNat
import DA.Daml.LF.Ast.Recursive
import DA.Daml.LF.Ast.Version (Version)

import qualified Data.Kind as K (Type, Constraint)
import GHC.TypeLits

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
templateChoiceExpr f (TemplateChoice loc name consuming controllers observers selfBinder argBinder typ update) =
  TemplateChoice loc name consuming
  <$> f controllers
  <*> traverse f observers
  <*> pure selfBinder
  <*> pure argBinder
  <*> pure typ
  <*> f update

templateExpr :: Traversal' Template Expr
templateExpr f (Template loc tpl param precond signatories observers agreement choices key implements) =
  Template loc tpl param
  <$> f precond
  <*> f signatories
  <*> f observers
  <*> f agreement
  <*> (NM.traverse . templateChoiceExpr) f choices
  <*> (traverse . templateKeyExpr) f key
  <*> (NM.traverse . templateImplementsExpr) f implements

templateImplementsExpr :: Traversal' TemplateImplements Expr
templateImplementsExpr f (TemplateImplements iface body) =
  TemplateImplements iface
    <$> interfaceInstanceBodyExpr f body

interfaceInstanceBodyExpr :: Traversal' InterfaceInstanceBody Expr
interfaceInstanceBodyExpr f (InterfaceInstanceBody methods view) =
  InterfaceInstanceBody
    <$> (NM.traverse . interfaceInstanceMethodExpr) f methods
    <*> f view

interfaceInstanceMethodExpr :: Traversal' InterfaceInstanceMethod Expr
interfaceInstanceMethodExpr f (InterfaceInstanceMethod name body) =
  InterfaceInstanceMethod name <$> f body

templateKeyExpr :: Traversal' TemplateKey Expr
templateKeyExpr f (TemplateKey typ body maintainers) =
  TemplateKey typ
  <$> f body
  <*> f maintainers

moduleExpr :: Traversal' Module Expr
moduleExpr f (Module name path flags synonyms dataTypes values templates exceptions interfaces) =
  Module name path flags synonyms dataTypes
  <$> (NM.traverse . _dvalBody) f values
  <*> (NM.traverse . templateExpr) f templates
  <*> pure exceptions
  <*> pure interfaces

dataConsType :: Traversal' DataCons Type
dataConsType f = \case
  DataRecord  fs -> DataRecord  <$> (traverse . _2) f fs
  DataVariant cs -> DataVariant <$> (traverse . _2) f cs
  DataEnum cs -> pure $ DataEnum cs
  DataInterface -> pure DataInterface

builtinType :: Traversal' Type BuiltinType
builtinType f =
    \case
        TVar n -> pure $ TVar n
        TCon tyCon -> pure $ TCon tyCon
        TSynApp syn args -> TSynApp syn <$> traverse (builtinType f) args
        TApp s t -> TApp <$> builtinType f s <*> builtinType f t
        TBuiltin x -> TBuiltin <$> f x
        TForall b body -> TForall b <$> builtinType f body
        TStruct fs -> TStruct <$> (traverse . _2) (builtinType f) fs
        TNat n -> pure $ TNat n

type ModuleRef = (PackageRef, ModuleName)

-- | Traverse all the module references contained in 'Qualified's in a 'Package'.
moduleModuleRef :: Traversal' Module ModuleRef
moduleModuleRef = monoTraverse

typeModuleRef :: Traversal' Type ModuleRef
typeModuleRef = monoTraverse

instance MonoTraversable ModuleRef (Qualified a) where
  monoTraverse f (Qualified pkg0 mod0 x) =
    (\(pkg1, mod1) -> Qualified pkg1 mod1 x) <$> f (pkg0, mod0)

instance (Ord a, MonoTraversable ModuleRef a) => MonoTraversable ModuleRef (S.Set a) where
  monoTraverse f = fmap S.fromList . traverse (monoTraverse f) . S.toList

instance MonoTraversable ModuleRef ChoiceName where monoTraverse _ = pure
instance MonoTraversable ModuleRef MethodName where monoTraverse _ = pure
instance MonoTraversable ModuleRef ExprValName where monoTraverse _ = pure
instance MonoTraversable ModuleRef ExprVarName where monoTraverse _ = pure
instance MonoTraversable ModuleRef FieldName where monoTraverse _ = pure
instance MonoTraversable ModuleRef ModuleName where monoTraverse _ = pure
instance MonoTraversable ModuleRef TypeSynName where monoTraverse _ = pure
instance MonoTraversable ModuleRef TypeConName where monoTraverse _ = pure
instance MonoTraversable ModuleRef TypeVarName where monoTraverse _ = pure
instance MonoTraversable ModuleRef VariantConName where monoTraverse _ = pure
instance MonoTraversable ModuleRef Version where monoTraverse _ = pure
instance MonoTraversable ModuleRef PackageName where monoTraverse _ = pure
instance MonoTraversable ModuleRef PackageVersion where monoTraverse _ = pure

-- NOTE(MH): This is an optimization to avoid running into a dead end.
instance {-# OVERLAPPING #-} MonoTraversable ModuleRef FilePath where monoTraverse _ = pure

-- NOTE(MH): Builtins are not supposed to contain references to other modules.
instance MonoTraversable ModuleRef Kind where monoTraverse _ = pure
instance MonoTraversable ModuleRef BuiltinType where monoTraverse _ = pure
instance MonoTraversable ModuleRef BuiltinExpr where monoTraverse _ = pure

-- NOTE(SC): SourceLoc *does* have a ModuleRef in it; however, its main use is
-- to collect all ModuleRefs in a module or package in order to figure out its
-- dependencies. Inlining can cause location information to reference the
-- original source file although there's not a proper dependency; in other
-- words, with a visible SourceLoc ModuleRef, the dep graph would be somewhere
-- between the actual dep graph and its transitive closure. See
-- https://github.com/digital-asset/daml/pull/2327#discussion_r308445649 for
-- discussion
instance MonoTraversable ModuleRef SourceLoc where monoTraverse _ = pure

instance MonoTraversable ModuleRef TypeLevelNat where monoTraverse _ = pure

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
instance MonoTraversable ModuleRef DefTypeSyn
instance MonoTraversable ModuleRef DefException

instance MonoTraversable ModuleRef InterfaceMethod
instance MonoTraversable ModuleRef DefInterface
instance MonoTraversable ModuleRef InterfaceCoImplements

instance MonoTraversable ModuleRef IsTest
instance MonoTraversable ModuleRef DefValue

instance MonoTraversable ModuleRef Bool where monoTraverse _ = pure
-- NOTE(MH): Stakeholder signatures /currently/ don't contain references to
-- other modules. This might change in the future.
instance MonoTraversable ModuleRef TemplateChoice
instance MonoTraversable ModuleRef TemplateKey
instance MonoTraversable ModuleRef Template
instance MonoTraversable ModuleRef TemplateImplements
instance MonoTraversable ModuleRef InterfaceInstanceBody
instance MonoTraversable ModuleRef InterfaceInstanceMethod

instance MonoTraversable ModuleRef FeatureFlags
instance MonoTraversable ModuleRef Module
instance MonoTraversable ModuleRef PackageMetadata
instance MonoTraversable ModuleRef Package
instance MonoTraversable ModuleRef T.Text where monoTraverse _ = pure

instance {-# OVERLAPPABLE #-} (MonoTraversableNameMap ModuleRef a) => MonoTraversable ModuleRef (NM.NameMap a) where
  monoTraverse = NM.traverse . monoTraverse

-- The following two instances overlap the one above.
--
-- They should be okay since 'NM.traverseAsList' doesn't fail if the names change,
-- and we don't care if these particular 'NM.NameMap's change names
-- (which isn't necessarily the case for other 'NM.NameMap's).
--
-- Note that if a traversal changes two elements so their new names are equal,
-- this will still be a runtime error, but 'DA.Daml.Compiler.Validate.validateWellTyped'
-- (through 'packageRefs'), the only place where these instances are used as a
-- traversal rather than just a fold, just rewrites 'PRImport's that refer to
-- the package being validated into 'PRSelf', and since the module was decoded with
-- 'DecodeAsDependency', there can be no pre-existing 'PRSelf's to clash with.
instance {-# OVERLAPPING #-} MonoTraversable ModuleRef (NM.NameMap TemplateImplements) where
  monoTraverse = NM.traverseAsList . monoTraverse
instance {-# OVERLAPPING #-} MonoTraversable ModuleRef (NM.NameMap InterfaceCoImplements) where
  monoTraverse = NM.traverseAsList . monoTraverse

type MonoTraversableNameMap e a =
  ( NM.Named a
  , MonoTraversable e a
  , MonoTraversableNameMapName e (NM.Name a) a
  )

-- | @MonoTraversableNameMapName e n a@ is used to check that a type @a@ such that
--
-- > instance NM.Named a where
-- >   type NM.Name a = n
-- >   name = (...)
--
-- is acceptable for use with
--
-- > monoTraverse @e @(NM.NameMap a)
--
-- This check is necessary because 'NM.traverse' fails at runtime if the
-- supplied function changes the names of the items in the map.
--
-- An empty instance of this class means that @n@ is an acceptable 'NM.Name'
-- for @a@ when 'monoTraverse' targets values of type @e@, in other words,
--
-- > monoTraverse @e @(NM.NameMap a)
--
-- is not expected to change the names of type @n@.
--
-- An instance with a constraint 'BadMonoTraversableNameMapName e n a' means
-- that @n@ is _not_ an acceptable name for @a@, because 'monoTraverse' would
-- change the names at runtime, causing 'NM.traverse' to fail.
--
class MonoTraversableNameMapName e n a
instance MonoTraversableNameMapName ModuleRef ModuleName a
instance MonoTraversableNameMapName ModuleRef TypeSynName a
instance MonoTraversableNameMapName ModuleRef ExprValName a
instance MonoTraversableNameMapName ModuleRef TypeConName a
instance MonoTraversableNameMapName ModuleRef MethodName a
instance MonoTraversableNameMapName ModuleRef ChoiceName a

-- | Given @type NM.Name a = Qualified _@,
--
-- > monoTraverse @ModuleRef @(NM.NameMap a)
--
-- is very likely to fail because @Qualified x@ is effectively a tuple of
-- @(ModuleRef, a)@, so any function on @ModuleRef@ would change the names.
--
instance BadMonoTraversableNameMapName ModuleRef (Qualified x) a
      => MonoTraversableNameMapName ModuleRef (Qualified x) a

type family BadMonoTraversableNameMapName (e :: K.Type) (n :: K.Type) (a :: K.Type) :: K.Constraint where
  BadMonoTraversableNameMapName e n a =
    TypeError
      ( 'Text "Cannot use " ':<>: 'ShowType n
        ':$$: 'Text "as the Name of " ':<>: 'ShowType a
        ':$$: 'Text "because it would break"
        ':$$: 'Text "  monoTraverse @"
          ':<>: 'ShowType e
          ':<>: 'Text " @("
          ':<>: 'ShowType (NM.NameMap a)
          ':<>: 'Text ")"
      )

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

packageRefs :: MonoTraversable ModuleRef a => Traversal' a PackageRef
packageRefs = monoTraverse @ModuleRef . _1
