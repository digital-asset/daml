-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Doc.Extract.TypeExpr
    ( typeToContext
    , typeToType
    ) where

import DA.Daml.Doc.Types as DDoc

import DA.Daml.Doc.Extract.Types
import DA.Daml.Doc.Extract.Util

import "ghc-lib-parser" TyCoRep
import "ghc-lib-parser" TyCon
import "ghc-lib-parser" Type

-- | Extract context from GHC type. Returns Nothing if there are no constraints.
typeToContext :: DocCtx -> TyCoRep.Type -> DDoc.Context
typeToContext dc ty = Context (typeToConstraints dc ty)

-- | Is this type a constraint? Constraints are either typeclass constraints,
-- constraint tuples, or whatever else GHC decides is a constraint.
isConstraintType :: TyCoRep.Type -> Bool
isConstraintType = tcIsConstraintKind . Type.typeKind

-- | Extract constraints from GHC type, returning list of constraints.
typeToConstraints :: DocCtx -> TyCoRep.Type -> [DDoc.Type]
typeToConstraints dc = \case
    FunTy a b | isConstraintType a ->
        typeToType dc a : typeToConstraints dc b
    FunTy _ b ->
        typeToConstraints dc b
    ForAllTy _ b -> -- TODO: I think forall can introduce constraints?
        typeToConstraints dc b
    _ -> []

-- | Convert GHC Type into a damldoc type, ignoring constraints.
typeToType :: DocCtx -> TyCoRep.Type -> DDoc.Type
typeToType ctx = \case
    TyVarTy var -> TypeApp Nothing (Typename $ packId var) []

    TyConApp tycon bs | isTupleTyCon tycon ->
        TypeTuple (map (typeToType ctx) bs)

    TyConApp tycon [b] | "[]" == packName (tyConName tycon) ->
        TypeList (typeToType ctx b)

    -- Special case for unsaturated (->) to remove the levity arguments.
    TyConApp tycon (_:_:bs) | isFunTyCon tycon ->
        TypeApp
            Nothing
            (Typename "->")
            (map (typeToType ctx) bs)

    TyConApp tycon bs ->
        TypeApp
            (tyConReference ctx tycon)
            (tyConTypename ctx tycon)
            (map (typeToType ctx) bs)

    AppTy a b ->
        case typeToType ctx a of
            TypeApp m f bs -> TypeApp m f (bs <> [typeToType ctx b]) -- flatten app chains
            TypeFun _ -> unexpected "function type in a type app"
            TypeList _ -> unexpected "list type in a type app"
            TypeTuple _ -> unexpected "tuple type in a type app"
            TypeLit _ -> unexpected "type-level literal in a type app"

    -- ignore context
    ForAllTy _ b -> typeToType ctx b
    FunTy a b | isConstraintType a ->
        typeToType ctx b

    FunTy a b ->
        case typeToType ctx b of
            TypeFun bs -> TypeFun (typeToType ctx a : bs) -- flatten function types
            b' -> TypeFun [typeToType ctx a, b']

    CastTy a _ -> typeToType ctx a
    LitTy lit -> TypeLit (packTyLit lit)
    CoercionTy _ -> unexpected "coercion" -- TODO?

  where
    -- | Unhandled case.
    unexpected x = error $ "typeToType: found an unexpected " <> x
