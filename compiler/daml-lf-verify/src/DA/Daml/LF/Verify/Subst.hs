-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Term substitions for DAML LF static verification
module DA.Daml.LF.Verify.Subst
  ( ExprSubst
  , singleExprSubst
  , singleTypeSubst
  , substituteTmTm
  , substituteTyTm
  ) where

import qualified Data.Map.Strict as Map
import DA.Daml.LF.Ast

-- | Substitution of expressions for expression variables.
type ExprSubst = Map.Map ExprVarName Expr
-- TODO: This already exists. remove duplication?
-- | Substitution of types for type variables.
type TypeSubst = Map.Map TypeVarName Type

singleExprSubst :: ExprVarName -> Expr -> ExprSubst
singleExprSubst = Map.singleton

singleTypeSubst :: TypeVarName -> Type -> TypeSubst
singleTypeSubst = Map.singleton

-- | Apply an expression substitution to an expression.
substituteTmTm :: ExprSubst -> Expr -> Expr
substituteTmTm = undefined -- TODO

-- | Apply a type substitution to an expression.
substituteTyTm :: TypeSubst -> Expr -> Expr
substituteTyTm = undefined -- TODO

