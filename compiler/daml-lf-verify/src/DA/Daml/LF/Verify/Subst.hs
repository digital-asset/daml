-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Term substitions for DAML LF static verification
module DA.Daml.LF.Verify.Subst
  ( ExprSubst
  , singleExprSubst
  , singleTypeSubst
  , createExprSubst
  , substituteTmTm
  , substituteTyTm
  ) where

import Control.Lens hiding (Context)
import qualified Data.Map.Strict as Map
import DA.Daml.LF.Ast
import DA.Daml.LF.Ast.Type

-- | Substitution of expressions for expression variables.
type ExprSubst = Map.Map ExprVarName Expr

singleExprSubst :: ExprVarName -> Expr -> ExprSubst
singleExprSubst = Map.singleton

singleTypeSubst :: TypeVarName -> Type -> Subst
singleTypeSubst = Map.singleton

createExprSubst :: [(ExprVarName,Expr)] -> ExprSubst
createExprSubst = Map.fromList

substDom :: ExprSubst -> [ExprVarName]
substDom = Map.keys

-- | Apply an expression substitution to an expression.
-- TODO: We assume that for any substitution x |-> e : x notin e
-- and a |-> t : a notin t.
-- TODO: I can't help but feel there has to be a nicer way to write this function
substituteTmTm :: ExprSubst -> Expr -> Expr
substituteTmTm s = \case
  EVar x
    | Just e <- Map.lookup x s -> e
    | otherwise -> EVar x
  ERecCon t fs -> ERecCon t $ map (over _2 (substituteTmTm s)) fs
  ERecProj t f e -> ERecProj t f $ substituteTmTm s e
  ERecUpd t f e1 e2 -> ERecUpd t f (substituteTmTm s e1) (substituteTmTm s e2)
  EVariantCon t v e -> EVariantCon t v (substituteTmTm s e)
  EStructCon fs -> EStructCon $ map (over _2 (substituteTmTm s)) fs
  EStructProj f e -> EStructProj f (substituteTmTm s e)
  EStructUpd f e1 e2 -> EStructUpd f (substituteTmTm s e1) (substituteTmTm s e2)
  ETmApp e1 e2 -> ETmApp (substituteTmTm s e1) (substituteTmTm s e2)
  ETyApp e t -> ETyApp (substituteTmTm s e) t
  ETmLam (x,t) e -> if x `elem` substDom s
    then ETmLam (x,t) e
    else ETmLam (x,t) (substituteTmTm s e)
  ETyLam (a,k) e -> ETyLam (a,k) (substituteTmTm s e)
  ECase e cs -> ECase (substituteTmTm s e)
    $ map (\CaseAlternative{..} -> CaseAlternative altPattern (substituteTmTm s altExpr)) cs
  ELet Binding{..} e -> ELet (Binding bindingBinder $ substituteTmTm s bindingBound)
    (substituteTmTm s e)
  ECons t e1 e2 -> ECons t (substituteTmTm s e1) (substituteTmTm s e2)
  ESome t e -> ESome t (substituteTmTm s e)
  EToAny t e -> EToAny t (substituteTmTm s e)
  EFromAny t e -> EFromAny t (substituteTmTm s e)
  EUpdate u -> EUpdate $ substituteTmUpd s u
  ELocation l e -> ELocation l (substituteTmTm s e)
  e -> e

-- | Apply an expression substitution to an update.
substituteTmUpd :: ExprSubst -> Update -> Update
substituteTmUpd s = \case
  UPure t e -> UPure t $ substituteTmTm s e
  UBind Binding{..} e -> UBind (Binding bindingBinder $ substituteTmTm s bindingBound)
    (substituteTmTm s e)
  UCreate t e -> UCreate t $ substituteTmTm s e
  UExercise t c e1 a e2 -> UExercise t c (substituteTmTm s e1) a (substituteTmTm s e2)
  UFetch t e -> UFetch t $ substituteTmTm s e
  UEmbedExpr t e -> UEmbedExpr t $ substituteTmTm s e
  u -> u

-- | Apply a type substitution to an expression.
substituteTyTm :: Subst -> Expr -> Expr
substituteTyTm s = \case
  ERecCon t fs -> ERecCon (substituteTyTCApp s t) $ map (over _2 (substituteTyTm s)) fs
  ERecProj t f e -> ERecProj (substituteTyTCApp s t) f $ substituteTyTm s e
  ERecUpd t f e1 e2 -> ERecUpd (substituteTyTCApp s t) f (substituteTyTm s e1)
    (substituteTyTm s e2)
  EVariantCon t v e -> EVariantCon (substituteTyTCApp s t) v (substituteTyTm s e)
  EStructCon fs -> EStructCon $ map (over _2 (substituteTyTm s)) fs
  EStructProj f e -> EStructProj f $ substituteTyTm s e
  EStructUpd f e1 e2 -> EStructUpd f (substituteTyTm s e1) (substituteTyTm s e2)
  ETmApp e1 e2 -> ETmApp (substituteTyTm s e1) (substituteTyTm s e2)
  ETyApp e t -> ETyApp (substituteTyTm s e) (substitute s t)
  ETmLam (n, t) e -> ETmLam (n, substitute s t) (substituteTyTm s e)
  ETyLam b e -> ETyLam b $ substituteTyTm s e
  ECase e cs -> ECase (substituteTyTm s e) (map (substituteTyCaseAlt s) cs)
  ELet Binding{..} e -> ELet (Binding (over _2 (substitute s) bindingBinder) (substituteTyTm s bindingBound))
    (substituteTyTm s e)
  ENil t -> ENil (substitute s t)
  ECons t e1 e2 -> ECons (substitute s t) (substituteTyTm s e1) (substituteTyTm s e2)
  ESome t e -> ESome (substitute s t) (substituteTyTm s e)
  ENone t -> ENone (substitute s t)
  EToAny t e -> EToAny (substitute s t) (substituteTyTm s e)
  EFromAny t e -> EFromAny (substitute s t) (substituteTyTm s e)
  ETypeRep t -> ETypeRep (substitute s t)
  EUpdate u -> EUpdate (substituteTyUpd s u)
  ELocation l e -> ELocation l (substituteTyTm s e)
  e -> e

-- | Apply a type substitution to an update.
substituteTyUpd :: Subst -> Update -> Update
substituteTyUpd s = \case
  UPure t e -> UPure (substitute s t) (substituteTyTm s e)
  UBind Binding{..} e -> UBind (Binding (over _2 (substitute s) bindingBinder) (substituteTyTm s bindingBound))
    (substituteTyTm s e)
  UCreate n e -> UCreate n (substituteTyTm s e)
  UExercise n c e1 a e2 -> UExercise n c (substituteTyTm s e1) a (substituteTyTm s e2)
  UFetch n e -> UFetch n (substituteTyTm s e)
  UEmbedExpr t e -> UEmbedExpr (substitute s t) (substituteTyTm s e)
  u -> u

substituteTyTCApp :: Subst -> TypeConApp -> TypeConApp
substituteTyTCApp s (TypeConApp n ts) = TypeConApp n (map (substitute s) ts)

substituteTyCaseAlt :: Subst -> CaseAlternative -> CaseAlternative
substituteTyCaseAlt s (CaseAlternative p e) = CaseAlternative p (substituteTyTm s e)
