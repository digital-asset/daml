-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Term substitions for DAML LF static verification
module DA.Daml.LF.Verify.Subst
  ( ExprSubst
  , singleExprSubst
  , singleTypeSubst
  , createExprSubst
  , SubstTm(..)
  , SubstTy(..)
  , InstPR(..)
  ) where

import Control.Lens hiding (Context)
import qualified Data.Map.Strict as Map
import DA.Daml.LF.Ast
import DA.Daml.LF.Ast.Type

-- | Substitution of expressions for expression variables.
type ExprSubst = Map.Map ExprVarName Expr

-- | Create an expression substitution from a single variable and expression.
singleExprSubst :: ExprVarName
  -- ^ The expression variable to substitute.
  -> Expr
  -- ^ The expression to substitute with.
  -> ExprSubst
singleExprSubst = Map.singleton

-- | Create a type substitution from a single type variable and type.
singleTypeSubst :: TypeVarName
  -- ^ The type variable to substitute.
  -> Type
  -- ^ The type to substitute with.
  -> Subst
singleTypeSubst = Map.singleton

-- | Create an expression substitution from a list of variables and expressions.
createExprSubst :: [(ExprVarName,Expr)]
  -- ^ The variables to substitute, together with the expressions to replace them with.
  -> ExprSubst
createExprSubst = Map.fromList

-- | Get the domain from an expression substitution.
substDom :: ExprSubst
  -- ^ The substitution to analyse.
  -> [ExprVarName]
substDom = Map.keys

-- | A class covering the data types to which an expression substitution can be applied.
class SubstTm a where
  -- | Apply an expression substitution.
  substituteTm :: ExprSubst
    -- ^ The expression substitution to apply.
    -> a
    -- ^ The data to apply the substitution to.
    -> a

-- | A class covering the data types to which a type substitution can be applied.
class SubstTy a where
  -- | Apply an type substitution.
  substituteTy :: Subst
    -- ^ The type substitution to apply.
    -> a
    -- ^ The data to apply the substitution to.
    -> a

-- TODO: We assume that for any substitution x |-> e : x notin e
-- and a |-> t : a notin t.
instance SubstTm Expr where
  substituteTm s = \case
    EVar x
      | Just e <- Map.lookup x s -> e
      | otherwise -> EVar x
    ERecCon t fs -> ERecCon t $ map (over _2 (substituteTm s)) fs
    ERecProj t f e -> ERecProj t f $ substituteTm s e
    ERecUpd t f e1 e2 -> ERecUpd t f (substituteTm s e1) (substituteTm s e2)
    EVariantCon t v e -> EVariantCon t v (substituteTm s e)
    EStructCon fs -> EStructCon $ map (over _2 (substituteTm s)) fs
    EStructProj f e -> EStructProj f (substituteTm s e)
    EStructUpd f e1 e2 -> EStructUpd f (substituteTm s e1) (substituteTm s e2)
    ETmApp e1 e2 -> ETmApp (substituteTm s e1) (substituteTm s e2)
    ETyApp e t -> ETyApp (substituteTm s e) t
    ETmLam (x,t) e -> if x `elem` substDom s
      then ETmLam (x,t) e
      else ETmLam (x,t) (substituteTm s e)
    ETyLam (a,k) e -> ETyLam (a,k) (substituteTm s e)
    ECase e cs -> ECase (substituteTm s e)
      $ map (\CaseAlternative{..} -> CaseAlternative altPattern (substituteTm s altExpr)) cs
    ELet Binding{..} e -> ELet (Binding bindingBinder $ substituteTm s bindingBound)
      (substituteTm s e)
    ECons t e1 e2 -> ECons t (substituteTm s e1) (substituteTm s e2)
    ESome t e -> ESome t (substituteTm s e)
    EToAny t e -> EToAny t (substituteTm s e)
    EFromAny t e -> EFromAny t (substituteTm s e)
    EUpdate u -> EUpdate $ substituteTm s u
    ELocation l e -> ELocation l (substituteTm s e)
    e -> e

instance SubstTm Update where
  substituteTm s = \case
    UPure t e -> UPure t $ substituteTm s e
    UBind Binding{..} e -> UBind (Binding bindingBinder $ substituteTm s bindingBound)
      (substituteTm s e)
    UCreate t e -> UCreate t $ substituteTm s e
    UExercise t c e1 a e2 -> UExercise t c (substituteTm s e1) a (substituteTm s e2)
    UFetch t e -> UFetch t $ substituteTm s e
    UEmbedExpr t e -> UEmbedExpr t $ substituteTm s e
    u -> u

instance SubstTm a => SubstTm (Maybe a) where
  substituteTm s m = substituteTm s <$> m

instance SubstTy Expr where
  substituteTy s = \case
    ERecCon t fs -> ERecCon (substituteTy s t) $ map (over _2 (substituteTy s)) fs
    ERecProj t f e -> ERecProj (substituteTy s t) f $ substituteTy s e
    ERecUpd t f e1 e2 -> ERecUpd (substituteTy s t) f (substituteTy s e1)
      (substituteTy s e2)
    EVariantCon t v e -> EVariantCon (substituteTy s t) v (substituteTy s e)
    EStructCon fs -> EStructCon $ map (over _2 (substituteTy s)) fs
    EStructProj f e -> EStructProj f $ substituteTy s e
    EStructUpd f e1 e2 -> EStructUpd f (substituteTy s e1) (substituteTy s e2)
    ETmApp e1 e2 -> ETmApp (substituteTy s e1) (substituteTy s e2)
    ETyApp e t -> ETyApp (substituteTy s e) (substitute s t)
    ETmLam (n, t) e -> ETmLam (n, substitute s t) (substituteTy s e)
    ETyLam b e -> ETyLam b $ substituteTy s e
    ECase e cs -> ECase (substituteTy s e) (map (substituteTy s) cs)
    ELet Binding{..} e -> ELet (Binding (over _2 (substitute s) bindingBinder) (substituteTy s bindingBound))
      (substituteTy s e)
    ENil t -> ENil (substitute s t)
    ECons t e1 e2 -> ECons (substitute s t) (substituteTy s e1) (substituteTy s e2)
    ESome t e -> ESome (substitute s t) (substituteTy s e)
    ENone t -> ENone (substitute s t)
    EToAny t e -> EToAny (substitute s t) (substituteTy s e)
    EFromAny t e -> EFromAny (substitute s t) (substituteTy s e)
    ETypeRep t -> ETypeRep (substitute s t)
    EUpdate u -> EUpdate (substituteTy s u)
    ELocation l e -> ELocation l (substituteTy s e)
    e -> e

instance SubstTy Update where
  substituteTy s = \case
    UPure t e -> UPure (substitute s t) (substituteTy s e)
    UBind Binding{..} e -> UBind (Binding (over _2 (substitute s) bindingBinder) (substituteTy s bindingBound))
      (substituteTy s e)
    UCreate n e -> UCreate n (substituteTy s e)
    UExercise n c e1 a e2 -> UExercise n c (substituteTy s e1) a (substituteTy s e2)
    UFetch n e -> UFetch n (substituteTy s e)
    UEmbedExpr t e -> UEmbedExpr (substitute s t) (substituteTy s e)
    u -> u

instance SubstTy TypeConApp where
  substituteTy s (TypeConApp n ts) = TypeConApp n (map (substitute s) ts)

instance SubstTy CaseAlternative where
  substituteTy s (CaseAlternative p e) = CaseAlternative p (substituteTy s e)

-- | A class covering the data types containing package references which can be
-- instantiated..
class InstPR a where
  -- | Instantiate `PRSelf` with the given package reference.
  instPRSelf :: PackageRef
    -- ^ The package reference to substitute with.
    -> a
    -- ^ The data type to substitute in.
    -> a

instance InstPR (Qualified a) where
  instPRSelf pac qx@(Qualified pac' mod x) = case pac' of
    PRSelf -> Qualified pac mod x
    _ -> qx

instance InstPR Expr where
  instPRSelf pac = \case
    EVal val -> EVal (instPRSelf pac val)
    ERecCon t fs -> ERecCon t $ map (over _2 (instPRSelf pac)) fs
    ERecProj t f e -> ERecProj t f $ instPRSelf pac e
    ERecUpd t f e1 e2 -> ERecUpd t f (instPRSelf pac e1) (instPRSelf pac e2)
    EVariantCon t v e -> EVariantCon t v (instPRSelf pac e)
    EStructCon fs -> EStructCon $ map (over _2 (instPRSelf pac)) fs
    EStructProj f e -> EStructProj f (instPRSelf pac e)
    EStructUpd f e1 e2 -> EStructUpd f (instPRSelf pac e1) (instPRSelf pac e2)
    ETmApp e1 e2 -> ETmApp (instPRSelf pac e1) (instPRSelf pac e2)
    ETyApp e t -> ETyApp (instPRSelf pac e) t
    ETmLam b e -> ETmLam b (instPRSelf pac e)
    ETyLam b e -> ETyLam b (instPRSelf pac e)
    ECase e cs -> ECase (instPRSelf pac e)
      $ map (\CaseAlternative{..} -> CaseAlternative altPattern (instPRSelf pac altExpr)) cs
    ELet Binding{..} e -> ELet (Binding bindingBinder $ instPRSelf pac bindingBound)
      (instPRSelf pac e)
    ECons t e1 e2 -> ECons t (instPRSelf pac e1) (instPRSelf pac e2)
    ESome t e -> ESome t (instPRSelf pac e)
    EToAny t e -> EToAny t (instPRSelf pac e)
    EFromAny t e -> EFromAny t (instPRSelf pac e)
    EUpdate u -> EUpdate $ instPRSelf pac u
    ELocation l e -> ELocation l (instPRSelf pac e)
    e -> e

instance InstPR Update where
  instPRSelf pac = \case
    UPure t e -> UPure t (instPRSelf pac e)
    UBind Binding{..} e -> UBind (Binding bindingBinder $ instPRSelf pac bindingBound)
      (instPRSelf pac e)
    UCreate tem arg -> UCreate (instPRSelf pac tem) (instPRSelf pac arg)
    UExercise tem ch cid act arg -> UExercise (instPRSelf pac tem) ch
      (instPRSelf pac cid) (instPRSelf pac <$> act) (instPRSelf pac arg)
    UFetch tem cid -> UFetch (instPRSelf pac tem) (instPRSelf pac cid)
    UEmbedExpr t e -> UEmbedExpr t (instPRSelf pac e)
    u -> u
