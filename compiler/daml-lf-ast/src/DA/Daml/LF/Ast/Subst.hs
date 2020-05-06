-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Substitution in LF expressions.
module DA.Daml.LF.Ast.Subst
    ( Subst (..)
    , typeSubst
    , typeSubst'
    , exprSubst
    , exprSubst'
    , substExpr
    , substType
    ) where

import DA.Daml.LF.Ast
import DA.Daml.LF.Ast.FreeVars
import qualified DA.Daml.LF.Ast.Type as Type

import Control.Arrow (second)
import qualified Data.Map.Strict as Map

data Subst = Subst
    { substTypes :: !(Map.Map TypeVarName Type)
        -- ^ types to substitute
    , substExprs :: !(Map.Map ExprVarName Expr)
        -- ^ expressions to substitute
    , substExhausted :: !FreeVars
        -- ^ exhausted variables. These are all the free variables that
        -- appear in substTypes or substExprs, plus the variables that
        -- were bound above us, plus the variables that were substituted
        -- away. This is mainly used to generate fresh variables that
        -- don't conflict with existing variables, so over-approximation
        -- is fine.
    }

instance Monoid Subst where
    mempty = Subst
        { substTypes = Map.empty
        , substExprs = Map.empty
        , substExhausted = mempty
        }

instance Semigroup Subst where
    s1 <> s2 = Subst
        { substTypes = substTypes s1 <> substTypes s2
        , substExprs = substExprs s1 <> substExprs s2
        , substExhausted = substExhausted s1 <> substExhausted s2
        }

typeSubst' :: TypeVarName -> Type -> FreeVars -> Subst
typeSubst' x t fvs = Subst
    { substTypes = Map.fromList [(x,t)]
    , substExprs = Map.empty
    , substExhausted = freeTypeVar x <> fvs
    }

exprSubst' :: ExprVarName -> Expr -> FreeVars -> Subst
exprSubst' x e fvs = Subst
    { substTypes = Map.empty
    , substExprs = Map.fromList [(x,e)]
    , substExhausted = freeExprVar x <> fvs
    }

typeSubst :: TypeVarName -> Type -> Subst
typeSubst x t = typeSubst' x t (freeVarsInType t)

exprSubst :: ExprVarName -> Expr -> Subst
exprSubst x e = exprSubst' x e (freeVarsInExpr e)

substType :: Subst -> Type -> Type
substType Subst{..} =
    Type.substituteAux (freeTypeVars substExhausted) substTypes

substTypeConApp :: Subst -> TypeConApp -> TypeConApp
substTypeConApp subst (TypeConApp qtcon ts) =
    TypeConApp qtcon (map (substType subst) ts)

substFields :: Subst -> [(FieldName, Expr)] -> [(FieldName, Expr)]
substFields subst = map (second (substExpr subst))

substWithBoundExprVar :: Subst -> ExprVarName -> (Subst -> ExprVarName -> t) -> t
substWithBoundExprVar subst@Subst{..} x f
    | isFreeExprVar x substExhausted =
        let x' = freshenExprVar substExhausted x
            subst' = subst
                { substExhausted = freeExprVar x' <> substExhausted
                , substExprs = Map.insert x (EVar x') substExprs
                }
        in f subst' x'

    | otherwise =
        let subst' = subst
                { substExhausted = freeExprVar x <> substExhausted
                }
        in f subst' x

substWithBoundTypeVar :: Subst -> TypeVarName -> (Subst -> TypeVarName -> t) -> t
substWithBoundTypeVar subst@Subst{..} x f
    | isFreeTypeVar x substExhausted =
        let x' = freshenTypeVar substExhausted x
            subst' = subst
                { substExhausted = freeTypeVar x' <> substExhausted
                , substTypes = Map.insert x (TVar x') substTypes
                }
        in f subst' x'

    | otherwise =
        let subst' = subst
                { substExhausted = freeTypeVar x <> substExhausted
                }
        in f subst' x

substExpr :: Subst -> Expr -> Expr
substExpr subst@Subst{..} = \case
    EVar x ->
        case Map.lookup x substExprs of
            Just e -> e
            Nothing -> EVar x
    e@(EVal _) -> e
    e@(EBuiltin _) -> e
    ERecCon t fs -> ERecCon
        (substTypeConApp subst t)
        (substFields subst fs)
    ERecProj t f e -> ERecProj
        (substTypeConApp subst t)
        f
        (substExpr subst e)
    ERecUpd t f e1 e2 -> ERecUpd
        (substTypeConApp subst t)
        f
        (substExpr subst e1)
        (substExpr subst e2)
    EVariantCon t v e -> EVariantCon
        (substTypeConApp subst t)
        v
        (substExpr subst e)
    e@(EEnumCon _ _) -> e
    EStructCon fs -> EStructCon
        (substFields subst fs)
    EStructProj f e -> EStructProj
        f
        (substExpr subst e)
    EStructUpd f e1 e2 -> EStructUpd
        f
        (substExpr subst e1)
        (substExpr subst e2)
    ETmApp e1 e2 -> ETmApp
        (substExpr subst e1)
        (substExpr subst e2)
    ETyApp e t -> ETyApp
        (substExpr subst e)
        (substType subst t)
    ETmLam (x,t) e ->
        substWithBoundExprVar subst x $ \ subst' x' ->
            ETmLam (x', substType subst t) (substExpr subst' e)
    ETyLam (x,k) e ->
        substWithBoundTypeVar subst x $ \ subst' x' ->
            ETyLam (x', k) (substExpr subst' e)
    ECase e alts -> ECase
        (substExpr subst e)
        (map (substAlternative subst) alts)
    ELet (Binding (x, t) e1) e2 ->
        substWithBoundExprVar subst x $ \ subst' x' ->
            ELet (Binding (x', substType subst t) (substExpr subst e1))
                (substExpr subst' e2)
    ENil t -> ENil
        (substType subst t)
    ECons t e1 e2 -> ECons
        (substType subst t)
        (substExpr subst e1)
        (substExpr subst e2)
    ENone t -> ENone
        (substType subst t)
    ESome t e -> ESome
        (substType subst t)
        (substExpr subst e)
    EToAny t e -> EToAny
        (substType subst t)
        (substExpr subst e)
    EFromAny t e -> EFromAny
        (substType subst t)
        (substExpr subst e)
    ETypeRep t -> ETypeRep
        (substType subst t)
    EUpdate u -> EUpdate
        (substUpdate subst u)
    EScenario s -> EScenario
        (substScenario subst s)
    ELocation l e -> ELocation
        l
        (substExpr subst e)

substAlternative :: Subst -> CaseAlternative -> CaseAlternative
substAlternative subst (CaseAlternative p e) =
    substWithPattern subst p $ \ subst' p' ->
        CaseAlternative p' (substExpr subst' e)

substWithPattern :: Subst -> CasePattern -> (Subst -> CasePattern -> t) -> t
substWithPattern subst p f = case p of
    CPVariant t v x ->
        substWithBoundExprVar subst x $ \ subst' x' ->
            f subst' (CPVariant t v x')
    CPEnum _ _ -> f subst p
    CPUnit -> f subst p
    CPBool _ -> f subst p
    CPNil -> f subst p
    CPCons x1 x2 ->
        substWithBoundExprVar subst  x1 $ \ subst'  x1' ->
        substWithBoundExprVar subst' x2 $ \ subst'' x2' ->
            f subst'' (CPCons x1' x2')
    CPNone -> f subst p
    CPSome x ->
        substWithBoundExprVar subst x $ \ subst' x' ->
            f subst' (CPSome x')
    CPDefault -> f subst p

substUpdate :: Subst -> Update -> Update
substUpdate subst = \case
    UPure t e -> UPure
        (substType subst t)
        (substExpr subst e)
    UBind (Binding (x, t) e1) e2 ->
        substWithBoundExprVar subst x $ \ subst' x' ->
            UBind (Binding (x', substType subst t) (substExpr subst e1))
                (substExpr subst' e2)
    UCreate templateName e -> UCreate
        templateName
        (substExpr subst e)
    UExercise templateName choiceName e1 e2M e3 -> UExercise
        templateName
        choiceName
        (substExpr subst e1)
        (substExpr subst <$> e2M)
        (substExpr subst e3)
    UFetch templateName e -> UFetch
        templateName
        (substExpr subst e)
    e@UGetTime -> e
    UEmbedExpr t e -> UEmbedExpr
        (substType subst t)
        (substExpr subst e)
    ULookupByKey (RetrieveByKey templateName e) -> ULookupByKey $ RetrieveByKey
        templateName
        (substExpr subst e)
    UFetchByKey (RetrieveByKey templateName e) -> UFetchByKey $ RetrieveByKey
        templateName
        (substExpr subst e)

substScenario :: Subst -> Scenario -> Scenario
substScenario subst = \case
    SPure t e -> SPure
        (substType subst t)
        (substExpr subst e)
    SBind (Binding (x,t) e1) e2 ->
        substWithBoundExprVar subst x $ \ subst' x' ->
            SBind (Binding (x', substType subst t) (substExpr subst e1))
                (substExpr subst' e2)
    SCommit t e1 e2 -> SCommit
        (substType subst t)
        (substExpr subst e1)
        (substExpr subst e2)
    SMustFailAt t e1 e2 -> SMustFailAt
        (substType subst t)
        (substExpr subst e1)
        (substExpr subst e2)
    SPass e -> SPass
        (substExpr subst e)
    e@SGetTime -> e
    SGetParty e -> SGetParty
        (substExpr subst e)
    SEmbedExpr t e -> SEmbedExpr
        (substType subst t)
        (substExpr subst e)