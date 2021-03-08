-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Substitution in LF expressions.
module DA.Daml.LF.Ast.Subst
    ( Subst (..)
    , typeSubst
    , typeSubst'
    , exprSubst
    , exprSubst'
    , applySubstInExpr
    , applySubstInType
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

applySubstInType :: Subst -> Type -> Type
applySubstInType Subst{..} =
    Type.substituteAux (freeTypeVars substExhausted) substTypes

applySubstInTypeConApp :: Subst -> TypeConApp -> TypeConApp
applySubstInTypeConApp subst (TypeConApp qtcon ts) =
    TypeConApp qtcon (map (applySubstInType subst) ts)

substFields :: Subst -> [(FieldName, Expr)] -> [(FieldName, Expr)]
substFields subst = map (second (applySubstInExpr subst))

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

applySubstInExpr :: Subst -> Expr -> Expr
applySubstInExpr subst@Subst{..} = \case
    EVar x ->
        case Map.lookup x substExprs of
            Just e -> e
            Nothing -> EVar x
    e@(EVal _) -> e
    e@(EBuiltin _) -> e
    ERecCon t fs -> ERecCon
        (applySubstInTypeConApp subst t)
        (substFields subst fs)
    ERecProj t f e -> ERecProj
        (applySubstInTypeConApp subst t)
        f
        (applySubstInExpr subst e)
    ERecUpd t f e1 e2 -> ERecUpd
        (applySubstInTypeConApp subst t)
        f
        (applySubstInExpr subst e1)
        (applySubstInExpr subst e2)
    EVariantCon t v e -> EVariantCon
        (applySubstInTypeConApp subst t)
        v
        (applySubstInExpr subst e)
    e@(EEnumCon _ _) -> e
    EStructCon fs -> EStructCon
        (substFields subst fs)
    EStructProj f e -> EStructProj
        f
        (applySubstInExpr subst e)
    EStructUpd f e1 e2 -> EStructUpd
        f
        (applySubstInExpr subst e1)
        (applySubstInExpr subst e2)
    ETmApp e1 e2 -> ETmApp
        (applySubstInExpr subst e1)
        (applySubstInExpr subst e2)
    ETyApp e t -> ETyApp
        (applySubstInExpr subst e)
        (applySubstInType subst t)
    ETmLam (x,t) e ->
        substWithBoundExprVar subst x $ \ subst' x' ->
            ETmLam (x', applySubstInType subst t) (applySubstInExpr subst' e)
    ETyLam (x,k) e ->
        substWithBoundTypeVar subst x $ \ subst' x' ->
            ETyLam (x', k) (applySubstInExpr subst' e)
    ECase e alts -> ECase
        (applySubstInExpr subst e)
        (map (applySubstInAlternative subst) alts)
    ELet (Binding (x, t) e1) e2 ->
        substWithBoundExprVar subst x $ \ subst' x' ->
            ELet (Binding (x', applySubstInType subst t) (applySubstInExpr subst e1))
                (applySubstInExpr subst' e2)
    ENil t -> ENil
        (applySubstInType subst t)
    ECons t e1 e2 -> ECons
        (applySubstInType subst t)
        (applySubstInExpr subst e1)
        (applySubstInExpr subst e2)
    ENone t -> ENone
        (applySubstInType subst t)
    ESome t e -> ESome
        (applySubstInType subst t)
        (applySubstInExpr subst e)
    EToAny t e -> EToAny
        (applySubstInType subst t)
        (applySubstInExpr subst e)
    EFromAny t e -> EFromAny
        (applySubstInType subst t)
        (applySubstInExpr subst e)
    ETypeRep t -> ETypeRep
        (applySubstInType subst t)
    EToAnyException t e -> EToAnyException
        (applySubstInType subst t)
        (applySubstInExpr subst e)
    EFromAnyException t e -> EFromAnyException
        (applySubstInType subst t)
        (applySubstInExpr subst e)
    EThrow t1 t2 e -> EThrow
        (applySubstInType subst t1)
        (applySubstInType subst t2)
        (applySubstInExpr subst e)
    EUpdate u -> EUpdate
        (applySubstInUpdate subst u)
    EScenario s -> EScenario
        (applySubstInScenario subst s)
    ELocation l e -> ELocation
        l
        (applySubstInExpr subst e)
    EExperimental name ty ->
        EExperimental name ty

applySubstInAlternative :: Subst -> CaseAlternative -> CaseAlternative
applySubstInAlternative subst (CaseAlternative p e) =
    applySubstWithPattern subst p $ \ subst' p' ->
        CaseAlternative p' (applySubstInExpr subst' e)

applySubstWithPattern :: Subst -> CasePattern -> (Subst -> CasePattern -> t) -> t
applySubstWithPattern subst p f = case p of
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

applySubstInUpdate :: Subst -> Update -> Update
applySubstInUpdate subst = \case
    UPure t e -> UPure
        (applySubstInType subst t)
        (applySubstInExpr subst e)
    UBind (Binding (x, t) e1) e2 ->
        substWithBoundExprVar subst x $ \ subst' x' ->
            UBind (Binding (x', applySubstInType subst t) (applySubstInExpr subst e1))
                (applySubstInExpr subst' e2)
    UCreate templateName e -> UCreate
        templateName
        (applySubstInExpr subst e)
    UExercise templateName choiceName e1 e2 -> UExercise
        templateName
        choiceName
        (applySubstInExpr subst e1)
        (applySubstInExpr subst e2)
    UExerciseByKey templateName choiceName e1 e2 -> UExerciseByKey
        templateName
        choiceName
        (applySubstInExpr subst e1)
        (applySubstInExpr subst e2)
    UFetch templateName e -> UFetch
        templateName
        (applySubstInExpr subst e)
    e@UGetTime -> e
    UEmbedExpr t e -> UEmbedExpr
        (applySubstInType subst t)
        (applySubstInExpr subst e)
    ULookupByKey (RetrieveByKey templateName e) -> ULookupByKey $ RetrieveByKey
        templateName
        (applySubstInExpr subst e)
    UFetchByKey (RetrieveByKey templateName e) -> UFetchByKey $ RetrieveByKey
        templateName
        (applySubstInExpr subst e)
    UTryCatch t e1 x e2 ->
        substWithBoundExprVar subst x $ \subst' x' -> UTryCatch
            (applySubstInType subst t)
            (applySubstInExpr subst e1)
            x'
            (applySubstInExpr subst' e2)

applySubstInScenario :: Subst -> Scenario -> Scenario
applySubstInScenario subst = \case
    SPure t e -> SPure
        (applySubstInType subst t)
        (applySubstInExpr subst e)
    SBind (Binding (x,t) e1) e2 ->
        substWithBoundExprVar subst x $ \ subst' x' ->
            SBind (Binding (x', applySubstInType subst t) (applySubstInExpr subst e1))
                (applySubstInExpr subst' e2)
    SCommit t e1 e2 -> SCommit
        (applySubstInType subst t)
        (applySubstInExpr subst e1)
        (applySubstInExpr subst e2)
    SMustFailAt t e1 e2 -> SMustFailAt
        (applySubstInType subst t)
        (applySubstInExpr subst e1)
        (applySubstInExpr subst e2)
    SPass e -> SPass
        (applySubstInExpr subst e)
    e@SGetTime -> e
    SGetParty e -> SGetParty
        (applySubstInExpr subst e)
    SEmbedExpr t e -> SEmbedExpr
        (applySubstInType subst t)
        (applySubstInExpr subst e)
