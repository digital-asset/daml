-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Substitution in LF expressions.
module DA.Daml.LF.Ast.Subst
    ( Subst (..)
    , typeSubst
    , exprSubst
    , substExpr
    , substType
    ) where

import DA.Daml.LF.Ast
import DA.Daml.LF.Ast.Recursive
import qualified DA.Daml.LF.Ast.Type as Type

import Control.Arrow (second)
import Data.Functor.Foldable (cata)
import Data.Maybe (fromMaybe)
import qualified Data.Map.Strict as Map
import qualified Data.Set as Set
import qualified Data.Text as T
import Safe (findJust)

data Subst = Subst
    { substTypes :: !(Map.Map TypeVarName Type)
        -- ^ types to substitute
    , substExprs :: !(Map.Map ExprVarName Expr)
        -- ^ expressions to substitute
    , substExhaustedTypeVars :: !(Set.Set TypeVarName)
        -- ^ set of exhausted type variables
        -- (these are all the free type vars that appear in substTypes
        -- or substExprs, plus the type vars that were bound above us,
        -- plus the type vars that were substituted away)
    , substExhaustedExprVars :: !(Set.Set ExprVarName)
        -- ^ set of exhausted expr variables
        -- (these are all the free expr vars that appear in substExprs,
        -- plus the expr vars that were bound above us, plus the expr
        -- vars that were substituted away)
    }

instance Monoid Subst where
    mempty = Subst
        { substTypes = Map.empty
        , substExprs = Map.empty
        , substExhaustedTypeVars = Set.empty
        , substExhaustedExprVars = Set.empty
        }

instance Semigroup Subst where
    s1 <> s2 = Subst
        { substTypes = substTypes s1 <> substTypes s2
        , substExprs = substExprs s1 <> substExprs s2
        , substExhaustedExprVars = substExhaustedExprVars s1 <> substExhaustedExprVars s2
        , substExhaustedTypeVars = substExhaustedTypeVars s1 <> substExhaustedTypeVars s2
        }

typeSubst :: TypeVarName -> Type -> Subst
typeSubst x t = Subst
    { substTypes = Map.fromList [(x,t)]
    , substExprs = Map.empty
    , substExhaustedTypeVars = Set.insert x (freeVarsInType t)
    , substExhaustedExprVars = Set.empty
    }

exprSubst :: ExprVarName -> Expr -> Subst
exprSubst x e = Subst
    { substTypes = Map.empty
    , substExprs = Map.fromList [(x,e)]
    , substExhaustedTypeVars = typeVars0
    , substExhaustedExprVars = Set.insert x exprVars0
    }
  where
    (typeVars0, exprVars0) = freeVarsInExpr e


freeVarsInType :: Type -> Set.Set TypeVarName
freeVarsInType = Type.freeVars

freeVarsInTypeConApp :: TypeConApp -> Set.Set TypeVarName
freeVarsInTypeConApp (TypeConApp _ tys) = foldMap freeVarsInType tys

freeVarsInExpr :: Expr -> (Set.Set TypeVarName, Set.Set ExprVarName)
freeVarsInExpr = cata go
  where
    go :: ExprF (Set.Set TypeVarName, Set.Set ExprVarName)
        -> (Set.Set TypeVarName, Set.Set ExprVarName)
    go = \case
        EVarF x -> (Set.empty, Set.singleton x)
        EValF _ -> (Set.empty, Set.empty)
        EBuiltinF _ -> (Set.empty, Set.empty)
        ERecConF t fs -> (freeVarsInTypeConApp t, Set.empty) <> foldMap snd fs
        ERecProjF t _ e -> (freeVarsInTypeConApp t, Set.empty) <> e
        ERecUpdF t _ e1 e2 -> (freeVarsInTypeConApp t, Set.empty) <> e1 <> e2
        EVariantConF t _ e -> (freeVarsInTypeConApp t, Set.empty) <> e
        EEnumConF _ _ -> (Set.empty, Set.empty)
        EStructConF fs -> foldMap snd fs
        EStructProjF _ e -> e
        EStructUpdF _ e1 e2 -> e1 <> e2
        ETmAppF e1 e2 -> e1 <> e2
        ETyAppF e t -> e <> (freeVarsInType t, Set.empty)
        ETmLamF (x, t) (ftv, fev) -> (ftv <> freeVarsInType t, Set.delete x fev)
        ETyLamF (x, _) (ftv, fev) -> (Set.delete x ftv, fev)
        ECaseF e cs -> e <> foldMap goCase cs
        ELetF b e -> goBinding b e
        ENilF t -> (freeVarsInType t, Set.empty)
        EConsF t e1 e2 -> (freeVarsInType t, Set.empty) <> e1 <> e2
        EUpdateF u -> goUpdate u
        EScenarioF s -> goScenario s
        ELocationF _ e -> e
        ENoneF t -> (freeVarsInType t, Set.empty)
        ESomeF t e -> (freeVarsInType t, Set.empty) <> e
        EToAnyF t e -> (freeVarsInType t, Set.empty) <> e
        EFromAnyF t e -> (freeVarsInType t, Set.empty) <> e
        ETypeRepF t -> (freeVarsInType t, Set.empty)

    goCase :: (CasePattern, (Set.Set TypeVarName, Set.Set ExprVarName))
        -> (Set.Set TypeVarName, Set.Set ExprVarName)
    goCase = snd -- overapproximation is fine and cheap

    goBinding :: BindingF (Set.Set TypeVarName, Set.Set ExprVarName)
        -> (Set.Set TypeVarName, Set.Set ExprVarName)
        -> (Set.Set TypeVarName, Set.Set ExprVarName)
    goBinding (BindingF (x, t) e) (ftv, fev) =
        e <> (ftv <> freeVarsInType t, Set.delete x fev)

    goUpdate :: UpdateF (Set.Set TypeVarName, Set.Set ExprVarName)
        -> (Set.Set TypeVarName, Set.Set ExprVarName)
    goUpdate = \case
        UPureF t e -> (freeVarsInType t, Set.empty) <> e
        UBindF b e -> goBinding b e
        UCreateF _ e -> e
        UExerciseF _ _ e1 e2M e3 -> e1 <> fromMaybe mempty e2M <> e3
        UFetchF _ e -> e
        UGetTimeF -> mempty
        UEmbedExprF t e -> (freeVarsInType t, Set.empty) <> e
        UFetchByKeyF r -> retrieveByKeyFKey r
        ULookupByKeyF r -> retrieveByKeyFKey r

    goScenario :: ScenarioF (Set.Set TypeVarName, Set.Set ExprVarName)
        -> (Set.Set TypeVarName, Set.Set ExprVarName)
    goScenario = \case
        SPureF t e -> (freeVarsInType t, Set.empty) <> e
        SBindF b e -> goBinding b e
        SCommitF t e1 e2 -> (freeVarsInType t, Set.empty) <> e1 <> e2
        SMustFailAtF t e1 e2 -> (freeVarsInType t, Set.empty) <> e1 <> e2
        SPassF e -> e
        SGetTimeF -> mempty
        SGetPartyF e -> e
        SEmbedExprF t e -> (freeVarsInType t, Set.empty) <> e

substType :: Subst -> Type -> Type
substType Subst{..} = Type.substituteAux substExhaustedTypeVars substTypes

substTypeConApp :: Subst -> TypeConApp -> TypeConApp
substTypeConApp subst (TypeConApp qtcon ts) =
    TypeConApp qtcon (map (substType subst) ts)

substFields :: Subst -> [(FieldName, Expr)] -> [(FieldName, Expr)]
substFields subst = map (second (substExpr subst))

substWithBoundExprVar :: Subst -> ExprVarName -> (Subst -> ExprVarName -> t) -> t
substWithBoundExprVar subst@Subst{..} x f
    | Set.member x substExhaustedExprVars =
        let x' = freshenExprVar subst x
            subst' = subst
                { substExhaustedExprVars =
                    Set.insert x (Set.insert x' substExhaustedExprVars)
                , substExprs =
                    Map.insert x (EVar x') substExprs
                }
        in f subst' x'

    | otherwise =
        let subst' = subst
                { substExhaustedExprVars =
                    Set.insert x substExhaustedExprVars
                }
        in f subst' x

substWithBoundTypeVar :: Subst -> TypeVarName -> (Subst -> TypeVarName -> t) -> t
substWithBoundTypeVar subst@Subst{..} x f
    | Set.member x substExhaustedTypeVars =
        let x' = freshenTypeVar subst x
            subst' = subst
                { substExhaustedTypeVars =
                    Set.insert x (Set.insert x' substExhaustedTypeVars)
                , substTypes =
                    Map.insert x (TVar x') substTypes
                }
        in f subst' x'

    | otherwise =
        let subst' = subst
                { substExhaustedTypeVars =
                    Set.insert x substExhaustedTypeVars
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

freshenTypeVar :: Subst -> TypeVarName -> TypeVarName
freshenTypeVar Subst{..} (TypeVarName v) =
  let candidates = map (\n -> TypeVarName (v <> T.pack (show n))) [1 :: Int ..]
  in findJust (`Set.notMember` substExhaustedTypeVars) candidates

freshenExprVar :: Subst -> ExprVarName -> ExprVarName
freshenExprVar Subst{..} (ExprVarName v) =
  let candidates = map (\n -> ExprVarName (v <> T.pack (show n))) [1 :: Int ..]
  in findJust (`Set.notMember` substExhaustedExprVars) candidates
