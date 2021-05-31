-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Ast.FreeVars
    ( FreeVars
    , freeTypeVars
    , freeExprVars
    , freeTypeVar
    , freeExprVar
    , bindTypeVar
    , bindExprVar
    , freeVarsNull
    , isFreeTypeVar
    , isFreeExprVar
    , freeVarsInType
    , freeVarsInTypeConApp
    , freeVarsInExpr
    , freeVarsStep
    , freshenTypeVar
    , freshenExprVar
    ) where

import DA.Daml.LF.Ast
import DA.Daml.LF.Ast.Recursive
import qualified DA.Daml.LF.Ast.Type as Type

import Data.Functor.Foldable (cata)
import qualified Data.Set as Set
import qualified Data.Text as T
import Safe (findJust)

data FreeVars = FreeVars
    { freeTypeVars :: Set.Set TypeVarName
    , freeExprVars :: Set.Set ExprVarName
    }

instance Semigroup FreeVars where
    FreeVars a1 b1 <> FreeVars a2 b2 =
        FreeVars (Set.union a1 a2) (Set.union b1 b2)

instance Monoid FreeVars where
    mempty = FreeVars Set.empty Set.empty

freeVarsNull :: FreeVars -> Bool
freeVarsNull FreeVars{..} =
    Set.null freeTypeVars && Set.null freeExprVars

freeTypeVar :: TypeVarName -> FreeVars
freeTypeVar x = FreeVars (Set.singleton x) Set.empty

freeExprVar :: ExprVarName -> FreeVars
freeExprVar x = FreeVars Set.empty (Set.singleton x)

bindTypeVar :: TypeVarName -> FreeVars -> FreeVars
bindTypeVar x fvs = fvs { freeTypeVars = Set.delete x (freeTypeVars fvs) }

bindExprVar :: ExprVarName -> FreeVars -> FreeVars
bindExprVar x fvs = fvs { freeExprVars = Set.delete x (freeExprVars fvs) }

isFreeTypeVar :: TypeVarName -> FreeVars -> Bool
isFreeTypeVar x = Set.member x . freeTypeVars

isFreeExprVar :: ExprVarName -> FreeVars -> Bool
isFreeExprVar x = Set.member x . freeExprVars

freeVarsInType :: Type -> FreeVars
freeVarsInType ty = FreeVars (Type.freeVars ty) Set.empty

freeVarsInTypeConApp :: TypeConApp -> FreeVars
freeVarsInTypeConApp (TypeConApp _ tys) = foldMap freeVarsInType tys

freeVarsInExpr :: Expr -> FreeVars
freeVarsInExpr = cata freeVarsStep

freeVarsStep :: ExprF FreeVars -> FreeVars
freeVarsStep = \case
    EVarF x -> freeExprVar x
    EValF _ -> mempty
    EBuiltinF _ -> mempty
    ERecConF t fs -> freeVarsInTypeConApp t <> foldMap snd fs
    ERecProjF t _ e -> freeVarsInTypeConApp t <> e
    ERecUpdF t _ e1 e2 -> freeVarsInTypeConApp t <> e1 <> e2
    EVariantConF t _ e -> freeVarsInTypeConApp t <> e
    EEnumConF _ _ -> mempty
    EStructConF fs -> foldMap snd fs
    EStructProjF _ e -> e
    EStructUpdF _ e1 e2 -> e1 <> e2
    ETmAppF e1 e2 -> e1 <> e2
    ETyAppF e t -> e <> freeVarsInType t
    ETmLamF (x, t) e -> freeVarsInType t <> bindExprVar x e
    ETyLamF (x, _) e -> bindTypeVar x e
    ECaseF e cs -> e <> foldMap goCase cs
    ELetF b e -> goBinding b e
    ENilF t -> freeVarsInType t
    EConsF t e1 e2 -> freeVarsInType t <> e1 <> e2
    EUpdateF u -> goUpdate u
    EScenarioF s -> goScenario s
    ELocationF _ e -> e
    ENoneF t -> freeVarsInType t
    ESomeF t e -> freeVarsInType t <> e
    EToAnyF t e -> freeVarsInType t <> e
    EFromAnyF t e -> freeVarsInType t <> e
    ETypeRepF t -> freeVarsInType t
    EToAnyExceptionF t e -> freeVarsInType t <> e
    EFromAnyExceptionF t e -> freeVarsInType t <> e
    EThrowF t1 t2 e -> freeVarsInType t1 <> freeVarsInType t2 <> e
    EExperimentalF _ t -> freeVarsInType t

  where

    goCase :: (CasePattern, FreeVars) -> FreeVars
    goCase = uncurry goPattern

    goPattern :: CasePattern -> FreeVars -> FreeVars
    goPattern = \case
        CPVariant _ _ x -> bindExprVar x
        CPEnum _ _ -> id
        CPUnit -> id
        CPBool _ -> id
        CPNil -> id
        CPCons x1 x2 -> bindExprVar x1 . bindExprVar x2
        CPNone -> id
        CPSome x -> bindExprVar x
        CPDefault -> id

    goBinding :: BindingF FreeVars -> FreeVars -> FreeVars
    goBinding (BindingF (x, t) e1) e2 =
        freeVarsInType t <> e1 <> bindExprVar x e2

    goUpdate :: UpdateF FreeVars -> FreeVars
    goUpdate = \case
        UPureF t e -> freeVarsInType t <> e
        UBindF b e -> goBinding b e
        UCreateF _ e -> e
        UExerciseF _ _ e1 e2 -> e1 <> e2
        UExerciseByKeyF _ _ e1 e2 -> e1 <> e2
        UFetchF _ e -> e
        UGetTimeF -> mempty
        UEmbedExprF t e -> freeVarsInType t <> e
        UFetchByKeyF r -> retrieveByKeyFKey r
        ULookupByKeyF r -> retrieveByKeyFKey r
        UTryCatchF t e1 x e2 -> freeVarsInType t <> e1 <> bindExprVar x e2

    goScenario :: ScenarioF FreeVars -> FreeVars
    goScenario = \case
        SPureF t e -> freeVarsInType t <> e
        SBindF b e -> goBinding b e
        SCommitF t e1 e2 -> freeVarsInType t <> e1 <> e2
        SMustFailAtF t e1 e2 -> freeVarsInType t <> e1 <> e2
        SPassF e -> e
        SGetTimeF -> mempty
        SGetPartyF e -> e
        SEmbedExprF t e -> freeVarsInType t <> e

freshenTypeVar :: FreeVars -> TypeVarName -> TypeVarName
freshenTypeVar fvs (TypeVarName v) =
  let candidates = map (\n -> TypeVarName (v <> T.pack (show n))) [1 :: Int ..]
  in findJust (\x -> not (isFreeTypeVar x fvs)) candidates

freshenExprVar :: FreeVars -> ExprVarName -> ExprVarName
freshenExprVar fvs (ExprVarName v) =
  let candidates = map (\n -> ExprVarName (v <> T.pack (show n))) [1 :: Int ..]
  in findJust (\x -> not (isFreeExprVar x fvs)) candidates
