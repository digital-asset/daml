-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE TypeFamilies #-}

-- | This module provides all the boilerplate necessary to make the DAML-LF AST
-- work with the recursion-schemes package.
module DA.Daml.LF.Ast.Recursive(
    ExprF(..),
    UpdateF(..),
    ScenarioF(..),
    BindingF(..),
    retrieveByKeyFKey
    ) where

import Data.Functor.Foldable
import qualified Data.Text as T

import DA.Daml.LF.Ast.Base

data ExprF expr
  = EVarF        !ExprVarName
  | EValF        !(Qualified ExprValName)
  | EBuiltinF    !BuiltinExpr
  | ERecConF     !TypeConApp ![(FieldName, expr)]
  | ERecProjF    !TypeConApp !FieldName !expr
  | ERecUpdF     !TypeConApp !FieldName !expr !expr
  | EVariantConF !TypeConApp !VariantConName !expr
  | EEnumConF    !(Qualified TypeConName) !VariantConName
  | EStructConF  ![(FieldName, expr)]
  | EStructProjF !FieldName !expr
  | EStructUpdF  !FieldName !expr !expr
  | ETmAppF      !expr !expr
  | ETyAppF      !expr !Type
  | ETmLamF      !(ExprVarName, Type) !expr
  | ETyLamF      !(TypeVarName, Kind) !expr
  | ECaseF       !expr ![(CasePattern, expr)]
  | ELetF        !(BindingF expr) !expr
  | ENilF        !Type
  | EConsF       !Type !expr !expr
  | EUpdateF     !(UpdateF expr)
  | EScenarioF   !(ScenarioF expr)
  | ELocationF   !SourceLoc !expr
  | ENoneF       !Type
  | ESomeF       !Type !expr
  | EToAnyF !Type !expr
  | EFromAnyF !Type !expr
  | ETypeRepF !Type
  | EToAnyExceptionF !Type !expr
  | EFromAnyExceptionF !Type !expr
  | EThrowF !Type !Type !expr
  | EExperimentalF !T.Text !Type
  deriving (Foldable, Functor, Traversable)

data BindingF expr = BindingF !(ExprVarName, Type) !expr
  deriving (Foldable, Functor, Traversable)

data UpdateF expr
  = UPureF     !Type !expr
  | UBindF     !(BindingF expr) !expr
  | UCreateF   !(Qualified TypeConName) !expr
  | UExerciseF !(Qualified TypeConName) !ChoiceName !expr !expr
  | UExerciseByKeyF !(Qualified TypeConName) !ChoiceName !expr !expr
  | UFetchF    !(Qualified TypeConName) !expr
  | UGetTimeF
  | UEmbedExprF !Type !expr
  | UFetchByKeyF !(RetrieveByKeyF expr)
  | ULookupByKeyF !(RetrieveByKeyF expr)
  | UTryCatchF !Type !expr !ExprVarName !expr
  deriving (Foldable, Functor, Traversable)

data RetrieveByKeyF expr = RetrieveByKeyF
  { retrieveByKeyFTemplate :: !(Qualified TypeConName)
  , retrieveByKeyFKey :: !expr
  }
  deriving (Foldable, Functor, Traversable)

data ScenarioF expr
  = SPureF       !Type !expr
  | SBindF       !(BindingF expr) !expr
  | SCommitF     !Type !expr !expr
  | SMustFailAtF !Type !expr !expr
  | SPassF       !expr
  | SGetTimeF
  | SGetPartyF   !expr
  | SEmbedExprF  !Type !expr
  deriving (Foldable, Functor, Traversable)

type instance Base Expr = ExprF

projectBinding :: Binding -> BindingF Expr
projectBinding (Binding a b) =  BindingF a b

embedBinding :: BindingF Expr -> Binding
embedBinding (BindingF a b) = Binding a b

projectCaseAlternative :: CaseAlternative -> (CasePattern, Expr)
projectCaseAlternative (CaseAlternative a b) = (a, b)

embedCaseAlternative :: (CasePattern, Expr) -> CaseAlternative
embedCaseAlternative (a, b) = CaseAlternative a b

projectUpdate :: Update -> UpdateF Expr
projectUpdate = \case
  UPure a b -> UPureF a b
  UBind a b -> UBindF (projectBinding a) b
  UCreate a b -> UCreateF a b
  UExercise a b c d -> UExerciseF a b c d
  UExerciseByKey a b c d -> UExerciseByKeyF a b c d
  UFetch a b -> UFetchF a b
  UGetTime -> UGetTimeF
  UEmbedExpr a b -> UEmbedExprF a b
  ULookupByKey a -> ULookupByKeyF (projectRetrieveByKey a)
  UFetchByKey a -> UFetchByKeyF (projectRetrieveByKey a)
  UTryCatch a b c d -> UTryCatchF a b c d

projectRetrieveByKey :: RetrieveByKey -> RetrieveByKeyF Expr
projectRetrieveByKey (RetrieveByKey tpl key) = RetrieveByKeyF tpl key

embedUpdate :: UpdateF Expr -> Update
embedUpdate = \case
  UPureF a b -> UPure a b
  UBindF a b -> UBind (embedBinding a) b
  UCreateF a b -> UCreate a b
  UExerciseF a b c d -> UExercise a b c d
  UExerciseByKeyF a b c d -> UExerciseByKey a b c d
  UFetchF a b -> UFetch a b
  UGetTimeF -> UGetTime
  UEmbedExprF a b -> UEmbedExpr a b
  UFetchByKeyF a -> UFetchByKey (embedRetrieveByKey a)
  ULookupByKeyF a -> ULookupByKey (embedRetrieveByKey a)
  UTryCatchF a b c d -> UTryCatch a b c d

embedRetrieveByKey :: RetrieveByKeyF Expr -> RetrieveByKey
embedRetrieveByKey RetrieveByKeyF{..} = RetrieveByKey
  { retrieveByKeyTemplate = retrieveByKeyFTemplate
  , retrieveByKeyKey = retrieveByKeyFKey
  }

projectScenario :: Scenario -> ScenarioF Expr
projectScenario = \case
  SPure a b -> SPureF a b
  SBind a b -> SBindF (projectBinding a) b
  SCommit a b c -> SCommitF a b c
  SMustFailAt a b c -> SMustFailAtF a b c
  SPass a -> SPassF a
  SGetTime -> SGetTimeF
  SGetParty a -> SGetPartyF a
  SEmbedExpr a b -> SEmbedExprF a b

embedScenario :: ScenarioF Expr -> Scenario
embedScenario = \case
  SPureF a b -> SPure a b
  SBindF a b -> SBind (embedBinding a) b
  SCommitF a b c -> SCommit a b c
  SMustFailAtF a b c -> SMustFailAt a b c
  SPassF a -> SPass a
  SGetTimeF -> SGetTime
  SGetPartyF a -> SGetParty a
  SEmbedExprF a b -> SEmbedExpr a b

instance Recursive Expr where
  project = \case
    EVar        a     -> EVarF          a
    EVal        a     -> EValF          a
    EBuiltin    a     -> EBuiltinF      a
    ERecCon     a b   -> ERecConF       a b
    ERecProj    a b c -> ERecProjF      a b c
    ERecUpd   a b c d -> ERecUpdF     a b c d
    EVariantCon a b c -> EVariantConF   a b c
    EEnumCon    a b   -> EEnumConF      a b
    EStructCon  a     -> EStructConF    a
    EStructProj a b   -> EStructProjF   a b
    EStructUpd  a b c -> EStructUpdF    a b c
    ETmApp      a b   -> ETmAppF        a b
    ETyApp      a b   -> ETyAppF        a b
    ETmLam      a b   -> ETmLamF        a b
    ETyLam      a b   -> ETyLamF        a b
    ENil        a     -> ENilF          a
    ECons       a b c -> EConsF         a b c
    ECase       a b   -> ECaseF         a (Prelude.map projectCaseAlternative b)
    ELet        a b   -> ELetF          (projectBinding a) b
    EUpdate     a     -> EUpdateF       (projectUpdate a)
    EScenario   a     -> EScenarioF     (projectScenario a)
    ELocation   a b   -> ELocationF     a b
    ENone       a     -> ENoneF         a
    ESome       a b   -> ESomeF         a b
    EToAny a b  -> EToAnyF a b
    EFromAny a b -> EFromAnyF a b
    ETypeRep a -> ETypeRepF a
    EToAnyException a b -> EToAnyExceptionF a b
    EFromAnyException a b -> EFromAnyExceptionF a b
    EThrow a b c -> EThrowF a b c
    EExperimental a b -> EExperimentalF a b

instance Corecursive Expr where
  embed = \case
    EVarF        a     -> EVar          a
    EValF        a     -> EVal          a
    EBuiltinF    a     -> EBuiltin      a
    ERecConF     a b   -> ERecCon       a b
    ERecProjF    a b c -> ERecProj      a b c
    ERecUpdF   a b c d -> ERecUpd     a b c d
    EVariantConF a b c -> EVariantCon   a b c
    EEnumConF    a b   -> EEnumCon      a b
    EStructConF  a     -> EStructCon    a
    EStructProjF a b   -> EStructProj   a b
    EStructUpdF  a b c -> EStructUpd    a b c
    ETmAppF      a b   -> ETmApp        a b
    ETyAppF      a b   -> ETyApp        a b
    ETmLamF      a b   -> ETmLam        a b
    ETyLamF      a b   -> ETyLam        a b
    ENilF        a     -> ENil          a
    EConsF       a b c -> ECons         a b c
    ECaseF       a b   -> ECase         a (Prelude.map embedCaseAlternative b)
    ELetF        a b   -> ELet          (embedBinding a) b
    EUpdateF     a     -> EUpdate       (embedUpdate a)
    EScenarioF   a     -> EScenario     (embedScenario a)
    ELocationF   a b   -> ELocation a b
    ENoneF       a     -> ENone a
    ESomeF       a b   -> ESome a b
    EToAnyF a b  -> EToAny a b
    EFromAnyF a b -> EFromAny a b
    ETypeRepF a -> ETypeRep a
    EToAnyExceptionF a b -> EToAnyException a b
    EFromAnyExceptionF a b -> EFromAnyException a b
    EThrowF a b c -> EThrow a b c
    EExperimentalF a b -> EExperimental a b
