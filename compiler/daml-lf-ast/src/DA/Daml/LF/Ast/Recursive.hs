-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE TypeFamilies #-}

-- | This module provides all the boilerplate necessary to make the Daml-LF AST
-- work with the recursion-schemes package.
module DA.Daml.LF.Ast.Recursive(
    ExprF(..),
    UpdateF(..),
    ScenarioF(..),
    BindingF(..),
    TypeF(..),
    retrieveByKeyFKey
    ) where

import Data.Functor.Foldable
import qualified Data.Text as T

import DA.Daml.LF.Ast.Base
import DA.Daml.LF.Ast.TypeLevelNat

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
  | EToInterfaceF !(Qualified TypeConName) !(Qualified TypeConName) !expr
  | EFromInterfaceF !(Qualified TypeConName) !(Qualified TypeConName) !expr
  | EUnsafeFromInterfaceF !(Qualified TypeConName) !(Qualified TypeConName) !expr !expr
  | ECallInterfaceF !(Qualified TypeConName) !MethodName !expr
  | EToRequiredInterfaceF !(Qualified TypeConName) !(Qualified TypeConName) !expr
  | EFromRequiredInterfaceF !(Qualified TypeConName) !(Qualified TypeConName) !expr
  | EUnsafeFromRequiredInterfaceF !(Qualified TypeConName) !(Qualified TypeConName) !expr !expr
  | EInterfaceTemplateTypeRepF !(Qualified TypeConName) !expr
  | ESignatoryInterfaceF !(Qualified TypeConName) !expr
  | EObserverInterfaceF !(Qualified TypeConName) !expr
  | EViewInterfaceF !(Qualified TypeConName) !expr
  | EChoiceControllerF !(Qualified TypeConName) !ChoiceName !expr !expr
  | EChoiceObserverF !(Qualified TypeConName) !ChoiceName !expr !expr
  | EExperimentalF !T.Text !Type
  deriving (Foldable, Functor, Traversable)

data BindingF expr = BindingF !(ExprVarName, Type) !expr
  deriving (Foldable, Functor, Traversable)

data UpdateF expr
  = UPureF     !Type !expr
  | UBindF     !(BindingF expr) !expr
  | UCreateF   !(Qualified TypeConName) !expr
  | UCreateInterfaceF !(Qualified TypeConName) !expr
  | UExerciseF !(Qualified TypeConName) !ChoiceName !expr !expr
  | USoftExerciseF !(Qualified TypeConName) !ChoiceName !expr !expr
  | UDynamicExerciseF !(Qualified TypeConName) !ChoiceName !expr !expr
  | UExerciseInterfaceF !(Qualified TypeConName) !ChoiceName !expr !expr !(Maybe expr)
  | UExerciseByKeyF !(Qualified TypeConName) !ChoiceName !expr !expr
  | UFetchF    !(Qualified TypeConName) !expr
  | USoftFetchF !(Qualified TypeConName) !expr
  | UFetchInterfaceF    !(Qualified TypeConName) !expr
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
  UCreateInterface a b -> UCreateInterfaceF a b
  UExercise a b c d -> UExerciseF a b c d
  USoftExercise a b c d -> USoftExerciseF a b c d
  UDynamicExercise a b c d -> UDynamicExerciseF a b c d
  UExerciseInterface a b c d e -> UExerciseInterfaceF a b c d e
  UExerciseByKey a b c d -> UExerciseByKeyF a b c d
  UFetch a b -> UFetchF a b
  USoftFetch a b -> USoftFetchF a b
  UFetchInterface a b -> UFetchInterfaceF a b
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
  UCreateInterfaceF a b -> UCreateInterface a b
  UExerciseF a b c d -> UExercise a b c d
  USoftExerciseF a b c d -> USoftExercise a b c d
  UDynamicExerciseF a b c d -> UDynamicExercise a b c d
  UExerciseInterfaceF a b c d e -> UExerciseInterface a b c d e
  UExerciseByKeyF a b c d -> UExerciseByKey a b c d
  UFetchF a b -> UFetch a b
  USoftFetchF a b -> USoftFetch a b
  UFetchInterfaceF a b -> UFetchInterface a b
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
    ECase       a b   -> ECaseF         a (map projectCaseAlternative b)
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
    EToInterface a b c -> EToInterfaceF a b c
    EFromInterface a b c -> EFromInterfaceF a b c
    EUnsafeFromInterface a b c d -> EUnsafeFromInterfaceF a b c d
    ECallInterface a b c -> ECallInterfaceF a b c
    EToRequiredInterface a b c -> EToRequiredInterfaceF a b c
    EFromRequiredInterface a b c -> EFromRequiredInterfaceF a b c
    EUnsafeFromRequiredInterface a b c d -> EUnsafeFromRequiredInterfaceF a b c d
    EInterfaceTemplateTypeRep a b -> EInterfaceTemplateTypeRepF a b
    ESignatoryInterface a b -> ESignatoryInterfaceF a b
    EObserverInterface a b -> EObserverInterfaceF a b
    EViewInterface a b -> EViewInterfaceF a b
    EChoiceController a b c d -> EChoiceControllerF a b c d
    EChoiceObserver a b c d -> EChoiceObserverF a b c d
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
    ECaseF       a b   -> ECase         a (map embedCaseAlternative b)
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
    EToInterfaceF a b c -> EToInterface a b c
    EFromInterfaceF a b c -> EFromInterface a b c
    EUnsafeFromInterfaceF a b c d -> EUnsafeFromInterface a b c d
    ECallInterfaceF a b c -> ECallInterface a b c
    EToRequiredInterfaceF a b c -> EToRequiredInterface a b c
    EFromRequiredInterfaceF a b c -> EFromRequiredInterface a b c
    EUnsafeFromRequiredInterfaceF a b c d -> EUnsafeFromRequiredInterface a b c d
    EInterfaceTemplateTypeRepF a b -> EInterfaceTemplateTypeRep a b
    ESignatoryInterfaceF a b -> ESignatoryInterface a b
    EObserverInterfaceF a b -> EObserverInterface a b
    EViewInterfaceF a b -> EViewInterface a b
    EChoiceControllerF a b c d -> EChoiceController a b c d
    EChoiceObserverF a b c d -> EChoiceObserver a b c d
    EExperimentalF a b -> EExperimental a b

data TypeF type_
  = TVarF       !TypeVarName
  | TConF       !(Qualified TypeConName)
  | TSynAppF    !(Qualified TypeSynName) ![type_]
  | TAppF       !type_ !type_
  | TBuiltinF   !BuiltinType
  | TForallF !(TypeVarName, Kind) !type_
  | TStructF     ![(FieldName, type_)]
  | TNatF !TypeLevelNat
  deriving (Foldable, Functor, Traversable)

type instance Base Type = TypeF

instance Recursive Type where
  project = \case
    TVar a -> TVarF a
    TCon a -> TConF a
    TSynApp a b -> TSynAppF a b
    TApp a b -> TAppF a b
    TBuiltin a -> TBuiltinF a
    TForall a b -> TForallF a b
    TStruct a -> TStructF a
    TNat a -> TNatF a

instance Corecursive Type where
  embed = \case
    TVarF a -> TVar a
    TConF a -> TCon a
    TSynAppF a b -> TSynApp a b
    TAppF a b -> TApp a b
    TBuiltinF a -> TBuiltin a
    TForallF a b -> TForall a b
    TStructF a -> TStruct a
    TNatF a -> TNat a
