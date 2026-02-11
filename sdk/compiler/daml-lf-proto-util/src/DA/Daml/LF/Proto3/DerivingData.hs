-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# OPTIONS_GHC -Wno-orphans #-}

module DA.Daml.LF.Proto3.DerivingData (
        module DA.Daml.LF.Proto3.DerivingData
) where


import qualified Com.Digitalasset.Daml.Lf.Archive.DamlLf2 as P

import qualified Proto3.Suite                             as P

import Data.Data
-- import Data.Generics.Aliases

deriving instance Data P.Unit
deriving instance Data P.SelfOrImportedPackageId
deriving instance Data P.SelfOrImportedPackageIdSum
deriving instance Data P.ModuleId
deriving instance Data P.TypeConId
deriving instance Data P.TypeSynId
deriving instance Data P.ValueId
deriving instance Data P.FieldWithType
deriving instance Data P.VarWithType
deriving instance Data P.TypeVarWithKind
deriving instance Data P.FieldWithExpr
deriving instance Data P.Binding
deriving instance Data P.Kind
deriving instance Data P.Kind_Arrow
deriving instance Data P.KindSum
deriving instance Data P.BuiltinType
deriving instance Data P.Type
deriving instance Data P.Type_Var
deriving instance Data P.Type_Con
deriving instance Data P.Type_Syn
deriving instance Data P.Type_Builtin
deriving instance Data P.Type_Forall
deriving instance Data P.Type_Struct
deriving instance Data P.Type_TApp
deriving instance Data P.TypeSum
deriving instance Data P.BuiltinCon
deriving instance Data P.BuiltinFunction
deriving instance Data P.BuiltinLit
deriving instance Data P.BuiltinLit_RoundingMode
deriving instance Data P.BuiltinLit_FailureCategory
deriving instance Data P.BuiltinLitSum
deriving instance Data P.Location
deriving instance Data P.Location_Range
deriving instance Data P.Expr
deriving instance Data P.Expr_RecCon
deriving instance Data P.Expr_RecProj
deriving instance Data P.Expr_RecUpd
deriving instance Data P.Expr_VariantCon
deriving instance Data P.Expr_EnumCon
deriving instance Data P.Expr_StructCon
deriving instance Data P.Expr_StructProj
deriving instance Data P.Expr_StructUpd
deriving instance Data P.Expr_App
deriving instance Data P.Expr_TyApp
deriving instance Data P.Expr_Abs
deriving instance Data P.Expr_TyAbs
deriving instance Data P.Expr_Nil
deriving instance Data P.Expr_Cons
deriving instance Data P.Expr_OptionalNone
deriving instance Data P.Expr_OptionalSome
deriving instance Data P.Expr_ToAny
deriving instance Data P.Expr_FromAny
deriving instance Data P.Expr_ToAnyException
deriving instance Data P.Expr_FromAnyException
deriving instance Data P.Expr_Throw
deriving instance Data P.Expr_ToInterface
deriving instance Data P.Expr_FromInterface
deriving instance Data P.Expr_CallInterface
deriving instance Data P.Expr_ViewInterface
deriving instance Data P.Expr_SignatoryInterface
deriving instance Data P.Expr_ObserverInterface
deriving instance Data P.Expr_UnsafeFromInterface
deriving instance Data P.Expr_ToRequiredInterface
deriving instance Data P.Expr_FromRequiredInterface
deriving instance Data P.Expr_UnsafeFromRequiredInterface
deriving instance Data P.Expr_InterfaceTemplateTypeRep
deriving instance Data P.Expr_ChoiceController
deriving instance Data P.Expr_ChoiceObserver
deriving instance Data P.Expr_Experimental
deriving instance Data P.ExprSum
deriving instance Data P.CaseAlt
deriving instance Data P.CaseAlt_Variant
deriving instance Data P.CaseAlt_Enum
deriving instance Data P.CaseAlt_Cons
deriving instance Data P.CaseAlt_OptionalSome
deriving instance Data P.CaseAltSum
deriving instance Data P.Case
deriving instance Data P.Block
deriving instance Data P.Pure
deriving instance Data P.Update
deriving instance Data P.Update_Create
deriving instance Data P.Update_CreateInterface
deriving instance Data P.Update_Exercise
deriving instance Data P.Update_ExerciseInterface
deriving instance Data P.Update_ExerciseByKey
deriving instance Data P.Update_Fetch
deriving instance Data P.Update_FetchInterface
deriving instance Data P.Update_EmbedExpr
deriving instance Data P.Update_RetrieveByKey
deriving instance Data P.Update_TryCatch
deriving instance Data P.Update_QueryNByKey
deriving instance Data P.UpdateSum
deriving instance Data P.TemplateChoice
deriving instance Data P.InterfaceInstanceBody
deriving instance Data P.InterfaceInstanceBody_InterfaceInstanceMethod
deriving instance Data P.DefTemplate
deriving instance Data P.DefTemplate_DefKey
deriving instance Data P.DefTemplate_Implements
deriving instance Data P.InterfaceMethod
deriving instance Data P.DefInterface
deriving instance Data P.DefException
deriving instance Data P.DefDataType
deriving instance Data P.DefDataType_Fields
deriving instance Data P.DefDataType_EnumConstructors
deriving instance Data P.DefDataTypeDataCons
deriving instance Data P.DefTypeSyn
deriving instance Data P.DefValue
deriving instance Data P.DefValue_NameWithType
deriving instance Data P.FeatureFlags
deriving instance Data P.Module
deriving instance Data P.InternedDottedName
deriving instance Data P.UpgradedPackageId
deriving instance Data P.PackageImports
deriving instance Data P.PackageImportsSum
deriving instance Data P.Package
deriving instance Data P.PackageMetadata

-- Manual data instance for enumerated, since deriving for newtypes does seem to
-- work properly. This instance is essentially what _should_ happen when
-- generically deriving data for newtypes.
instance Data a => Data (P.Enumerated a) where
  gfoldl f z (P.Enumerated x) = z P.Enumerated `f` x

  gunfold k z c = case constrIndex c of
    1 -> k (z P.Enumerated)
    _ -> error "gunfold: Bad constructor index for Enumerated"

  toConstr (P.Enumerated _) = enumeratedConstr

  dataTypeOf _ = enumeratedDataType

enumeratedConstr :: Constr
enumeratedConstr = mkConstr enumeratedDataType "Enumerated" [] Prefix

enumeratedDataType :: DataType
enumeratedDataType = mkDataType "Proto3.Suite.Types.Enumerated" [enumeratedConstr]
