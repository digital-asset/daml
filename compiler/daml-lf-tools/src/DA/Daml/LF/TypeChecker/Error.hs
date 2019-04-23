-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}
module DA.Daml.LF.TypeChecker.Error where

import DA.Pretty
import Data.Tagged(untag)
import qualified Data.Text as T

import DA.Daml.LF.Ast
import DA.Daml.LF.Ast.Pretty

-- TODO(MH): Rework the context machinery to avoid code duplication.
-- | Type checking context for error reporting purposes.
data Context
  = ContextNone
  | ContextDefDataType !Module !DefDataType
  | ContextTemplate !Module !Template !TemplatePart
  | ContextDefValue !Module !DefValue

data TemplatePart
  = TPWhole
  | TPStakeholders
  | TPPrecondition
  | TPSignatories
  | TPObservers
  | TPAgreement
  | TPKey
  | TPChoice TemplateChoice

data SerializabilityRequirement
  = SRTemplateArg
  | SRChoiceArg
  | SRChoiceRes
  | SRKey
  | SRDataType

-- | Reason why a type is not serializable.
data UnserializabilityReason
  = URFreeVar !TypeVarName  -- ^ It contains a free type variable.
  | URFunction  -- ^ It contains the function type (->).
  | URForall  -- ^ It has higher rank.
  | URUpdate  -- ^ It contains an update action.
  | URScenario  -- ^ It contains a scenario action.
  | URTuple  -- ^ It contains a structural record.
  | URList  -- ^ It contains an unapplied list type constructor.
  | UROptional  -- ^ It contains an unapplied optional type constructor.
  | URMap  -- ^ It contains an unapplied map type constructor.
  | URContractId  -- ^ It contains a ContractId which is not applied to a template type.
  | URDataType !(Qualified TypeConName)  -- ^ It uses a data type which is not serializable.
  | URHigherKinded !TypeVarName !Kind  -- ^ A data type has a higher kinded parameter.
  | URUninhabitatedType  -- ^ A type without values, e.g., a variant with no constructors.

data Error
  = EUnknownTypeVar        !TypeVarName
  | EShadowingTypeVar      !TypeVarName
  | EUnknownExprVar        !ExprVarName
  | EUnknownDefinition     !LookupError
  | ETypeConAppWrongArity  !TypeConApp
  | EDuplicateField        !FieldName
  | EDuplicateVariantCon   !VariantConName
  | EDuplicateModule       !ModuleName
  | EDuplicateScenario     !ExprVarName
  | EExpectedRecordType    !TypeConApp
  | EFieldMismatch         !TypeConApp ![(FieldName, Expr)]
  | EExpectedVariantType   !(Qualified TypeConName)
  | EUnknownVariantCon     !VariantConName
  | EUnknownField          !FieldName
  | EExpectedTupleType     !Type
  | EKindMismatch          {foundKind :: !Kind, expectedKind :: !Kind}
  | ETypeMismatch          {foundType :: !Type, expectedType :: !Type, expr :: !(Maybe Expr)}
  | EExpectedHigherKind    !Kind
  | EExpectedFunctionType  !Type
  | EExpectedUniversalType !Type
  | EExpectedUpdateType    !Type
  | EExpectedScenarioType  !Type
  | EExpectedSerializableType !SerializabilityRequirement !Type !UnserializabilityReason
  | ETypeConMismatch       !(Qualified TypeConName) !(Qualified TypeConName)
  | EExpectedDataType      !Type
  | EExpectedListType      !Type
  | EExpectedOptionalType  !Type
  | EEmptyCase
  | EExpectedTemplatableType !TypeConName
  | EImportCycle           ![ModuleName]
  | EDataTypeCycle         ![TypeConName]
  | EValueCycle            ![ExprValName]
  | EImpredicativePolymorphism !Type
  | EForbiddenPartyLiterals ![PartyLiteral] ![Qualified ExprValName]
  | EContext               !Context !Error
  | ENonValueDefinition    ![(String, Expr)] -- contains the non-value subexpressions and why they're bad
  | EKeyOperationOnTemplateWithNoKey !(Qualified TypeConName)
  | EUnsupportedFeature !T.Text !Version
  | EInvalidKeyExpression !Expr

contextLocation :: Context -> Maybe SourceLoc
contextLocation = \case
  ContextNone            -> Nothing
  ContextDefDataType _ d -> dataLocation d
  ContextTemplate _ t _  -> tplLocation t
  ContextDefValue _ v    -> dvalLocation v

errorLocation :: Error -> Maybe SourceLoc
errorLocation = \case
  EContext ctx _ -> contextLocation ctx
  _ -> Nothing

instance Show Context where
  show = \case
    ContextNone -> "<none>"
    ContextDefDataType m dt ->
      "data type " <> show (moduleName m) <> "." <> show (dataTypeCon dt)
    ContextTemplate m t p ->
      "template " <> show (moduleName m) <> "." <> show (tplTypeCon t) <> " " <> show p
    ContextDefValue m v ->
      "value " <> show (moduleName m) <> "." <> show (fst $ dvalBinder v)

instance Show TemplatePart where
  show = \case
    TPWhole -> ""
    TPStakeholders -> "stakeholders"
    TPPrecondition -> "precondition"
    TPSignatories -> "signatories"
    TPObservers -> "observers"
    TPAgreement -> "agreement"
    TPKey -> "key"
    TPChoice choice -> "choice " <> T.unpack (untag $ chcName choice)

instance Pretty SerializabilityRequirement where
  pPrint = \case
    SRTemplateArg -> "template argument"
    SRChoiceArg -> "choice argument"
    SRChoiceRes -> "choice result"
    SRDataType -> "serializable data type"
    SRKey -> "template key"

instance Pretty UnserializabilityReason where
  pPrint = \case
    URFreeVar v -> "free type variable" <-> prettyName v
    URFunction -> "function type"
    URForall -> "higher-ranked type"
    URUpdate -> "Update"
    URScenario -> "Scenario"
    URTuple -> "structual record"
    URList -> "unapplied List"
    UROptional -> "unapplied Optional"
    URMap -> "unapplied Map"
    URContractId -> "ContractId not applied to a template type"
    URDataType tcon ->
      "unserializable data type" <-> prettyQualified prettyDottedName tcon
    URHigherKinded v k -> "higher-kinded type variable" <-> prettyName v <:> pretty k
    URUninhabitatedType -> "variant type without constructors"

instance Pretty Error where
  pPrint = \case
    EContext ctx err ->
      vcat
      [ "error type checking " <> pretty ctx <> ":"
      , nest 2 (pretty err)
      ]

    EUnknownTypeVar v -> "unknown type variable: " <> prettyName v
    EShadowingTypeVar v -> "shadowing type variable: " <> prettyName v
    EUnknownExprVar v -> "unknown expr variable: " <> prettyName v
    EUnknownDefinition e -> pretty e
    ETypeConAppWrongArity tapp -> "wrong arity in typecon application: " <> string (show tapp)
    EDuplicateField name -> "duplicate field: " <> prettyName name
    EDuplicateVariantCon name -> "duplicate variant constructor: " <> prettyName name
    EDuplicateModule mname -> "duplicate module: " <> prettyDottedName mname
    EDuplicateScenario name -> "duplicate scenario: " <> prettyName name
    EExpectedRecordType tapp ->
      vcat [ "expected record type:", "* found: ", nest 4 $ string (show tapp) ]
    EFieldMismatch tapp rexpr ->
      vcat
      [ "field mismatch:"
      , "* expected: "
      , nest 4 (string $ show tapp)
      , "* record expression: "
      , nest 4 (string $ show rexpr)
      ]
    EExpectedVariantType qname -> "expected variant type: " <> prettyQualified prettyDottedName qname
    EUnknownVariantCon name -> "unknown variant constructor: " <> prettyName name
    EUnknownField name -> "unknown field: " <> prettyName name
    EExpectedTupleType foundType ->
      "expected tuple type, but found: " <> pretty foundType

    ETypeMismatch{foundType, expectedType, expr} ->
      vcat $
      [ "type mismatch:"
      , "* expected type:"
      , nest 4 (pretty expectedType)
      , "* found type:"
      , nest 4 (pretty foundType)
      ] ++
      maybe [] (\e -> ["* expression:", nest 4 (pretty e)]) expr
    EKindMismatch{foundKind, expectedKind} ->
      vcat
      [ "kind mismatch:"
      , "* expected kind:"
      , nest 4 (pretty expectedKind)
      , "* found Kind:"
      , nest 4 (pretty foundKind)
      ]

    EExpectedFunctionType foundType ->
      "expected function type, but found: " <> pretty foundType
    EExpectedHigherKind foundKind ->
      "expected higher kinded type, but found: " <> pretty foundKind
    EExpectedUniversalType foundType ->
      "expected universal type, but found: " <> pretty foundType
    EExpectedUpdateType foundType ->
      "expected update type, but found: " <> pretty foundType
    EExpectedScenarioType foundType ->
      "expected scenario type, but found: " <> pretty foundType
    ETypeConMismatch found expected ->
      vcat
      [ "type constructor mismatch:"
      , "* expected: "
      , nest 4 (prettyQualified prettyDottedName expected)
      , "* found: "
      , nest 4 (prettyQualified prettyDottedName found)
      ]
    EExpectedDataType foundType ->
      "expected data type, but found: " <> pretty foundType
    EExpectedListType foundType ->
      "expected list type, but found: " <> pretty foundType
    EEmptyCase -> "empty case"
    EExpectedTemplatableType tpl ->
      "expected monomorphic record type in template definition, but found:"
      <-> prettyDottedName tpl
    EImportCycle mods ->
      "found import cycle:" $$ vcat (map (\m -> "*" <-> prettyDottedName m) mods)
    EDataTypeCycle tycons ->
      "found data type cycle:" $$ vcat (map (\t -> "*" <-> prettyDottedName t) tycons)
    EValueCycle names ->
      "found value cycle:" $$ vcat (map (\n -> "*" <-> prettyName n) names)
    EExpectedSerializableType reason foundType info ->
      vcat
      [ "expected serializable type:"
      , "* reason:" <-> pretty reason
      , "* found:" <-> pretty foundType
      , "* problem:"
      , nest 4 (pretty info)
      ]
    EImpredicativePolymorphism typ ->
      vcat
      [ "impredicative polymorphism is not supported:"
      , "* found:" <-> pretty typ
      ]
    EForbiddenPartyLiterals parties badRefs ->
      vcat $ [partiesDoc | not (null parties)] ++ [badRefsDoc | not (null badRefs)]
      where
        partiesDoc =
          vcat $
            "Found forbidden party literals:"
            : map (\party -> "*" <-> pretty party) parties
        badRefsDoc =
          vcat $
            "Found forbidden references to functions containing party literals:"
            : map (\badRef -> "*" <-> prettyQualified prettyName badRef) badRefs
    ENonValueDefinition badSubExprs -> do
      vcat $ ("definition is not a value:" :) $ do
        (reason, expr) <- badSubExprs
        [string ("* " ++ reason), nest 4 (pretty expr)]
    EKeyOperationOnTemplateWithNoKey tpl -> do
      "tried to perform key lookup or fetch on template " <> prettyQualified prettyDottedName tpl
    EInvalidKeyExpression expr ->
      vcat
      [ "expected valid key expression:"
      , "* found:" <-> pretty expr
      ]
    EExpectedOptionalType typ -> do
      "expected list type, but found: " <> pretty typ
    EUnsupportedFeature feature version ->
      "unsupported feature:" <-> pretty feature
      <-> "only supported in DAML-LF version" <-> pretty version <-> "and later"

instance Pretty Context where
  pPrint = \case
    ContextNone ->
      string "<none>"
    ContextDefDataType m dt ->
      hsep [ "data type", prettyDottedName (moduleName m) <> "." <>  prettyDottedName (dataTypeCon dt) ]
    ContextTemplate m t p ->
      hsep [ "template", prettyDottedName (moduleName m) <> "." <>  prettyDottedName (tplTypeCon t), string (show p) ]
    ContextDefValue m v ->
      hsep [ "value", prettyDottedName (moduleName m) <> "." <> prettyName (fst $ dvalBinder v) ]
