-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.TypeChecker.Error(
    Context(..),
    Error(..),
    UpgradeError(..),
    TemplatePart(..),
    InterfacePart(..),
    UnserializabilityReason(..),
    SerializabilityRequirement(..),
    UpgradedRecordOrigin(..),
    errorLocation,
    toDiagnostic,
    Warning(..),
    ) where

import Control.Applicative
import DA.Daml.LF.Ast
import DA.Daml.LF.Ast.Pretty
import DA.Daml.UtilLF (sourceLocToRange)
import DA.Pretty
import Data.Text qualified as T
import Development.IDE.Types.Diagnostics
import Development.IDE.Types.Location
import Numeric.Natural

-- TODO(MH): Rework the context machinery to avoid code duplication.
-- | Type checking context for error reporting purposes.
data Context
  = ContextNone
  | ContextDefTypeSyn !Module !DefTypeSyn
  | ContextDefDataType !Module !DefDataType
  | ContextTemplate !Module !Template !TemplatePart
  | ContextDefValue !Module !DefValue
  | ContextDefException !Module !DefException
  | ContextDefInterface !Module !DefInterface !InterfacePart

data TemplatePart
  = TPWhole
  | TPPrecondition
  | TPSignatories
  | TPObservers
  | TPAgreement
  | TPKey
  -- ^ Specifically the `key` keyword, not maintainers
  | TPChoice TemplateChoice
  | TPInterfaceInstance InterfaceInstanceHead (Maybe SourceLoc)

data InterfacePart
  = IPWhole
  | IPMethod InterfaceMethod
  | IPChoice TemplateChoice
  | IPInterfaceInstance InterfaceInstanceHead (Maybe SourceLoc)

data SerializabilityRequirement
  = SRTemplateArg
  | SRChoiceArg
  | SRChoiceRes
  | SRKey
  | SRDataType
  | SRExceptionArg
  | SRView

-- | Reason why a type is not serializable.
data UnserializabilityReason
  = URFreeVar !TypeVarName  -- ^ It contains a free type variable.
  | URFunction  -- ^ It contains the function type (->).
  | URForall  -- ^ It has higher rank.
  | URUpdate  -- ^ It contains an update action.
  | URScenario  -- ^ It contains a scenario action.
  | URStruct  -- ^ It contains a structural record.
  | URList  -- ^ It contains an unapplied list type constructor.
  | UROptional  -- ^ It contains an unapplied optional type constructor.
  | URMap  -- ^ It contains an unapplied map type constructor.
  | URGenMap  -- ^ It contains an unapplied GenMap type constructor.
  | URContractId  -- ^ It contains a ContractId which is not applied to a template type.
  | URDataType !(Qualified TypeConName)  -- ^ It uses a data type which is not serializable.
  | URHigherKinded !TypeVarName !Kind  -- ^ A data type has a higher kinded parameter.
  | URUninhabitatedType  -- ^ A type without values, e.g., a variant with no constructors.
  | URNumeric -- ^ It contains an unapplied Numeric type constructor.
  | URNumericNotFixed
  | URNumericOutOfRange !Natural
  | URTypeLevelNat
  | URAny -- ^ It contains a value of type Any.
  | URRoundingMode -- ^ It contains a value of type RoundingMode
  | URBigNumeric -- ^ It contains a value of type BigBumeric
  | URAnyException -- ^ It contains a value of type AnyException.
  | URTypeRep -- ^ It contains a value of type TypeRep.
  | URTypeSyn  -- ^ It contains a type synonym.
  | URExperimental -- ^ It contains a experimental type
  | URInterface -- ^ It constains an interface

data Error
  = EUnknownTypeVar        !TypeVarName
  | EUnknownExprVar        !ExprVarName
  | EUnknownDefinition     !LookupError
  | ETypeConAppWrongArity  !TypeConApp
  | EDuplicateTypeParam    !TypeVarName
  | EDuplicateField        !FieldName
  | EDuplicateConstructor  !VariantConName
  | EDuplicateModule       !ModuleName
  | EDuplicateScenario     !ExprVarName
  | EEnumTypeWithParams
  | EExpectedRecordType    !TypeConApp
  | EFieldMismatch         !TypeConApp ![(FieldName, Expr)]
  | EExpectedVariantType   !(Qualified TypeConName)
  | EExpectedEnumType      !(Qualified TypeConName)
  | EUnknownDataCon        !VariantConName
  | EUnknownField          !FieldName !Type
  | EExpectedStructType    !Type
  | EKindMismatch          {foundKind :: !Kind, expectedKind :: !Kind}
  | ETypeMismatch          {foundType :: !Type, expectedType :: !Type, expr :: !(Maybe Expr)}
  | EFieldTypeMismatch     {fieldName :: !FieldName, targetRecord :: !Type, foundType :: !Type, expectedType :: !Type, expr :: !(Maybe Expr)}
  | EPatternTypeMismatch   {pattern :: !CasePattern, scrutineeType :: !Type}
  | ENonExhaustivePatterns {missingPattern :: !CasePattern, scrutineeType :: !Type}
  | EExpectedHigherKind    !Kind
  | EExpectedFunctionType  !Type
  | EExpectedUniversalType !Type
  | EExpectedUpdateType    !Type
  | EExpectedScenarioType  !Type
  | EExpectedSerializableType !SerializabilityRequirement !Type !UnserializabilityReason
  | EExpectedKeyTypeWithoutContractId !Type
  | EExpectedAnyType !Type
  | EExpectedExceptionType !Type
  | EExpectedExceptionTypeHasNoParams !ModuleName !TypeConName
  | EExpectedExceptionTypeIsRecord !ModuleName !TypeConName
  | EExpectedExceptionTypeIsNotTemplate !ModuleName !TypeConName
  | ETypeConMismatch       !(Qualified TypeConName) !(Qualified TypeConName)
  | EExpectedDataType      !Type
  | EExpectedListType      !Type
  | EExpectedOptionalType  !Type
  | EViewTypeHeadNotCon !Type !Type
  | EViewTypeHasVars !Type
  | EViewTypeConNotRecord !DataCons !Type
  | EViewTypeMismatch { evtmIfaceName :: !(Qualified TypeConName), evtmTplName :: !(Qualified TypeConName), evtmFoundType :: !Type, evtmExpectedType :: !Type, evtmExpr :: !(Maybe Expr) }
  | EMethodTypeMismatch { emtmIfaceName :: !(Qualified TypeConName), emtmTplName :: !(Qualified TypeConName), emtmMethodName :: !MethodName, emtmFoundType :: !Type, emtmExpectedType :: !Type, emtmExpr :: !(Maybe Expr) }
  | EEmptyCase
  | EClashingPatternVariables !ExprVarName
  | EExpectedTemplatableType !TypeConName
  | EImportCycle           ![ModuleName] -- TODO: implement check for this error
  | ETypeSynCycle          ![TypeSynName]
  | EDataTypeCycle         ![TypeConName] -- TODO: implement check for this error
  | EValueCycle            ![ExprValName]
  | EImpredicativePolymorphism !Type
  | EContext               !Context !Error
  | EKeyOperationOnTemplateWithNoKey !(Qualified TypeConName)
  | EUnsupportedFeature !Feature
  | EForbiddenNameCollision !T.Text ![T.Text]
  | ESynAppWrongArity       !DefTypeSyn ![Type]
  | ENatKindRightOfArrow    !Kind
  | EInterfaceTypeWithParams
  | EMissingInterfaceDefinition !TypeConName
  | EDuplicateTemplateChoiceViaInterfaces !TypeConName !ChoiceName
  | EDuplicateInterfaceChoiceName !TypeConName !ChoiceName
  | EDuplicateInterfaceMethodName !TypeConName !MethodName
  | EUnknownInterface !TypeConName
  | ECircularInterfaceRequires !TypeConName !(Maybe (Qualified TypeConName))
  | ENotClosedInterfaceRequires !TypeConName !(Qualified TypeConName) ![Qualified TypeConName]
  | EMissingRequiredInterfaceInstance !InterfaceInstanceHead !(Qualified TypeConName)
  | EBadInheritedChoices { ebicInterface :: !(Qualified TypeConName), ebicExpected :: ![ChoiceName], ebicGot :: ![ChoiceName] }
  | EMissingInterfaceChoice !ChoiceName
  | EMissingMethodInInterfaceInstance !MethodName
  | EUnknownMethodInInterfaceInstance { eumiiIface :: !(Qualified TypeConName), eumiiTpl :: !(Qualified TypeConName), eumiiMethodName :: !MethodName }
  | EWrongInterfaceRequirement !(Qualified TypeConName) !(Qualified TypeConName)
  | EUpgradeError !UpgradeError
  | EUnknownExperimental !T.Text !Type

data UpgradeError
  = MissingModule !ModuleName
  | MissingTemplate !TypeConName
  | MissingChoice !ChoiceName
  | MissingDataCon !TypeConName
  | MismatchDataConsVariety !TypeConName
  | RecordFieldsMissing !UpgradedRecordOrigin
  | RecordFieldsExistingChanged !UpgradedRecordOrigin
  | RecordFieldsNewNonOptional !UpgradedRecordOrigin
  | RecordChangedOrigin !TypeConName !UpgradedRecordOrigin !UpgradedRecordOrigin
  | TemplateChangedKeyType !TypeConName
  | ChoiceChangedReturnType !ChoiceName
  | TemplateRemovedKey !TypeConName !TemplateKey
  deriving (Eq, Ord, Show)

data UpgradedRecordOrigin
  = TemplateBody TypeConName
  | TemplateChoiceInput TypeConName ChoiceName
  | TopLevel
  deriving (Eq, Ord, Show)

contextLocation :: Context -> Maybe SourceLoc
contextLocation = \case
  ContextNone                -> Nothing
  ContextDefTypeSyn _ s      -> synLocation s
  ContextDefDataType _ d     -> dataLocation d
  ContextTemplate _ t tp     -> templateLocation t tp <|> tplLocation t -- Fallback to template header location if other locations are missing
  ContextDefValue _ v        -> dvalLocation v
  ContextDefException _ e    -> exnLocation e
  ContextDefInterface _ i ip -> interfaceLocation i ip <|> intLocation i -- Fallback to interface header location if other locations are missing

templateLocation :: Template -> TemplatePart -> Maybe SourceLoc
templateLocation t = \case
  TPWhole -> tplLocation t
  TPPrecondition -> extractExprSourceLoc $ tplPrecondition t
  TPSignatories -> extractExprSourceLoc $ tplSignatories t
  TPObservers -> extractExprSourceLoc $ tplObservers t 
  TPAgreement -> extractExprSourceLoc $ tplAgreement t
  TPKey -> tplKey t >>= extractExprSourceLoc . tplKeyBody
  TPChoice tc -> chcLocation tc
  TPInterfaceInstance _ loc -> loc

-- This function is untested and difficult to test with current architecture. It is written as best effort, but any failure on its part simply falls back to
-- template/interface header source location.
-- This function isn't easily testable because GHC catches these errors before daml gets to them.
extractExprSourceLoc :: Expr -> Maybe SourceLoc
extractExprSourceLoc (ELocation loc _) = Just loc
extractExprSourceLoc (ETmApp f _) = extractExprSourceLoc f -- All 4 of the Expr values in Template are wrapped in ($ this), so we match this out
extractExprSourceLoc (ECase c _) = extractExprSourceLoc c -- Precondition wraps the bool in a case when featureExceptions is supported
extractExprSourceLoc _ = Nothing

interfaceLocation :: DefInterface -> InterfacePart -> Maybe SourceLoc
interfaceLocation i = \case
  IPWhole -> intLocation i
  IPMethod im -> ifmLocation im
  IPChoice tc -> chcLocation tc
  IPInterfaceInstance _ loc -> loc

errorLocation :: Error -> Maybe SourceLoc
errorLocation = \case
  EContext ctx _ -> contextLocation ctx
  _ -> Nothing

instance Show Context where
  show = \case
    ContextNone -> "<none>"
    ContextDefTypeSyn m ts ->
      "type synonym " <> show (moduleName m) <> "." <> show (synName ts)
    ContextDefDataType m dt ->
      "data type " <> show (moduleName m) <> "." <> show (dataTypeCon dt)
    ContextTemplate m t p ->
      "template " <> show (moduleName m) <> "." <> show (tplTypeCon t) <> " " <> show p
    ContextDefValue m v ->
      "value " <> show (moduleName m) <> "." <> show (fst $ dvalBinder v)
    ContextDefException m e ->
      "exception " <> show (moduleName m) <> "." <> show (exnName e)
    ContextDefInterface m i p ->
      "interface " <> show (moduleName m) <> "." <> show (intName i) <> " " <> show p

instance Show TemplatePart where
  show = \case
    TPWhole -> ""
    TPPrecondition -> "precondition"
    TPSignatories -> "signatories"
    TPObservers -> "observers"
    TPAgreement -> "agreement"
    TPKey -> "key"
    TPChoice choice -> "choice " <> T.unpack (unChoiceName $ chcName choice)
    TPInterfaceInstance iiHead _ -> renderPretty iiHead

instance Show InterfacePart where
  show = \case
    IPWhole -> ""
    IPMethod method -> "method " <> T.unpack (unMethodName $ ifmName method)
    IPChoice choice -> "choice " <> T.unpack (unChoiceName $ chcName choice)
    IPInterfaceInstance iiHead _ -> renderPretty iiHead

instance Pretty SerializabilityRequirement where
  pPrint = \case
    SRTemplateArg -> "template argument"
    SRChoiceArg -> "choice argument"
    SRChoiceRes -> "choice result"
    SRDataType -> "serializable data type"
    SRKey -> "template key"
    SRExceptionArg -> "exception argument"
    SRView -> "view"

instance Pretty UnserializabilityReason where
  pPrint = \case
    URFreeVar v -> "free type variable" <-> pretty v
    URFunction -> "function type"
    URForall -> "higher-ranked type"
    URUpdate -> "Update"
    URScenario -> "Scenario"
    URStruct -> "structual record"
    URList -> "unapplied List"
    UROptional -> "unapplied Optional"
    URMap -> "unapplied Map"
    URGenMap -> "unapplied GenMap"
    URContractId -> "ContractId not applied to a template type"
    URDataType tcon ->
      "unserializable data type" <-> pretty tcon
    URHigherKinded v k -> "higher-kinded type variable" <-> pretty v <:> pretty k
    URUninhabitatedType -> "variant type without constructors"
    URNumeric -> "unapplied Numeric"
    URNumericNotFixed -> "Numeric scale is not fixed"
    URNumericOutOfRange n -> "Numeric scale " <> integer (fromIntegral n) <> " is out of range (needs to be between 0 and 38)"
    URTypeLevelNat -> "type-level nat"
    URAny -> "Any"
    URAnyException -> "AnyException"
    URTypeRep -> "TypeRep"
    URTypeSyn -> "type synonym"
    URRoundingMode -> "RoundingMode"
    URBigNumeric -> "BigNumeric"
    URExperimental -> "experimental type"
    URInterface -> "interface"

instance Pretty Error where
  pPrint = \case
    EContext ctx err ->
      vcat
      [ "error type checking " <> pretty ctx <> ":"
      , nest 2 (pretty err)
      ]

    EUnknownTypeVar v -> "unknown type variable: " <> pretty v
    EUnknownExprVar v -> "unknown expr variable: " <> pretty v
    EUnknownDefinition e -> pretty e
    ETypeConAppWrongArity tapp -> "wrong arity in typecon application: " <> string (show tapp)
    EDuplicateTypeParam name -> "duplicate type parameter: " <> pretty name
    EDuplicateField name -> "duplicate field: " <> pretty name
    EDuplicateConstructor name -> "duplicate constructor: " <> pretty name
    EDuplicateModule mname -> "duplicate module: " <> pretty mname
    EDuplicateScenario name -> "duplicate scenario: " <> pretty name
    EEnumTypeWithParams -> "enum type with type parameters"
    EInterfaceTypeWithParams -> "interface type with type parameters"
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
    EExpectedVariantType qname -> "expected variant type: " <> pretty qname
    EExpectedEnumType qname -> "expected enum type: " <> pretty qname
    EUnknownDataCon name -> "unknown data constructor: " <> pretty name
    EUnknownField fieldName targetType ->
      text "Tried to access nonexistent field " <> pretty fieldName <>
      text " on value of type " <> pretty targetType
    EExpectedStructType foundType ->
      "expected struct type, but found: " <> pretty foundType

    ETypeMismatch{foundType, expectedType, expr} ->
      vcat $
      [ "type mismatch:"
      , "* expected type:"
      , nest 4 (pretty expectedType)
      , "* found type:"
      , nest 4 (pretty foundType)
      ] ++
      maybe [] (\e -> ["* expression:", nest 4 (pretty e)]) expr

    EFieldTypeMismatch { fieldName, targetRecord, foundType, expectedType, expr } ->
      vcat $
      [ text "Tried to use field " <> pretty fieldName
         <> text " with type " <> pretty foundType
         <> text " on value of type " <> pretty targetRecord
         <> text ", but that field has type " <> pretty expectedType
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
    EPatternTypeMismatch{pattern, scrutineeType} ->
      vcat $
      [ "pattern type mismatch:"
      , "* pattern:"
      , nest 4 (pretty pattern)
      , "* scrutinee type:"
      , nest 4 (pretty scrutineeType)
      ]
    ENonExhaustivePatterns{missingPattern, scrutineeType} ->
      vcat $
      [ "non-exhaustive pattern match:"
      , "* missing pattern:"
      , nest 4 (pretty missingPattern)
      , "* scrutinee type:"
      , nest 4 (pretty scrutineeType)
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
      , nest 4 (pretty expected)
      , "* found: "
      , nest 4 (pretty found)
      ]
    EExpectedDataType foundType ->
      "expected data type, but found: " <> pretty foundType
    EExpectedListType foundType ->
      "expected list type, but found: " <> pretty foundType
    EEmptyCase -> "empty case"
    EClashingPatternVariables varName ->
      "the variable " <> pretty varName <> " is used more than once in the pattern"
    EExpectedTemplatableType tpl ->
      "expected monomorphic record type in template definition, but found:"
      <-> pretty tpl
    EImportCycle mods ->
      "found import cycle:" $$ vcat (map (\m -> "*" <-> pretty m) mods)
    ETypeSynCycle syns ->
      "found type synonym cycle:" $$ vcat (map (\t -> "*" <-> pretty t) syns)
    EDataTypeCycle tycons ->
      "found data type cycle:" $$ vcat (map (\t -> "*" <-> pretty t) tycons)
    EValueCycle names ->
      "found value cycle:" $$ vcat (map (\n -> "*" <-> pretty n) names)
    EExpectedSerializableType reason foundType info ->
      vcat
      [ "expected serializable type:"
      , "* reason:" <-> pretty reason
      , "* found:" <-> pretty foundType
      , "* problem:"
      , nest 4 (pretty info)
      ]
    EExpectedKeyTypeWithoutContractId foundType ->
      vcat
      [ "contract key type should not contain ContractId:"
      , "* found:" <-> pretty foundType
      ]
    EExpectedAnyType foundType ->
      "expected a type containing neither type variables nor quantifiers, but found: " <> pretty foundType
    EExpectedExceptionType foundType ->
      "expected an exception type, but found: " <> pretty foundType
    EExpectedExceptionTypeHasNoParams modName exnName ->
      "exception type must not have type parameters: " <> pretty modName <> "." <> pretty exnName
    EExpectedExceptionTypeIsRecord modName exnName ->
      "exception type must be a record type: " <> pretty modName <> "." <> pretty exnName
    EExpectedExceptionTypeIsNotTemplate modName exnName ->
      "exception type must not be a template: " <> pretty modName <> "." <> pretty exnName
    EImpredicativePolymorphism typ ->
      vcat
      [ "impredicative polymorphism is not supported:"
      , "* found:" <-> pretty typ
      ]
    EKeyOperationOnTemplateWithNoKey tpl -> do
      "tried to perform key lookup or fetch on template " <> pretty tpl
    EExpectedOptionalType typ -> do
      "expected list type, but found: " <> pretty typ
    EViewTypeHeadNotCon badHead typ ->
      let headName = case badHead of
            TVar {} -> "a type variable"
            TSynApp {} -> "a type synonym"
            TBuiltin {} -> "a built-in type"
            TForall {} -> "a forall-quantified type"
            TStruct {} -> "a structural record"
            TNat {} -> "a type-level natural number"
            TCon {} -> error "pPrint EViewTypeHeadNotCon got TCon: should not happen"
            TApp {} -> error "pPrint EViewTypeHeadNotCon got TApp: should not happen"
      in
      vcat
        [ "expected monomorphic record type in view type, but found " <> text headName <> ": " <> pretty typ
        , "record types are declared with one constructor using curly braces, i.e."
        , "data MyRecord = MyRecord { ... fields ... }"
        ]
    EViewTypeHasVars typ ->
      vcat
        [ "expected monomorphic record type in view type, but found a type constructor with type variables: " <> pretty typ
        , "record types are declared with one constructor using curly braces, i.e."
        , "data MyRecord = MyRecord { ... fields ... }"
        ]
    EViewTypeConNotRecord dataCons typ ->
      let headName = case dataCons of
            DataVariant {} -> "a variant type"
            DataEnum {} -> "an enum type"
            DataInterface {} -> "a interface type"
            DataRecord {} -> error "pPrint EViewTypeConNotRecord got DataRecord: should not happen"
      in
      vcat
        [ "expected monomorphic record type in view type, but found " <> text headName <> ": " <> pretty typ
        , "record types are declared with one constructor using curly braces, i.e."
        , "data MyRecord = MyRecord { ... fields ... }"
        ]
    EViewTypeMismatch { evtmIfaceName, evtmTplName, evtmFoundType, evtmExpectedType, evtmExpr } ->
      vcat $
        [ text "Tried to implement a view of type " <> pretty evtmFoundType
          <> text " on interface " <> pretty evtmIfaceName
          <> text " for template " <> pretty evtmTplName
          <> text ", but the definition of interface " <> pretty evtmIfaceName
          <> text " requires a view of type " <> pretty evtmExpectedType
        ] ++
        maybe [] (\e -> ["* in expression:", nest 4 (pretty e)]) evtmExpr
    EMethodTypeMismatch { emtmIfaceName, emtmMethodName, emtmFoundType, emtmExpectedType } ->
      text "Implementation of method " <> pretty emtmMethodName <> text " on interface " <> pretty emtmIfaceName
      <> text " should return " <> pretty emtmExpectedType <> text " but instead returns " <> pretty emtmFoundType
    EUnsupportedFeature Feature{..} ->
      "unsupported feature:" <-> pretty featureName
      <-> "only supported in Daml-LF versions" <-> pretty featureVersionReq
    EForbiddenNameCollision name names ->
      "name collision between" <-> pretty name <-> "and" <-> pretty (T.intercalate ", " names)
    ESynAppWrongArity DefTypeSyn{synName,synParams} args ->
      vcat ["wrong arity in type synonym application: " <> pretty synName,
            "expected: " <> pretty (length synParams) <> ", found: " <> pretty (length args)]
    ENatKindRightOfArrow k ->
      vcat
        [ "Kind is invalid: " <> pretty k
        , "Nat kind is not allowed on the right side of kind arrow."
        ]
    EMissingInterfaceDefinition iface ->
      "Missing interface definition for interface type: " <> pretty iface
    EDuplicateTemplateChoiceViaInterfaces tpl choice ->
      "Duplicate choice name '" <> pretty choice <> "' in template " <> pretty tpl <> " via interfaces."
    EDuplicateInterfaceChoiceName iface choice ->
      "Duplicate choice name '" <> pretty choice <> "' in interface definition for " <> pretty iface
    EDuplicateInterfaceMethodName iface method ->
      "Duplicate method name '" <> pretty method <> "' in interface definition for " <> pretty iface
    EUnknownInterface tcon -> "Unknown interface: " <> pretty tcon
    ECircularInterfaceRequires iface Nothing ->
      "Circular interface requirement is not allowed: interface " <> pretty iface <> " requires itself."
    ECircularInterfaceRequires iface (Just otherIface) ->
      "Circular interface requirement is not allowed: interface "
        <> pretty iface <> " requires "
        <> pretty otherIface <> " requires "
        <> pretty iface
    ENotClosedInterfaceRequires iface ifaceRequired ifaceMissing ->
      "Interface " <> pretty iface
        <> " is missing requirement " <> pretty ifaceMissing
        <> " required by " <> pretty ifaceRequired
    EMissingRequiredInterfaceInstance requiredInterfaceInstance requiringInterface ->
      hsep
        [ "Missing required"
        , quotes (pretty requiredInterfaceInstance) <> ","
        , "required by interface"
        , quotes (pretty requiringInterface)
        ]
    EBadInheritedChoices {ebicInterface, ebicExpected, ebicGot} ->
      vcat
      [ "List of inherited choices does not match interface definition for " <> pretty ebicInterface
      , "Expected: " <> pretty ebicExpected
      , "But got: " <> pretty ebicGot
      ]
    EMissingInterfaceChoice ch -> "Missing interface choice implementation for " <> pretty ch
    EMissingMethodInInterfaceInstance method ->
      "Interface instance lacks an implementation for method" <-> quotes (pretty method)
    EUnknownMethodInInterfaceInstance { eumiiIface, eumiiMethodName } ->
      text "Tried to implement method " <> quotes (pretty eumiiMethodName) <> text ", but interface " <> pretty eumiiIface <> text " does not have a method with that name."
    EWrongInterfaceRequirement requiringIface requiredIface ->
      "Interface " <> pretty requiringIface <> " does not require interface " <> pretty requiredIface
    EUpgradeError upgradeError -> pPrint upgradeError
    EUnknownExperimental name ty ->
      "Unknown experimental primitive " <> string (show name) <> " : " <> pretty ty

instance Pretty UpgradeError where
  pPrint = \case
    MissingModule moduleName -> "Module " <> pPrint moduleName <> " appears in package that is being upgraded, but does not appear in this package."
    MissingTemplate templateName -> "Template " <> pPrint templateName <> " appears in package that is being upgraded, but does not appear in this package."
    MissingChoice templateName -> "Choice " <> pPrint templateName <> " appears in package that is being upgraded, but does not appear in this package."
    MissingDataCon dataConName -> "Data type " <> pPrint dataConName <> " appears in package that is being upgraded, but does not appear in this package."
    MismatchDataConsVariety dataConName -> "EUpgradeMismatchDataConsVariety " <> pretty dataConName
    RecordFieldsMissing origin -> "The upgraded " <> pPrint origin <> " is missing some of its original fields."
    RecordFieldsExistingChanged origin -> "The upgraded " <> pPrint origin <> " has changed the types of some of its original fields."
    RecordFieldsNewNonOptional origin -> "The upgraded " <> pPrint origin <> " has added new fields, but those fields are not Optional."
    RecordChangedOrigin dataConName past present -> "The record " <> pPrint dataConName <> " has changed origin from " <> pPrint past <> " to " <> pPrint present
    ChoiceChangedReturnType choice -> "The upgraded choice " <> pPrint choice <> " cannot change its return type."
    TemplateChangedKeyType templateName -> "The upgraded template " <> pPrint templateName <> " cannot change its key type."
    TemplateRemovedKey templateName _key -> "The upgraded template " <> pPrint templateName <> " cannot remove its key."

instance Pretty UpgradedRecordOrigin where
  pPrint = \case
    TemplateBody tpl -> "template " <> pPrint tpl
    TemplateChoiceInput tpl chcName -> "input type of choice " <> pPrint chcName <> " on template " <> pPrint tpl
    TopLevel -> "record"

instance Pretty Context where
  pPrint = \case
    ContextNone ->
      string "<none>"
    ContextDefTypeSyn m ts ->
      hsep [ "type synonym", pretty (moduleName m) <> "." <>  pretty (synName ts) ]
    ContextDefDataType m dt ->
      hsep [ "data type", pretty (moduleName m) <> "." <>  pretty (dataTypeCon dt) ]
    ContextTemplate m t p ->
      hsep [ "template", pretty (moduleName m) <> "." <>  pretty (tplTypeCon t), string (show p) ]
    ContextDefValue m v ->
      hsep [ "value", pretty (moduleName m) <> "." <> pretty (fst $ dvalBinder v) ]
    ContextDefException m e ->
      hsep [ "exception", pretty (moduleName m) <> "." <> pretty (exnName e) ]
    ContextDefInterface m i p ->
      hsep [ "interface", pretty (moduleName m) <> "." <> pretty (intName i), string (show p)]

class ToDiagnostic a where
  toDiagnostic :: a -> Diagnostic

instance ToDiagnostic Error where
  toDiagnostic err = Diagnostic
      { _range = maybe noRange sourceLocToRange (errorLocation err)
      , _severity = Just DsError
      , _code = Nothing
      , _tags = Nothing
      , _source = Just "Daml-LF typechecker"
      , _message = renderPretty err
      , _relatedInformation = Nothing
      }

data Warning
  = WContext !Context !Warning
  | WTemplateChangedPrecondition !TypeConName
  | WTemplateChangedSignatories !TypeConName
  | WTemplateChangedObservers !TypeConName
  | WTemplateChangedAgreement !TypeConName
  | WChoiceChangedControllers !ChoiceName
  | WChoiceChangedObservers !ChoiceName
  | WChoiceChangedAuthorizers !ChoiceName
  | WTemplateChangedKeyExpression !TypeConName
  | WTemplateChangedKeyMaintainers !TypeConName
  | WTemplateAddedKeyDefinition !TypeConName !TemplateKey
  | WCouldNotExtractForUpgradeChecking !T.Text !(Maybe T.Text)
    -- ^ When upgrading, we extract relevant expressions for things like
    -- signatories. If the expression changes shape so that we can't get the
    -- underlying expression that has changed, this warning is emitted.
  deriving (Show)

warningLocation :: Warning -> Maybe SourceLoc
warningLocation = \case
  WContext ctx _ -> contextLocation ctx
  _ -> Nothing

instance Pretty Warning where
  pPrint = \case
    WContext ctx err ->
      vcat
      [ "warning while type checking " <> pretty ctx <> ":"
      , nest 2 (pretty err)
      ]
    WTemplateChangedPrecondition template -> "The upgraded template " <> pPrint template <> " has changed the definition of its precondition."
    WTemplateChangedSignatories template -> "The upgraded template " <> pPrint template <> " has changed the definition of its signatories."
    WTemplateChangedObservers template -> "The upgraded template " <> pPrint template <> " has changed the definition of its observers."
    WTemplateChangedAgreement template -> "The upgraded template " <> pPrint template <> " has changed the definition of agreement."
    WChoiceChangedControllers choice -> "The upgraded choice " <> pPrint choice <> " has changed the definition of controllers."
    WChoiceChangedObservers choice -> "The upgraded choice " <> pPrint choice <> " has changed the definition of observers."
    WChoiceChangedAuthorizers choice -> "The upgraded choice " <> pPrint choice <> " has changed the definition of authorizers."
    WTemplateChangedKeyExpression template -> "The upgraded template " <> pPrint template <> " has changed the expression for computing its key."
    WTemplateChangedKeyMaintainers template -> "The upgraded template " <> pPrint template <> " has changed the maintainers for its key."
    WTemplateAddedKeyDefinition template _key -> "The upgraded template " <> pPrint template <> " has added a key where it didn't have one previously."
    WCouldNotExtractForUpgradeChecking attribute mbExtra -> "Could not check if the upgrade of " <> text attribute <> " is valid because its expression is the not the right shape." <> foldMap (const " Extra context: " <> text) mbExtra

instance ToDiagnostic Warning where
  toDiagnostic warning = Diagnostic
      { _range = maybe noRange sourceLocToRange (warningLocation warning)
      , _severity = Just DsWarning
      , _code = Nothing
      , _tags = Nothing
      , _source = Just "Daml-LF typechecker"
      , _message = renderPretty warning
      , _relatedInformation = Nothing
      }
