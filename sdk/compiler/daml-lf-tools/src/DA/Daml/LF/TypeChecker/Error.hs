-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# OPTIONS_GHC -Wno-orphans #-}
module DA.Daml.LF.TypeChecker.Error(
    Context(..),
    Error(..),
    overUnwarnable,
    UnwarnableError(..),
    ErrorOrWarning(..),
    TemplatePart(..),
    InterfacePart(..),
    UnserializabilityReason(..),
    SerializabilityRequirement(..),
    UpgradedRecordOrigin(..),
    errorLocation,
    toDiagnostic,
    Warning(..),
    UnerrorableWarning(..),
    PackageUpgradeOrigin(..),
    UpgradeMismatchReason(..),
    DamlWarningFlag(..),
    DamlWarningFlags(..),
    DamlWarningFlagStatus(..),
    parseRawDamlWarningFlag,
    getWarningStatus,
    upgradeInterfacesFlag,
    upgradeExceptionsFlag,
    damlWarningFlagParserTypeChecker,
    mkDamlWarningFlags,
    combineParsers
    ) where

import Control.Applicative
import DA.Pretty
import qualified Data.Text as T
import Development.IDE.Types.Diagnostics
import Development.IDE.Types.Location
import Numeric.Natural
import qualified Data.List as L

import DA.Daml.LF.Ast
import DA.Daml.LF.Ast.Pretty
import DA.Daml.LF.Ast.Alpha (Mismatch(..), SomeName(..))
import DA.Daml.UtilLF (sourceLocToRange)
import DA.Daml.LF.TypeChecker.Error.WarningFlags

-- TODO(MH): Rework the context machinery to avoid code duplication.
-- | Type checking context for error reporting purposes.
data Context
  = ContextNone
  | ContextDefModule !Module
  | ContextDefTypeSyn !Module !DefTypeSyn
  | ContextDefDataType !Module !DefDataType
  | ContextTemplate !Module !Template !TemplatePart
  | ContextDefValue !Module !DefValue
  | ContextDefException !Module !DefException
  | ContextDefInterface !Module !DefInterface !InterfacePart
  | ContextDefUpgrading
      { cduPkgName :: !PackageName -- Name of package being checked for upgrade validity
      , cduPkgVersion :: !(Upgrading RawPackageVersion) -- Prior (upgradee) and current (upgrader) version of dep package
      , cduSubContext :: !Context -- Context within the package of the error
      , cduIsDependency :: !Bool -- Is the package a dependency package or is it the main package?
      }
  deriving (Eq)

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
  deriving (Eq)

data InterfacePart
  = IPWhole
  | IPMethod InterfaceMethod
  | IPChoice TemplateChoice
  | IPInterfaceInstance InterfaceInstanceHead (Maybe SourceLoc)
  deriving (Eq)

data SerializabilityRequirement
  = SRTemplateArg
  | SRChoiceArg
  | SRChoiceRes
  | SRKey
  | SRDataType
  | SRExceptionArg
  | SRView
  deriving (Show, Eq)

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
  deriving (Show)

data Error
  = EUnwarnableError !UnwarnableError
  | EErrorOrWarning !ErrorOrWarning
  | EContext !Context !Error
  deriving (Show)

overUnwarnable :: (UnwarnableError -> UnwarnableError) -> Error -> Error
overUnwarnable f (EUnwarnableError e) = EUnwarnableError (f e)
overUnwarnable _ x = x

data UnwarnableError
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
  | EUnknownExperimental !T.Text !Type
  | EUpgradeMissingModule !ModuleName
  | EUpgradeMissingTemplate !TypeConName
  | EUpgradeMissingChoice !ChoiceName
  | EUpgradeMissingDataCon !TypeConName
  | EUpgradeMismatchDataConsVariety !TypeConName !DataCons !DataCons
  | EUpgradeRecordFieldsMissing !UpgradedRecordOrigin ![FieldName]
  | EUpgradeRecordFieldsExistingChanged !UpgradedRecordOrigin ![(FieldName, Upgrading Type)]
  | EUpgradeRecordFieldsNewNonOptional !UpgradedRecordOrigin ![(FieldName, Type)]
  | EUpgradeRecordNewFieldsNotAtEnd !UpgradedRecordOrigin ![FieldName]
  | EUpgradeRecordFieldsOrderChanged !UpgradedRecordOrigin
  | EUpgradeVariantRemovedConstructor !UpgradedRecordOrigin ![VariantConName]
  | EUpgradeVariantChangedConstructorType !UpgradedRecordOrigin ![VariantConName]
  | EUpgradeVariantNewConstructorsNotAtEnd !UpgradedRecordOrigin ![VariantConName]
  | EUpgradeVariantConstructorsOrderChanged !UpgradedRecordOrigin
  | EUpgradeEnumRemovedConstructor !UpgradedRecordOrigin ![VariantConName]
  | EUpgradeEnumNewConstructorsNotAtEnd !UpgradedRecordOrigin ![VariantConName]
  | EUpgradeEnumConstructorsOrderChanged !UpgradedRecordOrigin
  | EUpgradeRecordChangedOrigin !TypeConName !UpgradedRecordOrigin !UpgradedRecordOrigin
  | EUpgradeTemplateChangedKeyType !TypeConName
  | EUpgradeChoiceChangedReturnType !ChoiceName
  | EUpgradeTemplateRemovedKey !TypeConName !TemplateKey
  | EUpgradeTemplateAddedKey !TypeConName !TemplateKey
  | EUpgradeTriedToUpgradeIface !TypeConName
  | EUpgradeMissingImplementation !TypeConName !TypeConName
  | EForbiddenNewImplementation !TypeConName !TypeConName
  | EUpgradeDependenciesFormACycle ![(PackageId, Maybe PackageMetadata)]
  | EUpgradeMultiplePackagesWithSameNameAndVersion !PackageName !RawPackageVersion ![PackageId]
  | EUpgradeTriedToUpgradeException !TypeConName
  | EUpgradeDifferentParamsCount !UpgradedRecordOrigin
  | EUpgradeDifferentParamsKinds !UpgradedRecordOrigin
  | EUpgradeDatatypeBecameUnserializable !UpgradedRecordOrigin
  deriving (Show)

data ErrorOrWarning
  = WEUpgradeShouldDefineIfacesAndTemplatesSeparately
  | WEUpgradeShouldDefineIfaceWithoutImplementation !ModuleName !TypeConName !TypeConName
  | WEUpgradeShouldDefineTplInSeparatePackage !TypeConName !TypeConName
  | WEDependencyHasUnparseableVersion !PackageName !PackageVersion !PackageUpgradeOrigin
  | WEDependencyHasNoMetadataDespiteUpgradeability !PackageId !PackageUpgradeOrigin
  | WEUpgradeShouldDefineExceptionsAndTemplatesSeparately
  | WEUpgradeDependsOnSerializableNonUpgradeableDataType (PackageId, Maybe PackageMetadata, Version) Version !(Qualified TypeConName)
  | WEDependsOnDatatypeFromNewDamlScript (PackageId, PackageMetadata) Version !(Qualified TypeConName)
  | WEUpgradeShouldNotImplementNonUpgradeableIfaces (PackageId, Maybe PackageMetadata) Version !(Qualified TypeConName) !(Qualified TypeConName)
  deriving (Eq, Show)

instance Pretty ErrorOrWarning where
  pPrint = \case
    WEUpgradeShouldDefineIfacesAndTemplatesSeparately ->
      vsep
        [ "This package defines both interfaces and templates. This may make this package and its dependents not upgradeable."
        , "It is recommended that interfaces are defined in their own package separate from their implementations."
        ]
    WEUpgradeShouldDefineIfaceWithoutImplementation implModule implIface implTpl ->
      vsep
        [ "The interface " <> pPrint implIface <> " was defined in this package and implemented in " <> pPrint implModule <> " by " <> pPrint implTpl
        , "This may make this package and its dependents not upgradeable."
        , "It is recommended that interfaces are defined in their own package separate from their implementations."
        ]
    WEUpgradeShouldDefineTplInSeparatePackage tpl iface ->
      vsep
        [ "The template " <> pPrint tpl <> " has implemented interface " <> pPrint iface <> ", which is defined in a previous version of this package."
        , "This may make this package and its dependents not upgradeable."
        , "It is recommended that interfaces are defined in their own package separate from their implementations."
        ]
    WEDependencyHasUnparseableVersion pkgName version packageOrigin ->
      "Dependency " <> pPrint pkgName <> " of " <> pPrint packageOrigin <> " has a version which cannot be parsed: '" <> pPrint version <> "'"
    WEDependencyHasNoMetadataDespiteUpgradeability pkgId packageOrigin ->
      "Dependency with package ID " <> pPrint pkgId <> " of " <> pPrint packageOrigin <> " has no metadata, despite being compiled with an SDK version that supports metadata."
    WEUpgradeShouldDefineExceptionsAndTemplatesSeparately ->
      vsep
        [ "This package defines both exceptions and templates. This may make this package and its dependents not upgradeable."
        , "It is recommended that exceptions are defined in their own package separate from their implementations."
        , "Ignore this error message with the -Wupgrade-exceptions flag."
        ]
    WEUpgradeDependsOnSerializableNonUpgradeableDataType (depPkgId, depMeta, depLfVersion) lfVersion tcn ->
      vsep
        [ "This package has LF version " <> pPrint lfVersion <> ", but it depends on a serializable type " <> pPrint tcn <> " from package " <> pprintDep (depPkgId, depMeta) <> " which has LF version " <> pPrint depLfVersion <> "."
        , "It is not recommended that >= LF1.17 packages depend on <= LF1.15 datatypes in places that may be serialized to the ledger, because those datatypes will not be upgradeable."
        ]
    WEDependsOnDatatypeFromNewDamlScript (depPkgId, depMeta) depLfVersion tcn ->
      vsep
        [ "This package depends on a datatype " <> pPrint tcn <> " from " <> pprintDep (depPkgId, Just depMeta) <> " with LF version " <> pPrint depLfVersion <> "."
        , "It is not recommended that >= LF1.17 packages use datatypes from Daml Script, because those datatypes will not be upgradeable."
        ]
    WEUpgradeShouldNotImplementNonUpgradeableIfaces dep depLfVersion iface tpl ->
      vsep
        [ "Template " <> pPrint tpl <> " implements interface " <> pPrint iface <> " from package " <> pprintDep dep <> " which has LF version " <> pPrint depLfVersion <> "."
        , "It is forbidden for upgradeable templates (LF version >= 1.17) to implement interfaces from non-upgradeable packages (LF version <= 1.15)."
        ]
    where
      pprintDep (pkgId, Just meta) = pPrint pkgId <> " (" <> pPrint (packageName meta) <> ", " <> pPrint (packageVersion meta) <> ")"
      pprintDep (pkgId, Nothing) = pPrint pkgId

damlWarningFlagParserTypeChecker :: DamlWarningFlagParser ErrorOrWarning
damlWarningFlagParserTypeChecker = DamlWarningFlagParser
  { dwfpFlagParsers =
      [ (upgradeInterfacesName, upgradeInterfacesFlag)
      , (upgradeExceptionsName, upgradeExceptionsFlag)
      , (upgradeDependencyMetadataName, upgradeDependencyMetadataFlag)
      , (upgradeSerializedLF15DependencyName, upgradeSerializedLF15DependencyFlag)
      , (referencesDamlScriptDatatypeName, referencesDamlScriptDatatypeFlag)
      , (upgradeImplementNonUpgradeableInterfacesName, upgradeImplementNonUpgradeableInterfacesFlag)
      ]
  , dwfpDefault = \case
      WEUpgradeShouldDefineIfacesAndTemplatesSeparately {} -> AsError
      WEUpgradeShouldDefineIfaceWithoutImplementation {} -> AsError
      WEUpgradeShouldDefineTplInSeparatePackage {} -> AsError
      WEUpgradeShouldDefineExceptionsAndTemplatesSeparately {} -> AsError
      WEDependencyHasUnparseableVersion {} -> AsWarning
      WEDependencyHasNoMetadataDespiteUpgradeability {} -> AsWarning
      WEUpgradeDependsOnSerializableNonUpgradeableDataType {} -> AsWarning
      WEDependsOnDatatypeFromNewDamlScript {} -> AsWarning
      WEUpgradeShouldNotImplementNonUpgradeableIfaces {} -> AsError
  }

filterNameForErrorOrWarning :: ErrorOrWarning -> Maybe String
filterNameForErrorOrWarning err | upgradeInterfacesFilter err = Just upgradeInterfacesName
filterNameForErrorOrWarning err | upgradeExceptionsFilter err = Just upgradeExceptionsName
filterNameForErrorOrWarning err | upgradeDependencyMetadataFilter err = Just upgradeDependencyMetadataName
filterNameForErrorOrWarning err | upgradeSerializedLF15DependencyFilter err = Just upgradeSerializedLF15DependencyName
filterNameForErrorOrWarning err | referencesDamlScriptDatatypeFilter err = Just referencesDamlScriptDatatypeName
filterNameForErrorOrWarning _ = Nothing

upgradeSerializedLF15DependencyFlag :: DamlWarningFlagStatus -> DamlWarningFlag ErrorOrWarning
upgradeSerializedLF15DependencyFlag status = RawDamlWarningFlag upgradeSerializedLF15DependencyName status upgradeSerializedLF15DependencyFilter

upgradeSerializedLF15DependencyName :: String
upgradeSerializedLF15DependencyName = "upgrade-serialized-non-upgradeable-dependency"

upgradeSerializedLF15DependencyFilter :: ErrorOrWarning -> Bool
upgradeSerializedLF15DependencyFilter =
    \case
        WEUpgradeDependsOnSerializableNonUpgradeableDataType {} -> True
        _ -> False

referencesDamlScriptDatatypeFlag :: DamlWarningFlagStatus -> DamlWarningFlag ErrorOrWarning
referencesDamlScriptDatatypeFlag status = RawDamlWarningFlag referencesDamlScriptDatatypeName status referencesDamlScriptDatatypeFilter

referencesDamlScriptDatatypeName :: String
referencesDamlScriptDatatypeName = "upgrade-serialized-daml-script"

referencesDamlScriptDatatypeFilter :: ErrorOrWarning -> Bool
referencesDamlScriptDatatypeFilter =
    \case
        WEDependsOnDatatypeFromNewDamlScript {} -> True
        _ -> False

upgradeInterfacesFlag :: DamlWarningFlagStatus -> DamlWarningFlag ErrorOrWarning
upgradeInterfacesFlag status = RawDamlWarningFlag upgradeInterfacesName status upgradeInterfacesFilter

upgradeInterfacesName :: String
upgradeInterfacesName = "upgrade-interfaces"

upgradeInterfacesFilter :: ErrorOrWarning -> Bool
upgradeInterfacesFilter =
    \case
        WEUpgradeShouldDefineIfacesAndTemplatesSeparately {} -> True
        WEUpgradeShouldDefineIfaceWithoutImplementation {} -> True
        WEUpgradeShouldDefineTplInSeparatePackage {} -> True
        _ -> False

upgradeExceptionsFlag :: DamlWarningFlagStatus -> DamlWarningFlag ErrorOrWarning
upgradeExceptionsFlag status = RawDamlWarningFlag upgradeExceptionsName status upgradeExceptionsFilter

upgradeExceptionsName :: String
upgradeExceptionsName = "upgrade-exceptions"

upgradeExceptionsFilter :: ErrorOrWarning -> Bool
upgradeExceptionsFilter =
    \case
        WEUpgradeShouldDefineExceptionsAndTemplatesSeparately {} -> True
        _ -> False

upgradeDependencyMetadataFlag :: DamlWarningFlagStatus -> DamlWarningFlag ErrorOrWarning
upgradeDependencyMetadataFlag status = RawDamlWarningFlag upgradeDependencyMetadataName status upgradeDependencyMetadataFilter

upgradeDependencyMetadataName :: String
upgradeDependencyMetadataName = "upgrade-dependency-metadata"

upgradeDependencyMetadataFilter :: ErrorOrWarning -> Bool
upgradeDependencyMetadataFilter =
    \case
        WEDependencyHasUnparseableVersion {} -> True
        WEDependencyHasNoMetadataDespiteUpgradeability {} -> True
        _ -> False

upgradeImplementNonUpgradeableInterfacesFlag :: DamlWarningFlagStatus -> DamlWarningFlag ErrorOrWarning
upgradeImplementNonUpgradeableInterfacesFlag status = RawDamlWarningFlag upgradeImplementNonUpgradeableInterfacesName status upgradeImplementNonUpgradeableInterfacesFilter

upgradeImplementNonUpgradeableInterfacesName :: String
upgradeImplementNonUpgradeableInterfacesName = "internal-upgrade-implement-non-upgradeable-interfaces"

upgradeImplementNonUpgradeableInterfacesFilter :: ErrorOrWarning -> Bool
upgradeImplementNonUpgradeableInterfacesFilter =
    \case
        WEUpgradeShouldNotImplementNonUpgradeableIfaces {} -> True
        _ -> False

data PackageUpgradeOrigin = UpgradingPackage | UpgradedPackage
  deriving (Eq, Ord, Show)

instance Pretty PackageUpgradeOrigin where
  pPrint = \case
    UpgradingPackage -> "new upgrading package"
    UpgradedPackage -> "previous package"

data UpgradedRecordOrigin
  = TemplateBody TypeConName
  | TemplateChoiceInput TypeConName ChoiceName
  | VariantConstructor TypeConName VariantConName
  | InterfaceBody TypeConName
  | TopLevel TypeConName
  deriving (Eq, Ord, Show)

contextLocation :: Context -> Maybe SourceLoc
contextLocation = \case
  ContextNone                -> Nothing
  ContextDefModule _         -> Nothing
  ContextDefTypeSyn _ s      -> synLocation s
  ContextDefDataType _ d     -> dataLocation d
  ContextTemplate _ t tp     -> templateLocation t tp <|> tplLocation t -- Fallback to template header location if other locations are missing
  ContextDefValue _ v        -> dvalLocation v
  ContextDefException _ e    -> exnLocation e
  ContextDefInterface _ i ip -> interfaceLocation i ip <|> intLocation i -- Fallback to interface header location if other locations are missing
  ContextDefUpgrading {} -> Nothing

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
    ContextDefModule m ->
      "module " <> show (moduleName m)
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
    ContextDefUpgrading { cduPkgName, cduPkgVersion, cduSubContext, cduIsDependency } ->
      let prettyPkgName =
            if cduIsDependency
            then "dependency " <> T.unpack (unPackageName cduPkgName)
            else T.unpack (unPackageName cduPkgName)
      in
      "upgrading " <> prettyPkgName <> " " <> show (_present cduPkgVersion) <> ", " <> show cduSubContext

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
    EUnwarnableError err -> pPrint err
    EErrorOrWarning err ->
      case filterNameForErrorOrWarning err of
        Just name ->
          vcat
            [ pPrint err
            , "Downgrade this error to a warning with -W" <> string name
            , "Disable this error entirely with -Wno-" <> string name
            ]
        Nothing -> pPrint err
    EContext ctx err -> prettyWithContext ctx (Right err)

instance Pretty UnwarnableError where
  pPrint = \case
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
    EUnknownExperimental name ty ->
      "Unknown experimental primitive " <> string (show name) <> " : " <> pretty ty
    EUpgradeMissingModule moduleName -> "Module " <> pPrint moduleName <> " appears in package that is being upgraded, but does not appear in this package."
    EUpgradeMissingTemplate templateName -> "Template " <> pPrint templateName <> " appears in package that is being upgraded, but does not appear in this package."
    EUpgradeMissingChoice templateName -> "Choice " <> pPrint templateName <> " appears in package that is being upgraded, but does not appear in this package."
    EUpgradeMissingDataCon dataConName -> "Data type " <> pPrint dataConName <> " appears in package that is being upgraded, but does not appear in this package."
    EUpgradeMismatchDataConsVariety dataConName pastCons presentCons ->
        "The upgraded data type " <> pretty dataConName <> " has changed from a " <> printCons pastCons <> " to a " <> printCons presentCons <> ". Datatypes cannot change variety via upgrades."
      where
        printCons :: DataCons -> Doc ann
        printCons DataRecord {} = "record"
        printCons DataVariant {} = "variant"
        printCons DataEnum {} = "enum"
        printCons DataInterface {} = "interface"
    EUpgradeRecordFieldsMissing origin fields -> "The upgraded " <> pPrint origin <> " is missing some of its original fields: " <> fcommasep (map pPrintWithQuotes fields)
    EUpgradeRecordFieldsExistingChanged origin fields ->
      vcat
        [ "The upgraded " <> pPrint origin <> " has changed the types of some of its original fields:"
        , nest 2 $ vcat (map pPrintChangedType fields)
        ]
      where
        pPrintChangedType (fieldName, Upgrading { _past = pastType, _present = presentType }) =
          "Field " <> pPrintWithQuotes fieldName <> " changed type from " <> pPrint pastType <> " to " <> pPrint presentType
    EUpgradeRecordNewFieldsNotAtEnd origin fields -> "The upgraded " <> pPrint origin <> " has added new fields, but the following fields need to be moved to the end: " <> fcommasep (map pPrintWithQuotes fields) <> ". All new fields in upgrades must be added to the end of the definition."
    EUpgradeRecordFieldsNewNonOptional origin fields ->
      vcat
        [ "The upgraded " <> pPrint origin <> " has added new fields, but the following new fields are not Optional:"
        , nest 2 $ vcat (map pPrintFieldType fields)
        ]
      where
        pPrintFieldType (fieldName, type_) = "Field " <> pPrintWithQuotes fieldName <> " with type " <> pPrint type_
    EUpgradeRecordFieldsOrderChanged origin -> "The upgraded " <> pPrint origin <> " has changed the order of its fields - any new fields must be added at the end of the record."
    EUpgradeVariantRemovedConstructor origin constructors -> "The upgraded " <> pPrint origin <> " is missing some of its original constructors: " <> fcommasep (map pPrint constructors)
    EUpgradeVariantChangedConstructorType origin constructors -> "The upgraded " <> pPrint origin <> " has changed the type of some of its original constructors: " <> fcommasep (map pPrint constructors)
    EUpgradeVariantNewConstructorsNotAtEnd origin constructors -> "The upgraded " <> pPrint origin <> " has added new constructors, but the following constructors need to be moved to the end: " <> fcommasep (map pPrintWithQuotes constructors) <> ". All new constructors in upgrades must be added to the end of the definition."
    EUpgradeVariantConstructorsOrderChanged origin -> "The upgraded " <> pPrint origin <> " has changed the order of its constructors - any new constructor must be added at the end of the variant."
    EUpgradeEnumRemovedConstructor origin constructors -> "The upgraded " <> pPrint origin <> " is missing some of its original constructors: " <> fcommasep (map pPrint constructors)
    EUpgradeEnumNewConstructorsNotAtEnd origin constructors -> "The upgraded " <> pPrint origin <> " has added new constructors, but the following constructors need to be moved to the end: " <> fcommasep (map pPrintWithQuotes constructors) <> ". All new variant constructors in upgrades must be added to the end of the definition."
    EUpgradeEnumConstructorsOrderChanged origin -> "The upgraded " <> pPrint origin <> " has changed the order of its constructors - any new enum constructor must be added at the end of the enum."
    EUpgradeRecordChangedOrigin dataConName past present -> "The record " <> pPrint dataConName <> " has changed origin from " <> pPrint past <> " to " <> pPrint present
    EUpgradeChoiceChangedReturnType choice -> "The upgraded choice " <> pPrint choice <> " cannot change its return type."
    EUpgradeTemplateChangedKeyType templateName -> "The upgraded template " <> pPrint templateName <> " cannot change its key type."
    EUpgradeTemplateRemovedKey templateName _key -> "The upgraded template " <> pPrint templateName <> " cannot remove its key."
    EUpgradeTemplateAddedKey template _key -> "The upgraded template " <> pPrint template <> " cannot add a key where it didn't have one previously."
    EUpgradeTriedToUpgradeIface iface -> "Tried to upgrade interface " <> pPrint iface <> ", but interfaces cannot be upgraded. They should be removed whenever a package is being upgraded."
    EUpgradeMissingImplementation tpl iface -> "Implementation of interface " <> pPrint iface <> " by template " <> pPrint tpl <> " appears in package that is being upgraded, but does not appear in this package."
    EForbiddenNewImplementation tpl iface -> "Implementation of interface " <> pPrint iface <> " by template " <> pPrint tpl <> " appears in this package, but does not appear in package that is being upgraded."
    EUpgradeDependenciesFormACycle deps ->
      vcat
        [ "Dependencies from the `upgrades:` field and dependencies defined on the current package form a cycle:"
        , nest 2 $ vcat $ map pprintDep deps
        ]
      where
      pprintDep (pkgId, Just meta) = pPrint pkgId <> " (" <> pPrint (packageName meta) <> ", " <> pPrint (packageVersion meta) <> ")"
      pprintDep (pkgId, Nothing) = pPrint pkgId
    EUpgradeMultiplePackagesWithSameNameAndVersion name version ids -> "Multiple packages with name " <> pPrint name <> " and version " <> pPrint (show version) <> ": " <> hcat (L.intersperse ", " (map pPrint ids))
    EUpgradeTriedToUpgradeException exception ->
      "Tried to upgrade exception " <> pPrint exception <> ", but exceptions cannot be upgraded. They should be removed in any upgrading package."
    EUpgradeDifferentParamsCount origin -> "The upgraded " <> pPrint origin <> " has changed the number of type variables it has."
    EUpgradeDifferentParamsKinds origin -> "The upgraded " <> pPrint origin <> " has changed the kind of one of its type variables."
    EUpgradeDatatypeBecameUnserializable origin -> "The upgraded " <> pPrint origin <> " was serializable and is now unserializable. Datatypes cannot change their serializability via upgrades."

pPrintWithQuotes :: Pretty a => a -> Doc ann
pPrintWithQuotes a = "'" <> pPrint a <> "'"

instance Pretty UpgradedRecordOrigin where
  pPrint = \case
    TemplateBody tpl -> "template " <> pPrint tpl
    TemplateChoiceInput tpl chcName -> "input type of choice " <> pPrint chcName <> " on template " <> pPrint tpl
    VariantConstructor variantName variantConName -> "constructor " <> pPrint variantConName <> " from variant " <> pPrint variantName
    InterfaceBody iface -> "interface " <> pPrint iface
    TopLevel datatype -> "data type " <> pPrint datatype

prettyWithContext :: Context -> Either Warning Error -> Doc a
prettyWithContext ctx warningOrErr =
  let header prettyCtx =
        vcat
        [ case warningOrErr of
            Right _ -> "error type checking " <> prettyCtx <> ":"
            Left _ -> "warning while type checking " <> prettyCtx <> ":"
        , nest 2 (either pretty pretty warningOrErr)
        ]
  in
  case ctx of
    ContextNone ->
      header $ string "<none>"
    ContextDefModule m ->
      header $ hsep [ "module" , pretty (moduleName m) ]
    ContextDefTypeSyn m ts ->
      header $ hsep [ "type synonym", pretty (moduleName m) <> "." <>  pretty (synName ts) ]
    ContextDefDataType m dt ->
      header $ hsep [ "data type", pretty (moduleName m) <> "." <>  pretty (dataTypeCon dt) ]
    ContextTemplate m t p ->
      header $ hsep [ "template", pretty (moduleName m) <> "." <>  pretty (tplTypeCon t), string (show p) ]
    ContextDefValue m v ->
      header $ hsep [ "value", pretty (moduleName m) <> "." <> pretty (fst $ dvalBinder v) ]
    ContextDefException m e ->
      header $ hsep [ "exception", pretty (moduleName m) <> "." <> pretty (exnName e) ]
    ContextDefInterface m i p ->
      header $ hsep [ "interface", pretty (moduleName m) <> "." <> pretty (intName i), string (show p)]
    ContextDefUpgrading { cduPkgName, cduPkgVersion, cduSubContext, cduIsDependency } ->
      let prettyPkgName = if cduIsDependency then hsep ["dependency", pretty cduPkgName] else pretty cduPkgName
          upgradeOrDowngrade = if _present cduPkgVersion > _past cduPkgVersion then "upgrade" else "downgrade"
      in
      vcat
      [ hsep [ "error while validating that", prettyPkgName, "version", string (show (_present cduPkgVersion)), "is a valid", upgradeOrDowngrade, "of version", string (show (_past cduPkgVersion)) ]
      , nest 2 $
        prettyWithContext cduSubContext warningOrErr
      ]

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

instance ToDiagnostic UnwarnableError where
  toDiagnostic err = Diagnostic
      { _range = maybe noRange sourceLocToRange (errorLocation (EUnwarnableError err))
      , _severity = Just DsError
      , _code = Nothing
      , _tags = Nothing
      , _source = Just "Daml-LF typechecker"
      , _message = renderPretty err
      , _relatedInformation = Nothing
      }

data Warning
  = WContext !Context !Warning
  | WUnerrorableWarning !UnerrorableWarning
  | WErrorToWarning !ErrorOrWarning
  deriving (Eq, Show)

data UnerrorableWarning
  = WTemplateChangedPrecondition !TypeConName ![Mismatch UpgradeMismatchReason]
  | WTemplateChangedSignatories !TypeConName ![Mismatch UpgradeMismatchReason]
  | WTemplateChangedObservers !TypeConName ![Mismatch UpgradeMismatchReason]
  | WTemplateChangedAgreement !TypeConName ![Mismatch UpgradeMismatchReason]
  | WChoiceChangedControllers !ChoiceName ![Mismatch UpgradeMismatchReason]
  | WChoiceChangedObservers !ChoiceName ![Mismatch UpgradeMismatchReason]
  | WChoiceChangedAuthorizers !ChoiceName ![Mismatch UpgradeMismatchReason]
  | WTemplateChangedKeyExpression !TypeConName ![Mismatch UpgradeMismatchReason]
  | WTemplateChangedKeyMaintainers !TypeConName ![Mismatch UpgradeMismatchReason]
  | WCouldNotExtractForUpgradeChecking !T.Text !(Maybe T.Text)
    -- ^ When upgrading, we extract relevant expressions for things like
    -- signatories. If the expression changes shape so that we can't get the
    -- underlying expression that has changed, this warning is emitted.
  deriving (Eq, Show)

warningLocation :: Warning -> Maybe SourceLoc
warningLocation = \case
  WContext ctx _ -> contextLocation ctx
  _ -> Nothing

instance Pretty UnerrorableWarning where
  pPrint = \case
    WTemplateChangedPrecondition template mismatches -> withMismatchInfo mismatches $ "The upgraded template " <> pPrint template <> " has changed the definition of its precondition."
    WTemplateChangedSignatories template mismatches -> withMismatchInfo mismatches $ "The upgraded template " <> pPrint template <> " has changed the definition of its signatories."
    WTemplateChangedObservers template mismatches -> withMismatchInfo mismatches $ "The upgraded template " <> pPrint template <> " has changed the definition of its observers."
    WTemplateChangedAgreement template mismatches -> withMismatchInfo mismatches $ "The upgraded template " <> pPrint template <> " has changed the definition of agreement."
    WChoiceChangedControllers choice mismatches -> withMismatchInfo mismatches $ "The upgraded choice " <> pPrint choice <> " has changed the definition of controllers."
    WChoiceChangedObservers choice mismatches -> withMismatchInfo mismatches $ "The upgraded choice " <> pPrint choice <> " has changed the definition of observers."
    WChoiceChangedAuthorizers choice mismatches -> withMismatchInfo mismatches $ "The upgraded choice " <> pPrint choice <> " has changed the definition of authorizers."
    WTemplateChangedKeyExpression template mismatches -> withMismatchInfo mismatches $ "The upgraded template " <> pPrint template <> " has changed the expression for computing its key."
    WTemplateChangedKeyMaintainers template mismatches -> withMismatchInfo mismatches $ "The upgraded template " <> pPrint template <> " has changed the maintainers for its key."
    WCouldNotExtractForUpgradeChecking attribute mbExtra -> "Could not check if the upgrade of " <> text attribute <> " is valid because its expression is the not the right shape." <> foldMap (const " Extra context: " <> text) mbExtra
    where
    withMismatchInfo :: [Mismatch UpgradeMismatchReason] -> Doc ann -> Doc ann
    withMismatchInfo [] doc = doc
    withMismatchInfo [mismatch] doc =
      vcat
        [ doc
        , "There is 1 difference in the expression:"
        , nest 2 $ pPrint mismatch
        ]
    withMismatchInfo mismatches doc =
      vcat
        [ doc
        , "There are " <> string (show (length mismatches)) <> " differences in the expression, including:"
        , nest 2 $ vcat $ map pPrint (take 3 mismatches)
        ]

instance Pretty Warning where
  pPrint = \case
    WContext ctx warning -> prettyWithContext ctx (Left warning)
    WUnerrorableWarning standaloneWarning -> pPrint standaloneWarning
    WErrorToWarning err ->
      case filterNameForErrorOrWarning err of
        Just name ->
          vcat
            [ pPrint err
            , "Upgrade this warning to an error -Werror=" <> string name
            , "Disable this warning entirely with -Wno-" <> string name
            ]
        Nothing -> pPrint err

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

instance Pretty SomeName where
  pPrint = \case
    SNTypeVarName typeVarName -> pPrint typeVarName
    SNExprVarName exprVarName -> pPrint exprVarName
    SNTypeConName typeConName -> pPrint typeConName
    SNExprValName exprValName -> pPrint exprValName
    SNFieldName fieldName -> pPrint fieldName
    SNChoiceName choiceName -> pPrint choiceName
    SNTypeSynName typeSynName -> pPrint typeSynName
    SNVariantConName variantConName -> pPrint variantConName
    SNMethodName methodName -> pPrint methodName
    SNQualified qualified -> pPrint qualified

instance Pretty reason => Pretty (Mismatch reason) where
  pPrint = \case
    NameMismatch name1 name2 reason -> "Name " <> pPrint name1 <> " and name " <> pPrint name2 <> " differ for the following reason: " <> pPrint reason
    BindingMismatch var1 var2 -> "Name " <> pPrint var1 <> " and name " <> pPrint var2 <> " refer to different bindings in the environment."
    StructuralMismatch -> "Expression is structurally different."

type MbUpgradingDep = Either PackageId UpgradingDep

data UpgradeMismatchReason
  = CustomReason String
  | OriginChangedFromSelfToImport MbUpgradingDep
  | OriginChangedFromImportToSelf MbUpgradingDep
  | PackageNameChanged (Upgrading UpgradingDep)
  | DifferentPackagesNeitherOfWhichSupportsUpgrades (Upgrading UpgradingDep)
  | PastPackageHasHigherVersion (Upgrading UpgradingDep)
  | PackageChangedFromUtilityToSchemaPackage (Upgrading UpgradingDep)
  | PackageChangedFromSchemaToUtilityPackage (Upgrading UpgradingDep)
  | PackageChangedFromDoesNotSupportUpgradesToSupportUpgrades (Upgrading UpgradingDep)
  | PackageChangedFromSupportUpgradesToDoesNotSupportUpgrades (Upgrading UpgradingDep)
  | CouldNotFindPackageForPastIdentifier MbUpgradingDep
  | CouldNotFindPackageForPresentIdentifier MbUpgradingDep
  deriving (Eq, Show)

instance Pretty UpgradingDep where
  pPrint = string . show

instance Pretty UpgradeMismatchReason where
  pPrint = \case
    CustomReason str -> string str
    OriginChangedFromSelfToImport import_ ->
      "Name came from the current package and now comes from different package '" <> tryShowPkgId import_ <> "'"
    OriginChangedFromImportToSelf import_ ->
      "Name came from different package '" <> tryShowPkgId import_ <> "' and now comes from the current package"
    PackageNameChanged pkg ->
      "Name came from package '" <> pPrint (_past pkg) <> "' and now comes from differently-named package '" <> pPrint (_present pkg) <> "'"
    DifferentPackagesNeitherOfWhichSupportsUpgrades pkg ->
      "Name came from package '" <> pPrint (_past pkg) <> "' and now comes from package '" <> pPrint (_present pkg) <> "'. Neither package supports upgrades, which may mean they have different implementations of the name."
    PastPackageHasHigherVersion pkg ->
      "Name came from package '" <> pPrint (_past pkg) <> "' and now comes from package '" <> pPrint (_present pkg) <> "'. Both packages support upgrades, but the previous package had a higher version than the current one."
    PackageChangedFromSchemaToUtilityPackage pkg ->
      "Name came from package '" <> pPrint (_past pkg) <> "' and now comes from package '" <> pPrint (_present pkg) <> "'. Both packages support upgrades, but the previous package was not a utility package and the current one is."
    PackageChangedFromUtilityToSchemaPackage pkg ->
      "Name came from package '" <> pPrint (_past pkg) <> "' and now comes from package '" <> pPrint (_present pkg) <> "'. Both packages support upgrades, but the previous package was a utility package and the current one is not."
    PackageChangedFromDoesNotSupportUpgradesToSupportUpgrades pkg ->
      "Name came from package '" <> pPrint (_past pkg) <> "' and now comes from package '" <> pPrint (_present pkg) <> "'. The previous package did not support upgrades and the current one does."
    PackageChangedFromSupportUpgradesToDoesNotSupportUpgrades pkg ->
      "Name came from package '" <> pPrint (_past pkg) <> "' and now comes from package '" <> pPrint (_present pkg) <> "'. The previous package supported upgrades and the current one does not."
    CouldNotFindPackageForPastIdentifier pkg ->
      "Could not find " <> tryShowPkgId pkg <> " in the package list for past version of this name."
    CouldNotFindPackageForPresentIdentifier pkg ->
      "Could not find " <> tryShowPkgId pkg <> " in the package list for present version of this name."
    where
      tryShowPkgId (Left pkgId) = pPrint pkgId
      tryShowPkgId (Right dep) = string (show dep)
