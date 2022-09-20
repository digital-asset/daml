// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package validation

import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast._
import com.daml.lf.language.Reference
import com.daml.lf.language.LanguageVersion

import scala.Ordering.Implicits.infixOrderingOps

sealed abstract class Context extends Product with Serializable {
  def pretty: String
}

object Context {

  final case object None extends Context {
    def pretty = ""
  }
  final case class Reference(ref: language.Reference) extends Context {
    def pretty = " in " + ref.pretty
  }
  final case class Location(loc: data.Ref.Location) extends Context {
    def pretty = " in " + loc.pretty
  }

  final class ReferenceBuilder private[Context] (mkRef: Identifier => language.Reference) {
    def apply(id: Identifier): Context.Reference = Context.Reference(mkRef(id))
    def apply(pkgId: PackageId, module: DottedName, name: DottedName): Context.Reference =
      apply(Identifier(pkgId, QualifiedName(module, name)))
  }

  val DefDataType = new ReferenceBuilder(language.Reference.DataType)
  val Template = new ReferenceBuilder(language.Reference.Template)
  val DefException = new ReferenceBuilder(language.Reference.Exception)
  val DefInterface = new ReferenceBuilder(language.Reference.Interface)
  val DefValue = new ReferenceBuilder(language.Reference.Value)

}

sealed abstract class TemplatePart extends Product with Serializable
case object TPWhole extends TemplatePart
case object TPStakeholders extends TemplatePart
case object TPPrecondition extends TemplatePart
case object TPSignatories extends TemplatePart
case object TPObservers extends TemplatePart
case object TPAgreement extends TemplatePart
final case class TPChoice(template: TemplateChoice) extends TemplatePart

sealed abstract class SerializabilityRequirement extends Product with Serializable {
  def pretty: String
}
case object SRTemplateArg extends SerializabilityRequirement {
  def pretty: String = "template argument"
}
case object SRChoiceArg extends SerializabilityRequirement {
  def pretty: String = "choice argument"
}
case object SRExceptionArg extends SerializabilityRequirement {
  def pretty: String = "exception argument"
}
case object SRInterfaceArg extends SerializabilityRequirement {
  def pretty: String = "interface argument"
}
case object SRChoiceRes extends SerializabilityRequirement {
  def pretty: String = "choice result"
}
case object SRView extends SerializabilityRequirement {
  def pretty: String = "view"
}
case object SRKey extends SerializabilityRequirement {
  def pretty: String = "serializable data type"
}
case object SRDataType extends SerializabilityRequirement {
  def pretty: String = "template key"
}

// Reason why a type is not serializable.
sealed abstract class UnserializabilityReason extends Product with Serializable {
  def pretty: String
}
final case class URFreeVar(varName: TypeVarName) extends UnserializabilityReason {
  def pretty: String = s"free type variable $varName"
}
case object URFunction extends UnserializabilityReason {
  def pretty: String = "function type"
}
case object URForall extends UnserializabilityReason {
  def pretty: String = "higher-ranked type"
}
case object URUpdate extends UnserializabilityReason {
  def pretty: String = "Update"
}
case object URScenario extends UnserializabilityReason {
  def pretty: String = "Scenario"
}
case object URStruct extends UnserializabilityReason {
  def pretty: String = "structural record"
}
case object URNumeric extends UnserializabilityReason {
  def pretty: String = "unapplied Numeric"
}
case object URNat extends UnserializabilityReason {
  def pretty: String = "Nat"
}
case object URList extends UnserializabilityReason {
  def pretty: String = "unapplied List"
}
case object UROptional extends UnserializabilityReason {
  def pretty: String = "unapplied Option"
}
case object URTextMap extends UnserializabilityReason {
  def pretty: String = "unapplied TextMap"
}
case object URGenMap extends UnserializabilityReason {
  def pretty: String = "unapplied GenMap"
}
case object URContractId extends UnserializabilityReason {
  def pretty: String = "ContractId not applied to a template type"
}
final case class URDataType(conName: TypeConName) extends UnserializabilityReason {
  def pretty: String = s"unserializable data type ${conName.qualifiedName}"
}
final case class URTypeSyn(synName: TypeSynName) extends UnserializabilityReason {
  def pretty: String = s"type synonym ${synName.qualifiedName}"
}
final case class URHigherKinded(varName: TypeVarName, kind: Kind) extends UnserializabilityReason {
  def pretty: String = s"higher-kinded type variable $varName : ${kind.pretty}"
}
case object URUninhabitatedType extends UnserializabilityReason {
  def pretty: String = "variant type without constructors"
}
case object URAny extends UnserializabilityReason {
  def pretty: String = "Any"
}
case object URTypeRep extends UnserializabilityReason {
  def pretty: String = "TypeRep"
}
case object URRoundingMode extends UnserializabilityReason {
  def pretty: String = "RoundingMode"
}
case object URBigNumeric extends UnserializabilityReason {
  def pretty: String = "BigNumeric"
}
case object URInterface extends UnserializabilityReason {
  def pretty: String = "Interface"
}

abstract class ValidationError extends java.lang.RuntimeException with Product with Serializable {
  def context: Context
  def pretty: String = s"validation error${context.pretty}: $prettyInternal"
  override def getMessage: String = pretty
  protected def prettyInternal: String
}

final case class ENatKindRightOfArrow(context: Context, kind: Kind) extends ValidationError {
  protected def prettyInternal: String = s"invalid kind ${kind.pretty}"
}
final case class EUnknownTypeVar(context: Context, varName: TypeVarName) extends ValidationError {
  protected def prettyInternal: String = s"unknown type variable: $varName"
}
final case class EUnknownExprVar(context: Context, varName: ExprVarName) extends ValidationError {
  protected def prettyInternal: String = s"unknown expr variable: $varName"
}
final case class EUnknownDefinition(context: Context, lookupError: language.LookupError)
    extends ValidationError {
  protected def prettyInternal: String = lookupError.pretty
}
final case class ETypeSynAppWrongArity(
    context: Context,
    expectedArity: Int,
    syn: TypeSynName,
    args: ImmArray[Type],
) extends ValidationError {
  protected def prettyInternal: String =
    s"wrong arity in type synonym application: ${syn.qualifiedName} ${args.toSeq.map(_.pretty).mkString(" ")}"
}
final case class ETypeConAppWrongArity(context: Context, expectedArity: Int, conApp: TypeConApp)
    extends ValidationError {
  protected def prettyInternal: String = s"wrong arity in typecon application: ${conApp.pretty}"
}
final case class EDuplicateTypeParam(context: Context, typeParam: TypeVarName)
    extends ValidationError {
  protected def prettyInternal: String = s"duplicate type parameter: $typeParam"
}
final case class EDuplicateField(context: Context, fieldName: FieldName) extends ValidationError {
  protected def prettyInternal: String = s"duplicate field: $fieldName"
}
final case class EDuplicateVariantCon(context: Context, conName: VariantConName)
    extends ValidationError {
  protected def prettyInternal: String = s"duplicate variant constructor: $conName"
}
final case class EDuplicateEnumCon(context: Context, conName: EnumConName) extends ValidationError {
  protected def prettyInternal: String = s"duplicate enum constructor: $conName"
}
final case class EEmptyConsFront(context: Context) extends ValidationError {
  protected def prettyInternal: String = s"empty Cons front"
}
final case class EExpectedRecordType(context: Context, conApp: TypeConApp) extends ValidationError {
  protected def prettyInternal: String = s"expected record type: * found: ${conApp.pretty}"
}
final case class EFieldMismatch(
    context: Context,
    conApp: TypeConApp,
    fields: ImmArray[(FieldName, Expr)],
) extends ValidationError {
  protected def prettyInternal: String =
    s"field mismatch: * expected: $conApp * record expression: $fields"
}
final case class EExpectedVariantType(context: Context, conName: TypeConName)
    extends ValidationError {
  protected def prettyInternal: String = s"expected variant type: ${conName.qualifiedName}"
}
final case class EExpectedEnumType(context: Context, conName: TypeConName) extends ValidationError {
  protected def prettyInternal: String = s"expected enum type: ${conName.qualifiedName}"
}
final case class EUnknownVariantCon(context: Context, conName: VariantConName)
    extends ValidationError {
  protected def prettyInternal: String = s"unknown variant constructor: $conName"
}
final case class EUnknownEnumCon(context: Context, conName: EnumConName) extends ValidationError {
  protected def prettyInternal: String = s"unknown enum constructor: $conName"
}
final case class EUnknownField(context: Context, fieldName: FieldName) extends ValidationError {
  protected def prettyInternal: String = s"unknown field: $fieldName"
}
final case class EExpectedStructType(context: Context, typ: Type) extends ValidationError {
  protected def prettyInternal: String = s"expected struct type, but found: ${typ.pretty}"
}
final case class EKindMismatch(context: Context, foundKind: Kind, expectedKind: Kind)
    extends ValidationError {
  protected def prettyInternal: String =
    s"""kind mismatch:
       | * expected kind: ${expectedKind.pretty}
       | * found Kind: ${foundKind.pretty}""".stripMargin
}
final case class ETypeMismatch(
    context: Context,
    foundType: Type,
    expectedType: Type,
    expr: Option[Expr],
) extends ValidationError {
  protected def prettyInternal: String =
    s"""type mismatch:
       | * expected type: ${expectedType.pretty}
       | * found type: ${foundType.pretty}""".stripMargin
}
final case class EPatternTypeMismatch(
    context: Context,
    pattern: CasePat,
    scrutineeType: Type,
) extends ValidationError {
  protected def prettyInternal: String =
    s"""pattern type mismatch:
       | * pattern: $pattern
       | * scrutinee type: ${scrutineeType.pretty}""".stripMargin
}
final case class ENonExhaustivePatterns(
    context: Context,
    missingPatterns: List[CasePat],
    scrutineeType: Type,
) extends ValidationError {
  protected def prettyInternal: String =
    s"""non-exhaustive pattern match:
       | * missing patterns: ${missingPatterns.mkString(", ")}
       | * scrutinee type: ${scrutineeType.pretty}""".stripMargin
}
final case class EExpectedAnyType(context: Context, typ: Type) extends ValidationError {
  protected def prettyInternal: String =
    s"expected a type containing neither type variables nor quantifiers, but found: ${typ.pretty}"
}
final case class EExpectedExceptionType(context: Context, typ: Type) extends ValidationError {
  protected def prettyInternal: String =
    s"expected an exception type, but found: ${typ.pretty}"
}
final case class EExpectedHigherKind(context: Context, kind: Kind) extends ValidationError {
  protected def prettyInternal: String = s"expected higher kinded type, but found: ${kind.pretty}"
}
final case class EExpectedFunctionType(context: Context, typ: Type) extends ValidationError {
  protected def prettyInternal: String = s"expected function type, but found: ${typ.pretty}"
}
final case class EExpectedUniversalType(context: Context, typ: Type) extends ValidationError {
  protected def prettyInternal: String = s"expected universal type, but found: ${typ.pretty}"
}
final case class EExpectedUpdateType(context: Context, typ: Type) extends ValidationError {
  protected def prettyInternal: String = s"expected update type, but found: ${typ.pretty}"
}
final case class EExpectedScenarioType(context: Context, typ: Type) extends ValidationError {
  protected def prettyInternal: String = s"expected scenario type, but found: ${typ.pretty}"
}
final case class EExpectedSerializableType(
    context: Context,
    requirement: SerializabilityRequirement,
    typ: Type,
    reason: UnserializabilityReason,
) extends ValidationError {
  protected def prettyInternal: String =
    s"""expected serializable type:
       | * reason: ${reason.pretty}
       | * found: ${typ.pretty}
       | * problem: ${requirement.pretty}
     """.stripMargin
}
final case class EEmptyCase(context: Context) extends ValidationError {
  protected def prettyInternal: String = "empty case"
}
final case class EClashingPatternVariables(context: Context, varName: ExprVarName)
    extends ValidationError {
  protected def prettyInternal: String = s"$varName is used more than one in pattern"
}
final case class EExpectedTemplatableType(context: Context, conName: TypeConName)
    extends ValidationError {
  protected def prettyInternal: String =
    s"expected monomorphic record type in template definition, but found: ${conName.qualifiedName}"
}
final case class EExpectedExceptionableType(context: Context, conName: TypeConName)
    extends ValidationError {
  protected def prettyInternal: String =
    s"expected monomorphic record type in exception definition, but found: ${conName.qualifiedName}"
}
final case class EExpectedViewType(context: Context, typ: Type) extends ValidationError {
  protected def prettyInternal: String =
    s"expected monomorphic record type in view type, but found: ${typ.pretty}"
}
final case class EViewTypeHeadNotCon(context: Context, badHead: Type, typ: Type) extends ValidationError  {
  protected def prettyInternal: String = {
    val prettyHead = badHead match {
      case _: TVar => "a type variable"
      case _: TSynApp => "a type synonym"
      case _: TBuiltin => "a built-in type"
      case _: TForall => "a forall-quantified type"
      case _: TStruct => "a structural record"
      case _: TNat => "a type-level natural number"
      case _: TTyCon => "EViewTypeHeadNotCon#prettyInternal got TCon: should not happen"
      case _: TApp => "EViewTypeHeadNotCon#prettyInternal got TApp: should not happen"
    }
    s"expected monomorphic record type in view type, but found ${prettyHead} instead: ${typ.pretty}"
  }
}
final case class EViewTypeHasVars(context: Context, typ: Type) extends ValidationError  {
  protected def prettyInternal: String =
    s"expected monomorphic record type in view type, but found a type constructor with type variables: ${typ.pretty}"
}
final case class EViewTypeConNotRecord(context: Context, badCons: DataCons, typ: Type) extends ValidationError  {
  protected def prettyInternal: String = {
    val prettyCons = badCons match {
      case _: DataVariant => "a variant type"
      case _: DataEnum => "an enum type"
      case _: DataInterface.type => "a interface type"
      case _: DataRecord => "EViewTypeConNotRecord#prettyInternal got DataRecord: should not happen"
    }
    s"expected monomorphic record type in view type, but found ${prettyCons} instead: ${typ.pretty}"
  }
}
final case class EImportCycle(context: Context, modName: List[ModuleName]) extends ValidationError {
  protected def prettyInternal: String = s"cycle in module dependency ${modName.mkString(" -> ")}"
}
final case class ETypeSynCycle(context: Context, names: List[TypeSynName]) extends ValidationError {
  protected def prettyInternal: String =
    s"cycle in type synonym definitions ${names.mkString(" -> ")}"
}
final case class EIllegalHigherEnumType(context: Context, defn: TypeConName)
    extends ValidationError {
  protected def prettyInternal: String = s"illegal higher order enum type"
}
final case class EIllegalHigherInterfaceType(context: Context, defn: TypeConName)
    extends ValidationError {
  protected def prettyInternal: String = s"illegal higher interface type"
}
sealed abstract class PartyLiteralRef extends Product with Serializable
final case class PartyLiteral(party: Party) extends PartyLiteralRef
final case class ValRefWithPartyLiterals(valueRef: ValueRef) extends PartyLiteralRef
/* Collision */

final case class ECollision(
    pkgId: PackageId,
    entity1: NamedEntity,
    entity2: NamedEntity,
) extends ValidationError {

  assert(entity1.fullyResolvedName == entity2.fullyResolvedName)

  def context: Context = Context.None

  def collisionName: DottedName = entity1.fullyResolvedName

  override protected def prettyInternal: String =
    s"collision between ${entity1.pretty} and ${entity2.pretty}"
}

final case class EModuleVersionDependencies(
    pkgId: PackageId,
    pkgLangVersion: LanguageVersion,
    depPkgId: PackageId,
    dependencyLangVersion: LanguageVersion,
) extends ValidationError {

  assert(pkgId != depPkgId)
  assert(pkgLangVersion < dependencyLangVersion)

  override protected def prettyInternal: String =
    s"package $pkgId using version $pkgLangVersion depends on package $depPkgId using newer version $dependencyLangVersion"

  override def context: Context = Context.None
}

final case class EMissingMethodInInterfaceInstance(
    context: Context,
    method: MethodName,
) extends ValidationError {
  override protected def prettyInternal: String =
    s"Interface instance lacks an implementation for method '$method'."
}

final case class EUnknownMethodInInterfaceInstance(
    context: Context,
    method: MethodName,
) extends ValidationError {
  override protected def prettyInternal: String =
    s"Interface instance has an implementation for method '$method', but this method is not part of the interface."
}

final case class EMissingInterfaceInstance(
    context: Context,
    interfaceId: TypeConName,
    templateId: TypeConName,
) extends ValidationError {
  override protected def prettyInternal: String =
    s"There is no interface instance $interfaceId for $templateId"
}

final case class EMissingRequiredInterfaceInstance(
    context: Context,
    requiringIface: TypeConName,
    missingRequiredInterfaceInstance: Reference.InterfaceInstance,
) extends ValidationError {
  override protected def prettyInternal: String =
    s"Missing required ${missingRequiredInterfaceInstance.pretty}, required by interface $requiringIface"
}
final case class EWrongInterfaceRequirement(
    context: Context,
    requiringIface: TypeConName,
    wrongRequiredIface: TypeConName,
) extends ValidationError {
  protected def prettyInternal: String =
    s"Interface $requiringIface does not require $wrongRequiredIface"
}
final case class ENotClosedInterfaceRequires(
    context: Context,
    iface: TypeConName,
    requiredIface: TypeConName,
    missingRequiredIface: TypeConName,
) extends ValidationError {
  protected def prettyInternal: String =
    s"Interface $iface is missing requirements $missingRequiredIface required by $requiredIface"
}
final case class ECircularInterfaceRequires(
    context: Context,
    iface: TypeConName,
) extends ValidationError {
  protected def prettyInternal: String =
    s"Circular interface requirement is not allowed: interface $iface requires itself."
}

final case class EAmbiguousInterfaceInstance(
    context: Context,
    interfaceId: TypeConName,
    templateId: TypeConName,
) extends ValidationError {
  protected def prettyInternal: String =
    s"A reference to interface instance $interfaceId for $templateId is ambiguous, " +
      "both the interface and the template define this interface instance."
}
