// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.validation

import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.language.Ast._

sealed abstract class LookupError extends Product with Serializable {
  def pretty: String
}
final case class LEPackage(packageId: PackageId) extends LookupError {
  def pretty: String = s"unknown package: $packageId"
}
final case class LEModule(packageId: PackageId, moduleRef: ModuleName) extends LookupError {
  def pretty: String = s"unknown module: $moduleRef"
}
final case class LETypeSyn(syn: TypeSynName) extends LookupError {
  def pretty: String = s"unknown type synonym: ${syn.qualifiedName}"
}
final case class LEDataType(conName: TypeConName) extends LookupError {
  def pretty: String = s"unknown data type: ${conName.qualifiedName}"
}
final case class LEValue(valName: ValueRef) extends LookupError {
  def pretty: String = s"unknown value: ${valName.qualifiedName}"
}
final case class LETemplate(conName: TypeConName) extends LookupError {
  def pretty: String = s"unknown template: ${conName.qualifiedName}"
}
final case class LEChoice(conName: TypeConName, choiceName: ChoiceName) extends LookupError {
  def pretty: String = s"unknown choice: ${conName.qualifiedName}:$choiceName"
}

sealed abstract class Context extends Product with Serializable {
  def pretty: String
}

case object NoContext extends Context {
  def pretty: String = ""
}
final case class ContextDefDataType(tycon: TypeConName) extends Context {
  def pretty: String = s"data type ${tycon.qualifiedName}"
}
final case class ContextTemplate(tycon: TypeConName) extends Context {
  def pretty: String = s"data type ${tycon.qualifiedName}"
}
final case class ContextDefValue(ref: ValueRef) extends Context {
  def pretty: String = s"value type ${ref.qualifiedName}"
}

object ContextDefDataType {
  def apply(pkgId: PackageId, module: DottedName, name: DottedName): ContextDefDataType =
    ContextDefDataType(TypeConName(pkgId, QualifiedName(module, name)))
}

object ContextTemplate {
  def apply(pkgId: PackageId, module: DottedName, name: DottedName): ContextTemplate =
    ContextTemplate(TypeConName(pkgId, QualifiedName(module, name)))
}

object ContextDefValue {
  def apply(pkgId: PackageId, module: DottedName, name: DottedName): ContextDefValue =
    ContextDefValue(ValueRef(pkgId, QualifiedName(module, name)))
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
case object SRChoiceRes extends SerializabilityRequirement {
  def pretty: String = "choice result"
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

abstract class ValidationError extends java.lang.RuntimeException with Product with Serializable {
  def context: Context
  override def toString: String = productPrefix + productIterator.mkString("(", ", ", ")")
  def pretty: String = s"validation error in ${context.pretty}: $prettyInternal"
  protected def prettyInternal: String
}

final case class EUnknownTypeVar(context: Context, varName: TypeVarName) extends ValidationError {
  protected def prettyInternal: String = s"unknown type variable: $varName"
}
final case class EIllegalShadowingExprVar(context: Context, varName: ExprVarName)
    extends ValidationError {
  protected def prettyInternal: String = s"illegal shadowing expr variable: $varName"
}
final case class EUnknownExprVar(context: Context, varName: ExprVarName) extends ValidationError {
  protected def prettyInternal: String = s"unknown expr variable: $varName"
}
final case class EUnknownDefinition(context: Context, lookupError: LookupError)
    extends ValidationError {
  protected def prettyInternal: String = lookupError.pretty
}
final case class ETypeSynAppWrongArity(
    context: Context,
    expectedArity: Int,
    syn: TypeSynName,
    args: ImmArray[Type])
    extends ValidationError {
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
    fields: ImmArray[(FieldName, Expr)])
    extends ValidationError {
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
    expr: Option[Expr])
    extends ValidationError {
  protected def prettyInternal: String =
    s"""type mismatch:
       | * expected type: ${expectedType.pretty}
       | * found type: ${foundType.pretty}""".stripMargin
}
final case class EExpectedAnyType(context: Context, typ: Type) extends ValidationError {
  protected def prettyInternal: String =
    s"expected a type containing neither type variables nor quantifiers, but found: ${typ.pretty}"
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
    reason: UnserializabilityReason)
    extends ValidationError {
  protected def prettyInternal: String =
    s"""expected serializable type:
       | * reason: ${reason.pretty}
       | * found: ${typ.pretty}
       | * problem: ${requirement.pretty}
     """.stripMargin
}
final case class ETypeConMismatch(
    context: Context,
    foundConName: TypeConName,
    expectedConName: TypeConName)
    extends ValidationError {
  protected def prettyInternal: String =
    s"""type constructor mismatch:
       | * expected: ${expectedConName.qualifiedName}
       | * found: ${foundConName.qualifiedName}""".stripMargin
}
final case class EExpectedDataType(context: Context, typ: Type) extends ValidationError {
  protected def prettyInternal: String = s"expected data type, but found: ${typ.pretty}"
}
final case class EExpectedListType(context: Context, typ: Type) extends ValidationError {
  protected def prettyInternal: String = s"expected list type, but found: ${typ.pretty}"
}
final case class EExpectedOptionType(context: Context, typ: Type) extends ValidationError {
  protected def prettyInternal: String = s"expected option type, but found: ${typ.pretty}"
}
final case class EEmptyCase(context: Context) extends ValidationError {
  protected def prettyInternal: String = "empty case"
}
final case class EExpectedTemplatableType(context: Context, conName: TypeConName)
    extends ValidationError {
  protected def prettyInternal: String =
    s"expected monomorphic record type in template definition, but found: ${conName.qualifiedName}"

}
final case class EImportCycle(context: Context, modName: List[ModuleName]) extends ValidationError {
  protected def prettyInternal: String = s"cycle in module dependency ${modName.mkString(" -> ")}"
}
final case class ETypeSynCycle(context: Context, names: List[TypeSynName]) extends ValidationError {
  protected def prettyInternal: String =
    s"cycle in type synonym definitions ${names.mkString(" -> ")}"
}
final case class EImpredicativePolymorphism(context: Context, typ: Type) extends ValidationError {
  protected def prettyInternal: String =
    s"impredicative polymorphism is not supported: ${typ.pretty}"
}
final case class EKeyOperationForTemplateWithNoKey(context: Context, template: TypeConName)
    extends ValidationError {
  protected def prettyInternal: String =
    s"tried to perform key lookup or fetch on template ${template.qualifiedName}"
}
final case class EIllegalKeyExpression(context: Context, expr: Expr) extends ValidationError {
  protected def prettyInternal: String = s"illegal template key expression"
}
final case class EIllegalHigherEnumType(context: Context, defn: TypeConName)
    extends ValidationError {
  protected def prettyInternal: String = s"illegal higher order enum type"
}
final case class EIllegalEnumArgument(context: Context, typ: Type) extends ValidationError {
  protected def prettyInternal: String = s"illegal non Unit enum argument"
}
sealed abstract class PartyLiteralRef extends Product with Serializable
final case class PartyLiteral(party: Party) extends PartyLiteralRef
final case class ValRefWithPartyLiterals(valueRef: ValueRef) extends PartyLiteralRef
final case class EForbiddenPartyLiterals(context: Context, ref: PartyLiteralRef)
    extends ValidationError {
  protected def prettyInternal: String = s"Found forbidden party literals in ${ref}"
}
/* Collision */

final case class ECollision(
    pkgId: PackageId,
    entity1: NamedEntity,
    entity2: NamedEntity
) extends ValidationError {

  assert(entity1.fullyResolvedName == entity2.fullyResolvedName)

  def context: Context = NoContext

  def collisionName: DottedName = entity1.fullyResolvedName

  override protected def prettyInternal: String =
    s"collision between ${entity1.pretty} and ${entity2.pretty}"
}
