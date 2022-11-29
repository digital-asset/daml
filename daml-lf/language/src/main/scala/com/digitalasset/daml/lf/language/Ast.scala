// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.language

import com.daml.lf.data.Ref._
import com.daml.lf.data._
import scala.collection.immutable.VectorMap

object Ast {
  //
  // Identifiers
  //

  /** Fully applied type constructor. */
  final case class TypeConApp(tycon: TypeConName, args: ImmArray[Type]) {
    def pretty: String = this.toType.pretty
    def toType: Type = args.foldLeft[Type](TTyCon(tycon))(TApp)
  }

  /* Expression variable name. */
  type ExprVarName = Name

  /* Type variable name. */
  type TypeVarName = Name

  /* Reference to a field in a record or variant. */
  type FieldName = Name

  /* Variant constructor name. */
  type VariantConName = Name

  /* Variant constructor name. */
  type EnumConName = Name

  /* Binding in a let/update/scenario block. */
  final case class Binding(binder: Option[ExprVarName], typ: Type, bound: Expr)

  //
  // Expressions
  //

  sealed abstract class Expr extends Product with Serializable {

    /** Infix alias for repeated [[EApp]] application. */
    @inline final def eApp(arg: Expr, args: Expr*): EApp =
      (args foldLeft EApp(this, arg))(EApp)

    /** Infix alias for repeated [[ETyApp]] application. */
    @inline final def eTyApp(typ: Type, typs: Type*): ETyApp =
      (typs foldLeft ETyApp(this, typ))(ETyApp)
  }

  // We use this type to reduce depth of pattern matching
  sealed abstract class ExprAtomic extends Expr

  /** Reference to a variable in current lexical scope. */
  final case class EVar(value: ExprVarName) extends ExprAtomic

  /** Reference to a value definition. */
  final case class EVal(value: ValueRef) extends ExprAtomic

  /** Reference to a builtin function. */
  final case class EBuiltin(value: BuiltinFunction) extends ExprAtomic

  /** Primitive constructor, e.g. True, False or Unit. */
  final case class EPrimCon(value: PrimCon) extends ExprAtomic

  /** Primitive literal. */
  final case class EPrimLit(value: PrimLit) extends ExprAtomic

  /** Record construction. */
  final case class ERecCon(tycon: TypeConApp, fields: ImmArray[(FieldName, Expr)]) extends Expr

  /** Record projection. */
  final case class ERecProj(tycon: TypeConApp, field: FieldName, record: Expr) extends Expr

  /** Non-destructive record update. */
  final case class ERecUpd(tycon: TypeConApp, field: FieldName, record: Expr, update: Expr)
      extends Expr

  /** Variant construction. */
  final case class EVariantCon(tycon: TypeConApp, variant: VariantConName, arg: Expr) extends Expr

  /** Variant construction. */
  final case class EEnumCon(tyConName: TypeConName, con: EnumConName) extends ExprAtomic

  /** Struct construction. */
  final case class EStructCon(fields: ImmArray[(FieldName, Expr)]) extends Expr

  /** Struct projection. */
  final case class EStructProj(field: FieldName, struct: Expr) extends Expr

  /** Struct update. */
  final case class EStructUpd(field: FieldName, struct: Expr, update: Expr) extends Expr

  /** Expression application. Function can be an abstraction or a builtin function. */
  final case class EApp(fun: Expr, arg: Expr) extends Expr

  /** Type application. */
  final case class ETyApp(expr: Expr, typ: Type) extends Expr

  /** Expression abstraction. */
  final case class EAbs(
      binder: (ExprVarName, Type),
      body: Expr,
      ref: Option[DefinitionRef], // The definition in which this abstraction is defined.
  ) extends Expr

  /** Type abstraction. */
  final case class ETyAbs(binder: (TypeVarName, Kind), body: Expr) extends Expr

  /** Pattern matching. */
  final case class ECase(scrut: Expr, alts: ImmArray[CaseAlt]) extends Expr

  /** Let binding. */
  final case class ELet(binding: Binding, body: Expr) extends Expr

  /** Empty list constructor. */
  final case class ENil(typ: Type) extends ExprAtomic

  /** List construction. */
  final case class ECons(typ: Type, front: ImmArray[Expr], tail: Expr) extends Expr

  /** Update expression */
  final case class EUpdate(update: Update) extends Expr

  /** Scenario expression */
  final case class EScenario(scenario: Scenario) extends Expr

  /** Location annotations */
  final case class ELocation(loc: Location, expr: Expr) extends Expr

  final case class ENone(typ: Type) extends ExprAtomic

  final case class ESome(typ: Type, body: Expr) extends Expr

  /** Any constructor * */
  final case class EToAny(ty: Type, body: Expr) extends Expr

  /** Extract the underlying value if it matches the ty * */
  final case class EFromAny(ty: Type, body: Expr) extends Expr

  /** Unique textual representation of template Id * */
  final case class ETypeRep(typ: Type) extends Expr

  /** Throw an exception */
  final case class EThrow(returnType: Type, exceptionType: Type, exception: Expr) extends Expr

  /** Construct an AnyException from its message and payload */
  final case class EToAnyException(typ: Type, value: Expr) extends Expr

  /** Extract the payload from an AnyException if it matches the given exception type */
  final case class EFromAnyException(typ: Type, value: Expr) extends Expr

  // We use this type to reduce depth of pattern matching
  sealed abstract class ExprInterface extends Expr

  /** Convert template payload to interface it implements */
  final case class EToInterface(interfaceId: TypeConName, templateId: TypeConName, value: Expr)
      extends ExprInterface

  /** Convert interface back to template payload if possible */
  final case class EFromInterface(interfaceId: TypeConName, templateId: TypeConName, value: Expr)
      extends ExprInterface

  /** Convert interface back to template payload,
    * or raise a WronglyTypedContracg error if not possible
    */
  final case class EUnsafeFromInterface(
      interfaceId: TypeConName,
      templateId: TypeConName,
      contractIdExpr: Expr,
      ifaceExpr: Expr,
  ) extends ExprInterface

  /** Upcast from an interface payload to an interface it requires. */
  final case class EToRequiredInterface(
      requiredIfaceId: TypeConName,
      requiringIfaceId: TypeConName,
      body: Expr,
  ) extends ExprInterface

  /** Downcast from an interface payload to an interface that requires it, if possible. */
  final case class EFromRequiredInterface(
      requiredIfaceId: TypeConName,
      requiringIfaceId: TypeConName,
      body: Expr,
  ) extends ExprInterface

  /** Downcast from an interface payload to an interface that requires it,
    * or raise a WronglyTypedContract error if not possible.
    */
  final case class EUnsafeFromRequiredInterface(
      requiredIfaceId: TypeConName,
      requiringIfaceId: TypeConName,
      contractIdExpr: Expr,
      ifaceExpr: Expr,
  ) extends ExprInterface

  /** Invoke an interface method */
  final case class ECallInterface(interfaceId: TypeConName, methodName: MethodName, value: Expr)
      extends ExprInterface

  /** Obtain the type representation of a contract's template through an interface. */
  final case class EInterfaceTemplateTypeRep(
      ifaceId: TypeConName,
      body: Expr,
  ) extends ExprInterface

  /** Obtain the signatories of a contract through an interface. */
  final case class ESignatoryInterface(
      ifaceId: TypeConName,
      body: Expr,
  ) extends ExprInterface

  /** Obtain the observers of a contract through an interface. */
  final case class EObserverInterface(
      ifaceId: TypeConName,
      body: Expr,
  ) extends ExprInterface

  /** Obtain the view of an interface. */
  final case class EViewInterface(
      ifaceId: TypeConName,
      expr: Expr,
  ) extends ExprInterface

  //
  // Kinds
  //

  sealed abstract class Kind extends Product with Serializable {
    def pretty: String = Kind.prettyKind(this)
  }

  object Kind {

    def prettyKind(kind: Kind, needParens: Boolean = false): String = kind match {
      case KStar => "*"
      case KNat => "nat"
      case KArrow(fun, arg) if needParens =>
        "(" + prettyKind(fun, true) + "->" + prettyKind(arg, false) +
          ")"
      case KArrow(fun, arg) =>
        prettyKind(fun, true) + "->" + prettyKind(arg, false)
    }
  }

  /** Kind of a proper data type. */
  case object KStar extends Kind

  /** Kind of nat tye */
  case object KNat extends Kind

  /** Kind of higher kinded type. */
  final case class KArrow(param: Kind, result: Kind) extends Kind

  //
  // Types
  //

  // Note that we rely on the equality of Type so this must stay sealed
  // and all inhabitants should be case classes or case objects.

  sealed abstract class Type extends Product with Serializable {
    def pretty: String = Type.prettyType(this)
  }

  object Type {

    def prettyType(typ: Type): String = {
      val precTApp = 2
      val precTFun = 1
      val precTForall = 0

      def maybeParens(needParens: Boolean, s: String): String =
        if (needParens) s"($s)" else s

      def prettyType(t0: Type, prec: Int = precTForall): String = t0 match {
        case TVar(n) => n
        case TNat(n) => n.toString
        case TSynApp(syn, args) =>
          maybeParens(
            prec > precTApp,
            syn.qualifiedName.toString + " " +
              args
                .map(t => prettyType(t, precTApp + 1))
                .toSeq
                .mkString(" "),
          )
        case TTyCon(con) => con.qualifiedName.toString
        case TBuiltin(BTArrow) => "(->)"
        case TBuiltin(bt) => bt.toString.stripPrefix("BT")
        case TApp(TApp(TBuiltin(BTArrow), param), result) =>
          maybeParens(
            prec > precTFun,
            prettyType(param, precTFun + 1) + " → " + prettyType(result, precTFun),
          )
        case TApp(fun, arg) =>
          maybeParens(
            prec > precTApp,
            prettyType(fun, precTApp) + " " + prettyType(arg, precTApp + 1),
          )
        case TForall((v, _), body) =>
          maybeParens(prec > precTForall, "∀" + v + prettyForAll(body))
        case TStruct(fields) =>
          "(" + fields.iterator
            .map { case (n, t) => n + ": " + prettyType(t, precTForall) }
            .toSeq
            .mkString(", ") + ")"
      }

      def prettyForAll(t: Type): String = t match {
        case TForall((v, _), body) => " " + v + prettyForAll(body)
        case _ => ". " + prettyType(t, precTForall)
      }

      prettyType(typ)
    }
  }

  /** Reference to a type variable. */
  final case class TVar(name: TypeVarName) extends Type

  /** nat type */
  // for now it can contains only a Numeric Scale
  final case class TNat(n: Numeric.Scale) extends Type

  object TNat {
    // works because Numeric.Scale.MinValue = 0
    val values = Numeric.Scale.values.map(new TNat(_))
    def apply(n: Numeric.Scale): TNat = values(n)
    val Decimal: TNat = values(10)
  }

  /** Fully applied type synonym. */
  final case class TSynApp(tysyn: TypeSynName, args: ImmArray[Type]) extends Type

  /** Reference to a type constructor. */
  final case class TTyCon(tycon: TypeConName) extends Type

  /** Reference to builtin type. */
  final case class TBuiltin(bt: BuiltinType) extends Type

  /** Application of a type function to a type. */
  final case class TApp(tyfun: Type, arg: Type) extends Type

  /** Universally quantified type. */
  final case class TForall(binder: (TypeVarName, Kind), body: Type) extends Type

  /** Structs */
  final case class TStruct(fields: Struct[Type]) extends Type

  sealed abstract class BuiltinType extends Product with Serializable

  case object BTInt64 extends BuiltinType
  case object BTNumeric extends BuiltinType
  case object BTText extends BuiltinType
  case object BTTimestamp extends BuiltinType
  case object BTParty extends BuiltinType
  case object BTUnit extends BuiltinType
  case object BTBool extends BuiltinType
  case object BTList extends BuiltinType
  case object BTOptional extends BuiltinType
  case object BTTextMap extends BuiltinType
  case object BTGenMap extends BuiltinType
  case object BTUpdate extends BuiltinType
  case object BTScenario extends BuiltinType
  case object BTDate extends BuiltinType
  case object BTContractId extends BuiltinType
  case object BTArrow extends BuiltinType
  case object BTAny extends BuiltinType
  case object BTTypeRep extends BuiltinType
  case object BTAnyException extends BuiltinType
  case object BTRoundingMode extends BuiltinType
  case object BTBigNumeric extends BuiltinType

  //
  // Primitive literals
  //

  sealed abstract class PrimLit extends Equals with Product with Serializable {
    def value: Any
  }

  final case class PLInt64(override val value: Long) extends PrimLit
  final case class PLNumeric(override val value: Numeric) extends PrimLit
  // Text should be treated as Utf8, data.Utf8 provide emulation functions for that
  final case class PLText(override val value: String) extends PrimLit
  final case class PLTimestamp(override val value: Time.Timestamp) extends PrimLit
  final case class PLDate(override val value: Time.Date) extends PrimLit
  final case class PLRoundingMode(override val value: java.math.RoundingMode) extends PrimLit

  //
  // Primitive constructors
  //

  sealed abstract class PrimCon extends Product with Serializable

  case object PCTrue extends PrimCon
  case object PCFalse extends PrimCon
  case object PCUnit extends PrimCon

  //
  // Builtin functions.
  //

  sealed abstract class BuiltinFunction extends Product with Serializable

  final case object BTrace extends BuiltinFunction // : ∀a. Text -> a -> a

  // Numeric arithmetic
  final case object BAddNumeric extends BuiltinFunction // :  ∀s. Numeric s → Numeric s → Numeric s
  final case object BSubNumeric extends BuiltinFunction // :  ∀s. Numeric s → Numeric s → Numeric s
  final case object BMulNumeric
      extends BuiltinFunction // :  ∀s1 s2 s. Numeric s1 → Numeric s2 → Numeric s
  final case object BDivNumeric
      extends BuiltinFunction // :  ∀s1 s2 s. Numeric s1 → Numeric s2 → Numeric s
  final case object BRoundNumeric extends BuiltinFunction // :  ∀s. Integer → Numeric s → Numeric s
  final case object BCastNumeric extends BuiltinFunction // : ∀s1 s2. Numeric s1 → Numeric s2
  final case object BShiftNumeric extends BuiltinFunction // : ∀s1 s2. Numeric s1 → Numeric s2

  // Int64 arithmetic
  final case object BAddInt64 extends BuiltinFunction // : Int64 → Int64 → Int64
  final case object BSubInt64 extends BuiltinFunction // : Int64 → Int64 → Int64
  final case object BMulInt64 extends BuiltinFunction // : Int64 → Int64 → Int64
  final case object BDivInt64 extends BuiltinFunction // : Int64 → Int64 → Int64
  final case object BModInt64 extends BuiltinFunction // : Int64 → Int64 → Int64
  final case object BExpInt64 extends BuiltinFunction // : Int64 → Int64 → Int64

  // Conversions
  final case object BInt64ToNumeric extends BuiltinFunction // : ∀s. Int64 → Numeric s
  final case object BNumericToInt64 extends BuiltinFunction // : ∀s. Numeric s → Int64
  final case object BDateToUnixDays extends BuiltinFunction // : Date -> Int64
  final case object BUnixDaysToDate extends BuiltinFunction // : Int64 -> Date
  final case object BTimestampToUnixMicroseconds extends BuiltinFunction // : Timestamp -> Int64
  final case object BUnixMicrosecondsToTimestamp extends BuiltinFunction // : Int64 -> Timestamp

  // Folds
  final case object BFoldl extends BuiltinFunction // : ∀a b. (b → a → b) → b → List a → b
  final case object BFoldr extends BuiltinFunction // : ∀a b. (a → b → b) → b → List a → b

  // Maps
  final case object BTextMapEmpty extends BuiltinFunction // : ∀ a. TextMap a
  final case object BTextMapInsert
      extends BuiltinFunction // : ∀ a. Text -> a -> TextMap a -> TextMap a
  final case object BTextMapLookup extends BuiltinFunction // : ∀ a. Text -> TextMap a -> Optional a
  final case object BTextMapDelete extends BuiltinFunction // : ∀ a. Text -> TextMap a -> TextMap a
  final case object BTextMapToList
      extends BuiltinFunction // : ∀ a. TextMap a -> [Struct("key":Text, "value":a)]
  final case object BTextMapSize extends BuiltinFunction // : ∀ a. TextMap a -> Int64

  // Generic Maps
  final case object BGenMapEmpty extends BuiltinFunction // : ∀ a b. GenMap a b
  final case object BGenMapInsert
      extends BuiltinFunction // : ∀ a b. a -> b -> GenMap a b -> GenMap a b
  final case object BGenMapLookup extends BuiltinFunction // : ∀ a b. a -> GenMap a b -> Optional b
  final case object BGenMapDelete extends BuiltinFunction // : ∀ a b. a -> GenMap a b -> GenMap a b
  final case object BGenMapKeys extends BuiltinFunction // : ∀ a b. GenMap a b -> [a]
  final case object BGenMapValues extends BuiltinFunction // : ∀ a b. GenMap a b -> [b]
  final case object BGenMapSize extends BuiltinFunction // : ∀ a b. GenMap a b -> Int64

  // Text functions
  final case object BExplodeText extends BuiltinFunction // : Text → List Char
  final case object BImplodeText extends BuiltinFunction // : List Text -> Text
  final case object BAppendText extends BuiltinFunction // : Text → Text → Text

  final case object BInt64ToText extends BuiltinFunction //  Int64 → Text
  final case object BNumericToText extends BuiltinFunction // : ∀s. Numeric s → Text
  final case object BTextToText extends BuiltinFunction // : Text → Text
  final case object BTimestampToText extends BuiltinFunction // : Timestamp → Text
  final case object BPartyToText extends BuiltinFunction // : Party → Text
  final case object BDateToText extends BuiltinFunction // : Date -> Text
  final case object BContractIdToText
      extends BuiltinFunction // : forall t. ContractId t -> Optional Text
  final case object BPartyToQuotedText extends BuiltinFunction // : Party -> Text
  final case object BCodePointsToText extends BuiltinFunction // : [Int64] -> Text
  final case object BTextToParty extends BuiltinFunction // : Text -> Optional Party
  final case object BTextToInt64 extends BuiltinFunction // : Text -> Optional Int64
  final case object BTextToNumeric extends BuiltinFunction // :  ∀s. Text -> Optional (Numeric s)
  final case object BTextToCodePoints extends BuiltinFunction // : Text -> List Int64

  final case object BSHA256Text extends BuiltinFunction // : Text -> Text

  // Errors
  final case object BError extends BuiltinFunction // : ∀a. Text → a

  // Comparisons
  final case object BLessNumeric extends BuiltinFunction // :  ∀s. Numeric s → Numeric s → Bool
  final case object BLessEqNumeric extends BuiltinFunction // :  ∀s. Numeric →  ∀s. Numeric → Bool
  final case object BGreaterNumeric extends BuiltinFunction // :  ∀s. Numeric s → Numeric s → Bool
  final case object BGreaterEqNumeric extends BuiltinFunction // : ∀s. Numeric s → Numeric s → Bool
  final case object BEqualNumeric
      extends BuiltinFunction // :  ∀s. Numeric s ->  ∀s. Numeric s -> Bool

  final case object BEqualList
      extends BuiltinFunction // : ∀a. (a -> a -> Bool) -> List a -> List a -> Bool
  final case object BEqualContractId
      extends BuiltinFunction // : ∀a. ContractId a -> ContractId a -> Bool
  final case object BEqual extends BuiltinFunction // ∀a. a -> a -> Bool
  final case object BLess extends BuiltinFunction // ∀a. a -> a -> Bool
  final case object BLessEq extends BuiltinFunction // ∀a. a -> a -> Bool
  final case object BGreater extends BuiltinFunction // ∀a. a -> a -> Bool
  final case object BGreaterEq extends BuiltinFunction // ∀a. a -> a -> Bool

  final case object BCoerceContractId
      extends BuiltinFunction // : ∀a b. ContractId a -> ContractId b

  // Exceptions
  final case object BAnyExceptionMessage extends BuiltinFunction // AnyException → Text

  // Numeric arithmetic
  final case object BScaleBigNumeric extends BuiltinFunction // : BigNumeric → Int64
  final case object BPrecisionBigNumeric extends BuiltinFunction // : BigNumeric → Int64
  final case object BAddBigNumeric extends BuiltinFunction // : BigNumeric → BigNumeric → BigNumeric
  final case object BSubBigNumeric
      extends BuiltinFunction // :  BigNumeric → BigNumeric → BigNumeric
  final case object BMulBigNumeric extends BuiltinFunction // : BigNumeric → BigNumeric → BigNumeric
  final case object BDivBigNumeric
      extends BuiltinFunction // : Int64 -> RoundingMode → BigNumeric → BigNumeric → BigNumeric s
  final case object BShiftRightBigNumeric
      extends BuiltinFunction // : Int64 → BigNumeric → BigNumeric
  final case object BBigNumericToNumeric extends BuiltinFunction // :  ∀s. BigNumeric → Numeric s
  final case object BNumericToBigNumeric extends BuiltinFunction // :  ∀s. Numeric s → BigNumeric
  final case object BBigNumericToText extends BuiltinFunction // : BigNumeric → Text

  // TypeRep
  final case object BTypeRepTyConName extends BuiltinFunction // : TypeRep → Optional Text

  // Unstable Text Primitives
  final case object BTextToUpper extends BuiltinFunction // Text → Text
  final case object BTextToLower extends BuiltinFunction // : Text → Text
  final case object BTextSlice extends BuiltinFunction // : Int64 → Int64 → Text → Text
  final case object BTextSliceIndex extends BuiltinFunction // : Text → Text → Optional Int64
  final case object BTextContainsOnly extends BuiltinFunction // : Text → Text → Bool
  final case object BTextReplicate extends BuiltinFunction // : Int64 → Text → Text
  final case object BTextSplitOn extends BuiltinFunction // : Text → Text → List Text
  final case object BTextIntercalate extends BuiltinFunction // : Text → List Text → Text

  final case class EExperimental(name: String, typ: Type) extends Expr

  //
  // Update expressions
  //

  final case class RetrieveByKey(templateId: TypeConName, key: Expr)

  sealed abstract class Update extends Product with Serializable

  final case class UpdatePure(t: Type, expr: Expr) extends Update
  final case class UpdateBlock(bindings: ImmArray[Binding], body: Expr) extends Update
  final case class UpdateCreate(templateId: TypeConName, arg: Expr) extends Update
  final case class UpdateCreateInterface(interfaceId: TypeConName, arg: Expr) extends Update
  final case class UpdateFetchTemplate(templateId: TypeConName, contractId: Expr) extends Update
  final case class UpdateFetchInterface(interfaceId: TypeConName, contractId: Expr) extends Update
  final case class UpdateExercise(
      templateId: TypeConName,
      choice: ChoiceName,
      cidE: Expr,
      argE: Expr,
  ) extends Update
  final case class UpdateExerciseInterface(
      interfaceId: TypeConName,
      choice: ChoiceName,
      cidE: Expr,
      argE: Expr,
      guardE: Option[Expr],
      // `guardE` is an expression of type Interface -> Bool which is evaluated after
      // fetching the contract but before running the exercise body. If the guard returns
      // false, or an exception is raised during evaluation, the transaction is aborted.
  ) extends Update
  final case class UpdateExerciseByKey(
      templateId: TypeConName,
      choice: ChoiceName,
      keyE: Expr,
      argE: Expr,
  ) extends Update
  case object UpdateGetTime extends Update
  final case class UpdateFetchByKey(rbk: RetrieveByKey) extends Update
  final case class UpdateLookupByKey(rbk: RetrieveByKey) extends Update
  final case class UpdateEmbedExpr(typ: Type, body: Expr) extends Update
  final case class UpdateTryCatch(
      typ: Type,
      body: Expr,
      binder: ExprVarName,
      handler: Expr,
  ) extends Update

  //
  // Scenario expressions
  //

  sealed abstract class Scenario extends Product with Serializable

  final case class ScenarioPure(t: Type, expr: Expr) extends Scenario
  final case class ScenarioBlock(bindings: ImmArray[Binding], body: Expr) extends Scenario
  final case class ScenarioCommit(partyE: Expr, updateE: Expr, retType: Type) extends Scenario
  final case class ScenarioMustFailAt(partyE: Expr, updateE: Expr, retType: Type) extends Scenario
  final case class ScenarioPass(relTimeE: Expr) extends Scenario
  case object ScenarioGetTime extends Scenario
  final case class ScenarioGetParty(nameE: Expr) extends Scenario
  final case class ScenarioEmbedExpr(typ: Type, body: Expr) extends Scenario

  //
  // Pattern matching
  //

  sealed abstract class CasePat extends Product with Serializable

  // Match on variant
  final case class CPVariant(tycon: TypeConName, variant: VariantConName, binder: ExprVarName)
      extends CasePat
  // Match on enum
  final case class CPEnum(tycon: TypeConName, constructor: EnumConName) extends CasePat
  // Match on primitive constructor.
  final case class CPPrimCon(pc: PrimCon) extends CasePat
  // Match on an empty list.
  case object CPNil extends CasePat
  // Match on a non-empty list.
  final case class CPCons(head: ExprVarName, tail: ExprVarName) extends CasePat
  // Match on anything. Should be the last alternative.
  case object CPDefault extends CasePat
  // match on none
  case object CPNone extends CasePat
  // match on some
  final case class CPSome(body: ExprVarName) extends CasePat

  // Case alternative
  final case class CaseAlt(pattern: CasePat, expr: Expr)

  //
  // Definitions
  //

  sealed abstract class GenDefinition[+E] extends Product with Serializable

  final case class DTypeSyn(params: ImmArray[(TypeVarName, Kind)], typ: Type)
      extends GenDefinition[Nothing]
  final case class DDataType(
      serializable: Boolean,
      params: ImmArray[(TypeVarName, Kind)],
      cons: DataCons,
  ) extends GenDefinition[Nothing]
  object DDataType {
    val Interface = DDataType(false, ImmArray.empty, DataInterface)
  }

  final case class GenDValue[E](
      typ: Type,
      body: E,
      isTest: Boolean,
  ) extends GenDefinition[E]

  final class GenDValueCompanion[E] private[Ast] {
    def apply(typ: Type, body: E, isTest: Boolean): GenDValue[E] =
      GenDValue(typ = typ, body = body, isTest = isTest)

    def unapply(arg: GenDValue[E]): Some[(Type, E, Boolean)] =
      Some((arg.typ, arg.body, arg.isTest))
  }

  type DValue = GenDValue[Expr]
  val DValue = new GenDValueCompanion[Expr]
  type DValueSignature = GenDValue[Unit]
  val DValueSignature = new GenDValueCompanion[Unit]

  type Definition = GenDefinition[Expr]
  type DefinitionSignature = GenDefinition[Unit]

  // Data constructor in data type definition.
  sealed abstract class DataCons extends Product with Serializable

  final case class DataRecord(fields: ImmArray[(FieldName, Type)]) extends DataCons {
    lazy val fieldInfo: Map[FieldName, (Type, Int)] =
      fields.iterator.zipWithIndex.map { case ((field, typ), rank) => (field, (typ, rank)) }.toMap
  }
  final case class DataVariant(variants: ImmArray[(VariantConName, Type)]) extends DataCons {
    lazy val constructorInfo: Map[VariantConName, (Type, Int)] =
      variants.iterator.zipWithIndex.map { case ((cons, typ), rank) => (cons, (typ, rank)) }.toMap
  }
  final case class DataEnum(constructors: ImmArray[EnumConName]) extends DataCons {
    lazy val constructorRank: Map[EnumConName, Int] = constructors.iterator.zipWithIndex.toMap
  }
  case object DataInterface extends DataCons

  final case class GenTemplateKey[E](
      typ: Type,
      body: E,
      // function from key type to [Party]
      maintainers: E,
  )

  final class GenTemplateKeyCompanion[E] private[Ast] {
    def apply(typ: Type, body: E, maintainers: E): GenTemplateKey[E] =
      GenTemplateKey(typ = typ, body = body, maintainers = maintainers)

    def unapply(arg: GenTemplateKey[E]): Some[(Type, E, E)] =
      Some((arg.typ, arg.body, arg.maintainers))
  }

  type TemplateKey = GenTemplateKey[Expr]
  val TemplateKey = new GenTemplateKeyCompanion[Expr]

  type TemplateKeySignature = GenTemplateKey[Unit]
  val TemplateKeySignature = new GenTemplateKeyCompanion[Unit]

  final case class GenDefInterface[E](
      requires: Set[TypeConName],
      param: ExprVarName, // Binder for template argument.
      choices: Map[ChoiceName, GenTemplateChoice[E]],
      methods: Map[MethodName, InterfaceMethod],
      coImplements: Map[TypeConName, GenInterfaceCoImplements[E]],
      view: Type,
  )

  final class GenDefInterfaceCompanion[E] {
    @throws[PackageError]
    def build(
        requires: Iterable[TypeConName],
        param: ExprVarName, // Binder for template argument.
        choices: Iterable[GenTemplateChoice[E]],
        methods: Iterable[InterfaceMethod],
        coImplements: Iterable[GenInterfaceCoImplements[E]],
        view: Type,
    ): GenDefInterface[E] = {
      val requiresSet = toSetWithoutDuplicate(
        requires,
        (name: TypeConName) => PackageError(s"repeated required interface $name"),
      )
      val choiceMap = toMapWithoutDuplicate(
        choices.view.map(c => c.name -> c),
        (name: ChoiceName) => PackageError(s"collision on interface choice name $name"),
      )
      val methodMap = toMapWithoutDuplicate(
        methods.view.map(c => c.name -> c),
        (name: MethodName) => PackageError(s"collision on interface method name $name"),
      )
      val coImplementsMap = toMapWithoutDuplicate(
        coImplements.view.map(c => c.templateId -> c),
        (templateId: TypeConName) =>
          PackageError(s"repeated interface co-implementation ${templateId.toString}"),
      )
      GenDefInterface(requiresSet, param, choiceMap, methodMap, coImplementsMap, view)
    }

    def apply(
        requires: Set[TypeConName],
        param: ExprVarName,
        choices: Map[ChoiceName, GenTemplateChoice[E]],
        methods: Map[MethodName, InterfaceMethod],
        coImplements: Map[TypeConName, GenInterfaceCoImplements[E]],
        view: Type,
    ): GenDefInterface[E] =
      GenDefInterface(requires, param, choices, methods, coImplements, view)

    def unapply(arg: GenDefInterface[E]): Some[
      (
          Set[TypeConName],
          ExprVarName,
          Map[ChoiceName, GenTemplateChoice[E]],
          Map[MethodName, InterfaceMethod],
          Map[TypeConName, GenInterfaceCoImplements[E]],
          Type,
      )
    ] =
      Some((arg.requires, arg.param, arg.choices, arg.methods, arg.coImplements, arg.view))
  }

  type DefInterface = GenDefInterface[Expr]
  val DefInterface = new GenDefInterfaceCompanion[Expr]

  type DefInterfaceSignature = GenDefInterface[Unit]
  val DefInterfaceSignature = new GenDefInterfaceCompanion[Unit]

  final case class InterfaceMethod(
      name: MethodName,
      returnType: Type,
  )

  final case class GenInterfaceCoImplements[E](
      templateId: TypeConName,
      body: GenInterfaceInstanceBody[E],
  )

  final class GenInterfaceCoImplementsCompanion[E] private[Ast] {
    def build(
        templateId: TypeConName,
        body: GenInterfaceInstanceBody[E],
    ): GenInterfaceCoImplements[E] =
      new GenInterfaceCoImplements[E](
        templateId = templateId,
        body = body,
      )

    def apply(
        templateId: TypeConName,
        body: GenInterfaceInstanceBody[E],
    ): GenInterfaceCoImplements[E] =
      GenInterfaceCoImplements[E](templateId, body)

    def unapply(
        arg: GenInterfaceCoImplements[E]
    ): Some[(TypeConName, GenInterfaceInstanceBody[E])] =
      Some((arg.templateId, arg.body))
  }

  type InterfaceCoImplements = GenInterfaceCoImplements[Expr]
  val InterfaceCoImplements = new GenInterfaceCoImplementsCompanion[Expr]

  type InterfaceCoImplementsSignature = GenInterfaceCoImplements[Unit]
  val InterfaceCoImplementsSignature = new GenInterfaceCoImplementsCompanion[Unit]

  final case class GenTemplate[E](
      param: ExprVarName, // Binder for template argument.
      precond: E, // Template creation precondition.
      signatories: E, // Parties agreeing to the contract.
      agreementText: E, // Text the parties agree to.
      choices: Map[ChoiceName, GenTemplateChoice[E]], // Choices available in the template.
      observers: E, // Observers of the contract.
      key: Option[GenTemplateKey[E]],
      implements: VectorMap[TypeConName, GenTemplateImplements[
        E
      ]], // We use a VectorMap to preserve insertion order. The order of the implements determines the order in which to evaluate interface preconditions.
  )

  final class GenTemplateCompanion[E] private[Ast] {
    @throws[PackageError]
    def build(
        param: ExprVarName,
        precond: E,
        signatories: E,
        agreementText: E,
        choices: Iterable[GenTemplateChoice[E]],
        observers: E,
        key: Option[GenTemplateKey[E]],
        implements: Iterable[GenTemplateImplements[E]],
    ): GenTemplate[E] =
      GenTemplate[E](
        param = param,
        precond = precond,
        signatories = signatories,
        agreementText = agreementText,
        choices = toMapWithoutDuplicate(
          choices.view.map(c => c.name -> c),
          (choiceName: ChoiceName) => PackageError(s"collision on choice name $choiceName"),
        ),
        observers = observers,
        key = key,
        implements = toVectorMapWithoutDuplicate(
          implements.map(i => i.interfaceId -> i),
          (ifaceId: TypeConName) =>
            PackageError(s"repeated interface implementation ${ifaceId.toString}"),
        ),
      )

    def apply(
        param: ExprVarName,
        precond: E,
        signatories: E,
        agreementText: E,
        choices: Map[ChoiceName, GenTemplateChoice[E]],
        observers: E,
        key: Option[GenTemplateKey[E]],
        implements: VectorMap[TypeConName, GenTemplateImplements[E]],
    ) = GenTemplate(
      param = param,
      precond = precond,
      signatories = signatories,
      agreementText = agreementText,
      choices = choices,
      observers = observers,
      key = key,
      implements = implements,
    )

    def unapply(arg: GenTemplate[E]): Some[
      (
          ExprVarName,
          E,
          E,
          E,
          Map[ChoiceName, GenTemplateChoice[E]],
          E,
          Option[GenTemplateKey[E]],
          VectorMap[TypeConName, GenTemplateImplements[E]],
      )
    ] = Some(
      (
        arg.param,
        arg.precond,
        arg.signatories,
        arg.agreementText,
        arg.choices,
        arg.observers,
        arg.key,
        arg.implements,
      )
    )
  }

  type Template = GenTemplate[Expr]
  val Template = new GenTemplateCompanion[Expr]

  type TemplateSignature = GenTemplate[Unit]
  val TemplateSignature = new GenTemplateCompanion[Unit]

  final case class GenTemplateChoice[E](
      name: ChoiceName, // Name of the choice.
      consuming: Boolean, // Flag indicating whether exercising the choice consumes the contract.
      controllers: E, // Parties that can exercise the choice.
      choiceObservers: Option[E], // Additional informees for the choice.
      selfBinder: ExprVarName, // Self ContractId binder.
      argBinder: (ExprVarName, Type), // Choice argument binder.
      returnType: Type, // Return type of the choice follow-up.
      update: E, // The choice follow-up.
  )

  final class GenTemplateChoiceCompanion[E] private[Ast] {
    def apply(
        name: ChoiceName,
        consuming: Boolean,
        controllers: E,
        choiceObservers: Option[E],
        selfBinder: ExprVarName,
        argBinder: (ExprVarName, Type),
        returnType: Type,
        update: E,
    ): GenTemplateChoice[E] =
      GenTemplateChoice(
        name = name,
        consuming = consuming,
        controllers = controllers,
        choiceObservers = choiceObservers,
        selfBinder = selfBinder,
        argBinder = argBinder,
        returnType = returnType,
        update = update,
      )

    def unapply(
        arg: GenTemplateChoice[E]
    ): Some[(ChoiceName, Boolean, E, Option[E], ExprVarName, (ExprVarName, Type), Type, E)] =
      Some(
        (
          arg.name,
          arg.consuming,
          arg.controllers,
          arg.choiceObservers,
          arg.selfBinder,
          arg.argBinder,
          arg.returnType,
          arg.update,
        )
      )
  }

  type TemplateChoice = GenTemplateChoice[Expr]
  val TemplateChoice = new GenTemplateChoiceCompanion[Expr]

  type TemplateChoiceSignature = GenTemplateChoice[Unit]
  val TemplateChoiceSignature = new GenTemplateChoiceCompanion[Unit]

  final case class GenTemplateImplements[E](
      interfaceId: TypeConName,
      body: GenInterfaceInstanceBody[E],
  )

  final class GenTemplateImplementsCompanion[E] private[Ast] {
    def build(
        interfaceId: TypeConName,
        body: GenInterfaceInstanceBody[E],
    ): GenTemplateImplements[E] =
      new GenTemplateImplements[E](
        interfaceId = interfaceId,
        body = body,
      )

    def apply(
        interfaceId: TypeConName,
        body: GenInterfaceInstanceBody[E],
    ): GenTemplateImplements[E] =
      new GenTemplateImplements[E](interfaceId, body)

    def unapply(
        arg: GenTemplateImplements[E]
    ): Some[(TypeConName, GenInterfaceInstanceBody[E])] =
      Some((arg.interfaceId, arg.body))
  }

  type TemplateImplements = GenTemplateImplements[Expr]
  val TemplateImplements = new GenTemplateImplementsCompanion[Expr]

  type TemplateImplementsSignature = GenTemplateImplements[Unit]
  val TemplateImplementsSignature = new GenTemplateImplementsCompanion[Unit]

  final case class GenInterfaceInstanceBody[E](
      methods: Map[MethodName, GenInterfaceInstanceMethod[E]],
      view: E,
  )

  final class GenInterfaceInstanceBodyCompanion[E] private[Ast] {
    @throws[PackageError]
    def build(
        methods: Iterable[GenInterfaceInstanceMethod[E]],
        view: E,
    ): GenInterfaceInstanceBody[E] =
      new GenInterfaceInstanceBody[E](
        methods = toMapWithoutDuplicate(
          methods.map(m => m.name -> m),
          (name: MethodName) => PackageError(s"repeated method implementation $name"),
        ),
        view,
      )

    def apply(
        methods: Map[MethodName, GenInterfaceInstanceMethod[E]],
        view: E,
    ): GenInterfaceInstanceBody[E] =
      new GenInterfaceInstanceBody[E](methods, view)

    def unapply(
        arg: GenInterfaceInstanceBody[E]
    ): Some[(Map[MethodName, GenInterfaceInstanceMethod[E]], E)] =
      Some((arg.methods, arg.view))
  }

  type InterfaceInstanceBody = GenInterfaceInstanceBody[Expr]
  val InterfaceInstanceBody = new GenInterfaceInstanceBodyCompanion[Expr]

  type InterfaceInstanceBodySignature = GenInterfaceInstanceBody[Unit]
  val InterfaceInstanceBodySignature = new GenInterfaceInstanceBodyCompanion[Unit]

  final case class GenInterfaceInstanceMethod[E](
      name: MethodName,
      value: E,
  )

  final class GenInterfaceInstanceMethodCompanion[E] {
    def apply(methodName: MethodName, value: E): GenInterfaceInstanceMethod[E] =
      GenInterfaceInstanceMethod[E](methodName, value)

    def unapply(
        arg: GenInterfaceInstanceMethod[E]
    ): Some[(MethodName, E)] =
      Some((arg.name, arg.value))
  }

  type InterfaceInstanceMethod = GenInterfaceInstanceMethod[Expr]
  val InterfaceInstanceMethod = new GenInterfaceInstanceMethodCompanion[Expr]

  type InterfaceInstanceMethodSignature = GenInterfaceInstanceMethod[Unit]
  val InterfaceInstanceMethodSignature = new GenInterfaceInstanceMethodCompanion[Unit]

  final case class GenDefException[E](message: E)

  final class GenDefExceptionCompanion[E] private[Ast] {
    def apply(message: E): GenDefException[E] =
      GenDefException(message = message)

    def unapply(arg: GenDefException[E]): Some[E] =
      Some(arg.message)
  }

  type DefException = GenDefException[Expr]
  val DefException = new GenDefExceptionCompanion[Expr]

  type DefExceptionSignature = GenDefException[Unit]
  val DefExceptionSignature = GenDefException(())

  final case class FeatureFlags()

  object FeatureFlags {
    val default = FeatureFlags()
  }

  //
  // Modules and packages
  //

  final case class GenModule[E](
      name: ModuleName,
      definitions: Map[DottedName, GenDefinition[E]],
      templates: Map[DottedName, GenTemplate[E]],
      exceptions: Map[DottedName, GenDefException[E]],
      interfaces: Map[DottedName, GenDefInterface[E]],
      featureFlags: FeatureFlags,
  ) {
    if (
      !(templates.keySet.intersect(exceptions.keySet).isEmpty
        && templates.keySet.intersect(interfaces.keySet).isEmpty
        && exceptions.keySet.intersect(interfaces.keySet).isEmpty)
    )
      throw PackageError(
        s"Collision between exception and template/interface name ${name.toString}"
      )
  }

  private[this] def toMapWithoutDuplicate[Key, Value](
      xs: Iterable[(Key, Value)],
      error: Key => PackageError,
  ): Map[Key, Value] =
    xs.foldLeft(Map.empty[Key, Value]) { case (acc, (key, value)) =>
      if (acc.contains(key))
        throw error(key)
      else
        acc.updated(key, value)
    }

  private[this] def toVectorMapWithoutDuplicate[Key, Value](
      xs: Iterable[(Key, Value)],
      error: Key => PackageError,
  ): VectorMap[Key, Value] =
    xs.foldRight(VectorMap.empty[Key, Value]) { case ((key, value), acc) =>
      if (acc.contains(key))
        throw error(key)
      else
        acc.updated(key, value)
    }

  private[this] def toSetWithoutDuplicate[X](
      xs: Iterable[X],
      error: X => PackageError,
  ): Set[X] =
    xs.foldLeft(Set.empty[X])((acc, x) =>
      if (acc.contains(x))
        throw error(x)
      else
        acc + x
    )

  final class GenModuleCompanion[E] private[Ast] {
    @throws[PackageError]
    def build(
        name: ModuleName,
        definitions: Iterable[(DottedName, GenDefinition[E])],
        templates: Iterable[(DottedName, GenTemplate[E])],
        exceptions: Iterable[(DottedName, GenDefException[E])],
        interfaces: Iterable[(DottedName, GenDefInterface[E])],
        featureFlags: FeatureFlags,
    ): GenModule[E] =
      GenModule(
        name = name,
        definitions = toMapWithoutDuplicate(
          definitions,
          (name: DottedName) => PackageError(s"Collision on definition name ${name.toString}"),
        ),
        templates = toMapWithoutDuplicate(
          templates,
          (name: DottedName) => PackageError(s"Collision on template name ${name.toString}"),
        ),
        exceptions = toMapWithoutDuplicate(
          exceptions,
          (name: DottedName) => PackageError(s"Collision on exception name ${name.toString}"),
        ),
        interfaces = toMapWithoutDuplicate(
          interfaces,
          (name: DottedName) => PackageError(s"Collision on interface name ${name.toString}"),
        ),
        featureFlags = featureFlags,
      )

    def apply(
        name: ModuleName,
        definitions: Map[DottedName, GenDefinition[E]],
        templates: Map[DottedName, GenTemplate[E]],
        exceptions: Map[DottedName, GenDefException[E]],
        interfaces: Map[DottedName, GenDefInterface[E]],
        featureFlags: FeatureFlags,
    ) =
      GenModule(
        name = name,
        definitions = definitions,
        templates = templates,
        exceptions = exceptions,
        interfaces = interfaces,
        featureFlags = featureFlags,
      )

    def unapply(arg: GenModule[E]): Some[
      (
          ModuleName,
          Map[DottedName, GenDefinition[E]],
          Map[DottedName, GenTemplate[E]],
          Map[DottedName, GenDefException[E]],
          Map[DottedName, GenDefInterface[E]],
          FeatureFlags,
      )
    ] = Some(
      (arg.name, arg.definitions, arg.templates, arg.exceptions, arg.interfaces, arg.featureFlags)
    )
  }

  type Module = GenModule[Expr]
  val Module = new GenModuleCompanion[Expr]

  type ModuleSignature = GenModule[Unit]
  val ModuleSignature = new GenModuleCompanion[Unit]

  final case class PackageMetadata(name: PackageName, version: PackageVersion)

  final case class GenPackage[E](
      modules: Map[ModuleName, GenModule[E]],
      directDeps: Set[PackageId],
      languageVersion: LanguageVersion,
      metadata: Option[PackageMetadata],
  )

  final class GenPackageCompanion[E] private[Ast] {
    @throws[PackageError]
    def build(
        modules: Iterable[GenModule[E]],
        directDeps: Iterable[PackageId],
        languageVersion: LanguageVersion,
        metadata: Option[PackageMetadata],
    ): GenPackage[E] =
      GenPackage(
        modules = toMapWithoutDuplicate(
          modules.view.map(m => m.name -> m),
          (modName: ModuleName) => PackageError(s"Collision on module name ${modName.toString}"),
        ),
        directDeps = directDeps.toSet,
        languageVersion = languageVersion,
        metadata = metadata,
      )
    def apply(
        modules: Map[ModuleName, GenModule[E]],
        directDeps: Set[PackageId],
        languageVersion: LanguageVersion,
        metadata: Option[PackageMetadata],
    ): GenPackage[E] =
      GenPackage(
        modules = modules,
        directDeps = directDeps,
        languageVersion = languageVersion,
        metadata = metadata,
      )

    def unapply(arg: GenPackage[E]): Some[
      (
          Map[ModuleName, GenModule[E]],
          Set[PackageId],
          LanguageVersion,
          Option[PackageMetadata],
      )
    ] = {
      Some(
        (
          arg.modules,
          arg.directDeps,
          arg.languageVersion,
          arg.metadata,
        )
      )
    }

  }

  type Package = GenPackage[Expr]
  val Package = new GenPackageCompanion[Expr]

  // [PackageSignature] is a version of the AST that does not contain
  // LF expression. This should save memory in the [CompiledPackages]
  // where those expressions once compiled into Speedy Expression
  // become useless.
  // Here the term "Signature" refers to all the type information of
  // all the (serializable or not) data of a given package including
  // values and type synonyms. This contrasts with the term
  // "Interface" we conventional use to speak only about all the
  // types of the serializable data of a package. See for instance
  // [InterfaceReader]
  type PackageSignature = GenPackage[Unit]
  val PackageSignature = new GenPackageCompanion[Unit]

  val keyFieldName = Name.assertFromString("key")
  val valueFieldName = Name.assertFromString("value")
  val maintainersFieldName = Name.assertFromString("maintainers")
  val contractIdFieldName = Name.assertFromString("contractId")
  val contractFieldName = Name.assertFromString("contract")
  val signatoriesFieldName = Name.assertFromString("signatories")
  val observersFieldName = Name.assertFromString("observers")

  final case class PackageError(error: String) extends RuntimeException(error)

}
