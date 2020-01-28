// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.language

import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data._

object Ast {
  //
  // Identifiers
  //

  /**  Fully applied type constructor. */
  case class TypeConApp(tycon: TypeConName, args: ImmArray[Type]) {
    def pretty: String =
      args.foldLeft(TTyCon(tycon): Type) { case (arg, acc) => TApp(acc, arg) }.pretty
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
  case class Binding(binder: Option[ExprVarName], typ: Type, bound: Expr)

  //
  // Expressions
  //

  sealed abstract class Expr extends Product with Serializable {

    /** Infix alias for repeated [[EApp]] application. */
    @inline final def eApp(arg: Expr, args: Expr*): EApp =
      (EApp(this, arg) /: args)(EApp)

    /** Infix alias for repeated [[ETyApp]] application. */
    @inline final def eTyApp(typ: Type, typs: Type*): ETyApp =
      (ETyApp(this, typ) /: typs)(ETyApp)
  }

  /** Reference to a variable in current lexical scope. */
  final case class EVar(value: ExprVarName) extends Expr

  /** Reference to a value definition. */
  final case class EVal(value: ValueRef) extends Expr

  /** Reference to a builtin function. */
  final case class EBuiltin(value: BuiltinFunction) extends Expr

  /** Primitive constructor, e.g. True, False or Unit. */
  final case class EPrimCon(value: PrimCon) extends Expr

  /** Primitive literal. */
  final case class EPrimLit(value: PrimLit) extends Expr

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
  final case class EEnumCon(tyConName: TypeConName, con: EnumConName) extends Expr

  /** Struct construction. */
  final case class EStructCon(fields: ImmArray[(FieldName, Expr)]) extends Expr

  /** Struct projection. */
  final case class EStructProj(field: FieldName, struct: Expr) extends Expr

  /** Non-destructive struct update. */
  final case class EStructUpd(field: FieldName, struct: Expr, update: Expr) extends Expr

  /** Expression application. Function can be an abstraction or a builtin function. */
  final case class EApp(fun: Expr, arg: Expr) extends Expr

  /** Type application. */
  final case class ETyApp(expr: Expr, typ: Type) extends Expr

  /** Expression abstraction. */
  final case class EAbs(
      binder: (ExprVarName, Type),
      body: Expr,
      ref: Option[DefinitionRef] // The definition in which this abstraction is defined.
  ) extends Expr

  /** Type abstraction. */
  final case class ETyAbs(binder: (TypeVarName, Kind), body: Expr) extends Expr

  /** Pattern matching. */
  final case class ECase(scrut: Expr, alts: ImmArray[CaseAlt]) extends Expr

  /** Let binding. */
  final case class ELet(binding: Binding, body: Expr) extends Expr

  /** Empty list constructor. */
  final case class ENil(typ: Type) extends Expr

  /** List construction. */
  final case class ECons(typ: Type, front: ImmArray[Expr], tail: Expr) extends Expr

  /** Update expression */
  final case class EUpdate(update: Update) extends Expr

  /** Scenario expression */
  final case class EScenario(scenario: Scenario) extends Expr

  /** Location annotations */
  final case class ELocation(loc: Location, expr: Expr) extends Expr

  final case class ENone(typ: Type) extends Expr

  final case class ESome(typ: Type, body: Expr) extends Expr

  /** Any constructor **/
  final case class EToAny(ty: Type, body: Expr) extends Expr

  /** Extract the underlying value if it matches the ty **/
  final case class EFromAny(ty: Type, body: Expr) extends Expr

  /** Unique textual representation of template Id **/
  final case class ETypeRep(typ: Type) extends Expr

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
        "(" + prettyKind(fun, true) + "->" + prettyKind(arg, false)
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
            syn.qualifiedName.name.toString + " " +
              args
                .map { t =>
                  prettyType(t, precTApp + 1)
                }
                .toSeq
                .mkString(" ")
          )
        case TTyCon(con) => con.qualifiedName.name.toString
        case TBuiltin(BTArrow) => "(->)"
        case TBuiltin(bt) => bt.toString.stripPrefix("BT")
        case TApp(TApp(TBuiltin(BTArrow), param), result) =>
          maybeParens(
            prec > precTFun,
            prettyType(param, precTFun + 1) + " → " + prettyType(result, precTFun))
        case TApp(fun, arg) =>
          maybeParens(
            prec > precTApp,
            prettyType(fun, precTApp) + " " + prettyType(arg, precTApp + 1))
        case TForall((v, _), body) =>
          maybeParens(prec > precTForall, "∀" + v + prettyForAll(body))
        case TStruct(fields) =>
          "(" + fields
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
  final case class TStruct private (sortedFields: ImmArray[(FieldName, Type)]) extends Type

  object TStruct extends (ImmArray[(FieldName, Type)] => TStruct) {
    // should be dropped once the compiler sort fields.
    def apply(fields: ImmArray[(FieldName, Type)]): TStruct =
      new TStruct(ImmArray(fields.toSeq.sortBy(_._1: String)))
  }

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
  final case class PLParty(override val value: Party) extends PrimLit
  final case class PLDate(override val value: Time.Date) extends PrimLit

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

  sealed abstract class BuiltinFunction(val arity: Int) extends Product with Serializable

  final case object BTrace extends BuiltinFunction(2) // : ∀a. Text -> a -> a

  // Numeric arithmetic
  final case object BAddNumeric extends BuiltinFunction(2) // :  ∀s. Numeric s → Numeric s → Numeric s
  final case object BSubNumeric extends BuiltinFunction(2) // :  ∀s. Numeric s → Numeric s → Numeric s
  final case object BMulNumeric extends BuiltinFunction(2) // :  ∀s1 s2 s. Numeric s1 → Numeric s2 → Numeric s
  final case object BDivNumeric extends BuiltinFunction(2) // :  ∀s1 s2 s. Numeric s1 → Numeric s2 → Numeric s
  final case object BRoundNumeric extends BuiltinFunction(2) // :  ∀s. Integer → Numeric s → Numeric s
  final case object BCastNumeric extends BuiltinFunction(1) // : ∀s1 s2. Numeric s1 → Numeric s2
  final case object BShiftNumeric extends BuiltinFunction(1) // : ∀s1 s2. Numeric s1 → Numeric s2

  // Int64 arithmetic
  final case object BAddInt64 extends BuiltinFunction(2) // : Int64 → Int64 → Int64
  final case object BSubInt64 extends BuiltinFunction(2) // : Int64 → Int64 → Int64
  final case object BMulInt64 extends BuiltinFunction(2) // : Int64 → Int64 → Int64
  final case object BDivInt64 extends BuiltinFunction(2) // : Int64 → Int64 → Int64
  final case object BModInt64 extends BuiltinFunction(2) // : Int64 → Int64 → Int64
  final case object BExpInt64 extends BuiltinFunction(2) // : Int64 → Int64 → Int64

  // Conversions
  final case object BInt64ToNumeric extends BuiltinFunction(1) // : ∀s. Int64 → Numeric s
  final case object BNumericToInt64 extends BuiltinFunction(1) // : ∀s. Numeric s → Int64
  final case object BDateToUnixDays extends BuiltinFunction(1) // : Date -> Int64
  final case object BUnixDaysToDate extends BuiltinFunction(1) // : Int64 -> Date
  final case object BTimestampToUnixMicroseconds extends BuiltinFunction(1) // : Timestamp -> Int64
  final case object BUnixMicrosecondsToTimestamp extends BuiltinFunction(1) // : Int64 -> Timestamp

  // Folds
  final case object BFoldl extends BuiltinFunction(3) // : ∀a b. (b → a → b) → b → List a → b
  final case object BFoldr extends BuiltinFunction(3) // : ∀a b. (a → b → b) → b → List a → b

  // Maps
  final case object BTextMapEmpty extends BuiltinFunction(0) // : ∀ a. TextMap a
  final case object BTextMapInsert extends BuiltinFunction(3) // : ∀ a. Text -> a -> TextMap a -> TextMap a
  final case object BTextMapLookup extends BuiltinFunction(2) // : ∀ a. Text -> TextMap a -> Optional a
  final case object BTextMapDelete extends BuiltinFunction(2) // : ∀ a. Text -> TextMap a -> TextMap a
  final case object BTextMapToList extends BuiltinFunction(1) // : ∀ a. TextMap a -> [Struct("key":Text, "value":a)]
  final case object BTextMapSize extends BuiltinFunction(1) // : ∀ a. TextMap a -> Int64

  // Generic Maps
  final case object BGenMapEmpty extends BuiltinFunction(0) // : ∀ a b. GenMap a b
  final case object BGenMapInsert extends BuiltinFunction(3) // : ∀ a b. a -> b -> GenMap a b -> GenMap a b
  final case object BGenMapLookup extends BuiltinFunction(2) // : ∀ a b. a -> GenMap a b -> Optional b
  final case object BGenMapDelete extends BuiltinFunction(2) // : ∀ a b. a -> GenMap a b -> GenMap a b
  final case object BGenMapKeys extends BuiltinFunction(1) // : ∀ a b. GenMap a b -> [a]
  final case object BGenMapValues extends BuiltinFunction(1) // : ∀ a b. GenMap a b -> [b]
  final case object BGenMapSize extends BuiltinFunction(1) // : ∀ a b. GenMap a b -> Int64

  // Text functions
  final case object BExplodeText extends BuiltinFunction(1) // : Text → List Char
  final case object BImplodeText extends BuiltinFunction(1) // : List Text -> Text
  final case object BAppendText extends BuiltinFunction(2) // : Text → Text → Text

  final case object BToTextInt64 extends BuiltinFunction(1) //  Int64 → Text
  final case object BToTextNumeric extends BuiltinFunction(1) // : ∀s. Numeric s → Text
  final case object BToTextText extends BuiltinFunction(1) // : Text → Text
  final case object BToTextTimestamp extends BuiltinFunction(1) // : Timestamp → Text
  final case object BToTextParty extends BuiltinFunction(1) // : Party → Text
  final case object BToTextDate extends BuiltinFunction(1) // : Date -> Text
  final case object BToQuotedTextParty extends BuiltinFunction(1) // : Party -> Text
  final case object BToTextCodePoints extends BuiltinFunction(1) // : [Int64] -> Text
  final case object BFromTextParty extends BuiltinFunction(1) // : Text -> Optional Party
  final case object BFromTextInt64 extends BuiltinFunction(1) // : Text -> Optional Int64
  final case object BFromTextNumeric extends BuiltinFunction(1) // :  ∀s. Text -> Optional (Numeric s)
  final case object BFromTextCodePoints extends BuiltinFunction(1) // : Text -> List Int64

  final case object BSHA256Text extends BuiltinFunction(arity = 1) // : Text -> Text

  // Errors
  final case object BError extends BuiltinFunction(1) // : ∀a. Text → a

  // Comparisons
  final case object BLessInt64 extends BuiltinFunction(2) // : Int64 → Int64 → Bool
  final case object BLessNumeric extends BuiltinFunction(2) // :  ∀s. Numeric s → Numeric s → Bool
  final case object BLessText extends BuiltinFunction(2) // : Text → Text → Bool
  final case object BLessTimestamp extends BuiltinFunction(2) // : Timestamp → Timestamp → Bool
  final case object BLessDate extends BuiltinFunction(2) // : Date → Date → Bool
  final case object BLessParty extends BuiltinFunction(2) // : Party → Party → Bool

  final case object BLessEqInt64 extends BuiltinFunction(2) // : Int64 → Int64 → Bool
  final case object BLessEqNumeric extends BuiltinFunction(2) // :  ∀s. Numeric →  ∀s. Numeric → Bool
  final case object BLessEqText extends BuiltinFunction(2) // : Text → Text → Bool
  final case object BLessEqTimestamp extends BuiltinFunction(2) // : Timestamp → Timestamp → Bool
  final case object BLessEqDate extends BuiltinFunction(2) // : Date → Date → Bool
  final case object BLessEqParty extends BuiltinFunction(2) // : Party → Party → Bool

  final case object BGreaterInt64 extends BuiltinFunction(2) // : Int64 → Int64 → Bool
  final case object BGreaterNumeric extends BuiltinFunction(2) // :  ∀s. Numeric s → Numeric s → Bool
  final case object BGreaterText extends BuiltinFunction(2) // : Text → Text → Bool
  final case object BGreaterTimestamp extends BuiltinFunction(2) // : Timestamp → Timestamp → Bool
  final case object BGreaterDate extends BuiltinFunction(2) // : Date → Date → Bool
  final case object BGreaterParty extends BuiltinFunction(2) // : Party → Party → Bool

  final case object BGreaterEqInt64 extends BuiltinFunction(2) // : Int64 → Int64 → Bool
  final case object BGreaterEqNumeric extends BuiltinFunction(2) // : ∀s. Numeric s → Numeric s → Bool
  final case object BGreaterEqText extends BuiltinFunction(2) // : Text → Text → Bool
  final case object BGreaterEqTimestamp extends BuiltinFunction(2) // : Timestamp → Timestamp → Bool
  final case object BGreaterEqDate extends BuiltinFunction(2) // : Date → Date → Bool
  final case object BGreaterEqParty extends BuiltinFunction(2) // : Party → Party → Bool

  final case object BEqualNumeric extends BuiltinFunction(2) // :  ∀s. Numeric s ->  ∀s. Numeric s -> Bool
  final case object BEqualList extends BuiltinFunction(3) // : ∀a. (a -> a -> Bool) -> List a -> List a -> Bool
  final case object BEqualContractId extends BuiltinFunction(2) // : ∀a. ContractId a -> ContractId a -> Bool
  final case object BEqual extends BuiltinFunction(2) // ∀a. a -> a -> Bool
  final case object BCoerceContractId extends BuiltinFunction(1) // : ∀a b. ContractId a -> ContractId b

  // Unstable Text Primitives
  final case object BTextToUpper extends BuiltinFunction(1) // Text → Text
  final case object BTextToLower extends BuiltinFunction(1) // : Text → Text
  final case object BTextSlice extends BuiltinFunction(3) // : Int64 → Int64 → Text → Text
  final case object BTextSliceIndex extends BuiltinFunction(2) // : Text → Text → Optional Int64
  final case object BTextContainsOnly extends BuiltinFunction(2) // : Text → Text → Bool
  final case object BTextReplicate extends BuiltinFunction(2) // : Int64 → Text → Text
  final case object BTextSplitOn extends BuiltinFunction(2) // : Text → Text → List Text
  final case object BTextIntercalate extends BuiltinFunction(2) // : Text → List Text → Text

  //
  // Update expressions
  //

  case class RetrieveByKey(templateId: TypeConName, key: Expr)

  sealed abstract class Update extends Product with Serializable

  final case class UpdatePure(t: Type, expr: Expr) extends Update
  final case class UpdateBlock(bindings: ImmArray[Binding], body: Expr) extends Update
  final case class UpdateCreate(templateId: TypeConName, arg: Expr) extends Update
  final case class UpdateFetch(templateId: TypeConName, contractId: Expr) extends Update
  final case class UpdateExercise(
      templateId: TypeConName,
      choice: ChoiceName,
      cidE: Expr,
      actorsE: Option[Expr],
      argE: Expr)
      extends Update
  case object UpdateGetTime extends Update
  final case class UpdateFetchByKey(rbk: RetrieveByKey) extends Update
  final case class UpdateLookupByKey(rbk: RetrieveByKey) extends Update
  final case class UpdateEmbedExpr(typ: Type, body: Expr) extends Update

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
  case class CaseAlt(pattern: CasePat, expr: Expr)

  //
  // Definitions
  //

  sealed abstract class Definition extends Product with Serializable

  final case class DTypeSyn(params: ImmArray[(TypeVarName, Kind)], typ: Type) extends Definition
  final case class DDataType(
      serializable: Boolean,
      params: ImmArray[(TypeVarName, Kind)],
      cons: DataCons)
      extends Definition
  final case class DValue(
      typ: Type,
      noPartyLiterals: Boolean,
      body: Expr,
      isTest: Boolean
  ) extends Definition

  // Data constructor in data type definition.
  sealed abstract class DataCons extends Product with Serializable
  final case class DataRecord(fields: ImmArray[(FieldName, Type)], optTemplate: Option[Template])
      extends DataCons
  final case class DataVariant(variants: ImmArray[(VariantConName, Type)]) extends DataCons
  final case class DataEnum(constructors: ImmArray[EnumConName]) extends DataCons

  case class TemplateKey(
      typ: Type,
      body: Expr,
      // function from key type to [Party]
      maintainers: Expr,
  )

  case class Template private (
      param: ExprVarName, // Binder for template argument.
      precond: Expr, // Template creation precondition.
      signatories: Expr, // Parties agreeing to the contract.
      agreementText: Expr, // Text the parties agree to.
      choices: Map[ChoiceName, TemplateChoice], // Choices available in the template.
      observers: Expr, // Observers of the contract.
      key: Option[TemplateKey]
  )

  object Template {

    def apply(
        param: ExprVarName,
        precond: Expr,
        signatories: Expr,
        agreementText: Expr,
        choices: Traversable[(ChoiceName, TemplateChoice)],
        observers: Expr,
        key: Option[TemplateKey]
    ): Template = {

      findDuplicate(choices).foreach { choiceName =>
        throw PackageError(s"collision on choice name $choiceName")
      }

      new Template(
        param,
        precond,
        signatories,
        agreementText,
        choices.toMap,
        observers,
        key,
      )
    }
  }

  case class TemplateChoice(
      name: ChoiceName, // Name of the choice.
      consuming: Boolean, // Flag indicating whether exercising the choice consumes the contract.
      controllers: Expr, // Parties that can exercise the choice.
      selfBinder: ExprVarName, // Self ContractId binder.
      argBinder: (Option[ExprVarName], Type), // Choice argument binder.
      returnType: Type, // Return type of the choice follow-up.
      update: Expr // The choice follow-up.
  )

  case class FeatureFlags(
      forbidPartyLiterals: Boolean // If set to true, party literals are not allowed to appear in daml-lf packages.
      /*
      These flags are present in DAML-LF, but our ecosystem does not support them anymore:
      dontDivulgeContractIdsInCreateArguments: Boolean, // If set to true, arguments to creates are not divulged.
      // Instead target contract id's of exercises are divulged
      // and fetches are authorized.
      dontDiscloseNonConsumingChoicesToObservers: Boolean // If set to true, exercise nodes of
      // non-consuming choices are only
      // disclosed to the signatories and
      // controllers of the target contract/choice
      // and not to the observers of the target contract.
   */
  )

  object FeatureFlags {
    val default = FeatureFlags(
      forbidPartyLiterals = false,
    )
  }

  //
  // Modules and packages
  //

  case class Module private (
      name: ModuleName,
      definitions: Map[DottedName, Definition],
      languageVersion: LanguageVersion,
      featureFlags: FeatureFlags
  )

  private def findDuplicate[Key, Value](xs: Traversable[(Key, Value)]) =
    xs.groupBy(_._1).collectFirst { case (key, values) if values.size > 1 => key }

  object Module {

    def apply(
        name: ModuleName,
        definitions: Traversable[(DottedName, Definition)],
        languageVersion: LanguageVersion,
        featureFlags: FeatureFlags
    ): Module =
      Module(name, definitions, List.empty, languageVersion, featureFlags)

    def apply(
        name: ModuleName,
        definitions: Traversable[(DottedName, Definition)],
        templates: Traversable[(DottedName, Template)],
        languageVersion: LanguageVersion,
        featureFlags: FeatureFlags
    ): Module = {

      findDuplicate(definitions).foreach { defName =>
        throw PackageError(s"Collision on definition name ${defName.toString}")
      }

      val defsMap = definitions.toMap

      findDuplicate(templates).foreach { templName =>
        throw PackageError(s"Collision on template name ${templName.toString}")
      }

      val updatedRecords = templates.map {
        case (templName, template) =>
          defsMap.get(templName) match {
            case Some(DDataType(serializable, params, DataRecord(fields, _))) =>
              templName -> DDataType(serializable, params, DataRecord(fields, Some(template)))
            case _ =>
              throw PackageError(
                s"Data type definition not found for template ${templName.toString}")
          }
      }

      new Module(name, defsMap ++ updatedRecords, languageVersion, featureFlags)
    }
  }

  case class Package(modules: Map[ModuleName, Module], directDeps: Set[PackageId]) {
    def lookupIdentifier(identifier: QualifiedName): Either[String, Definition] = {
      this.modules.get(identifier.module) match {
        case None =>
          Left(
            s"Could not find module ${identifier.module.toString} for name ${identifier.toString}")
        case Some(module) =>
          module.definitions.get(identifier.name) match {
            case None =>
              Left(
                s"Could not find name ${identifier.name.toString} in module ${identifier.module.toString}")
            case Some(defn) => Right(defn)
          }
      }
    }
  }

  object Package {

    def apply(modules: Traversable[Module], directDeps: Traversable[PackageId]): Package = {
      val modulesWithNames = modules.map(m => m.name -> m)
      findDuplicate(modulesWithNames).foreach { modName =>
        throw PackageError(s"Collision on module name ${modName.toString}")
      }
      Package(modulesWithNames.toMap, directDeps.toSet)
    }
  }

  val keyFieldName = Name.assertFromString("key")
  val valueFieldName = Name.assertFromString("value")
  val maintainersFieldName = Name.assertFromString("maintainers")
  val contractIdFieldName = Name.assertFromString("contractId")
  val contractFieldName = Name.assertFromString("contract")

  final case class PackageError(error: String) extends RuntimeException(error)

}
