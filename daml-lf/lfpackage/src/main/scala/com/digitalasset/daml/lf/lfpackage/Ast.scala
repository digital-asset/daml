// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.lfpackage

import com.digitalasset.daml.lf.archive.LanguageVersion
import com.digitalasset.daml.lf.archive.Reader.ParseError
import com.digitalasset.daml.lf.data.Decimal.Decimal
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data.{ImmArray, Time}

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
  type ExprVarName = String

  /* Type variable name. */
  type TypeVarName = String

  /* Reference to a field in a record or variant. */
  type FieldName = String

  /* Variant constructor name. */
  type VariantConName = String

  /* Binding in a let/update/scenario block. */
  case class Binding(binder: Option[String], typ: Type, bound: Expr)

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

  /** Tuple construction. */
  final case class ETupleCon(fields: ImmArray[(FieldName, Expr)]) extends Expr

  /** Tuple projection. */
  final case class ETupleProj(field: FieldName, tuple: Expr) extends Expr

  /** Non-destructive tuple update. */
  final case class ETupleUpd(field: FieldName, tuple: Expr, update: Expr) extends Expr

  /** Expression application. Function can be an abstraction or a builtin function. */
  final case class EApp(fun: Expr, arg: Expr) extends Expr

  /** Type application. */
  final case class ETyApp(expr: Expr, typ: Type) extends Expr

  /** Expression abstraction. */
  final case class EAbs(
      binder: (ExprVarName, Type),
      body: Expr,
      ref: Option[DefinitionRef[PackageId]] // The definition in which this abstraction is defined.
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

  /** Contract ids. Note that:
    *
    * * The only reason why we have these here is that we want to translate Ledger API commands
    *   to update expressions. Serialized DAML-LF programs never have these.
    * * Since we only care about Ledger API commands we only allow absolute contract ids, here
    *   represented as strings.
    *
    * Why not just parametrize the whole of Expr with ContractId, like we do to other structures?
    * It is too annoying to do so, what pushed me over the edge is that ImmArray[T] is invariant
    * in T, and thus it's not the case that `ImmArray[Expr[Nothing]] <: ImmArray[Expr[String]]`. We
    * Might want to revisit this in the future.
    */
  final case class EContractId(coId: String, tmplId: TypeConName) extends Expr

  /** Location annotations */
  final case class ELocation(loc: Location, expr: Expr) extends Expr

  final case class ENone(typ: Type) extends Expr

  final case class ESome(typ: Type, body: Expr) extends Expr

  //
  // Kinds
  //

  sealed abstract class Kind extends Product with Serializable {
    def pretty: String = Kind.prettyKind(this)
  }

  object Kind {

    def prettyKind(kind: Kind, needParens: Boolean = false): String = kind match {
      case KStar => "*"
      case KArrow(fun, arg) if needParens =>
        "(" + prettyKind(fun, true) + "->" + prettyKind(arg, false)
        ")"
      case KArrow(fun, arg) if needParens =>
        prettyKind(fun, true) + "->" + prettyKind(arg, false)
    }
  }

  /** Kind of a proper data type. */
  case object KStar extends Kind

  /** Kind of higher kinded type. */
  final case class KArrow(param: Kind, result: Kind) extends Kind

  //
  // Types
  //

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
        case TTuple(fields) =>
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

  /** Reference to a type constructor. */
  final case class TTyCon(tycon: TypeConName) extends Type

  /** Reference to builtin type. */
  final case class TBuiltin(bt: BuiltinType) extends Type

  /** Application of a type function to a type. */
  final case class TApp(tyfun: Type, arg: Type) extends Type

  /** Universally quantified type. */
  final case class TForall(binder: (TypeVarName, Kind), body: Type) extends Type

  /** Tuples */
  final case class TTuple private (sortedFields: ImmArray[(String, Type)]) extends Type

  object TTuple extends (ImmArray[(FieldName, Type)] => TTuple) {
    // should be dropped once the compiler sort fields.
    def apply(fields: ImmArray[(String, Type)]): TTuple =
      new TTuple(ImmArray(fields.toSeq.sortBy(_._1)))
  }

  sealed abstract class BuiltinType extends Product with Serializable

  case object BTInt64 extends BuiltinType
  case object BTDecimal extends BuiltinType
  case object BTText extends BuiltinType
  case object BTTimestamp extends BuiltinType
  case object BTParty extends BuiltinType
  case object BTUnit extends BuiltinType
  case object BTBool extends BuiltinType
  case object BTList extends BuiltinType
  case object BTOptional extends BuiltinType
  case object BTMap extends BuiltinType
  case object BTUpdate extends BuiltinType
  case object BTScenario extends BuiltinType
  case object BTDate extends BuiltinType
  case object BTContractId extends BuiltinType
  case object BTArrow extends BuiltinType

  //
  // Primitive literals
  //

  sealed abstract class PrimLit extends Equals with Product with Serializable {
    def value: Any
  }

  final case class PLInt64(override val value: Long) extends PrimLit
  final case class PLDecimal(override val value: Decimal) extends PrimLit
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

  final case object BTrace extends BuiltinFunction(2) // :: ∀a. Text -> a -> a

  // Decimal arithmetic
  final case object BAddDecimal extends BuiltinFunction(2) // ∷ Decimal → Decimal → Decimal
  final case object BSubDecimal extends BuiltinFunction(2) // ∷ Decimal → Decimal → Decimal
  final case object BMulDecimal extends BuiltinFunction(2) // ∷ Decimal → Decimal → Decimal
  final case object BDivDecimal extends BuiltinFunction(2) // ∷ Decimal → Decimal → Decimal
  final case object BRoundDecimal extends BuiltinFunction(2) // ∷ Integer → Decimal → Decimal

  // Int64 arithmetic
  final case object BAddInt64 extends BuiltinFunction(2) // ∷ Int64 → Int64 → Int64
  final case object BSubInt64 extends BuiltinFunction(2) // ∷ Int64 → Int64 → Int64
  final case object BMulInt64 extends BuiltinFunction(2) // ∷ Int64 → Int64 → Int64
  final case object BDivInt64 extends BuiltinFunction(2) // ∷ Int64 → Int64 → Int64
  final case object BModInt64 extends BuiltinFunction(2) // ∷ Int64 → Int64 → Int64
  final case object BExpInt64 extends BuiltinFunction(2) // ∷ Int64 → Int64 → Int64

  // Conversions
  final case object BInt64ToDecimal extends BuiltinFunction(1) // ∷ Int64 → Decimal
  final case object BDecimalToInt64 extends BuiltinFunction(1) // ∷ Decimal → Int64
  final case object BDateToUnixDays extends BuiltinFunction(1) // :: Date -> Int64
  final case object BUnixDaysToDate extends BuiltinFunction(1) // :: Int64 -> Date
  final case object BTimestampToUnixMicroseconds extends BuiltinFunction(1) // :: Timestamp -> Int64
  final case object BUnixMicrosecondsToTimestamp extends BuiltinFunction(1) // :: Int64 -> Timestamp

  // Folds
  final case object BFoldl extends BuiltinFunction(3) // ∷ ∀a b. (b → a → b) → b → List a → b
  final case object BFoldr extends BuiltinFunction(3) // ∷ ∀a b. (a → b → b) → b → List a → b

  // Maps
  final case object BMapEmpty extends BuiltinFunction(0) // :: ∀ a. Map a
  final case object BMapInsert extends BuiltinFunction(3) // :: ∀ a. Text -> a -> Map a -> Map a
  final case object BMapLookup extends BuiltinFunction(2) // :: ∀ a. Text -> Map a -> Optional a
  final case object BMapDelete extends BuiltinFunction(2) // :: ∀ a. Text -> Map a -> Map a
  final case object BMapToList extends BuiltinFunction(1) // :: ∀ a. Map a -> [Text]
  final case object BMapSize extends BuiltinFunction(1) // :: ∀ a. Map a -> Int64

  // Text functions
  final case object BExplodeText extends BuiltinFunction(1) // ∷ Text → List Char
  final case object BImplodeText extends BuiltinFunction(1) // :: List Text -> Text
  final case object BAppendText extends BuiltinFunction(2) // ∷ Text → Text → Text

  final case object BToTextInt64 extends BuiltinFunction(1) // ∷ Int64 → Text
  final case object BToTextDecimal extends BuiltinFunction(1) // ∷ Decimal → Text
  final case object BToTextText extends BuiltinFunction(1) // ∷ Text → Text
  final case object BToTextTimestamp extends BuiltinFunction(1) // ∷ Timestamp → Text
  final case object BToTextParty extends BuiltinFunction(1) // ∷ Party → Text
  final case object BToTextDate extends BuiltinFunction(1) // :: Date -> Text
  final case object BToQuotedTextParty extends BuiltinFunction(1) // :: Party -> Text
  final case object BFromTextParty extends BuiltinFunction(1) // :: Text -> Optional Party

  final case object BSHA256Text extends BuiltinFunction(arity = 1) // :: Text -> Text

  // Errors
  final case object BError extends BuiltinFunction(1) // ∷ ∀a. Text → a

  // Comparisons
  final case object BLessInt64 extends BuiltinFunction(2) // ∷ Int64 → Int64 → Bool
  final case object BLessDecimal extends BuiltinFunction(2) // ∷ Decimal → Decimal → Bool
  final case object BLessText extends BuiltinFunction(2) // ∷ Text → Text → Bool
  final case object BLessTimestamp extends BuiltinFunction(2) // ∷ Timestamp → Timestamp → Bool
  final case object BLessDate extends BuiltinFunction(2) // ∷ Date → Date → Bool
  final case object BLessParty extends BuiltinFunction(2) // ∷ Party → Party → Bool

  final case object BLessEqInt64 extends BuiltinFunction(2) // ∷ Int64 → Int64 → Bool
  final case object BLessEqDecimal extends BuiltinFunction(2) // ∷ Decimal → Decimal → Bool
  final case object BLessEqText extends BuiltinFunction(2) // ∷ Text → Text → Bool
  final case object BLessEqTimestamp extends BuiltinFunction(2) // ∷ Timestamp → Timestamp → Bool
  final case object BLessEqDate extends BuiltinFunction(2) // ∷ Date → Date → Bool
  final case object BLessEqParty extends BuiltinFunction(2) // ∷ Party → Party → Bool

  final case object BGreaterInt64 extends BuiltinFunction(2) // ∷ Int64 → Int64 → Bool
  final case object BGreaterDecimal extends BuiltinFunction(2) // ∷ Decimal → Decimal → Bool
  final case object BGreaterText extends BuiltinFunction(2) // ∷ Text → Text → Bool
  final case object BGreaterTimestamp extends BuiltinFunction(2) // ∷ Timestamp → Timestamp → Bool
  final case object BGreaterDate extends BuiltinFunction(2) // ∷ Date → Date → Bool
  final case object BGreaterParty extends BuiltinFunction(2) // ∷ Party → Party → Bool

  final case object BGreaterEqInt64 extends BuiltinFunction(2) // ∷ Int64 → Int64 → Bool
  final case object BGreaterEqDecimal extends BuiltinFunction(2) // ∷ Decimal → Decimal → Bool
  final case object BGreaterEqText extends BuiltinFunction(2) // ∷ Text → Text → Bool
  final case object BGreaterEqTimestamp extends BuiltinFunction(2) // ∷ Timestamp → Timestamp → Bool
  final case object BGreaterEqDate extends BuiltinFunction(2) // ∷ Date → Date → Bool
  final case object BGreaterEqParty extends BuiltinFunction(2) // ∷ Party → Party → Bool

  final case object BEqualInt64 extends BuiltinFunction(2) // :: Int64 -> Int64 -> Bool
  final case object BEqualDecimal extends BuiltinFunction(2) // :: Decimal -> Decimal -> Bool
  final case object BEqualText extends BuiltinFunction(2) // :: Text -> Text -> Bool
  final case object BEqualTimestamp extends BuiltinFunction(2) // :: Timestamp -> Timestamp -> Bool
  final case object BEqualDate extends BuiltinFunction(2) // :: Date -> Date -> Bool
  final case object BEqualParty extends BuiltinFunction(2) // :: Party -> Party -> Bool
  final case object BEqualBool extends BuiltinFunction(2) // :: Bool -> Bool -> Bool
  final case object BEqualList extends BuiltinFunction(3) // :: ∀a. (a -> a -> Bool) -> List a -> List a -> Bool
  final case object BEqualContractId extends BuiltinFunction(2) // :: ∀a. ContractId a -> ContractId a -> Bool

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
      actorsE: Expr,
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
        throw ParseError(s"collision on choice name $choiceName")
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
      forbidPartyLiterals: Boolean, // If set to true, party literals are not allowed to appear in daml-lf packages.
      dontDivulgeContractIdsInCreateArguments: Boolean, // If set to true, arguments to creates are not divulged.
      // Instead target contract id's of exercises are divulged
      // and fetches are authorized.
      dontDiscloseNonConsumingChoicesToObservers: Boolean // If set to true, exercise nodes of
      // non-consuming choices are only
      // disclosed to the signatories and
      // controllers of the target contract/choice
      // and not to the observers of the target contract.
  )

  object FeatureFlags {
    val default = FeatureFlags(
      forbidPartyLiterals = false,
      dontDivulgeContractIdsInCreateArguments = false,
      dontDiscloseNonConsumingChoicesToObservers = false)
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
        throw ParseError(s"Collision on definition name ${defName.toString}")
      }

      val defsMap = definitions.toMap

      findDuplicate(templates).foreach { templName =>
        throw ParseError(s"Collision on template name ${templName.toString}")
      }

      val updatedRecords = templates.map {
        case (templName, template) =>
          defsMap.get(templName) match {
            case Some(DDataType(serializable, params, DataRecord(fields, _))) =>
              templName -> DDataType(serializable, params, DataRecord(fields, Some(template)))
            case _ =>
              throw ParseError(s"Data type definition not found for template ${templName.toString}")
          }
      }

      new Module(name, defsMap ++ updatedRecords, languageVersion, featureFlags)
    }
  }

  case class Package(modules: Map[ModuleName, Module]) {
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

    def apply(modules: Traversable[Module]): Package = {
      val modulesWithNames = modules.map(m => m.name -> m)
      findDuplicate(modulesWithNames).foreach { modName =>
        throw ParseError(s"Collision on module name ${modName.toString}")
      }
      Package(modulesWithNames.toMap)
    }
  }

}
