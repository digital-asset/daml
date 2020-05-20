// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.Ref._
import com.daml.lf.data.{ImmArray, Numeric, Time}
import com.daml.lf.language.Ast._
import com.daml.lf.speedy.SBuiltin._
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SValue._
import com.daml.lf.validation.{EUnknownDefinition, LEPackage, Validation, ValidationError}
import org.slf4j.LoggerFactory

import scala.annotation.tailrec

/** Compiles LF expressions into Speedy expressions.
  * This includes:
  *  - Writing variable references into de Bruijn indices.
  *  - Closure conversion: EAbs turns into SEMakeClo, which creates a closure by copying free variables into a closure object.
  *   - Rewriting of update and scenario actions into applications of builtin functions that take an "effect" token.
  *
  * If you're working on the code here note that there's
  * a pretty-printer defined in lf.speedy.Pretty, which
  * is exposed via ':speedy' command in the REPL.
  */
private[lf] object Compiler {

  case class CompilationError(error: String) extends RuntimeException(error, null, true, false)
  case class PackageNotFound(pkgId: PackageId)
      extends RuntimeException(s"Package not found $pkgId", null, true, false)

  // NOTE(MH): We make this an enum type to avoid boolean blindness. In fact,
  // other profiling modes like "only trace the ledger interactions" might also
  // be useful.
  sealed abstract class ProfilingMode extends Product with Serializable
  case object NoProfile extends ProfilingMode
  case object FullProfile extends ProfilingMode

  private val SEGetTime = SEBuiltin(SBGetTime)

  private def SBCompareNumeric(b: SBuiltin) =
    SEAbs(3, SEApp(SEBuiltin(b), Array(SEVar(2), SEVar(1))))
  private val SBLessNumeric = SBCompareNumeric(SBLess)
  private val SBLessEqNumeric = SBCompareNumeric(SBLessEq)
  private val SBGreaterNumeric = SBCompareNumeric(SBGreater)
  private val SBGreaterEqNumeric = SBCompareNumeric(SBGreaterEq)
  private val SBEqualNumeric = SBCompareNumeric(SBEqual)

  private val SEDropSecondArgument = SEAbs(2, SEVar(2))
  private val SEUpdatePureUnit = SEAbs(1, SEValue.Unit)
  private val SEAppBoundHead = SEApp(SEVar(2), Array(SEVar(1)))

  private val SENat: Numeric.Scale => Some[SEValue] =
    Numeric.Scale.values.map(n => Some(SEValue(STNat(n))))

  /** Validates and Compiles all the definitions in the packages provided. Returns them in a Map.
    *
    * The packages do not need to be in any specific order, as long as they and all the packages
    * they transitively reference are in the [[packages]] in the compiler.
    */
  def compilePackages(
      packages: Map[PackageId, Package],
      profiling: ProfilingMode,
      validation: Boolean = true,
  ): Either[String, Map[SDefinitionRef, SExpr]] = {
    val compiler = Compiler(packages, profiling)
    try {
      Right(
        packages.keys.foldLeft(Map.empty[SDefinitionRef, SExpr])(
          _ ++ compiler.unsafeCompilePackage(_, validation))
      )
    } catch {
      case CompilationError(msg) => Left(s"Compilation Error: $msg")
      case PackageNotFound(pkgId) => Left(s"Package not found $pkgId")
      case e: ValidationError => Left(e.pretty)
    }
  }

}

private[lf] final case class Compiler(
    packages: PackageId PartialFunction Package,
    profiling: Compiler.ProfilingMode) {

  import Compiler._

  private val logger = LoggerFactory.getLogger(this.getClass)

  private abstract class VarRef { def name: Name }
  // corresponds to DAML-LF expression variable.
  private case class EVarRef(name: ExprVarName) extends VarRef
  // corresponds to DAML-LF type variable.
  private case class TVarRef(name: TypeVarName) extends VarRef

  private case class Env(position: Int = 0, varIndices: List[(VarRef, Option[Int])] = List.empty) {
    def incrPos: Env = copy(position = position + 1)
    def addExprVar(name: Option[ExprVarName], index: Int): Env =
      name.fold(this)(n => copy(varIndices = (EVarRef(n), Some(index)) :: varIndices))
    def addExprVar(name: ExprVarName, index: Int): Env =
      addExprVar(Some(name), index)
    def addExprVar(name: Option[ExprVarName]): Env =
      incrPos.addExprVar(name, position)
    def addExprVar(name: ExprVarName): Env =
      addExprVar(Some(name))
    def addTypeVar(name: TypeVarName): Env =
      incrPos.copy(varIndices = (TVarRef(name), Some(position)) :: varIndices)
    def hideTypeVar(name: TypeVarName): Env =
      copy(varIndices = (TVarRef(name), None) :: varIndices)

    def vars: List[VarRef] = varIndices.map(_._1)

    private def lookUpVar(varRef: VarRef): Option[Int] =
      varIndices
        .find(_._1 == varRef)
        .flatMap(_._2)
        // The de Bruijin index for the binder, e.g.
        // the distance to the binder. The closest binder
        // is at distance 1.
        .map(position - _)

    def lookUpExprVar(name: ExprVarName): Int =
      lookUpVar(EVarRef(name))
        .getOrElse(
          throw CompilationError(s"Unknown variable: $name. Known: ${env.vars.mkString(",")}"))

    def lookUpTypeVar(name: TypeVarName): Option[Int] =
      lookUpVar(TVarRef(name))

  }

  /** Environment mapping names into stack positions */
  private var env = Env()

  private val withLabel: (AnyRef, SExpr) => SExpr =
    profiling match {
      case NoProfile => { (_, expr) =>
        expr
      }
      case FullProfile => { (label, expr) =>
        expr match {
          case SELabelClosure(_, expr1) => SELabelClosure(label, expr1)
          case _ => SELabelClosure(label, expr)
        }
      }
    }

  private def withOptLabel(optLabel: Option[AnyRef], expr: SExpr): SExpr =
    optLabel match {
      case Some(label) => withLabel(label, expr)
      case None => expr
    }

  @throws[PackageNotFound]
  @throws[CompilationError]
  def unsafeCompile(cmds: ImmArray[Command]): SExpr =
    validate(closureConvert(Map.empty, translateCommands(cmds)))

  @throws[PackageNotFound]
  @throws[CompilationError]
  def unsafeCompile(expr: Expr): SExpr =
    validate(closureConvert(Map.empty, translate(expr)))

  @throws[PackageNotFound]
  @throws[CompilationError]
  def unsafeClosureConvert(sexpr: SExpr): SExpr =
    validate(closureConvert(Map.empty, sexpr))

  @throws[PackageNotFound]
  @throws[CompilationError]
  def unsafeCompileDefn(
      identifier: Identifier,
      defn: Definition,
  ): List[(SDefinitionRef, SExpr)] =
    defn match {
      case DValue(_, _, body, _) =>
        val ref = LfDefRef(identifier)
        List(ref -> withLabel(ref, unsafeCompile(body)))

      case DDataType(_, _, DataRecord(_, Some(tmpl))) =>
        // Compile choices into top-level definitions that exercise
        // the choice.
        tmpl.choices.toList.map {
          case (cname, choice) =>
            ChoiceDefRef(identifier, cname) ->
              compileChoice(identifier, tmpl, cname, choice)
        }

      case _ =>
        List()
    }

  /** Validates and compiles all the definitions in the package provided.
    *
    * Fails with [[PackageNotFound]] if the package or any of the packages it refers
    * to are not in the [[packages]].
    *
    * @throws ValidationError if the package does not pass validations.
    */
  @throws[PackageNotFound]
  @throws[CompilationError]
  @throws[ValidationError]
  def unsafeCompilePackage(
      pkgId: PackageId,
      validation: Boolean = true,
  ): Iterable[(SDefinitionRef, SExpr)] = {
    logger.trace(s"compilePackage: Compiling $pkgId...")

    val t0 = Time.Timestamp.now()

    if (validation)
      Validation.checkPackage(packages, pkgId).left.foreach {
        case EUnknownDefinition(_, LEPackage(pkgId_)) =>
          logger.trace(s"compilePackage: Missing $pkgId_, requesting it...")
          throw PackageNotFound(pkgId_)
        case e =>
          throw e
      }

    val t1 = Time.Timestamp.now()

    val defns = for {
      module <- lookupPackage(pkgId).modules.values
      defnWithId <- module.definitions
      (defnId, defn) = defnWithId
      fullId = Identifier(pkgId, QualifiedName(module.name, defnId))
      exprWithId <- unsafeCompileDefn(fullId, defn)
    } yield exprWithId
    val t2 = Time.Timestamp.now()

    logger.trace(
      s"compilePackage: $pkgId ready, typecheck=${(t1.micros - t0.micros) / 1000}ms, compile=${(t2.micros - t1.micros) / 1000}ms",
    )

    defns
  }

  private def patternNArgs(pat: SCasePat): Int = pat match {
    case _: SCPEnum | _: SCPPrimCon | SCPNil | SCPDefault | SCPNone => 0
    case _: SCPVariant | SCPSome => 1
    case SCPCons => 2
  }

  private def translate(expr0: Expr): SExpr =
    expr0 match {
      case EVar(name) => SEVar(env.lookUpExprVar(name))
      case EVal(ref) => SEVal(LfDefRef(ref))
      case EBuiltin(bf) =>
        bf match {
          case BFoldl =>
            val ref = SEBuiltinRecursiveDefinition.FoldL
            withLabel(ref, ref)
          case BFoldr =>
            val ref = SEBuiltinRecursiveDefinition.FoldR
            withLabel(ref, ref)
          case BEqualList =>
            val ref = SEBuiltinRecursiveDefinition.EqualList
            withLabel(ref, ref)
          case BCoerceContractId => SEAbs.identity
          // Numeric Comparisons
          case BLessNumeric => SBLessNumeric
          case BLessEqNumeric => SBLessEqNumeric
          case BGreaterNumeric => SBGreaterNumeric
          case BGreaterEqNumeric => SBGreaterEqNumeric
          case BEqualNumeric => SBEqualNumeric

          case BTextMapEmpty => SEValue.EmptyMap
          case BGenMapEmpty => SEValue.EmptyGenMap
          case _ =>
            SEBuiltin(bf match {
              case BTrace => SBTrace

              // Decimal arithmetic
              case BAddNumeric => SBAddNumeric
              case BSubNumeric => SBSubNumeric
              case BMulNumeric => SBMulNumeric
              case BDivNumeric => SBDivNumeric
              case BRoundNumeric => SBRoundNumeric
              case BCastNumeric => SBCastNumeric
              case BShiftNumeric => SBShiftNumeric

              // Int64 arithmetic
              case BAddInt64 => SBAddInt64
              case BSubInt64 => SBSubInt64
              case BMulInt64 => SBMulInt64
              case BModInt64 => SBModInt64
              case BDivInt64 => SBDivInt64
              case BExpInt64 => SBExpInt64

              // Conversions
              case BInt64ToNumeric => SBInt64ToNumeric
              case BNumericToInt64 => SBNumericToInt64
              case BDateToUnixDays => SBDateToUnixDays
              case BUnixDaysToDate => SBUnixDaysToDate
              case BTimestampToUnixMicroseconds => SBTimestampToUnixMicroseconds
              case BUnixMicrosecondsToTimestamp => SBUnixMicrosecondsToTimestamp

              // Text functions
              case BExplodeText => SBExplodeText
              case BImplodeText => SBImplodeText
              case BAppendText => SBAppendText

              case BToTextInt64 => SBToText
              case BToTextNumeric => SBToTextNumeric
              case BToTextText => SBToText
              case BToTextTimestamp => SBToText
              case BToTextParty => SBToText
              case BToTextDate => SBToText
              case BToQuotedTextParty => SBToQuotedTextParty
              case BToTextCodePoints => SBToTextCodePoints
              case BFromTextParty => SBFromTextParty
              case BFromTextInt64 => SBFromTextInt64
              case BFromTextNumeric => SBFromTextNumeric
              case BFromTextCodePoints => SBFromTextCodePoints

              case BSHA256Text => SBSHA256Text

              // Errors
              case BError => SBError

              // Comparison
              case BEqualContractId => SBEqual
              case BEqual => SBEqual
              case BLess => SBLess
              case BLessEq => SBLessEq
              case BGreater => SBGreater
              case BGreaterEq => SBGreaterEq

              // TextMap

              case BTextMapInsert => SBTextMapInsert
              case BTextMapLookup => SBTextMapLookup
              case BTextMapDelete => SBTextMapDelete
              case BTextMapToList => SBTextMapToList
              case BTextMapSize => SBTextMapSize

              // GenMap

              case BGenMapInsert => SBGenMapInsert
              case BGenMapLookup => SBGenMapLookup
              case BGenMapDelete => SBGenMapDelete
              case BGenMapKeys => SBGenMapKeys
              case BGenMapValues => SBGenMapValues
              case BGenMapSize => SBGenMapSize

              // Unstable Text Primitives
              case BTextToUpper => SBTextToUpper
              case BTextToLower => SBTextToLower
              case BTextSlice => SBTextSlice
              case BTextSliceIndex => SBTextSliceIndex
              case BTextContainsOnly => SBTextContainsOnly
              case BTextReplicate => SBTextReplicate
              case BTextSplitOn => SBTextSplitOn
              case BTextIntercalate => SBTextIntercalate

              // Implemented using normal SExpr
              case BFoldl | BFoldr | BCoerceContractId | BEqual | BEqualList | BLessEq | BLess |
                  BGreaterEq | BGreater | BLessNumeric | BLessEqNumeric | BGreaterNumeric |
                  BGreaterEqNumeric | BEqualNumeric | BTextMapEmpty | BGenMapEmpty =>
                throw CompilationError(s"unexpected $bf")
            })
        }

      case EPrimCon(con) =>
        con match {
          case PCTrue => SEValue.True
          case PCFalse => SEValue.False
          case PCUnit => SEValue.Unit
        }
      case EPrimLit(lit) =>
        SEValue(lit match {
          case PLInt64(i) => SInt64(i)
          case PLNumeric(d) => SNumeric(d)
          case PLText(t) => SText(t)
          case PLTimestamp(ts) => STimestamp(ts)
          case PLParty(p) => SParty(p)
          case PLDate(d) => SDate(d)
        })

      case EAbs(_, _, _) | ETyAbs(_, _) =>
        withEnv { _ =>
          translateAbss(expr0)
        }

      case EApp(_, _) | ETyApp(_, _) =>
        translateApps(expr0)

      case ERecCon(tApp, fields) =>
        if (fields.isEmpty)
          SEBuiltin(SBRecCon(tApp.tycon, Name.Array.empty))
        else {
          SEApp(
            SEBuiltin(SBRecCon(tApp.tycon, Name.Array(fields.map(_._1).toSeq: _*))),
            fields.iterator.map(f => translate(f._2)).toArray,
          )
        }

      case ERecProj(tapp, field, record) =>
        SBRecProj(tapp.tycon, lookupRecordIndex(tapp, field))(
          translate(record),
        )

      case ERecUpd(tapp, field, record, update) =>
        SBRecUpd(tapp.tycon, lookupRecordIndex(tapp, field))(
          translate(record),
          translate(update),
        )

      case EStructCon(fields) =>
        SEApp(SEBuiltin(SBStructCon(Name.Array(fields.map(_._1).toSeq: _*))), fields.iterator.map {
          case (_, e) => translate(e)
        }.toArray)

      case EStructProj(field, struct) =>
        SBStructProj(field)(translate(struct))

      case EStructUpd(field, struct, update) =>
        SBStructUpd(field)(translate(struct), translate(update))

      case ECase(scrut, alts) =>
        SECase(
          translate(scrut),
          alts.iterator.map {
            case CaseAlt(pat, expr) =>
              pat match {
                case CPVariant(tycon, variant, binder) =>
                  val variantDef = lookupVariantDefinition(tycon).getOrElse(
                    throw CompilationError(s"variant $tycon not found"))
                  withBinders(binder) { _ =>
                    SCaseAlt(
                      SCPVariant(tycon, variant, variantDef.constructorRank(variant)),
                      translate(expr),
                    )
                  }

                case CPEnum(tycon, constructor) =>
                  val enumDef = lookupEnumDefinition(tycon).getOrElse(
                    throw CompilationError(s"enum $tycon not found"))
                  SCaseAlt(
                    SCPEnum(tycon, constructor, enumDef.constructorRank(constructor)),
                    translate(expr),
                  )

                case CPNil =>
                  SCaseAlt(SCPNil, translate(expr))

                case CPCons(head, tail) =>
                  withBinders(head, tail) { _ =>
                    SCaseAlt(SCPCons, translate(expr))
                  }

                case CPPrimCon(pc) =>
                  SCaseAlt(SCPPrimCon(pc), translate(expr))

                case CPNone =>
                  SCaseAlt(SCPNone, translate(expr))

                case CPSome(body) =>
                  withBinders(body) { _ =>
                    SCaseAlt(SCPSome, translate(expr))
                  }

                case CPDefault =>
                  SCaseAlt(SCPDefault, translate(expr))
              }
          }.toArray,
        )

      case ENil(_) => SEValue.EmptyList
      case ECons(_, front, tail) =>
        // TODO(JM): Consider emitting SEValue(SList(...)) for
        // constant lists?
        SEApp(
          SEBuiltin(SBConsMany(front.length)),
          front.iterator.map(translate).toArray :+ translate(tail),
        )

      case ENone(_) => SEValue.None

      case ESome(_, body) =>
        SEApp(
          SEBuiltin(SBSome),
          Array(translate(body)),
        )

      case EEnumCon(tyCon, constructor) =>
        val enumDef =
          lookupEnumDefinition(tyCon).getOrElse(throw CompilationError(s"enum $tyCon not found"))
        SEValue(SEnum(tyCon, constructor, enumDef.constructorRank(constructor)))

      case EVariantCon(tapp, variant, arg) =>
        val variantDef = lookupVariantDefinition(tapp.tycon)
          .getOrElse(throw CompilationError(s"variant ${tapp.tycon} not found"))
        SBVariantCon(tapp.tycon, variant, variantDef.constructorRank(variant))(translate(arg))

      case let: ELet =>
        val (bindings, body) = collectLets(let)
        withEnv { _ =>
          SELet(
            bindings.map {
              case Binding(optBinder, _, bound) =>
                val bound2 = withOptLabel(optBinder, translate(bound))
                env = env.addExprVar(optBinder)
                bound2
            }.toArray,
            translate(body),
          )
        }

      case EUpdate(upd) =>
        upd match {
          case UpdatePure(_, e) =>
            translatePure(e)

          case UpdateBlock(bindings, body) =>
            translateBlock(bindings, body)

          case UpdateFetch(tmplId, coidE) =>
            // FIXME(JM): Lift to top-level?
            // Translates 'fetch <coid>' into
            // let coid = <coidE>
            // in \token ->
            //   let arg = $fetch coid token
            //       _ = $insertFetch coid <signatories> <observers>
            //   in arg
            val coid = translate(coidE)
            compileFetch(tmplId, coid)

          case UpdateEmbedExpr(_, e) =>
            translateEmbedExpr(e)

          case UpdateCreate(tmplId, arg) =>
            // FIXME(JM): Lift to top-level?
            // Translates 'create Foo with <params>' into:
            // let arg = <params>
            // let key = if (we have a key definition in the template) {
            //   let keyBody = <key>
            //   in Some {key: keyBody, maintainers: <key maintainers> keyBody}
            // } else {
            //   None
            // }
            // in \token ->
            //   $create arg <precond> <agreement text> <signatories> <observers> <token> <key>
            compileCreate(tmplId, translate(arg))

          case UpdateExercise(tmplId, chId, cidE, actorsE, argE) =>
            compileExercise(
              tmplId = tmplId,
              contractId = translate(cidE),
              choiceId = chId,
              optActors = actorsE.map(translate),
              byKey = false,
              argument = translate(argE),
            )

          case UpdateGetTime =>
            SEGetTime

          case UpdateLookupByKey(retrieveByKey) =>
            // Translates 'lookupByKey Foo <key>' into:
            // let keyWithMaintainers = {key: <key>, maintainers: <key maintainers> <key>}
            // in \token ->
            //    let mbContractId = $lookupKey keyWithMaintainers
            //        _ = $insertLookup Foo keyWithMaintainers
            //    in mbContractId
            compileLookupByKey(retrieveByKey.templateId, translate(retrieveByKey.key))

          case UpdateFetchByKey(retrieveByKey) =>
            // Translates 'fetchByKey Foo <key>' into:
            // let keyWithMaintainers = {key: <key>, maintainers: <key maintainers> <key>}
            // in \token ->
            //    let coid = $fetchKey keyWithMaintainers token
            //        contract = $fetch coid token
            //        _ = $insertFetch coid <signatories> <observers> Some(keyWithMaintainers)
            //    in { contractId: ContractId Foo, contract: Foo }
            val template = lookupTemplate(retrieveByKey.templateId)
            withEnv { _ =>
              val key = translate(retrieveByKey.key)
              val keyTemplate = template.key.getOrElse(
                throw CompilationError(
                  s"Expecting to find key for template ${retrieveByKey.templateId}, but couldn't",
                ),
              )
              SELet(encodeKeyWithMaintainers(key, keyTemplate)) in {
                env = env.incrPos // key with maintainers
                withLabel(
                  s"<fetch_by_key ${retrieveByKey.templateId}",
                  SEAbs(1) {
                    env = env.incrPos // token
                    env = env.addExprVar(template.param)
                    // TODO should we evaluate this before we even construct
                    // the update expression? this might be better for the user
                    val signatories = translate(template.signatories)
                    val observers = translate(template.observers)
                    SELet(
                      SBUFetchKey(retrieveByKey.templateId)(
                        SEVar(2), // key with maintainers
                        SEVar(1) // token
                      ),
                      SBUFetch(retrieveByKey.templateId)(
                        SEVar(1), /* coid */
                        SEVar(2) /* token */
                      ),
                      SBUInsertFetchNode(retrieveByKey.templateId, byKey = true)(
                        SEVar(2), // coid
                        signatories,
                        observers,
                        SEApp(SEBuiltin(SBSome), Array(SEVar(4))),
                        SEVar(3) // token
                      ),
                    ) in SBStructCon(Name.Array(contractIdFieldName, contractFieldName))(
                      SEVar(3), // contract id
                      SEVar(2) // contract
                    )
                  }
                )
              }
            }
        }

      case ELocation(loc, EScenario(scen)) =>
        SELocation(loc, translateScenario(scen, Some(loc)))

      case EScenario(scen) =>
        translateScenario(scen, None)

      case ELocation(loc, e) =>
        SELocation(loc, translate(e))

      case EToAny(ty, e) =>
        SEApp(SEBuiltin(SBToAny(ty)), Array(translate(e)))

      case EFromAny(ty, e) =>
        SEApp(SEBuiltin(SBFromAny(ty)), Array(translate(e)))

      case ETypeRep(typ) =>
        SEValue(STypeRep(typ))
    }

  @tailrec
  private def translateAbss(expr0: Expr, arity: Int = 0): SExpr =
    expr0 match {
      case EAbs((binder, typ @ _), body, ref @ _) =>
        env = env.addExprVar(binder)
        translateAbss(body, arity + 1)
      case ETyAbs((binder, KNat), body) =>
        env = env.addTypeVar(binder)
        translateAbss(body, arity + 1)
      case ETyAbs((binder, _), body) =>
        env = env.hideTypeVar(binder)
        translateAbss(body, arity)
      case _ if arity == 0 =>
        translate(expr0)
      case _ =>
        withLabel(AnonymousClosure, SEAbs(arity, translate(expr0)))
    }

  @tailrec
  private def translateApps(expr0: Expr, args: List[SExpr] = List.empty): SExpr =
    expr0 match {
      case EApp(fun, arg) =>
        translateApps(fun, translate(arg) :: args)
      case ETyApp(fun, arg) =>
        translateApps(fun, translateType(arg).fold(args)(_ :: args))
      case _ if args.isEmpty =>
        translate(expr0)
      case _ =>
        SEApp(translate(expr0), args.toArray)
    }

  private def translateType(typ: Type): Option[SExpr] =
    typ match {
      case TNat(n) => SENat(n)
      case TVar(name) => env.lookUpTypeVar(name).map(SEVar)
      case _ => None
    }

  private def translateScenario(scen: Scenario, optLoc: Option[Location]): SExpr =
    scen match {
      case ScenarioPure(_, e) =>
        translatePure(e)

      case ScenarioBlock(bindings, body) =>
        translateBlock(bindings, body)

      case ScenarioCommit(partyE, updateE, _retType @ _) =>
        // let party = <partyE>
        //     update = <updateE>
        // in \token ->
        //   let _ = $beginCommit party token
        //       r = update token
        //   in $endCommit[mustFail = false] r token
        withEnv { _ =>
          val party = translate(partyE)
          env = env.incrPos // party
          val update = translate(updateE)
          env = env.incrPos // update
          env = env.incrPos // $beginCommit
          SELet(party, update) in
            withLabel(
              "<submit>",
              SEAbs(1) {
                SELet(
                  // stack: <party> <update> <token>
                  SBSBeginCommit(optLoc)(SEVar(3), SEVar(1)),
                  // stack: <party> <update> <token> ()
                  SEApp(SEVar(3), Array(SEVar(2))),
                  // stack: <party> <update> <token> () result
                ) in
                  SBSEndCommit(false)(SEVar(1), SEVar(3))
              }
            )
        }

      case ScenarioMustFailAt(partyE, updateE, _retType @ _) =>
        // \token ->
        //   let _ = $beginCommit <party> token
        //       r = $catch (<updateE> token) true false
        //   in $endCommit[mustFail = true] r token
        withEnv { _ =>
          env = env.incrPos // token
          val party = translate(partyE)
          env = env.incrPos // $beginCommit
          val update = translate(updateE)
          withLabel(
            "<submit_must_fail>",
            SEAbs(1) {
              SELet(
                SBSBeginCommit(optLoc)(party, SEVar(1)),
                SECatch(SEApp(update, Array(SEVar(2))), SEValue.True, SEValue.False),
              ) in SBSEndCommit(true)(SEVar(1), SEVar(3))
            }
          )
        }

      case ScenarioGetTime =>
        SEGetTime

      case ScenarioGetParty(e) =>
        withEnv { _ =>
          env = env.incrPos // token
          withLabel(
            "<get_party>",
            SEAbs(1) {
              SBSGetParty(translate(e), SEVar(1))
            }
          )
        }

      case ScenarioPass(relTimeE) =>
        withEnv { _ =>
          env = env.incrPos // token
          withLabel(
            "<pass>",
            SEAbs(1) {
              SBSPass(translate(relTimeE), SEVar(1))
            }
          )
        }

      case ScenarioEmbedExpr(_, e) =>
        translateEmbedExpr(e)
    }
  private def translateEmbedExpr(expr: Expr): SExpr = {
    withEnv { _ =>
      env = env.incrPos // token
      // EmbedExpr's get wrapped into an extra layer of abstraction
      // to delay evaluation.
      // e.g.
      // embed (error "foo") => \token -> error "foo"
      SEAbs(1) {
        SEApp(translate(expr), Array(SEVar(1)))
      }
    }
  }

  private def translatePure(body: Expr): SExpr =
    // pure <E>
    // =>
    // ((\x token -> x) <E>)
    SEApp(SEDropSecondArgument, Array(translate(body)))

  private def translateBlock(bindings: ImmArray[Binding], body: Expr): SExpr = {
    // do
    //   x <- f
    //   y <- g x
    //   z x y
    // =>
    // let f' = f
    // in \token ->
    //   let x = f' token
    //       y = g x token
    //   in z x y token
    withEnv { _ =>
      val boundHead = translate(bindings.head.bound)
      env = env.incrPos // evaluated body of first binding

      val tokenPosition = env.position
      env = env.incrPos // token

      // add the first binding into the environment
      val appBoundHead = SEApp(SEVar(2), Array(SEVar(1)))
      env = env.addExprVar(bindings.head.binder)

      // and then the rest
      val boundTail = bindings.tail.toList.map {
        case Binding(optB, _, bound) =>
          val sbound = translate(bound)
          val tokenIndex = env.position - tokenPosition
          env = env.addExprVar(optB)
          SEApp(sbound, Array(SEVar(tokenIndex)))
      }
      val allBounds = appBoundHead +: boundTail
      SELet(boundHead) in
        SEAbs(1) {
          SELet(allBounds: _*) in
            SEApp(translate(body), Array(SEVar(env.position - tokenPosition)))
        }
    }
  }

  private def encodeKeyWithMaintainers(key: SExpr, tmplKey: TemplateKey): SExpr =
    SELet(key) in
      SBStructCon(Name.Array(keyFieldName, maintainersFieldName))(
        SEVar(1), // key
        SEApp(translate(tmplKey.maintainers), Array(SEVar(1) /* key */ )),
      )

  private def translateKeyWithMaintainers(tmplKey: TemplateKey): SExpr =
    encodeKeyWithMaintainers(translate(tmplKey.body), tmplKey)

  /** Compile a choice into a top-level function for exercising that choice */
  private def compileChoice(
      tmplId: TypeConName,
      tmpl: Template,
      cname: ChoiceName,
      choice: TemplateChoice): SExpr =
    // Compiles a choice into:
    // SomeTemplate$SomeChoice = \<byKey flag> <actors> <cid> <choice arg> <token> ->
    //   let targ = fetch <cid>
    //       _ = $beginExercise[tmplId, chId] <choice arg> <cid> <actors> <byKey flag> sigs obs ctrls mbKey <token>
    //       result = <updateE>
    //       _ = $endExercise[tmplId]
    //   in result
    validate(
      closureConvert(
        Map.empty,
        withEnv { _ =>
          env = env.incrPos // <byKey flag>
          env = env.incrPos // <actors>
          val selfBinderPos = env.position
          env = env.incrPos // <cid>
          val choiceArgumentPos = env.position
          env = env.incrPos // <choice argument>
          env = env.incrPos // <token>

          // <template argument>
          env = env.addExprVar(tmpl.param)

          val signatories = translate(tmpl.signatories)
          val observers = translate(tmpl.observers)
          // now allow access to the choice argument
          env = env.addExprVar(choice.argBinder._1, choiceArgumentPos)
          val controllers = translate(choice.controllers)
          val mbKey: SExpr = tmpl.key match {
            case None => SEValue.None
            case Some(k) => SEApp(SEBuiltin(SBSome), Array(translateKeyWithMaintainers(k)))
          }
          env = env.incrPos // beginExercise's ()

          // allow access to the self contract id
          env = env.addExprVar(choice.selfBinder, selfBinderPos)
          val update = translate(choice.update)
          withLabel(
            s"<exercise ${tmplId}:${cname}>",
            SEAbs(5) {
              SELet(
                // stack: <byKey flag> <actors> <cid> <choice arg> <token>
                SBUFetch(tmplId)(SEVar(3) /* <cid> */, SEVar(1) /* <token> */ ),
                // stack: <byKey flag> <actors> <cid> <choice arg> <token> <template arg>
                SBUBeginExercise(tmplId, choice.name, choice.consuming)(
                  SEVar(3), // <choice arg>
                  SEVar(4), // <cid>
                  SEVar(5), // <actors>
                  SEVar(6), // <byKey flag>
                  signatories,
                  observers,
                  controllers,
                  mbKey,
                  SEVar(2),
                ),
                // stack: <byKey flag> <actors> <cid> <choice arg> <token> <template arg> ()
                SEApp(update, Array(SEVar(3))),
                // stack: <byKey flag> <actors> <cid> <choice arg> <token> <template arg> () <ret value>
                SBUEndExercise(tmplId)(SEVar(4), SEVar(1)),
              ) in
                // stack: <byKey flag> <actors> <cid> <choice arg> <token> <template arg> () <ret value> ()
                SEVar(2)
            }
          )
        },
      ),
    )

  // ELet(a, ELet(b, body)) => ([a, b], body)
  private def collectLets(expr: Expr): (List[Binding], Expr) =
    expr match {
      case ELet(binding, body) =>
        val (bindings, body2) = collectLets(body)
        (binding :: bindings, body2)
      case e => (List.empty, e)
    }

  private def lookupPackage(pkgId: PackageId): Package =
    if (packages.isDefinedAt(pkgId)) packages(pkgId)
    else throw PackageNotFound(pkgId)

  private def lookupDefinition(tycon: TypeConName): Option[Definition] =
    lookupPackage(tycon.packageId).modules
      .get(tycon.qualifiedName.module)
      .flatMap(mod => mod.definitions.get(tycon.qualifiedName.name))

  private def lookupTemplate(tycon: TypeConName): Template =
    lookupDefinition(tycon)
      .flatMap {
        case DDataType(_, _, DataRecord(_, tmpl)) => tmpl
        case _ => None
      }
      .getOrElse(throw CompilationError(s"template $tycon not found"))

  private def lookupVariantDefinition(tycon: TypeConName): Option[DataVariant] =
    lookupDefinition(tycon).flatMap {
      case DDataType(_, _, data: DataVariant) =>
        Some(data)
      case _ =>
        None
    }

  private def lookupEnumDefinition(tycon: TypeConName): Option[DataEnum] =
    lookupDefinition(tycon).flatMap {
      case DDataType(_, _, data: DataEnum) =>
        Some(data)
      case _ =>
        None
    }

  private def lookupRecordIndex(tapp: TypeConApp, field: FieldName): Int =
    lookupDefinition(tapp.tycon)
      .flatMap {
        case DDataType(_, _, DataRecord(fields, _)) =>
          val idx = fields.indexWhere(_._1 == field)
          if (idx < 0) None else Some(idx)
        case _ => None
      }
      .getOrElse(throw CompilationError(s"record type $tapp not found"))

  private def withEnv[A](f: Unit => A): A = {
    val oldEnv = env
    val x = f(())
    env = oldEnv
    x
  }

  private def withBinders[A](binders: ExprVarName*)(f: Unit => A): A =
    withEnv { _ =>
      env = (binders foldLeft env)(_ addExprVar _)
      f(())
    }

  def stripLocation(e: SExpr): SExpr =
    e match {
      case SELocation(_, e2) => stripLocation(e2)
      case _ => e
    }

  /** Convert abstractions in a speedy expression into
    * explicit closure creations.
    * This step computes the free variables in an abstraction
    * body, then translates the references in the body into
    * references to the immediate top of the argument stack,
    * and changes the abstraction into a closure creation node
    * describing the free variables that need to be captured.
    *
    * For example:
    *   SELet(..two-bindings..) in
    *     SEAbs(2,
    *       SEVar(4) ..             [reference to first let-bound variable]
    *       SEVar(2))               [reference to first function-arg]
    * =>
    *   SELet(..two-bindings..) in
    *     SEMakeClo(
    *       Array(SELocS(2)),       [capture the first let-bound variable, from the stack]
    *       2,
    *       SELocF(0) ..            [reference the first let-bound variable via the closure]
    *       SELocA(0))              [reference the first function arg]
    */
  def closureConvert(remaps: Map[Int, SELoc], expr: SExpr): SExpr = {
    // remaps is a function which maps the relative offset from variables (SEVar) to their runtime location
    // The Map must contain a binding for every variable referenced.
    // The Map is consulted when translating variable references (SEVar) and free variables of an abstraction (SEAbs)
    def remap(i: Int): SELoc = {
      remaps.get(i) match {
        case None => throw CompilationError(s"remap($i),remaps=$remaps")
        case Some(loc) => loc
      }
    }
    expr match {
      case SEVar(i) => remap(i)
      case v: SEVal => v
      case be: SEBuiltin => be
      case pl: SEValue => pl
      case f: SEBuiltinRecursiveDefinition => f
      case SELocation(loc, body) =>
        SELocation(loc, closureConvert(remaps, body))

      case SEAbs(0, _) =>
        throw CompilationError("empty SEAbs")

      case SEAbs(arity, body) =>
        val fvs = freeVars(body, arity).toList.sorted
        val newRemapsF: Map[Int, SELoc] = fvs.zipWithIndex.map {
          case (orig, i) =>
            (orig + arity) -> SELocF(i)
        }.toMap
        val newRemapsA = (1 to arity).map {
          case i =>
            i -> SELocA(arity - i)
        }
        // The keys in newRemapsF and newRemapsA are disjoint
        val newBody = closureConvert(newRemapsF ++ newRemapsA, body)
        SEMakeClo(fvs.map(remap).toArray, arity, newBody)

      case x: SELoc =>
        throw CompilationError(s"closureConvert: unexpected SELoc: $x")

      case x: SEMakeClo =>
        throw CompilationError(s"closureConvert: unexpected SEMakeClo: $x")

      case SEApp(fun, args) =>
        val newFun = closureConvert(remaps, fun)
        val newArgs = args.map(closureConvert(remaps, _))
        SEApp(newFun, newArgs)

      case SECase(scrut, alts) =>
        SECase(
          closureConvert(remaps, scrut),
          alts.map {
            case SCaseAlt(pat, body) =>
              val n = patternNArgs(pat)
              SCaseAlt(
                pat,
                closureConvert(shift(remaps, n), body),
              )
          },
        )

      case SELet(bounds, body) =>
        SELet(bounds.zipWithIndex.map {
          case (b, i) =>
            closureConvert(shift(remaps, i), b)
        }, closureConvert(shift(remaps, bounds.length), body))

      case SECatch(body, handler, fin) =>
        SECatch(
          closureConvert(remaps, body),
          closureConvert(remaps, handler),
          closureConvert(remaps, fin),
        )

      case SELabelClosure(label, expr) =>
        SELabelClosure(label, closureConvert(remaps, expr))

      case x: SEWronglyTypeContractId =>
        throw CompilationError(s"unexpected SEWronglyTypeContractId: $x")

      case x: SEImportValue =>
        throw CompilationError(s"unexpected SEImportValue: $x")
    }
  }

  // Modify/extend `remaps` to reflect when new values are pushed on the stack.  This
  // happens as we traverse into SELet and SECase bodies which have bindings which at
  // runtime will appear on the stack.
  // We must modify `remaps` because it is keyed by indexes relative to the end of the stack.
  // And any values in the map which are of the form SELocS must also be _shifted_
  // because SELocS indexes are also relative to the end of the stack.
  def shift(remaps: Map[Int, SELoc], n: Int): Map[Int, SELoc] = {

    // We must update both the keys of the map (the relative-indexes from the original SEVar)
    // And also any values in the map which are stack located (SELocS), which are also indexed relatively
    val m1 = remaps.map { case (k, loc) => (n + k, shiftLoc(loc, n)) }

    // And create mappings for the `n` new stack items
    val m2 = (1 to n).map(i => (i, SELocS(i)))

    m1 ++ m2
  }

  def shiftLoc(loc: SELoc, n: Int): SELoc = loc match {
    case SELocS(i) => SELocS(i + n)
    case SELocA(_) | SELocF(_) => loc
  }

  /** Compute the free variables in a speedy expression.
    * The returned free variables are de bruijn indices
    * adjusted to the stack of the caller. */
  def freeVars(expr: SExpr, initiallyBound: Int): Set[Int] = {
    var bound = initiallyBound
    var free = Set.empty[Int]

    def go(expr: SExpr): Unit =
      expr match {
        case SEVar(i) =>
          if (i > bound)
            free += i - bound /* adjust to caller's environment */
        case _: SEVal => ()
        case _: SEBuiltin => ()
        case _: SEValue => ()
        case _: SEBuiltinRecursiveDefinition => ()
        case SELocation(_, body) =>
          go(body)
        case SEApp(fun, args) =>
          go(fun)
          args.foreach(go)
        case SEAbs(n, body) =>
          bound += n
          go(body)
          bound -= n
        case x: SELoc =>
          throw CompilationError(s"freeVars: unexpected SELoc: $x")
        case x: SEMakeClo =>
          throw CompilationError(s"freeVars: unexpected SEMakeClo: $x")
        case SECase(scrut, alts) =>
          go(scrut)
          alts.foreach {
            case SCaseAlt(pat, body) =>
              val n = patternNArgs(pat)
              bound += n; go(body); bound -= n
          }
        case SELet(bounds, body) =>
          bounds.foreach { e =>
            go(e)
            bound += 1
          }
          go(body)
          bound -= bounds.size
        case SECatch(body, handler, fin) =>
          go(body)
          go(handler)
          go(fin)
        case SELabelClosure(_, expr) =>
          go(expr)
        case x: SEWronglyTypeContractId =>
          throw CompilationError(s"unexpected SEWronglyTypeContractId: $x")
        case x: SEImportValue =>
          throw CompilationError(s"unexpected SEImportValue: $x")
      }
    go(expr)
    free
  }

  /** Validate variable references in a speedy expression */
  // valiate that we correctly captured all free-variables, and so reference to them is
  // via the surrounding closure, instead of just finding them higher up on the stack
  def validate(expr0: SExpr): SExpr = {

    def goV(v: SValue): Unit = {
      v match {
        case _: SPrimLit | STNat(_) | STypeRep(_) =>
        case SList(a) => a.iterator.foreach(goV)
        case SOptional(x) => x.foreach(goV)
        case STextMap(map) => map.values.foreach(goV)
        case SGenMap(values) =>
          values.foreach {
            case (k, v) =>
              goV(k)
              goV(v)
          }
        case SRecord(_, _, args) => args.forEach(goV)
        case SVariant(_, _, _, value) => goV(value)
        case SEnum(_, _, _) => ()
        case SAny(_, v) => goV(v)
        case _: SPAP | SToken | SStruct(_, _) =>
          throw CompilationError("validate: unexpected SEValue")
      }
    }

    def goBody(maxS: Int, maxA: Int, maxF: Int): SExpr => Unit = {

      def goLoc(loc: SELoc) = loc match {
        case SELocS(i) =>
          if (i < 1 || i > maxS)
            throw CompilationError(s"validate: SELocS: index $i out of range ($maxS..1)")
        case SELocA(i) =>
          if (i < 0 || i >= maxA)
            throw CompilationError(s"validate: SELocA: index $i out of range (0..$maxA-1)")
        case SELocF(i) =>
          if (i < 0 || i >= maxF)
            throw CompilationError(s"validate: SELocF: index $i out of range (0..$maxF-1)")
      }

      def go(expr: SExpr): Unit = expr match {
        case loc: SELoc => goLoc(loc)
        case _: SEVal => ()
        case _: SEBuiltin => ()
        case _: SEBuiltinRecursiveDefinition => ()
        case SEValue(v) => goV(v)
        case SEApp(fun, args) =>
          go(fun)
          args.foreach(go)
        case x: SEVar =>
          throw CompilationError(s"validate: SEVar encountered: $x")
        case abs: SEAbs =>
          throw CompilationError(s"validate: SEAbs encountered: $abs")
        case SEMakeClo(fvs, n, body) =>
          fvs.foreach(goLoc)
          goBody(0, n, fvs.length)(body)
        case SECase(scrut, alts) =>
          go(scrut)
          alts.foreach {
            case SCaseAlt(pat, body) =>
              val n = patternNArgs(pat)
              goBody(maxS + n, maxA, maxF)(body)
          }
        case SELet(bounds, body) =>
          bounds.zipWithIndex.foreach {
            case (rhs, i) =>
              goBody(maxS + i, maxA, maxF)(rhs)
          }
          goBody(maxS + bounds.length, maxA, maxF)(body)
        case SECatch(body, handler, fin) =>
          go(body)
          go(handler)
          go(fin)
        case SELocation(_, body) =>
          go(body)
        case SELabelClosure(_, expr) =>
          go(expr)
        case x: SEWronglyTypeContractId =>
          throw CompilationError(s"unexpected SEWronglyTypeContractId: $x")
        case x: SEImportValue =>
          throw CompilationError(s"unexpected SEImportValue: $x")
      }
      go
    }
    goBody(0, 0, 0)(expr0)
    expr0
  }

  private def compileFetch(tmplId: Identifier, coid: SExpr): SExpr = {
    val tmpl = lookupTemplate(tmplId)
    withEnv { _ =>
      env = env.incrPos // token
      env = env.addExprVar(tmpl.param) // argument
      val signatories = translate(tmpl.signatories)
      val observers = translate(tmpl.observers)
      val key = tmpl.key match {
        case None => SEValue.None
        case Some(k) => SEApp(SEBuiltin(SBSome), Array(translateKeyWithMaintainers(k)))
      }
      SELet(coid) in
        withLabel(
          s"<fetch ${tmplId}>",
          SEAbs(1) {
            SELet(
              SBUFetch(tmplId)(
                SEVar(2), /* coid */
                SEVar(1) /* token */
              ),
              SBUInsertFetchNode(tmplId, byKey = false)(
                SEVar(3), /* coid */
                signatories,
                observers,
                key,
                SEVar(2) /* token */
              ),
            ) in SEVar(2) /* fetch result */
          }
        )
    }
  }

  private def compileCreate(tmplId: Identifier, arg: SExpr): SExpr = {
    // FIXME(JM): Lift to top-level?
    // Translates 'create Foo with <params>' into:
    // let arg = <params>
    // let key = if (we have a key definition in the template) {
    //   let keyBody = <key>
    //   in Some {key: keyBody, maintainers: <key maintainers> keyBody}
    // } else {
    //   None
    // }
    // in \token ->
    //   $create arg <precond> <agreement text> <signatories> <observers> <token> <key>
    val tmpl = lookupTemplate(tmplId)
    withEnv { _ =>
      env = env.addExprVar(tmpl.param) // argument
      env = env.incrPos // token
      val precond = translate(tmpl.precond)

      env = env.incrPos // unit returned by SBCheckPrecond
      val agreement = translate(tmpl.agreementText)
      val signatories = translate(tmpl.signatories)
      val observers = translate(tmpl.observers)
      val key = tmpl.key match {
        case None => SEValue.None
        case Some(k) => SEApp(SEBuiltin(SBSome), Array(translateKeyWithMaintainers(k)))
      }

      SELet(arg) in
        withLabel(
          s"<create ${tmplId}>",
          SEAbs(1) {
            // We check precondition in a separated builtin to prevent
            // further evaluation of agreement, signatories, observers and key
            // in case of failed precondition.
            SELet(SBCheckPrecond(tmplId)(SEVar(2), precond)) in
              SBUCreate(tmplId)(
                SEVar(3), /* argument */
                agreement,
                signatories,
                observers,
                key,
                SEVar(2) /* token */
              )
          }
        )
    }
  }

  private def compileExercise(
      tmplId: Identifier,
      contractId: SExpr,
      choiceId: ChoiceName,
      // actors are only present when compiling old LF update expressions;
      // they are computed from the controllers in newer versions
      optActors: Option[SExpr],
      byKey: Boolean,
      argument: SExpr,
  ): SExpr =
    // Translates 'A does exercise cid Choice with <params>'
    // into:
    // SomeTemplate$SomeChoice <actorsE> <cidE> <argE>
    withEnv { _ =>
      val actors: SExpr = optActors match {
        case None => SEValue.None
        case Some(actors) => SEApp(SEBuiltin(SBSome), Array(actors))
      }
      SEApp(
        SEVal(ChoiceDefRef(tmplId, choiceId)),
        Array(SEValue.bool(byKey), actors, contractId, argument))
    }

  private def compileExerciseByKey(
      tmplId: Identifier,
      key: SExpr,
      choiceId: ChoiceName,
      // actors are either the singleton set of submitter of an exercise command,
      // or the acting parties of an exercise node
      // of a transaction under reconstruction for validation
      optActors: Option[SExpr],
      argument: SExpr,
  ): SExpr = {
    // Translates 'exerciseByKey Foo <key> <choiceName> <optActors> <argument>' into:
    // let key = <key>
    // let maintainers = keyMaintainers key
    // in \token ->
    //    let coid = $fetchKey key maintainers token
    //        exerciseResult = exercise coid coid <optActors> <argument> token
    //    in exerciseResult
    val template = lookupTemplate(tmplId)
    withEnv { _ =>
      val tmplKey = template.key.getOrElse(
        throw CompilationError(s"Expecting to find key for template ${tmplId}, but couldn't"),
      )
      SELet(encodeKeyWithMaintainers(key, tmplKey)) in {
        env = env.incrPos // key with maintainers
        withLabel(
          s"<exercise_by_key ${tmplId}:${choiceId}>",
          SEAbs(1) {
            env = env.incrPos // token
            SELet(
              SBUFetchKey(tmplId)(SEVar(2), SEVar(1)),
              SEApp(
                compileExercise(
                  tmplId = tmplId,
                  contractId = SEVar(1),
                  choiceId = choiceId,
                  byKey = true,
                  optActors = optActors,
                  argument = argument),
                Array(SEVar(2)),
              ),
            ) in SEVar(1)
          }
        )
      }
    }
  }

  private def compileCreateAndExercise(
      tmplId: Identifier,
      createArg: SValue,
      choiceId: ChoiceName,
      choiceArg: SValue,
  ): SExpr = {

    withEnv { _ =>
      withLabel(
        s"<create_and_exercise ${tmplId}:${choiceId}>",
        SEAbs(1) {
          env = env.incrPos // token
          SELet(
            SEApp(compileCreate(tmplId, SEValue(createArg)), Array(SEVar(1))),
            SEApp(
              compileExercise(
                tmplId = tmplId,
                contractId = SEVar(1),
                choiceId = choiceId,
                optActors = None,
                byKey = false,
                argument = SEValue(choiceArg),
              ),
              Array(SEVar(2)),
            ),
          ) in SEVar(1)
        }
      )
    }
  }

  private def compileLookupByKey(templateId: Identifier, key: SExpr): SExpr = {
    val template = lookupTemplate(templateId)
    withEnv { _ =>
      val templateKey = template.key.getOrElse(
        throw CompilationError(
          s"Expecting to find key for template ${templateId}, but couldn't",
        ),
      )
      SELet(encodeKeyWithMaintainers(key, templateKey)) in {
        env = env.incrPos // keyWithM
        withLabel(
          s"<lookup_by_key ${templateId}>",
          SEAbs(1) {
            env = env.incrPos // token
            SELet(
              SBULookupKey(templateId)(
                SEVar(2), // key with maintainers
                SEVar(1) // token
              ),
              SBUInsertLookupNode(templateId)(
                SEVar(3), // key with maintainers
                SEVar(1), // mb contract id
                SEVar(2) // token
              ),
            ) in SEVar(2) // mb contract id
          }
        )
      }
    }
  }

  private def translateCommand(cmd: Command): SExpr = cmd match {
    case Command.Create(templateId, argument) =>
      compileCreate(templateId, SEValue(argument))
    case Command.Exercise(templateId, contractId, choiceId, argument) =>
      compileExercise(
        tmplId = templateId,
        contractId = SEValue(contractId),
        choiceId = choiceId,
        optActors = None,
        byKey = false,
        argument = SEValue(argument),
      )
    case Command.ExerciseByKey(templateId, contractKey, choiceId, argument) =>
      compileExerciseByKey(templateId, SEValue(contractKey), choiceId, None, SEValue(argument))
    case Command.Fetch(templateId, coid) =>
      compileFetch(templateId, SEValue(coid))
    case Command.CreateAndExercise(templateId, createArg, choice, choiceArg) =>
      compileCreateAndExercise(
        templateId,
        createArg,
        choice,
        choiceArg,
      )
    case Command.LookupByKey(templateId, contractKey) =>
      compileLookupByKey(templateId, SEValue(contractKey))
  }

  private def translateCommands(bindings: ImmArray[Command]): SExpr = {

    if (bindings.isEmpty)
      SEUpdatePureUnit
    else
      withEnv { _ =>
        val boundHead = translateCommand(bindings.head)
        env = env.incrPos // evaluated body of first binding

        val tokenPosition = env.position
        env = env.incrPos // token

        // add the first binding into the environment
        env = env.incrPos

        // and then the rest
        val boundTail = bindings.tail.toList.map { cmd =>
          val tokenIndex = env.position - tokenPosition
          env = env.incrPos
          SEApp(translateCommand(cmd), Array(SEVar(tokenIndex)))
        }
        val allBounds = SEAppBoundHead +: boundTail
        SELet(boundHead) in
          SEAbs(1) {
            SELet(allBounds: _*) in
              SEApp(SEUpdatePureUnit, Array(SEVar(env.position - tokenPosition)))
          }
      }
  }

}
