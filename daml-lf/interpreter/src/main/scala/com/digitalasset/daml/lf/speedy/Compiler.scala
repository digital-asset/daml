// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy

import com.digitalasset.daml.lf.data.{FrontStack, ImmArray}
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.lfpackage.Ast
import com.digitalasset.daml.lf.lfpackage.Ast._
import com.digitalasset.daml.lf.speedy.Compiler.{CompileError, PackageNotFound}
import com.digitalasset.daml.lf.speedy.SBuiltin._
import com.digitalasset.daml.lf.speedy.SExpr._
import com.digitalasset.daml.lf.speedy.SValue._
import com.digitalasset.daml.lf.validation.{
  EUnknownDefinition,
  LEPackage,
  Validation,
  ValidationError
}
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId

import scala.collection.mutable

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
object Compiler {
  case class CompileError(error: String) extends RuntimeException(error, null, true, false)
  case class PackageNotFound(pkgId: PackageId)
      extends RuntimeException(s"Package not found $pkgId", null, true, false)
}

final case class Compiler(packages: PackageId PartialFunction Package) {

  /** The current absolute position in the environment stack. */
  private var currentPosition: Int = 0

  /** Environment mapping names into stack positions */
  private var env = List[(ExprVarName, Int)]()

  def compile(cmds: ImmArray[Command]): SExpr =
    validate(closureConvert(Map.empty, 0, translateCommands(cmds)))

  def compile(cmd: Command): SExpr =
    validate(closureConvert(Map.empty, 0, translateCommand(cmd)))

  def compile(expr: Expr): SExpr =
    validate(closureConvert(Map.empty, 0, translate(expr)))

  def compileDefn(
      identifier: Identifier,
      defn: Definition
  ): List[(SDefinitionRef, SExpr)] =
    defn match {
      case DValue(_, _, body, _) =>
        List(LfDefRef(identifier) -> compile(body))

      case DDataType(_, _, DataRecord(_, Some(tmpl))) =>
        // Compile choices into top-level definitions that exercise
        // the choice.
        tmpl.choices.toList.map {
          case (cname, choice) =>
            ChoiceDefRef(identifier, cname) ->
              compileChoice(identifier, tmpl, choice)
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
  @throws(classOf[PackageNotFound])
  @throws(classOf[ValidationError])
  def compilePackage(pkgId: PackageId): Iterable[(SDefinitionRef, SExpr)] = {

    Validation.checkPackage(packages, pkgId).left.foreach {
      case EUnknownDefinition(_, LEPackage(pkgId_)) =>
        throw PackageNotFound(pkgId_)
      case e =>
        throw e
    }

    for {
      module <- lookupPackage(pkgId).modules.values
      defnWithId <- module.definitions
      (defnId, defn) = defnWithId
      fullId = Identifier(pkgId, QualifiedName(module.name, defnId))
      exprWithId <- compileDefn(fullId, defn)
    } yield exprWithId

  }

  /** Validates and Compiles all the definitions in the packages provided. Returns them in a Map.
    *
    * The packages do not need to be in any specific order, as long as they and all the packages
    * they transitively reference are in the [[packages]] in the compiler.
    */
  @throws(classOf[PackageNotFound])
  def compilePackages(toCompile0: Iterable[PackageId]): Map[SDefinitionRef, SExpr] = {
    var defns = Map.empty[SDefinitionRef, SExpr]
    val compiled = mutable.Set.empty[PackageId]
    var toCompile = toCompile0.toList
    val foundDependencies = mutable.Set.empty[PackageId]

    while (toCompile.nonEmpty) {
      val pkgId = toCompile.head
      toCompile = toCompile.tail
      try {
        if (!compiled.contains(pkgId))
          defns ++= compilePackage(pkgId)
      } catch {
        case PackageNotFound(dependency) if dependency != pkgId =>
          if (foundDependencies.contains(dependency)) {
            throw CompileError(s"Cyclical packages, stumbled upon $dependency twice")
          }
          foundDependencies += dependency
          toCompile = dependency :: pkgId :: toCompile
      }
      compiled += pkgId
    }

    defns
  }

  private def translate(expr: Expr): SExpr =
    expr match {
      case EVar(name) => SEVar(lookupIndex(name))
      case EVal(ref) => SEVal(LfDefRef(ref), None)
      case EBuiltin(bf) =>
        bf match {
          case BFoldl => SEBuiltinRecursiveDefinition.FoldL
          case BFoldr => SEBuiltinRecursiveDefinition.FoldR
          case BEqualList => SEBuiltinRecursiveDefinition.EqualList
          case _ =>
            SEBuiltin(bf match {
              case BTrace => SBTrace

              // Decimal arithmetic
              case BAddDecimal => SBAdd
              case BSubDecimal => SBSub
              case BMulDecimal => SBMul
              case BDivDecimal => SBDiv
              case BRoundDecimal => SBRoundDecimal

              // Int64 arithmetic
              case BAddInt64 => SBAdd
              case BSubInt64 => SBSub
              case BMulInt64 => SBMul
              case BModInt64 => SBMod
              case BDivInt64 => SBDiv
              case BExpInt64 => SBExpInt64

              // Conversions
              case BInt64ToDecimal => SBInt64ToDecimal
              case BDecimalToInt64 => SBDecimalToInt64
              case BDateToUnixDays => SBDateToUnixDays
              case BUnixDaysToDate => SBUnixDaysToDate
              case BTimestampToUnixMicroseconds => SBTimestampToUnixMicroseconds
              case BUnixMicrosecondsToTimestamp => SBUnixMicrosecondsToTimestamp

              // Text functions
              case BExplodeText => SBExplodeText
              case BImplodeText => SBImplodeText
              case BAppendText => SBAppendText

              case BToTextInt64 => SBToText
              case BToTextDecimal => SBToText
              case BToTextText => SBToText
              case BToTextTimestamp => SBToText
              case BToTextParty => SBToText
              case BToTextDate => SBToText
              case BToQuotedTextParty => SBToQuotedTextParty
              case BFromTextParty => SBFromTextParty

              case BSHA256Text => SBSHA256Text

              // Errors
              case BError => SBError

              // Comparisons
              case BLessInt64 => SBLess
              case BLessEqInt64 => SBLessEq
              case BGreaterInt64 => SBGreater
              case BGreaterEqInt64 => SBGreaterEq

              case BLessDecimal => SBLess
              case BLessEqDecimal => SBLessEq
              case BGreaterDecimal => SBGreater
              case BGreaterEqDecimal => SBGreaterEq

              case BLessText => SBLess
              case BLessEqText => SBLessEq
              case BGreaterText => SBGreater
              case BGreaterEqText => SBGreaterEq

              case BLessTimestamp => SBLess
              case BLessEqTimestamp => SBLessEq
              case BGreaterTimestamp => SBGreater
              case BGreaterEqTimestamp => SBGreaterEq

              case BLessDate => SBLess
              case BLessEqDate => SBLessEq
              case BGreaterDate => SBGreater
              case BGreaterEqDate => SBGreaterEq

              case BLessParty => SBLess
              case BLessEqParty => SBLessEq
              case BGreaterParty => SBGreater
              case BGreaterEqParty => SBGreaterEq

              // Equality
              case BEqualText => SBEqual
              case BEqualInt64 => SBEqual
              case BEqualDecimal => SBEqual
              case BEqualTimestamp => SBEqual
              case BEqualDate => SBEqual
              case BEqualParty => SBEqual
              case BEqualBool => SBEqual
              case BEqualContractId => SBEqual

              // Map

              case BMapEmpty => SBMapEmpty
              case BMapInsert => SBMapInsert
              case BMapLookup => SBMapLookup
              case BMapDelete => SBMapDelete
              case BMapToList => SBMapToList
              case BMapSize => SBMapSize

              // Implemented using normal functions
              case BFoldl => throw CompileError(s"unexpected BFoldl")
              case BFoldr => throw CompileError(s"unexpected BFoldr")
              case BEqualList => throw CompileError(s"unexpected BEqualList")
            })
        }

      case EPrimCon(con) =>
        con match {
          case PCTrue => SEValue(SBool(true))
          case PCFalse => SEValue(SBool(false))
          case PCUnit => SEValue(SUnit(()))
        }
      case EPrimLit(lit) =>
        SEValue(lit match {
          case PLInt64(i) => SInt64(i)
          case PLDecimal(d) => SDecimal(d)
          case PLText(t) => SText(t)
          case PLTimestamp(ts) => STimestamp(ts)
          case PLParty(p) => SParty(p)
          case PLDate(d) => SDate(d)
        })
      case app: EApp =>
        val (fun, args) = collectApps(app)
        SEApp(translate(fun), args.map(translate).toArray)
      case eabs: EAbs =>
        val (binders, body) = collectAbs(eabs)
        withBinders(binders) { _ =>
          SEAbs(binders.length, translate(body))
        }
      case ERecCon(tApp, fields) =>
        if (fields.isEmpty)
          SEBuiltin(SBRecCon(tApp.tycon, Name.Array.empty))
        else {
          SEApp(
            SEBuiltin(SBRecCon(tApp.tycon, Name.Array(fields.map(_._1).toSeq: _*))),
            fields.iterator.map(f => translate(f._2)).toArray
          )
        }

      case ERecProj(tapp, field, record) =>
        SBRecProj(tapp.tycon, lookupRecordIndex(tapp, field))(
          translate(record)
        )

      case ERecUpd(tapp, field, record, update) =>
        SBRecUpd(tapp.tycon, lookupRecordIndex(tapp, field))(
          translate(record),
          translate(update)
        )

      case ETupleCon(fields) =>
        SEApp(SEBuiltin(SBTupleCon(Name.Array(fields.map(_._1).toSeq: _*))), fields.iterator.map {
          case (_, e) => translate(e)
        }.toArray)

      case ETupleProj(field, tuple) =>
        SBTupleProj(field)(translate(tuple))

      case ETupleUpd(field, tuple, update) =>
        SBTupleUpd(field)(translate(tuple), translate(update))

      case ETyAbs(_, body) =>
        translate(body)
      case ETyApp(expr, _) =>
        translate(expr)
      case ECase(scrut, alts) =>
        SECase(
          translate(scrut),
          alts.iterator.map {
            case CaseAlt(pat, expr) =>
              pat match {
                case CPVariant(tycon, variant, binder) =>
                  withBinder(binder) { _ =>
                    SCaseAlt(SCPVariant(tycon, variant), translate(expr))
                  }
                case CPNil =>
                  SCaseAlt(SCPNil, translate(expr))

                case CPCons(head, tail) =>
                  withBinders(Seq(head, tail)) { _ =>
                    SCaseAlt(SCPCons, translate(expr))
                  }

                case CPPrimCon(pc) =>
                  SCaseAlt(SCPPrimCon(pc), translate(expr))

                case CPNone =>
                  SCaseAlt(SCPNone, translate(expr))

                case CPSome(body) =>
                  withBinder(body) { _ =>
                    SCaseAlt(SCPSome, translate(expr))
                  }

                case CPDefault =>
                  SCaseAlt(SCPDefault, translate(expr))
              }
          }.toArray
        )

      case ENil(_) => SEValue(SList(FrontStack.empty))
      case ECons(_, front, tail) =>
        // TODO(JM): Consider emitting SEValue(SList(...)) for
        // constant lists?
        SEApp(
          SEBuiltin(SBConsMany(front.length)),
          front.iterator.map(translate).toArray :+ translate(tail),
        )

      case ENone(_) =>
        SEValue(SOptional(None))

      case ESome(_, body) =>
        SEApp(
          SEBuiltin(SBSome),
          Array(translate(body)),
        )

      case EVariantCon(tapp, variant, arg) =>
        SBVariantCon(tapp.tycon, variant)(translate(arg))

      case EContractId(coid, _) =>
        SEValue(SContractId(AbsoluteContractId(coid)))

      case let: ELet =>
        val (bindings, body) = collectLets(let)
        withEnv { _ =>
          SELet(
            bindings.map {
              case Binding(optBinder, _, bound) =>
                val bound2 = translate(bound)
                optBinder.foreach { b =>
                  env = (b -> currentPosition) :: env
                }
                currentPosition += 1
                bound2
            }.toArray,
            translate(body)
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
            compileExercise(tmplId, translate(cidE), chId, translate(actorsE), translate(argE))

          case UpdateGetTime =>
            SEAbs(1) { SBGetTime(SEVar(1)) }

          case UpdateLookupByKey(retrieveByKey) =>
            // Translates 'lookupByKey Foo <key>' into:
            // let key = <key>
            // let maintainers = keyMaintainers key
            // in \token ->
            //    let mbContractId = $lookupKey key
            //        _ = $insertLookup Foo key
            //    in mbContractId
            val template = lookupTemplate(retrieveByKey.templateId)
            withEnv { _ =>
              val key = translate(retrieveByKey.key)
              val keyMaintainers = template.key match {
                case None =>
                  throw CompileError(
                    s"Expecting to find key for template ${retrieveByKey.templateId}, but couldn't")
                case Some(tplKey) => translate(tplKey.maintainers)
              }
              SELet(key, SEApp(keyMaintainers, Array(SEVar(1)))) in {
                currentPosition += 1 // key
                currentPosition += 1 // keyMaintainers
                SEAbs(1) {
                  currentPosition += 1 // token
                  SELet(
                    SBULookupKey(retrieveByKey.templateId)(
                      SEVar(3), // key
                      SEVar(1) // token
                    ),
                    SBUInsertLookupNode(retrieveByKey.templateId)(
                      SEVar(4), // key
                      SEVar(3), // maintainers
                      SEVar(1), // mb contract id
                      SEVar(2) // token
                    )
                  ) in SEVar(2) // mb contract id
                }
              }
            }

          case UpdateFetchByKey(retrieveByKey) =>
            // Translates 'fetchByKey Foo <key>' into:
            // let key = <key>
            // let maintainers = keyMaintainers key
            // in \token ->
            //    let coid = $fetchKey key
            //        contract = $fetch coid token
            //        _ = $insertFetch coid <signatories> <observers>
            //    in { contractId: ContractId Foo, contract: Foo }
            val template = lookupTemplate(retrieveByKey.templateId)
            withEnv { _ =>
              val key = translate(retrieveByKey.key)
              val keyMaintainers = template.key match {
                case None =>
                  throw CompileError(
                    s"Expecting to find key for template ${retrieveByKey.templateId}, but couldn't")
                case Some(tplKey) => translate(tplKey.maintainers)
              }
              SELet(key, SEApp(keyMaintainers, Array(SEVar(1)))) in {
                currentPosition += 1 // key
                currentPosition += 1 // keyMaintainers
                SEAbs(1) {
                  currentPosition += 1 // token
                  env = (template.param -> currentPosition) :: env
                  currentPosition += 1 // argument
                  // TODO should we evaluate this before we even construct
                  // the update expression? this might be better for the user
                  val signatories = translate(template.signatories)
                  val observers = translate(template.observers)
                  SELet(
                    SBUFetchKey(retrieveByKey.templateId)(
                      SEVar(3), // key
                      SEVar(1) // token
                    ),
                    SBUFetch(retrieveByKey.templateId)(
                      SEVar(1), /* coid */
                      SEVar(2) /* token */
                    ),
                    SBUInsertFetchNode(retrieveByKey.templateId)(
                      SEVar(2), // coid
                      signatories,
                      observers,
                      SEVar(3) // token
                    )
                  ) in SBTupleCon(Name.Array(Ast.contractIdFieldName, Ast.contractFieldName))(
                    SEVar(3), // contract id
                    SEVar(2) // contract
                  )
                }
              }
            }
        }

      case ELocation(loc, EScenario(scen)) =>
        SELocation(loc, translateScenario(scen, Some(loc)))

      case EScenario(scen) =>
        translateScenario(scen, None)

      case ELocation(loc, e) =>
        SELocation(loc, translate(e))
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
          currentPosition += 1 // party
          val update = translate(updateE)
          currentPosition += 1 // update
          currentPosition += 1 // $beginCommit
          SELet(party, update) in
            SEAbs(1) {
              SELet(
                // stack: <party> <update> <token>
                SBSBeginCommit(optLoc)(SEVar(3), SEVar(1)),
                // stack: <party> <update> <token> ()
                SEVar(3)(SEVar(2)),
                // stack: <party> <update> <token> () result
              ) in
                SBSEndCommit(false)(SEVar(1), SEVar(3))
            }
        }

      case ScenarioMustFailAt(partyE, updateE, _retType @ _) =>
        // \token ->
        //   let _ = $beginCommit <party> token
        //       r = $catch (<updateE> token) true false
        //   in $endCommit[mustFail = true] r token
        withEnv { _ =>
          currentPosition += 1 // token
          val party = translate(partyE)
          currentPosition += 1 // $beginCommit
          val update = translate(updateE)
          SEAbs(1) {
            SELet(
              SBSBeginCommit(optLoc)(party, SEVar(1)),
              SECatch(update(SEVar(2)), SEValue(SBool(true)), SEValue(SBool(false)))
            ) in SBSEndCommit(true)(SEVar(1), SEVar(3))
          }
        }

      case ScenarioGetTime =>
        SEAbs(1) { SBGetTime(SEVar(1)) }

      case ScenarioGetParty(e) =>
        withEnv { _ =>
          currentPosition += 1 // token
          SEAbs(1) {
            SBSGetParty(translate(e), SEVar(1))
          }
        }

      case ScenarioPass(relTimeE) =>
        withEnv { _ =>
          currentPosition += 1 // token
          SEAbs(1) {
            SBSPass(translate(relTimeE), SEVar(1))
          }
        }

      case ScenarioEmbedExpr(_, e) =>
        translateEmbedExpr(e)
    }
  private def translateEmbedExpr(expr: Expr): SExpr = {
    withEnv { _ =>
      currentPosition += 1 // token
      // EmbedExpr's get wrapped into an extra layer of abstraction
      // to delay evaluation.
      // e.g.
      // embed (error "foo") => \token -> error "foo"
      SEAbs(1) {
        translate(expr)(SEVar(1))
      }
    }
  }

  private def translatePure(body: Expr): SExpr = {
    // pure <E>
    // =>
    // ((\x token -> x) <E>)
    SEAbs(2) { SEVar(2) }(
      translate(body)
    )
  }

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
      currentPosition += 1 // evaluated body of first binding

      val tokenPosition = currentPosition
      currentPosition += 1 // token

      // add the first binding into the environment
      val appBoundHead = SEVar(2)(SEVar(1))
      bindings.head.binder.foreach { b =>
        env = (b -> currentPosition) :: env
      }
      currentPosition += 1

      // and then the rest
      val boundTail = bindings.tail.toList.map {
        case Binding(optB, _, bound) =>
          val sbound = translate(bound)
          optB.foreach { b =>
            env = (b -> currentPosition) :: env
          }
          val tokenIndex = currentPosition - tokenPosition
          currentPosition += 1
          SEApp(sbound, Array(SEVar(tokenIndex)))
      }
      val allBounds = appBoundHead +: boundTail
      SELet(boundHead) in
        SEAbs(1) {
          SELet(allBounds: _*) in
            translate(body)(SEVar(currentPosition - tokenPosition))
        }
    }
  }

  /** Compile a choice into a top-level function for exercising that choice */
  private def compileChoice(tmplId: TypeConName, tmpl: Template, choice: TemplateChoice): SExpr =
    // Compiles a choice into:
    // SomeTemplate$SomeChoice = \actors cid arg token ->
    //   let targ = fetch cid
    //       _ = $beginExercise[tmplId, chId] arg cid actors <sigs> <obs> <ctrls> token
    //       result = <updateE>
    //       _ = $endExercise[tmplId]
    //   in result
    validate(
      closureConvert(
        Map.empty,
        0,
        withEnv { _ =>
          val selfBinderPos = currentPosition + 1
          val choiceArgumentPos = currentPosition + 2
          currentPosition += 4 // actors, cid, choice argument and token

          // template argument
          env = (tmpl.param -> currentPosition) :: env
          currentPosition += 1

          val signatories = translate(tmpl.signatories)
          val observers = translate(tmpl.observers)
          // now allow access to the choice argument
          choice.argBinder._1.foreach { b =>
            env = (b -> choiceArgumentPos) :: env
          }
          val controllers = translate(choice.controllers)
          val mbKey: SExpr = tmpl.key match {
            case None => SEValue(SOptional(None))
            case Some(k) =>
              SEApp(
                SEBuiltin(SBSome),
                Array(translate(k.body)),
              )
          }
          currentPosition += 1 // beginExercise's ()

          // allow access to the self contract id
          env = (choice.selfBinder -> selfBinderPos) :: env
          val update = translate(choice.update)
          SEAbs(4) {
            SELet(
              // stack: <actors> <cid> <choice arg> <token>
              SBUFetch(tmplId)(SEVar(3) /* cid */, SEVar(1) /* token */ ),
              // stack: <actors> <cid> <choice arg> <token> <template arg>
              SBUBeginExercise(tmplId, choice.name, choice.consuming)(
                SEVar(3), // choice argument
                SEVar(4), // cid
                SEVar(5), // actors
                signatories,
                observers,
                controllers,
                mbKey,
                SEVar(2)),
              // stack: <actors> <cid> <choice arg> <token> <template arg> ()
              update(SEVar(3)),
              // stack: <actors> <cid> <choice arg> <token> <template arg> () <ret value>
              SBUEndExercise(tmplId)(SEVar(4), SEVar(1))
            ) in
              // stack: <actors> <cid> <choice arg> <token> <template arg> () <ret value> ()
              SEVar(2)
          }
        }
      ))

  private def lookupIndex(binder: ExprVarName): Int = {
    val idx =
      env
        .find(_._1 == binder)
        .getOrElse(throw CompileError("Unknown variable: " + binder + ". Known: " + env.toString))
        ._2
    // The de Bruijin index for the binder, e.g.
    // the distance to the binder. The closest binder
    // is at distance 1.
    currentPosition - idx
  }

  // ELet(a, ELet(b, body)) => ([a, b], body)
  private def collectLets(expr: Expr): (List[Binding], Expr) =
    expr match {
      case ELet(binding, body) =>
        val (bindings, body2) = collectLets(body)
        (binding :: bindings, body2)
      case e => (List.empty, e)
    }

  // EApp(EApp(fun, arg1), arg2) => (fun, [arg1, arg2])
  private def collectApps(app: Expr): (Expr, List[Expr]) = {
    def go(expr: Expr): (Expr, List[Expr]) =
      expr match {
        case EApp(fun, arg) =>
          val (fun2, args) = collectApps(fun)
          (fun2, args :+ arg)
        case e => (e, List.empty[Expr])
      }
    go(app)
  }
  // EAbs(binder, EAbs(binder2, ..., ref), ref) => ([binder, binder2], body)
  private def collectAbs(expr: Expr): (List[ExprVarName], Expr) =
    expr match {
      case EAbs((binder, typ @ _), body, ref @ _) =>
        val (binders, body2) = collectAbs(body)
        (binder :: binders, body2)
      case e => (List.empty, e)
    }

  private def lookupPackage(pkgId: PackageId): Package =
    if (packages.isDefinedAt(pkgId)) packages(pkgId)
    else throw PackageNotFound(pkgId)

  private def lookupTemplate(tycon: TypeConName): Template = {
    val pkg = lookupPackage(tycon.packageId)
    pkg.modules
      .get(tycon.qualifiedName.module)
      .flatMap(mod => mod.definitions.get(tycon.qualifiedName.name))
      .flatMap(defn =>
        defn match {
          case DDataType(_, _, DataRecord(_, tmpl)) => tmpl
          case _ => None
      })
      .getOrElse(throw CompileError(s"template $tycon not found"))
  }

  private def lookupRecordIndex(tapp: TypeConApp, field: FieldName): Int = {
    val pkg = lookupPackage(tapp.tycon.packageId)
    pkg.modules
      .get(tapp.tycon.qualifiedName.module)
      .flatMap(mod => mod.definitions.get(tapp.tycon.qualifiedName.name))
      .flatMap(defn =>
        defn match {
          case DDataType(_, _, DataRecord(fields, _)) =>
            val idx = fields.indexWhere(_._1 == field)
            if (idx < 0) None else Some(idx)
          case _ => None
      })
      .getOrElse(throw CompileError(s"record type $tapp not found"))
  }

  private def withBinder[A](binder: ExprVarName)(f: Unit => A): A =
    withBinders(Seq(binder))(f)

  private def withBinders[A](binders: Seq[ExprVarName])(f: Unit => A): A = {
    val oldEnv = env
    val oldPosition = currentPosition
    binders.foreach { binder =>
      env = (binder -> currentPosition) :: env
      currentPosition += 1
    }
    val x = f(())
    env = oldEnv
    currentPosition = oldPosition
    x
  }

  private def withEnv[A](f: Unit => A): A = {
    val oldEnv = env
    val oldPosition = currentPosition
    val x = f(())
    env = oldEnv
    currentPosition = oldPosition
    x
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
    *   SELet(...) in
    *     SEAbs(2, SEVar(4))
    * =>
    *   SELet(...) in
    *     SEMakeClo(
    *       Array(SEVar(2)), (capture 2nd value)
    *       2, (still takes two arguments)
    *       SEVar(3)) (variable now first value after args)
    */
  def closureConvert(remaps: Map[Int, Int], bound: Int, expr: SExpr): SExpr = {
    def remap(i: Int): Int =
      remaps
        .get(bound - i)
        // map the absolute stack position back into a
        // relative position
        .map(bound - _)
        .getOrElse(i)
    expr match {
      case SEVar(i) => SEVar(remap(i))
      case v: SEVal => v
      case be: SEBuiltin => be
      case pl: SEValue => pl
      case f: SEBuiltinRecursiveDefinition => f
      case SELocation(loc, body) =>
        SELocation(loc, closureConvert(remaps, bound, body))

      case SEAbs(0, _) =>
        throw CompileError("empty SEAbs")

      case SEAbs(n, body) =>
        val fv = freeVars(body, n).toList.sorted
        // remap free variables to new indices.
        // the index is the absolute position in stack.
        val newRemaps = fv.zipWithIndex.map {
          case (orig, i) =>
            // mapping from old position in the stack
            // to the new position
            (bound - orig) -> (bound - i - 1)
        }.toMap
        val newBody = closureConvert(newRemaps, bound + n, body)
        SEMakeClo(fv.reverse.map(remap).toArray, n, newBody)

      case x: SEMakeClo =>
        throw CompileError(s"unexpected SEMakeClo: $x")

      case SEApp(fun, args) =>
        val newFun = closureConvert(remaps, bound, fun)
        val newArgs = args.map(closureConvert(remaps, bound, _))
        SEApp(newFun, newArgs)

      case SECase(scrut, alts) =>
        SECase(
          closureConvert(remaps, bound, scrut),
          alts.map {
            case SCaseAlt(pat, body) =>
              SCaseAlt(
                pat,
                pat match {
                  case _: SCPVariant =>
                    closureConvert(remaps, bound + 1, body)
                  case _: SCPPrimCon =>
                    closureConvert(remaps, bound, body)
                  case SCPNil =>
                    closureConvert(remaps, bound, body)
                  case SCPCons =>
                    closureConvert(remaps, bound + 2, body)
                  case SCPDefault =>
                    closureConvert(remaps, bound, body)
                  case SCPNone =>
                    closureConvert(remaps, bound, body)
                  case SCPSome =>
                    closureConvert(remaps, bound + 1, body)
                }
              )
          }
        )

      case SELet(bounds, body) =>
        SELet(bounds.zipWithIndex.map {
          case (b, i) =>
            closureConvert(remaps, bound + i, b)
        }, closureConvert(remaps, bound + bounds.size, body))

      case SECatch(body, handler, fin) =>
        SECatch(
          closureConvert(remaps, bound, body),
          closureConvert(remaps, bound, handler),
          closureConvert(remaps, bound, fin))
    }
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
        case x: SEMakeClo =>
          throw CompileError(s"unexpected SEMakeClo: $x")
        case SECase(scrut, alts) =>
          go(scrut)
          alts.foreach {
            case SCaseAlt(pat, body) =>
              pat match {
                case _: SCPVariant =>
                  bound += 1; go(body); bound -= 1
                case _: SCPPrimCon => go(body)
                case SCPNil => go(body)
                case SCPCons => bound += 2; go(body); bound -= 2
                case SCPDefault => go(body)
                case SCPNone => go(body)
                case SCPSome => bound += 1; go(body); bound -= 1
              }
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
      }
    go(expr)
    free
  }

  /** Validate variable references in a speedy expression */
  def validate(expr: SExpr): SExpr = {
    var bound = 0

    def goV(v: SValue): Unit = {
      v match {
        case _: SPrimLit =>
        case SList(a) => a.iterator.foreach(goV)
        case SOptional(x) => x.foreach(goV)
        case SMap(map) => map.values.foreach(goV)
        case SRecord(_, _, args) => args.forEach(goV)
        case SVariant(_, _, value) => goV(value)
        case _ => throw CompileError("validate: unexpected SEValue")
      }
    }

    def go(expr: SExpr): Unit =
      expr match {
        case SEVar(i) =>
          if (i < 1 || i > bound) {
            throw CompileError(s"validate: SEVar: index $i out of bound $bound")
          }
        case _: SEVal => ()
        case _: SEBuiltin => ()
        case _: SEBuiltinRecursiveDefinition => ()
        case SEValue(v) => goV(v)
        case SEApp(fun, args) =>
          go(fun)
          args.foreach(go)
        case abs: SEAbs =>
          throw CompileError(s"validate: SEAbs encountered: $abs")
        case SEMakeClo(fv, n, body) =>
          fv.foreach { i =>
            if (i < 1 || i > bound) {
              throw CompileError(s"validate: SEMakeClo: free variable $i is out of bounds ($bound)")
            }
          }
          val oldBound = bound
          bound = n + fv.size
          go(body)
          bound = oldBound
        case SECase(scrut, alts) =>
          go(scrut)
          alts.foreach {
            case SCaseAlt(pat, body) =>
              pat match {
                case _: SCPVariant =>
                  bound += 1; go(body); bound -= 1
                case _: SCPPrimCon => go(body)
                case SCPNil => go(body)
                case SCPCons => bound += 2; go(body); bound -= 2
                case SCPDefault => go(body)
                case SCPNone => go(body)
                case SCPSome => bound += 1; go(body); bound -= 1
              }
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
        case SELocation(_, body) =>
          go(body)
      }
    go(expr)
    expr
  }

  private def compileFetch(tmplId: Identifier, coid: SExpr): SExpr = {
    val tmpl = lookupTemplate(tmplId)
    withEnv { _ =>
      currentPosition += 1 // token
      env = (tmpl.param -> currentPosition) :: env
      currentPosition += 1 // argument
      val signatories = translate(tmpl.signatories)
      val observers = translate(tmpl.observers)
      SELet(coid) in
        SEAbs(1) {
          SELet(
            SBUFetch(tmplId)(
              SEVar(2), /* coid */
              SEVar(1) /* token */
            ),
            SBUInsertFetchNode(tmplId)(
              SEVar(3), /* coid */
              signatories,
              observers,
              SEVar(2) /* token */
            )
          ) in SEVar(2) /* fetch result */
        }
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
      env = (tmpl.param -> currentPosition) :: env
      currentPosition += 1 // argument

      val key = tmpl.key match {
        case None => SEValue(SOptional(None))
        case Some(tmplKey) =>
          SELet(translate(tmplKey.body)) in
            SBSome(
              SBTupleCon(Name.Array(Ast.keyFieldName, Ast.maintainersFieldName))(
                SEVar(1), // key
                SEApp(translate(tmplKey.maintainers), Array(SEVar(1) /* key */ ))))
      }

      currentPosition += 1 // key
      currentPosition += 1 // token

      val precond = translate(tmpl.precond)

      currentPosition += 1 // unit returned by SBCheckPrecond
      val agreement = translate(tmpl.agreementText)
      val signatories = translate(tmpl.signatories)
      val observers = translate(tmpl.observers)

      SELet(arg, key) in
        SEAbs(1) {
          // We check precondition in a separated builtin to prevent
          // further evaluation of agreement, signatories and observers
          // in case of failed precondition.
          SELet(SBCheckPrecond(tmplId)(SEVar(3), precond)) in
            SBUCreate(tmplId)(
              SEVar(4), /* argument */
              agreement,
              signatories,
              observers,
              SEVar(3), /* key */
              SEVar(2) /* token */
            )
        }
    }
  }

  private def compileExercise(
      tmplId: Identifier,
      contractId: SExpr,
      choiceId: ChoiceName,
      // actors are either the singleton set of submitter of an exercise command,
      // or the acting parties of an exercise node
      // of a transaction under reconstruction for validation
      actors: SExpr,
      argument: SExpr): SExpr =
    // Translates 'A does exercise cid Choice with <params>'
    // into:
    // SomeTemplate$SomeChoice <actorsE> <cidE> <argE>
    withEnv { _ =>
      SEApp(SEVal(ChoiceDefRef(tmplId, choiceId), None), Array(actors, contractId, argument))
    }

  private def compileCreateAndExercise(
      tmplId: Identifier,
      createArg: SValue,
      choiceId: ChoiceName,
      choiceArg: SValue,
      // actors are either the singleton set of submitter of an exercise command,
      // or the acting parties of an exercise node
      // of a transaction under reconstruction for validation
      actors: SExpr): SExpr = {

    withEnv { _ =>
      SEAbs(1) {
        SELet(
          SEApp(compileCreate(tmplId, SEValue(createArg)), Array(SEVar(1))),
          SEApp(
            compileExercise(tmplId, SEVar(1), choiceId, actors, SEValue(choiceArg)),
            Array(SEVar(2)))
        ) in SEVar(1)
      }
    }
  }

  private def translateCommand(cmd: Command): SExpr = cmd match {
    case Command.Create(templateId, argument) =>
      compileCreate(templateId, SEValue(argument))
    case Command.Exercise(templateId, contractId, choiceId, submitters, argument) =>
      compileExercise(
        templateId,
        SEValue(contractId),
        choiceId,
        SEValue(SList(FrontStack(submitters))),
        SEValue(argument))
    case Command.Fetch(templateId, coid) =>
      compileFetch(templateId, SEValue(coid))
    case Command.CreateAndExercise(templateId, createArg, choice, choiceArg, submitters) =>
      compileCreateAndExercise(
        templateId,
        createArg,
        choice,
        choiceArg,
        SEValue(SList(FrontStack(submitters))))
  }

  private def translateCommands(bindings: ImmArray[Command]): SExpr = {

    if (bindings.isEmpty)
      translate(EUpdate(UpdatePure(TBuiltin(BTUnit), EPrimCon(PCUnit))))
    else
      withEnv { _ =>
        val boundHead = translateCommand(bindings.head)
        currentPosition += 1 // evaluated body of first binding

        val tokenPosition = currentPosition
        currentPosition += 1 // token

        // add the first binding into the environment
        val appBoundHead = SEVar(2)(SEVar(1))
        currentPosition += 1

        // and then the rest
        val boundTail = bindings.tail.toList.map { cmd =>
          val tokenIndex = currentPosition - tokenPosition
          currentPosition += 1
          SEApp(translateCommand(cmd), Array(SEVar(tokenIndex)))
        }
        val allBounds = appBoundHead +: boundTail
        SELet(boundHead) in
          SEAbs(1) {
            SELet(allBounds: _*) in
              translate(EUpdate(UpdatePure(TBuiltin(BTUnit), EPrimCon(PCUnit))))(
                SEVar(currentPosition - tokenPosition))
          }
      }
  }

}
