// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.Ref._
import com.daml.lf.data.{ImmArray, Ref, Struct, Time}
import com.daml.lf.language.Ast._
import com.daml.lf.language.{LanguageVersion, LookupError, PackageInterface, StablePackage}
import com.daml.lf.speedy.Anf.flattenToAnf
import com.daml.lf.speedy.ClosureConversion.closureConvert
import com.daml.lf.speedy.PhaseOne.{Env, Position}
import com.daml.lf.speedy.Profile.LabelModule
import com.daml.lf.speedy.SBuiltin._
import com.daml.lf.speedy.SExpr.{ContractKeyWithMaintainersDefRef, ToCachedContractDefRef}
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.{SExpr => t} // target expressions
import com.daml.lf.speedy.{SExpr0 => s} // source expressions
import com.daml.lf.validation.{Validation, ValidationError}
import com.daml.scalautil.Statement.discard
import org.slf4j.LoggerFactory

import scala.annotation.nowarn

/** Compiles LF expressions into Speedy expressions.
  * This includes:
  *  - Translating variable references into de Bruijn levels.
  *  - Closure conversion: EAbs turns into SEMakeClo, which creates a closure by copying free variables into a closure object.
  *   - Rewriting of update and scenario actions into applications of builtin functions that take an "effect" token.
  *
  * If you're working on the code here note that there's
  * a pretty-printer defined in lf.speedy.Pretty, which
  * is exposed via ':speedy' command in the REPL.
  */
private[lf] object Compiler {

  final case class CompilationError(error: String)
      extends RuntimeException(error, null, true, false)
  final case class LanguageVersionError(
      packageId: Ref.PackageId,
      languageVersion: language.LanguageVersion,
      allowedLanguageVersions: VersionRange[language.LanguageVersion],
  ) extends RuntimeException(s"Disallowed language version $languageVersion", null, true, false)
  final case class PackageNotFound(pkgId: PackageId, context: language.Reference)
      extends RuntimeException(
        language.LookupError.MissingPackage.pretty(pkgId, context),
        null,
        true,
        false,
      )

  // NOTE(MH): We make this an enum type to avoid boolean blindness. In fact,
  // other profiling modes like "only trace the ledger interactions" might also
  // be useful.
  sealed abstract class ProfilingMode extends Product with Serializable
  case object NoProfile extends ProfilingMode
  case object FullProfile extends ProfilingMode

  sealed abstract class StackTraceMode extends Product with Serializable
  case object NoStackTrace extends StackTraceMode
  case object FullStackTrace extends StackTraceMode

  sealed abstract class PackageValidationMode extends Product with Serializable
  case object NoPackageValidation extends PackageValidationMode
  case object FullPackageValidation extends PackageValidationMode

  final case class Config(
      allowedLanguageVersions: VersionRange[LanguageVersion],
      packageValidation: PackageValidationMode,
      profiling: ProfilingMode,
      stacktracing: StackTraceMode,
  )

  object Config {
    val Default = Config(
      allowedLanguageVersions = LanguageVersion.StableVersions,
      packageValidation = FullPackageValidation,
      profiling = NoProfile,
      stacktracing = NoStackTrace,
    )
    val Dev = Config(
      allowedLanguageVersions = LanguageVersion.DevVersions,
      packageValidation = FullPackageValidation,
      profiling = NoProfile,
      stacktracing = NoStackTrace,
    )
  }

  /** Validates and Compiles all the definitions in the packages provided. Returns them in a Map.
    *
    * The packages do not need to be in any specific order, as long as they and all the packages
    * they transitively reference are in the [[packages]] in the compiler.
    */

  private[lf] def compilePackages(
      pkgInterface: PackageInterface,
      packages: Map[PackageId, Package],
      compilerConfig: Compiler.Config,
  ): Either[String, Map[t.SDefinitionRef, SDefinition]] = {
    val compiler = new Compiler(pkgInterface, compilerConfig)
    try {
      Right(packages.foldLeft(Map.empty[t.SDefinitionRef, SDefinition]) {
        case (acc, (pkgId, pkg)) =>
          acc ++ compiler.compilePackage(pkgId, pkg)
      })
    } catch {
      case CompilationError(msg) => Left(s"Compilation Error: $msg")
      case PackageNotFound(pkgId, context) =>
        Left(LookupError.MissingPackage.pretty(pkgId, context))
      case e: ValidationError => Left(e.pretty)
    }
  }

}

private[lf] final class Compiler(
    pkgInterface: PackageInterface,
    config: Compiler.Config,
) {

  import Compiler._

  // Compilation entry points...

  @throws[PackageNotFound]
  @throws[CompilationError]
  def unsafeCompile(cmds: ImmArray[Command]): t.SExpr = compileCommands(cmds)

  @throws[PackageNotFound]
  @throws[CompilationError]
  def unsafeCompileWithContractDisclosures(
      compiledCommands: t.SExpr,
      disclosures: ImmArray[DisclosedContract],
  ): t.SExpr =
    compileWithContractDisclosures(compiledCommands, disclosures)

  @throws[PackageNotFound]
  @throws[CompilationError]
  def unsafeCompileForReinterpretation(cmd: Command): t.SExpr =
    compileCommandForReinterpretation(cmd)

  @throws[PackageNotFound]
  @throws[CompilationError]
  def unsafeCompile(expr: Expr): t.SExpr = compileExp(expr)

  @throws[PackageNotFound]
  @throws[CompilationError]
  def unsafeClosureConvert(sexpr: s.SExpr): t.SExpr = pipeline(sexpr)

  @throws[PackageNotFound]
  @throws[CompilationError]
  @throws[ValidationError]
  def unsafeCompilePackage(
      pkgId: PackageId,
      pkg: Package,
  ): Iterable[(t.SDefinitionRef, SDefinition)] = compilePackage(pkgId, pkg)

  @throws[PackageNotFound]
  @throws[CompilationError]
  def unsafeCompileModule( // called by scenario-service
      pkgId: PackageId,
      module: Module,
  ): Iterable[(t.SDefinitionRef, SDefinition)] = compileModule(pkgId, module)

  @throws[PackageNotFound]
  @throws[CompilationError]
  def unsafeCompileInterfaceView(view: InterfaceView): t.SExpr = {
    val e0 = s.SEApp(
      s.SEBuiltin(SBViewInterface(view.interfaceId)),
      List(s.SEApp(s.SEBuiltin(SBToAnyContract(view.templateId)), List(s.SEValue(view.argument)))),
    )
    pipeline(e0)
  }

  private[this] val stablePackageIds = StablePackage.ids(config.allowedLanguageVersions)

  private[this] val logger = LoggerFactory.getLogger(this.getClass)

  // We add labels before and after flattening

  private[this] val withLabelS: (Profile.Label, s.SExpr) => s.SExpr =
    config.profiling match {
      case NoProfile => { (_, expr) =>
        expr
      }
      case FullProfile => { (label, expr) =>
        expr match {
          case s.SELabelClosure(_, expr1) => s.SELabelClosure(label, expr1)
          case _ => s.SELabelClosure(label, expr)
        }
      }
    }

  private[this] val withLabelT: (Profile.Label, t.SExpr) => t.SExpr =
    config.profiling match {
      case NoProfile => { (_, expr) =>
        expr
      }
      case FullProfile => { (label, expr) =>
        expr match {
          case t.SELabelClosure(_, expr1) => t.SELabelClosure(label, expr1)
          case _ => t.SELabelClosure(label, expr)
        }
      }
    }

  private[this] def app(f: s.SExpr, a: s.SExpr): s.SExpr = s.SEApp(f, List(a))

  private[this] def let(env: Env, bound: s.SExpr)(f: (Position, Env) => s.SExpr): s.SELet =
    f(env.nextPosition, env.pushVar) match {
      case s.SELet(bounds, body) =>
        s.SELet(bound :: bounds, body)
      case otherwise =>
        s.SELet(List(bound), otherwise)
    }

  private[this] def checkPreCondition(
      env: Env,
      templateId: Identifier,
      contract: s.SExpr,
  )(
      body: Env => s.SExpr
  ): s.SExpr = {
    let(env, s.SEApp(s.SEVal(t.TemplatePreConditionDefRef(templateId)), List(contract))) {
      (preConditionCheck, env) =>
        s.SECase(
          env.toSEVar(preConditionCheck),
          List(
            s.SCaseAlt(
              t.SCPPrimCon(PCTrue),
              body(env),
            ),
            s.SCaseAlt(
              t.SCPDefault,
              s.SEApp(
                s.SEBuiltin(SBTemplatePreconditionViolated(templateId)),
                List(contract),
              ),
            ),
          ),
        )
    }
  }

  private[this] def unaryFunction(env: Env)(f: (Position, Env) => s.SExpr): s.SEAbs =
    f(env.nextPosition, env.pushVar) match {
      case s.SEAbs(n, body) => s.SEAbs(n + 1, body)
      case otherwise => s.SEAbs(1, otherwise)
    }

  private[this] def labeledUnaryFunction[L: Profile.LabelModule.Allowed](
      label: L with AnyRef,
      env: Env,
  )(
      body: (Position, Env) => s.SExpr
  ): s.SExpr =
    unaryFunction(env)((positions, env) => withLabelS(label, body(positions, env)))

  private[this] def topLevelFunction[SDefRef <: t.SDefinitionRef: LabelModule.Allowed](
      ref: SDefRef
  )(body: s.SExpr): (SDefRef, SDefinition) =
    ref -> SDefinition(pipeline(withLabelS(ref, body)))

  private val Pos1 = Env.Empty.nextPosition
  private val Env1 = Env.Empty.pushVar
  private val Pos2 = Env1.nextPosition
  private val Env2 = Env1.pushVar
  private val Pos3 = Env2.nextPosition
  private val Env3 = Env2.pushVar
  private val Pos4 = Env3.nextPosition
  private val Env4 = Env3.pushVar

  private[this] def fun1(body: (Position, Env) => s.SExpr): s.SExpr =
    s.SEAbs(1, body(Pos1, Env1))

  private[this] def fun2(body: (Position, Position, Env) => s.SExpr): s.SExpr =
    s.SEAbs(2, body(Pos1, Pos2, Env2))

  private[this] def fun3(body: (Position, Position, Position, Env) => s.SExpr): s.SExpr =
    s.SEAbs(3, body(Pos1, Pos2, Pos3, Env3))

  private[this] def fun4(body: (Position, Position, Position, Position, Env) => s.SExpr): s.SExpr =
    s.SEAbs(4, body(Pos1, Pos2, Pos3, Pos4, Env4))

  private[this] def topLevelFunction1[SDefRef <: t.SDefinitionRef: LabelModule.Allowed](
      ref: SDefRef
  )(
      body: (Position, Env) => s.SExpr
  ): (SDefRef, SDefinition) =
    topLevelFunction(ref)(fun1(body))

  private[this] def unlabelledTopLevelFunction2(ref: t.SDefinitionRef)(
      body: (Position, Position, Env) => s.SExpr
  ): (t.SDefinitionRef, SDefinition) =
    ref -> SDefinition(pipeline(fun2(body)))

  private[this] def topLevelFunction2[SDefRef <: t.SDefinitionRef: LabelModule.Allowed](
      ref: SDefRef
  )(
      body: (Position, Position, Env) => s.SExpr
  ): (SDefRef, SDefinition) =
    topLevelFunction(ref)(fun2(body))

  private[this] def topLevelFunction3[SDefRef <: t.SDefinitionRef: LabelModule.Allowed](
      ref: SDefRef
  )(
      body: (Position, Position, Position, Env) => s.SExpr
  ): (SDefRef, SDefinition) =
    topLevelFunction(ref)(fun3(body))

  private[this] def topLevelFunction4[SDefRef <: t.SDefinitionRef: LabelModule.Allowed](
      ref: SDefRef
  )(
      body: (Position, Position, Position, Position, Env) => s.SExpr
  ): (SDefRef, SDefinition) =
    topLevelFunction(ref)(fun4(body))

  val phaseOne: PhaseOne = {
    val config1 =
      PhaseOne.Config(
        profiling = config.profiling,
        stacktracing = config.stacktracing,
      )
    new PhaseOne(pkgInterface, config1)
  }

  // "translate" indicates the first stage of compilation only (producing: SExpr0)
  // "compile" indicates the full compilation pipeline (producing: SExpr)
  private[this] def translateExp(env: Env, expr0: Expr): s.SExpr =
    phaseOne.translateFromLF(env, expr0)

  private[this] def compileExp(expr: Expr): t.SExpr =
    pipeline(translateExp(Env.Empty, expr))

  private[this] def compileCommands(cmds: ImmArray[Command]): t.SExpr =
    pipeline(translateCommands(Env.Empty, cmds))

  private[this] def compileWithContractDisclosures(
      sexpr: t.SExpr,
      disclosures: ImmArray[DisclosedContract],
  ): t.SExpr = {
    val disclosureLambda = pipeline(
      translateContractDisclosureLambda(Env.Empty, disclosures)
    )
    t.SELet1(
      t.SEApp(disclosureLambda, Array(SValue.Unit)),
      sexpr,
    )
  }

  private[this] def compileCommandForReinterpretation(cmd: Command): t.SExpr =
    pipeline(translateCommandForReinterpretation(cmd))

  // speedy compilation phases 2,3 (i.e post translate-from-LF)
  private[this] def pipeline(sexpr: s.SExpr): t.SExpr =
    flattenToAnf(closureConvert(sexpr))

  private[this] def compileModule(
      pkgId: PackageId,
      module: Module,
  ): Iterable[(t.SDefinitionRef, SDefinition)] = {
    val builder = Iterable.newBuilder[(t.SDefinitionRef, SDefinition)]
    def addDef(binding: (t.SDefinitionRef, SDefinition)): Unit = discard(builder += binding)

    module.exceptions.foreach { case (defName, GenDefException(message)) =>
      val ref = t.ExceptionMessageDefRef(Identifier(pkgId, QualifiedName(module.name, defName)))
      builder += (ref -> SDefinition(withLabelT(ref, compileExp(message))))
    }

    module.definitions.foreach {
      case (defName, DValue(_, body, _)) =>
        val ref = t.LfDefRef(Identifier(pkgId, QualifiedName(module.name, defName)))
        builder += (ref -> SDefinition(withLabelT(ref, compileExp(body))))
      case _ =>
    }

    module.templates.foreach { case (tmplName, tmpl) =>
      val tmplId = Identifier(pkgId, QualifiedName(module.name, tmplName))
      addDef(compileCreate(tmplId, tmpl))
      addDef(compileFetchTemplate(tmplId, tmpl))
      addDef(compileTemplatePreCondition(tmplId, tmpl))
      addDef(compileAgreementText(tmplId, tmpl))
      addDef(compileSignatories(tmplId, tmpl))
      addDef(compileObservers(tmplId, tmpl))
      addDef(compileToCachedContract(tmplId, tmpl))
      tmpl.implements.values.foreach { impl =>
        compileInterfaceInstance(
          parent = tmplId,
          tmplParam = tmpl.param,
          interfaceId = impl.interfaceId,
          templateId = tmplId,
          interfaceInstanceBody = impl.body,
        ).foreach(addDef)
      }

      tmpl.choices.values.foreach(x => addDef(compileTemplateChoice(tmplId, tmpl, x)))

      tmpl.key.foreach { tmplKey =>
        addDef(compileContractKeyWithMaintainers(tmplId, tmpl, tmplKey))
        addDef(compileFetchByKey(tmplId, tmpl, tmplKey))
        addDef(compileLookupByKey(tmplId, tmplKey))
        tmpl.choices.values.foreach(x => addDef(compileChoiceByKey(tmplId, tmpl, tmplKey, x)))
      }
    }

    module.interfaces.foreach { case (ifaceName, iface) =>
      val ifaceId = Identifier(pkgId, QualifiedName(module.name, ifaceName))
      addDef(compileFetchInterface(ifaceId))
      iface.choices.values.foreach(choice =>
        addDef(compileInterfaceChoice(ifaceId, iface.param, choice))
      )
      iface.coImplements.values.foreach { coimpl =>
        compileInterfaceInstance(
          parent = ifaceId,
          tmplParam = iface.param,
          interfaceId = ifaceId,
          templateId = coimpl.templateId,
          interfaceInstanceBody = coimpl.body,
        ).foreach(addDef)
      }
    }

    builder.result()
  }

  /** Validates and compiles all the definitions in the package provided.
    *
    * Fails with [[PackageNotFound]] if the package or any of the packages it refers
    * to are not in the [[pkgInterface]].
    *
    * @throws ValidationError if the package does not pass validations.
    */
  private def compilePackage(
      pkgId: PackageId,
      pkg: Package,
  ): Iterable[(t.SDefinitionRef, SDefinition)] = {
    logger.trace(s"compilePackage: Compiling $pkgId...")

    val t0 = Time.Timestamp.now()

    pkgInterface.lookupPackage(pkgId) match {
      case Right(pkg)
          if !stablePackageIds.contains(pkgId) && !config.allowedLanguageVersions
            .contains(pkg.languageVersion) =>
        throw LanguageVersionError(pkgId, pkg.languageVersion, config.allowedLanguageVersions)
      case _ =>
    }

    config.packageValidation match {
      case Compiler.NoPackageValidation =>
      case Compiler.FullPackageValidation =>
        Validation.checkPackage(pkgInterface, pkgId, pkg).left.foreach(throw _)
    }

    val t1 = Time.Timestamp.now()

    val result = pkg.modules.values.flatMap(compileModule(pkgId, _))

    val t2 = Time.Timestamp.now()
    logger.trace(
      s"compilePackage: $pkgId ready, typecheck=${(t1.micros - t0.micros) / 1000}ms, compile=${(t2.micros - t1.micros) / 1000}ms"
    )

    result
  }

  private[this] val KeyWithMaintainersStruct =
    SBStructCon(Struct.assertFromSeq(List(keyFieldName, maintainersFieldName).zipWithIndex))

  private[this] def translateKeyWithMaintainers(
      env: Env,
      keyPos: Position,
      tmplKey: TemplateKey,
  ): s.SExpr =
    KeyWithMaintainersStruct(
      env.toSEVar(keyPos),
      app(translateExp(env, tmplKey.maintainers), env.toSEVar(keyPos)),
    )

  private[this] def translateChoiceBody(
      env: Env,
      typeId: TypeConName,
      tmpl: Template,
      choice: TemplateChoice,
  )(
      choiceArgPos: Position,
      cidPos: Position,
      mbKey: Option[Position], // defined for byKey operation
      tokenPos: Position,
  ): s.SExpr =
    let(
      env,
      SBCastAnyContract(typeId)(
        env.toSEVar(cidPos),
        SBFetchAny(
          env.toSEVar(cidPos),
          mbKey.fold(s.SEValue.None: s.SExpr)(pos => SBSome(env.toSEVar(pos))),
        ),
      ),
    ) { (tmplArgPos, _env) =>
      val env =
        _env.bindExprVar(tmpl.param, tmplArgPos).bindExprVar(choice.argBinder._1, choiceArgPos)

      let(
        env,
        SBUBeginExercise(
          templateId = typeId,
          interfaceId = None,
          choiceId = choice.name,
          consuming = choice.consuming,
          byKey = mbKey.isDefined,
        )(
          env.toSEVar(choiceArgPos),
          env.toSEVar(cidPos),
          s.SEPreventCatch(translateExp(env, choice.controllers)),
          choice.choiceObservers match {
            case Some(observers) => s.SEPreventCatch(translateExp(env, observers))
            case None => s.SEValue.EmptyList
          },
        ),
      ) { (_, _env) =>
        val env = _env.bindExprVar(choice.selfBinder, cidPos)
        s.SEScopeExercise(
          app(translateExp(env, choice.update), env.toSEVar(tokenPos))
        )
      }
    }

  // TODO https://github.com/digital-asset/daml/issues/12051
  //   Try to factorise this with compileChoiceBody above.
  private[this] def translateInterfaceChoiceBody(
      env: Env,
      ifaceId: TypeConName,
      param: ExprVarName,
      choice: TemplateChoice,
  )(
      guardPos: Position,
      cidPos: Position,
      choiceArgPos: Position,
      tokenPos: Position,
  ): s.SExpr =
    let(
      env,
      SBCastAnyInterface(ifaceId)(
        env.toSEVar(cidPos),
        SBFetchAny(env.toSEVar(cidPos), s.SEValue.None),
      ),
    ) { (payloadPos, env) =>
      let(
        env,
        s.SEPreventCatch(SBViewInterface(ifaceId)(env.toSEVar(payloadPos))),
      ) { (_, _env) =>
        val env = _env.bindExprVar(param, payloadPos).bindExprVar(choice.argBinder._1, choiceArgPos)
        let(
          env,
          SBApplyChoiceGuard(choice.name, Some(ifaceId))(
            env.toSEVar(guardPos),
            env.toSEVar(payloadPos),
            env.toSEVar(cidPos),
          ),
        ) { (_, env) =>
          let(
            env,
            SBResolveSBUBeginExercise(
              interfaceId = ifaceId,
              choiceName = choice.name,
              consuming = choice.consuming,
              byKey = false,
            )(
              env.toSEVar(payloadPos),
              env.toSEVar(choiceArgPos),
              env.toSEVar(cidPos),
              s.SEPreventCatch(translateExp(env, choice.controllers)),
              choice.choiceObservers match {
                case Some(observers) => s.SEPreventCatch(translateExp(env, observers))
                case None => s.SEValue.EmptyList
              },
            ),
          ) { (_, _env) =>
            val env = _env.bindExprVar(choice.selfBinder, cidPos)
            s.SEScopeExercise(app(translateExp(env, choice.update), env.toSEVar(tokenPos)))
          }
        }
      }
    }

  private[this] def compileInterfaceChoice(
      ifaceId: TypeConName,
      param: ExprVarName,
      choice: TemplateChoice,
  ): (t.SDefinitionRef, SDefinition) =
    topLevelFunction4(t.InterfaceChoiceDefRef(ifaceId, choice.name)) {
      (guardPos, cidPos, choiceArgPos, tokenPos, env) =>
        translateInterfaceChoiceBody(env, ifaceId, param, choice)(
          guardPos,
          cidPos,
          choiceArgPos,
          tokenPos,
        )
    }

  private[this] def compileTemplateChoice(
      tmplId: TypeConName,
      tmpl: Template,
      choice: TemplateChoice,
  ): (t.SDefinitionRef, SDefinition) =
    // Compiles a choice into:
    // ChoiceDefRef(SomeTemplate, SomeChoice) = \<actors> <cid> <choiceArg> <token> ->
    //   let targ = fetch(tmplId) <cid>
    //       _ = $beginExercise(tmplId, choice.name, choice.consuming, false) <choiceArg> <cid> <actors> [tmpl.signatories] [tmpl.observers] [choice.controllers] [tmpl.key]
    //       <retValue> = [update] <token>
    //       _ = $endExercise[tmplId] <retValue>
    //   in <retValue>
    topLevelFunction3(t.TemplateChoiceDefRef(tmplId, choice.name)) {
      (cidPos, choiceArgPos, tokenPos, env) =>
        translateChoiceBody(env, tmplId, tmpl, choice)(
          choiceArgPos,
          cidPos,
          None,
          tokenPos,
        )
    }

  /** Compile a choice into a top-level function for exercising that choice */
  private[this] def compileChoiceByKey(
      tmplId: TypeConName,
      tmpl: Template,
      tmplKey: TemplateKey,
      choice: TemplateChoice,
  ): (t.SDefinitionRef, SDefinition) =
    // Compiles a choice into:
    // ChoiceByKeyDefRef(SomeTemplate, SomeChoice) = \ <actors> <key> <choiceArg> <token> ->
    //    let <keyWithM> = { key = <key> ; maintainers = [tmpl.maintainers] <key> }
    //        <cid> = $fetchKey(tmplId) <keyWithM>
    //        targ = fetch <cid>
    //       _ = $beginExercise[tmplId,  choice.name, choice.consuming, true] <choiceArg> <cid> <actors> [tmpl.signatories] [tmpl.observers] [choice.controllers] (Some <keyWithM>)
    //       <retValue> = <updateE> <token>
    //       _ = $endExercise[tmplId] <retValue>
    //   in  <retValue>
    topLevelFunction3(t.ChoiceByKeyDefRef(tmplId, choice.name)) {
      (keyPos, choiceArgPos, tokenPos, env) =>
        let(env, translateKeyWithMaintainers(env, keyPos, tmplKey)) { (keyWithMPos, env) =>
          let(env, SBUFetchKey(tmplId)(env.toSEVar(keyWithMPos))) { (cidPos, env) =>
            translateChoiceBody(env, tmplId, tmpl, choice)(
              choiceArgPos,
              cidPos,
              Some(keyWithMPos),
              tokenPos,
            )
          }
        }
    }

  @nowarn("msg=parameter value tokenPos.* is never used")
  private[this] def translateFetchTemplateBody(env: Env, tmplId: Identifier, tmpl: Template)(
      cidPos: Position,
      mbKey: Option[Position], // defined for byKey operation
      tokenPos: Position,
  ): s.SExpr =
    let(
      env,
      SBCastAnyContract(tmplId)(
        env.toSEVar(cidPos),
        SBFetchAny(
          env.toSEVar(cidPos),
          mbKey.fold(s.SEValue.None: s.SExpr)(pos => SBSome(env.toSEVar(pos))),
        ),
      ),
    ) { (tmplArgPos, _env) =>
      val env = _env.bindExprVar(tmpl.param, tmplArgPos)
      let(
        env,
        SBUInsertFetchNode(tmplId, byKey = mbKey.isDefined)(env.toSEVar(cidPos)),
      ) { (_, env) =>
        env.toSEVar(tmplArgPos)
      }
    }

  private[this] def compileFetchTemplate(
      tmplId: Identifier,
      tmpl: Template,
  ): (t.SDefinitionRef, SDefinition) =
    // compile a template to
    // FetchDefRef(tmplId) = \ <coid> <token> ->
    //   let <tmplArg> = $fetch(tmplId) <coid>
    //       _ = $insertFetch(tmplId, false) coid [tmpl.signatories] [tmpl.observers] [tmpl.key]
    //   in <tmplArg>
    topLevelFunction2(t.FetchTemplateDefRef(tmplId)) { (cidPos, tokenPos, env) =>
      translateFetchTemplateBody(env, tmplId, tmpl)(cidPos, None, tokenPos)
    }

  private[this] def compileFetchInterface(ifaceId: Identifier): (t.SDefinitionRef, SDefinition) =
    topLevelFunction2(t.FetchInterfaceDefRef(ifaceId)) { (cidPos, _, env) =>
      let(
        env,
        SBCastAnyInterface(ifaceId)(
          env.toSEVar(cidPos),
          SBFetchAny(env.toSEVar(cidPos), s.SEValue.None),
        ),
      ) { (payloadPos, env) =>
        let(
          env,
          s.SEPreventCatch(SBViewInterface(ifaceId)(env.toSEVar(payloadPos))),
        ) { (_, env) =>
          let(
            env,
            SBResolveSBUInsertFetchNode(env.toSEVar(payloadPos), env.toSEVar(cidPos)),
          ) { (_, env) =>
            env.toSEVar(payloadPos)
          }
        }
      }
    }

  private[this] def compileAgreementText(
      tmplId: Identifier,
      tmpl: Template,
  ): (t.SDefinitionRef, SDefinition) =
    topLevelFunction1(t.AgreementTextDefRef(tmplId)) { (tmplArgPos, env) =>
      translateExp(env.bindExprVar(tmpl.param, tmplArgPos), tmpl.agreementText)
    }

  private[this] def compileSignatories(
      tmplId: Identifier,
      tmpl: Template,
  ): (t.SDefinitionRef, SDefinition) =
    topLevelFunction1(t.SignatoriesDefRef(tmplId)) { (tmplArgPos, env) =>
      translateExp(env.bindExprVar(tmpl.param, tmplArgPos), tmpl.signatories)
    }

  private[this] def compileObservers(
      tmplId: Identifier,
      tmpl: Template,
  ): (t.SDefinitionRef, SDefinition) =
    topLevelFunction1(t.ObserversDefRef(tmplId)) { (tmplArgPos, env) =>
      translateExp(env.bindExprVar(tmpl.param, tmplArgPos), tmpl.observers)
    }

  private[this] def compileToCachedContract(
      tmplId: Identifier,
      tmpl: Template,
  ): (t.SDefinitionRef, SDefinition) =
    unlabelledTopLevelFunction2(t.ToCachedContractDefRef(tmplId)) { (tmplArgPos, mbKeyPos, env) =>
      SBuildCachedContract(
        s.SEValue(STypeRep(TTyCon(tmplId))),
        env.toSEVar(tmplArgPos),
        t.AgreementTextDefRef(tmplId)(env.toSEVar(tmplArgPos)),
        t.SignatoriesDefRef(tmplId)(env.toSEVar(tmplArgPos)),
        t.ObserversDefRef(tmplId)(env.toSEVar(tmplArgPos)),
        tmpl.key match {
          case None =>
            s.SEValue.None
          case Some(tmplKey) =>
            s.SECase(
              env.toSEVar(mbKeyPos),
              List(
                s.SCaseAlt(
                  t.SCPNone,
                  let(env, translateExp(env.bindExprVar(tmpl.param, tmplArgPos), tmplKey.body)) {
                    (keyPos, env) =>
                      SBSome(translateKeyWithMaintainers(env, keyPos, tmplKey))
                  },
                ),
                s.SCaseAlt(t.SCPDefault, env.toSEVar(mbKeyPos)),
              ),
            )
        },
      )
    }

  private[this] val UnitDef = SDefinition(t.SEValue.Unit)

  // Compile the contents of an interface instance, including a witness for
  // the existence of said interface instance.
  private[this] def compileInterfaceInstance(
      parent: TypeConName,
      tmplParam: Name,
      interfaceId: TypeConName,
      templateId: TypeConName,
      interfaceInstanceBody: InterfaceInstanceBody,
  ): Iterable[(t.SDefinitionRef, SDefinition)] = {
    val builder = Iterable.newBuilder[(t.SDefinitionRef, SDefinition)]
    def addDef(binding: (t.SDefinitionRef, SDefinition)): Unit = discard(builder += binding)

    val interfaceInstanceDefRef = t.InterfaceInstanceDefRef(parent, interfaceId, templateId)
    addDef(interfaceInstanceDefRef -> UnitDef)

    interfaceInstanceBody.methods.values.foreach(method =>
      addDef(compileInterfaceInstanceMethod(interfaceInstanceDefRef, tmplParam, method))
    )

    addDef(
      compileInterfaceInstanceView(interfaceInstanceDefRef, tmplParam, interfaceInstanceBody.view)
    )

    builder.result()
  }

  // Compile the implementation of an interface method.
  private[this] def compileInterfaceInstanceMethod(
      interfaceInstanceDefRef: t.InterfaceInstanceDefRef,
      tmplParam: Name,
      method: InterfaceInstanceMethod,
  ): (t.SDefinitionRef, SDefinition) = {
    topLevelFunction1(t.InterfaceInstanceMethodDefRef(interfaceInstanceDefRef, method.name)) {
      (tmplArgPos, env) =>
        translateExp(env.bindExprVar(tmplParam, tmplArgPos), method.value)
    }
  }

  // Compile the implementation of an interface view.
  private[this] def compileInterfaceInstanceView(
      interfaceInstanceDefRef: t.InterfaceInstanceDefRef,
      tmplParam: Name,
      body: Expr,
  ): (t.SDefinitionRef, SDefinition) = {
    topLevelFunction1(t.InterfaceInstanceViewDefRef(interfaceInstanceDefRef)) { (tmplArgPos, env) =>
      translateExp(env.bindExprVar(tmplParam, tmplArgPos), body)
    }
  }

  private[this] def translateCreateBody(
      templateId: Identifier,
      template: Template,
      contractPos: Position,
      env: Env,
  ): s.SExpr = {
    val env2 = env.bindExprVar(template.param, contractPos)

    checkPreCondition(
      env2,
      templateId,
      env2.toSEVar(contractPos),
    ) { (env: Env) =>
      SBUCreate(t.ToCachedContractDefRef(templateId)(env.toSEVar(contractPos), s.SEValue.None))
    }
  }

  private[this] def compileCreate(
      tmplId: Identifier,
      tmpl: Template,
  ): (t.SDefinitionRef, SDefinition) = {
    // Translates 'create Foo with <params>' into:
    // CreateDefRef(tmplId) = \ <tmplArg> <token> ->
    //   let _ = checkPreCondition(tmplId, <tmplArg>)
    //   in $create <tmplArg> [tmpl.agreementText] [tmpl.signatories] [tmpl.observers] [tmpl.key]
    topLevelFunction2(t.CreateDefRef(tmplId))((tmplArgPos, _, env) =>
      translateCreateBody(tmplId, tmpl, tmplArgPos, env)
    )
  }

  private[this] def compileTemplatePreCondition(
      tmplId: Identifier,
      tmpl: Template,
  ): (t.SDefinitionRef, SDefinition) = {
    topLevelFunction1(t.TemplatePreConditionDefRef(tmplId))((tmplArgPos, env) =>
      translateExp(env.bindExprVar(tmpl.param, tmplArgPos), tmpl.precond)
    )
  }

  private[this] def translateCreateAndExercise(
      env: Env,
      tmplId: Identifier,
      createArg: SValue,
      choiceId: ChoiceName,
      choiceArg: SValue,
  ): s.SExpr =
    labeledUnaryFunction(Profile.CreateAndExerciseLabel(tmplId, choiceId), env) { (tokenPos, env) =>
      let(env, t.CreateDefRef(tmplId)(s.SEValue(createArg), env.toSEVar(tokenPos))) {
        (cidPos, env) =>
          t.TemplateChoiceDefRef(tmplId, choiceId)(
            env.toSEVar(cidPos),
            s.SEValue(choiceArg),
            env.toSEVar(tokenPos),
          )
      }
    }

  private[this] def compileContractKeyWithMaintainers(
      tmplId: Identifier,
      tmpl: Template,
      tmplKey: TemplateKey,
  ): (t.SDefinitionRef, SDefinition) =
    // compile a template with key into:
    // ContractKeyWithMaintainersDefRef(tmplId) = \ <tmplArg> ->
    //   let <key> = tmplKey.body(<tmplArg>)
    //   in { key = <key> ; maintainers = [tmplKey.maintainers] <key> }
    topLevelFunction1(t.ContractKeyWithMaintainersDefRef(tmplId)) { (tmplArg, env) =>
      let(env, translateExp(env.bindExprVar(tmpl.param, tmplArg), tmplKey.body)) { (keyPos, env) =>
        translateKeyWithMaintainers(env, keyPos, tmplKey)
      }
    }

  private[this] def compileLookupByKey(
      tmplId: Identifier,
      tmplKey: TemplateKey,
  ): (t.SDefinitionRef, SDefinition) =
    // compile a template with key into
    // LookupByKeyDefRef(tmplId) = \ <key> <token> ->
    //    let <keyWithM> = { key = <key> ; maintainers = [tmplKey.maintainers] <key> }
    //        <mbCid> = $lookupKey(tmplId) <keyWithM>
    //        _ = $insertLookup(tmplId> <keyWithM> <mbCid>
    //    in <mbCid>
    topLevelFunction2(t.LookupByKeyDefRef(tmplId)) { (keyPos, _, env) =>
      let(env, translateKeyWithMaintainers(env, keyPos, tmplKey)) { (keyWithMPos, env) =>
        let(env, SBULookupKey(tmplId)(env.toSEVar(keyWithMPos))) { (maybeCidPos, env) =>
          let(
            env,
            SBUInsertLookupNode(tmplId)(env.toSEVar(keyWithMPos), env.toSEVar(maybeCidPos)),
          ) { (_, env) =>
            env.toSEVar(maybeCidPos)
          }
        }
      }
    }

  private[this] val FetchByKeyResult =
    SBStructCon(Struct.assertFromSeq(List(contractIdFieldName, contractFieldName).zipWithIndex))

  @inline
  private[this] def compileFetchByKey(
      tmplId: TypeConName,
      tmpl: Template,
      tmplKey: TemplateKey,
  ): (t.SDefinitionRef, SDefinition) =
    // compile a template with key into
    // FetchByKeyDefRef(tmplId) = \ <key> <token> ->
    //    let <keyWithM> = { key = <key> ; maintainers = [tmpl.maintainers] <key> }
    //        <coid> = $fetchKey(tmplId) <keyWithM>
    //        <contract> = $fetch(tmplId) <coid>
    //        _ = $insertFetch <coid> <signatories> <observers> (Some <keyWithM> )
    //    in { contractId: ContractId Foo, contract: Foo }
    topLevelFunction2(t.FetchByKeyDefRef(tmplId)) { (keyPos, tokenPos, env) =>
      let(env, translateKeyWithMaintainers(env, keyPos, tmplKey)) { (keyWithMPos, env) =>
        let(env, SBUFetchKey(tmplId)(env.toSEVar(keyWithMPos))) { (cidPos, env) =>
          let(
            env,
            translateFetchTemplateBody(env, tmplId, tmpl)(cidPos, Some(keyWithMPos), tokenPos),
          ) { (contractPos, env) =>
            FetchByKeyResult(env.toSEVar(cidPos), env.toSEVar(contractPos))
          }
        }
      }
    }

  private[this] def translateCommand(env: Env, cmd: Command): s.SExpr = cmd match {
    case Command.Create(templateId, argument) =>
      t.CreateDefRef(templateId)(s.SEValue(argument))
    case Command.ExerciseTemplate(templateId, contractId, choiceId, argument) =>
      t.TemplateChoiceDefRef(templateId, choiceId)(s.SEValue(contractId), s.SEValue(argument))
    case Command.ExerciseInterface(interfaceId, contractId, choiceId, argument) =>
      t.InterfaceChoiceDefRef(interfaceId, choiceId)(
        s.SEBuiltin(SBGuardConstTrue),
        s.SEValue(contractId),
        s.SEValue(argument),
      )
    case Command.ExerciseByKey(templateId, contractKey, choiceId, argument) =>
      t.ChoiceByKeyDefRef(templateId, choiceId)(s.SEValue(contractKey), s.SEValue(argument))
    case Command.FetchTemplate(templateId, coid) =>
      t.FetchTemplateDefRef(templateId)(s.SEValue(coid))
    case Command.FetchInterface(interfaceId, coid) =>
      t.FetchInterfaceDefRef(interfaceId)(s.SEValue(coid))
    case Command.FetchByKey(templateId, key) =>
      t.FetchByKeyDefRef(templateId)(s.SEValue(key))
    case Command.CreateAndExercise(templateId, createArg, choice, choiceArg) =>
      translateCreateAndExercise(
        env,
        templateId,
        createArg,
        choice,
        choiceArg,
      )
    case Command.LookupByKey(templateId, contractKey) =>
      t.LookupByKeyDefRef(templateId)(s.SEValue(contractKey))
  }

  private val SEUpdatePureUnit = unaryFunction(Env.Empty)((_, _) => s.SEValue.Unit)

  private[this] val handleEverything: s.SExpr = SBSome(SEUpdatePureUnit)

  private[this] def catchEverything(e: s.SExpr): s.SExpr =
    unaryFunction(Env.Empty) { (tokenPos, env0) =>
      s.SETryCatch(
        app(e, env0.toSEVar(tokenPos)), {
          val binderPos = env0.nextPosition
          val env1 = env0.pushVar
          SBTryHandler(handleEverything, env1.toSEVar(binderPos), env1.toSEVar(tokenPos))
        },
      )
    }

  private[this] def translateCommandForReinterpretation(cmd: Command): s.SExpr =
    catchEverything(translateCommand(Env.Empty, cmd))

  private[this] def translateCommands(env: Env, bindings: ImmArray[Command]): s.SExpr =
    // commands are compiled similarly to update block - see compileBlock
    bindings.toList match {
      case Nil =>
        SEUpdatePureUnit
      case first :: rest =>
        let(env, translateCommand(env, first)) { (firstPos, env) =>
          unaryFunction(env) { (tokenPos, env) =>
            let(env, app(env.toSEVar(firstPos), env.toSEVar(tokenPos))) { (_, _env) =>
              // we cannot process `rest` recursively without exposing ourselves to stack overflow.
              var env = _env
              val exprs = rest.map { cmd =>
                val expr = app(translateCommand(env, cmd), env.toSEVar(tokenPos))
                env = env.pushVar
                expr
              }
              s.SELet(exprs, s.SEValue.Unit)
            }
          }
        }
    }

  private[this] def translateDisclosedContractWithTemplateChecked(
      env: Env,
      templateId: Identifier,
      disclosedContract: DisclosedContract,
  ): s.SExpr = {
    let(env, s.SEApp(s.SEBuiltin(SBCheckTemplateKey(templateId)), List(s.SEValue.Unit))) {
      (templateKeyCheck, env) =>
        val contract = s.SEValue(disclosedContract.argument)

        s.SECase(
          env.toSEVar(templateKeyCheck),
          List(
            s.SCaseAlt(
              t.SCPPrimCon(PCTrue),
              let(
                env,
                s.SEApp(
                  s.SEVal(ContractKeyWithMaintainersDefRef(templateId)),
                  List(contract),
                ),
              ) { (contractPos, env) =>
                let(env, s.SEApp(s.SEBuiltin(SBSome), List(env.toSEVar(contractPos)))) {
                  (optionalContractPos, env) =>
                    s.SEApp(
                      s.SEVal(ToCachedContractDefRef(templateId)),
                      List(contract, env.toSEVar(optionalContractPos)),
                    )
                }
              },
            ),
            s.SCaseAlt(
              t.SCPDefault,
              s.SEApp(
                s.SEVal(ToCachedContractDefRef(templateId)),
                List(contract, s.SEValue.None),
              ),
            ),
          ),
        )
    }
  }

  private[this] def translateDisclosedContract(
      env: Env,
      disclosedContract: DisclosedContract,
  ): s.SExpr = {
    val templateId = disclosedContract.templateId
    val contract = s.SEValue(disclosedContract.argument)

    let(env, s.SEApp(s.SEBuiltin(SBCheckTemplate(templateId)), List(s.SEValue.Unit))) {
      (templateCheck, env) =>
        s.SECase(
          env.toSEVar(templateCheck),
          List(
            s.SCaseAlt(
              t.SCPPrimCon(PCTrue),
              checkPreCondition(
                env,
                templateId,
                contract,
              ) { (env: Env) =>
                translateDisclosedContractWithTemplateChecked(env, templateId, disclosedContract)
              },
            ),
            s.SCaseAlt(
              t.SCPDefault,
              s.SEApp(
                s.SEBuiltin(SBCrash(s"Template $templateId does not exist and it should")),
                List(s.SEValue.Unit),
              ),
            ),
          ),
        )
    }
  }

  private[this] def translateContractDisclosureLambda(
      env: Env,
      disclosures: ImmArray[DisclosedContract],
  ): s.SExpr = {
    // The next free environment variable will be the bound variable in the contract disclosure lambda
    val baseIndex = env.nextPosition.idx

    s.SEAbs(
      1,
      s.SELet(
        disclosures.toList.zipWithIndex.flatMap { case (disclosedContract, offset) =>
          // Let bounded variables occur after the contract disclosure bound variable - hence baseIndex+1
          // For each disclosed contract, we add 2 members to our let bounded list - hence 2*offset
          val contractIndex = baseIndex + 2 * offset + 1

          List(
            translateDisclosedContract(env.copy(contractIndex), disclosedContract),
            app(
              s.SEBuiltin(SBCacheDisclosedContract(disclosedContract.contractId.value)),
              s.SEVarLevel(contractIndex),
            ),
          )
        },
        s.SEVarLevel(baseIndex),
      ),
    )
  }
}
