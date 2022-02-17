// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.Ref._
import com.daml.lf.data.{ImmArray, Ref, Struct, Time}
import com.daml.lf.language.Ast._
import com.daml.lf.language.{LanguageVersion, LookupError, PackageInterface, StablePackages}
import com.daml.lf.speedy.Anf.flattenToAnf
import com.daml.lf.speedy.ClosureConversion.closureConvert
import com.daml.lf.speedy.PhaseOne.{Env, Position}
import com.daml.lf.speedy.Profile.LabelModule
import com.daml.lf.speedy.SBuiltin._
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.ValidateCompilation.validateCompilation
import com.daml.lf.speedy.{SExpr => t}
import com.daml.lf.speedy.{SExpr0 => s}
import com.daml.lf.validation.{EUnknownDefinition, Validation, ValidationError}
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

  case class CompilationError(error: String) extends RuntimeException(error, null, true, false)
  case class LanguageVersionError(
      packageId: Ref.PackageId,
      languageVersion: language.LanguageVersion,
      allowedLanguageVersions: VersionRange[language.LanguageVersion],
  ) extends RuntimeException(s"Disallowed language version $languageVersion", null, true, false)
  case class PackageNotFound(pkgId: PackageId, context: language.Reference)
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

  case class Config(
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
      interface: PackageInterface,
      packages: Map[PackageId, Package],
      compilerConfig: Compiler.Config,
  ): Either[String, Map[t.SDefinitionRef, SDefinition]] = {
    val compiler = new Compiler(interface, compilerConfig)
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
    interface: PackageInterface,
    config: Compiler.Config,
) {

  import Compiler._

  // Compilation entry points...

  @throws[PackageNotFound]
  @throws[CompilationError]
  def unsafeCompile(cmds: ImmArray[Command]): t.SExpr = compileCommands(cmds)

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
  def unsafeCompileModule( //called by scenario-service
      pkgId: PackageId,
      module: Module,
  ): Iterable[(t.SDefinitionRef, SDefinition)] = compileModule(pkgId, module)

  private[this] val stablePackageIds = StablePackages.ids(config.allowedLanguageVersions)

  private[this] val logger = LoggerFactory.getLogger(this.getClass)

  // We add labels before and after flattenning

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

  private[this] def app(f: s.SExpr, a: s.SExpr) = s.SEApp(f, List(a))

  private[this] def let(env: Env, bound: s.SExpr)(f: (Position, Env) => s.SExpr): s.SELet =
    f(env.nextPosition, env.pushVar) match {
      case s.SELet(bounds, body) =>
        s.SELet(bound :: bounds, body)
      case otherwise =>
        s.SELet(List(bound), otherwise)
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

  private val Position1 = Env.Empty.nextPosition
  private val Env1 = Env.Empty.pushVar
  private val Position2 = Env1.nextPosition
  private val Env2 = Env1.pushVar
  private val Position3 = Env2.nextPosition
  private val Env3 = Env2.pushVar
  private val Position4 = Env3.nextPosition
  private val Env4 = Env3.pushVar
  private val Position5 = Env4.nextPosition
  private val Env5 = Env4.pushVar

  private[this] def topLevelFunction1[SDefRef <: t.SDefinitionRef: LabelModule.Allowed](
      ref: SDefRef
  )(
      body: (Position, Env) => s.SExpr
  ): (SDefRef, SDefinition) =
    topLevelFunction(ref)(s.SEAbs(1, body(Position1, Env1)))

  private[this] def topLevelFunction2[SDefRef <: t.SDefinitionRef: LabelModule.Allowed](
      ref: SDefRef
  )(
      body: (Position, Position, Env) => s.SExpr
  ): (SDefRef, SDefinition) =
    topLevelFunction(ref)(s.SEAbs(2, body(Position1, Position2, Env2)))

  private[this] def topLevelFunction3[SDefRef <: t.SDefinitionRef: LabelModule.Allowed](
      ref: SDefRef
  )(
      body: (Position, Position, Position, Env) => s.SExpr
  ): (SDefRef, SDefinition) =
    topLevelFunction(ref)(s.SEAbs(3, body(Position1, Position2, Position3, Env3)))

  private[this] def topLevelFunction5[SDefRef <: t.SDefinitionRef: LabelModule.Allowed](
      ref: SDefRef
  )(
      body: (Position, Position, Position, Position, Position, Env) => s.SExpr
  ): (SDefRef, SDefinition) =
    topLevelFunction(ref)(
      s.SEAbs(5, body(Position1, Position2, Position3, Position4, Position5, Env5))
    )

  val phaseOne = {
    val config1 =
      PhaseOne.Config(
        profiling = config.profiling,
        stacktracing = config.stacktracing,
      )
    new PhaseOne(interface, config1)
  }

  // "translate" indicates the first stage of compilation only (producing: SExpr0)
  // "compile" indicates the full compilation pipeline (producing: SExpr)
  private[this] def translateExp(env: Env, expr0: Expr): s.SExpr =
    phaseOne.translateFromLF(env, expr0)

  private[this] def compileExp(expr: Expr): t.SExpr =
    pipeline(translateExp(Env.Empty, expr))

  private[this] def compileCommands(cmds: ImmArray[Command]): t.SExpr =
    pipeline(translateCommands(Env.Empty, cmds))

  private[this] def compileCommandForReinterpretation(cmd: Command): t.SExpr =
    pipeline(translateCommandForReinterpretation(cmd))

  // speedy compilation phases 2,3,4 (i.e post translate-from-LF)
  private[this] def pipeline(sexpr: s.SExpr): t.SExpr =
    validateCompilation(flattenToAnf(closureConvert(sexpr)))

  private[this] def compileModule(
      pkgId: PackageId,
      module: Module,
  ): Iterable[(t.SDefinitionRef, SDefinition)] = {
    val builder = Iterable.newBuilder[(t.SDefinitionRef, SDefinition)]
    def addDef(binding: (t.SDefinitionRef, SDefinition)) = discard(builder += binding)

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
      val identifier = Identifier(pkgId, QualifiedName(module.name, tmplName))
      addDef(compileCreate(identifier, tmpl))
      addDef(compileFetch(identifier, tmpl))
      addDef(compileKey(identifier, tmpl))
      addDef(compileSignatories(identifier, tmpl))
      addDef(compileObservers(identifier, tmpl))
      tmpl.implements.values.foreach { impl =>
        addDef(compileCreateByInterface(identifier, tmpl, impl.interfaceId))
        addDef(compileImplements(identifier, impl.interfaceId))
        impl.methods.values.foreach(method =>
          addDef(compileImplementsMethod(identifier, impl.interfaceId, method))
        )
      }

      tmpl.choices.values.foreach(x => addDef(compileChoice(identifier, tmpl, x)))

      tmpl.key.foreach { tmplKey =>
        addDef(compileFetchByKey(identifier, tmpl, tmplKey))
        addDef(compileLookupByKey(identifier, tmplKey))
        tmpl.choices.values.foreach(x => addDef(compileChoiceByKey(identifier, tmpl, tmplKey, x)))
      }
    }

    module.interfaces.foreach { case (ifaceName, iface) =>
      val identifier = Identifier(pkgId, QualifiedName(module.name, ifaceName))
      addDef(compileCreateInterface(identifier))
      addDef(compileFetchInterface(identifier))
      addDef(compileInterfacePrecond(identifier, iface.param, iface.precond))
      iface.fixedChoices.values.foreach { choice =>
        addDef(compileInterfaceChoice(identifier, iface.param, choice))
        addDef(compileInterfaceGuardedChoice(identifier, iface.param, choice))
      }
    }

    builder.result()
  }

  /** Validates and compiles all the definitions in the package provided.
    *
    * Fails with [[PackageNotFound]] if the package or any of the packages it refers
    * to are not in the [[interface]].
    *
    * @throws ValidationError if the package does not pass validations.
    */
  private def compilePackage(
      pkgId: PackageId,
      pkg: Package,
  ): Iterable[(t.SDefinitionRef, SDefinition)] = {
    logger.trace(s"compilePackage: Compiling $pkgId...")

    val t0 = Time.Timestamp.now()

    interface.lookupPackage(pkgId) match {
      case Right(pkg)
          if !stablePackageIds.contains(pkgId) && !config.allowedLanguageVersions
            .contains(pkg.languageVersion) =>
        throw LanguageVersionError(pkgId, pkg.languageVersion, config.allowedLanguageVersions)
      case _ =>
    }

    config.packageValidation match {
      case Compiler.NoPackageValidation =>
      case Compiler.FullPackageValidation =>
        Validation.checkPackage(interface, pkgId, pkg).left.foreach {
          case EUnknownDefinition(_, LookupError.MissingPackage(pkgId_, context)) =>
            logger.trace(s"compilePackage: Missing $pkgId_, requesting it...")
            throw PackageNotFound(pkgId_, context)
          case e =>
            throw e
        }
    }

    val t1 = Time.Timestamp.now()

    val result = pkg.modules.values.flatMap(compileModule(pkgId, _))

    val t2 = Time.Timestamp.now()
    logger.trace(
      s"compilePackage: $pkgId ready, typecheck=${(t1.micros - t0.micros) / 1000}ms, compile=${(t2.micros - t1.micros) / 1000}ms"
    )

    result
  }

  @inline
  private[this] def translateIdentity(env: Env) = s.SEAbs(1, s.SEVarLevel(env.position))

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

  private[this] def translateKeyWithMaintainers(
      env: Env,
      maybeTmplKey: Option[TemplateKey],
  ): s.SExpr =
    maybeTmplKey match {
      case None => s.SEValue.None
      case Some(tmplKey) =>
        let(env, translateExp(env, tmplKey.body)) { (keyPos, env) =>
          SBSome(translateKeyWithMaintainers(env, keyPos, tmplKey))
        }
    }

  private[this] def translateChoiceBody(
      env: Env,
      tmplId: TypeConName,
      tmpl: Template,
      choice: TemplateChoice,
  )(
      choiceArgPos: Position,
      cidPos: Position,
      mbKey: Option[Position], // defined for byKey operation
      tokenPos: Position,
  ) =
    let(
      env,
      SBUFetch(
        tmplId
      )(env.toSEVar(cidPos), mbKey.fold(s.SEValue.None: s.SExpr)(pos => SBSome(env.toSEVar(pos)))),
    ) { (tmplArgPos, _env) =>
      val env =
        _env.bindExprVar(tmpl.param, tmplArgPos).bindExprVar(choice.argBinder._1, choiceArgPos)
      let(
        env,
        SBUBeginExercise(
          tmplId,
          choice.name,
          choice.consuming,
          byKey = mbKey.isDefined,
          byInterface = None,
        )(
          env.toSEVar(choiceArgPos),
          env.toSEVar(cidPos),
          translateExp(env, choice.controllers),
          choice.choiceObservers match {
            case Some(observers) => translateExp(env, observers)
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
      choiceArgPos: Position,
      cidPos: Position,
      tokenPos: Position,
      typeRepPos: Position,
      guardPos: Position,
  ) =
    let(env, SBUFetchInterface(ifaceId)(env.toSEVar(cidPos), env.toSEVar(typeRepPos))) {
      (payloadPos, _env) =>
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
              choice.name,
              choice.consuming,
              byKey = false,
              ifaceId = ifaceId,
            )(
              env.toSEVar(payloadPos),
              env.toSEVar(choiceArgPos),
              env.toSEVar(cidPos),
              translateExp(env, choice.controllers),
              choice.choiceObservers match {
                case Some(observers) => translateExp(env, observers)
                case None => s.SEValue.EmptyList
              },
            ),
          ) { (_, _env) =>
            val env = _env.bindExprVar(choice.selfBinder, cidPos)
            s.SEScopeExercise(app(translateExp(env, choice.update), env.toSEVar(tokenPos)))
          }
        }
    }

  private[this] def compileInterfaceChoice(
      ifaceId: TypeConName,
      param: ExprVarName,
      choice: TemplateChoice,
  ): (t.SDefinitionRef, SDefinition) =
    topLevelFunction3(t.ChoiceDefRef(ifaceId, choice.name)) {
      (cidPos, choiceArgPos, tokenPos, env) =>
        let(env, s.SEValue(SOptional(None))) { (typeRepPos, env) =>
          let(env, s.SEBuiltin(SBGuardConstTrue)) { (guardPos, env) =>
            translateInterfaceChoiceBody(env, ifaceId, param, choice)(
              choiceArgPos,
              cidPos,
              tokenPos,
              typeRepPos,
              guardPos,
            )
          }
        }
    }

  private[this] def compileInterfaceGuardedChoice(
      ifaceId: TypeConName,
      param: ExprVarName,
      choice: TemplateChoice,
  ): (t.SDefinitionRef, SDefinition) =
    topLevelFunction5(t.GuardedChoiceDefRef(ifaceId, choice.name)) {
      (cidPos, choiceArgPos, typeRepPos, guardPos, tokenPos, env) =>
        translateInterfaceChoiceBody(env, ifaceId, param, choice)(
          choiceArgPos,
          cidPos,
          tokenPos,
          typeRepPos,
          guardPos,
        )
    }

  private[this] def compileChoice(
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
    topLevelFunction3(t.ChoiceDefRef(tmplId, choice.name)) {
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
    //        <cid> = $fecthKey(tmplId) <keyWithM>
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

  @nowarn("msg=parameter value tokenPos in method compileFetchBody is never used")
  private[this] def compileFetchBody(env: Env, tmplId: Identifier, tmpl: Template)(
      cidPos: Position,
      mbKey: Option[Position], //defined for byKey operation
      tokenPos: Position,
  ) =
    let(
      env,
      SBUFetch(
        tmplId
      )(env.toSEVar(cidPos), mbKey.fold(s.SEValue.None: s.SExpr)(pos => SBSome(env.toSEVar(pos)))),
    ) { (tmplArgPos, _env) =>
      val env = _env.bindExprVar(tmpl.param, tmplArgPos)
      let(
        env,
        SBUInsertFetchNode(tmplId, byKey = mbKey.isDefined, byInterface = None)(env.toSEVar(cidPos)),
      ) { (_, env) =>
        env.toSEVar(tmplArgPos)
      }
    }

  private[this] def compileFetch(
      tmplId: Identifier,
      tmpl: Template,
  ): (t.SDefinitionRef, SDefinition) =
    // compile a template to
    // FetchDefRef(tmplId) = \ <coid> <token> ->
    //   let <tmplArg> = $fetch(tmplId) <coid>
    //       _ = $insertFetch(tmplId, false) coid [tmpl.signatories] [tmpl.observers] [tmpl.key]
    //   in <tmplArg>
    topLevelFunction2(t.FetchDefRef(tmplId)) { (cidPos, tokenPos, env) =>
      compileFetchBody(env, tmplId, tmpl)(cidPos, None, tokenPos)
    }

  private[this] def compileFetchInterfaceBody(
      env: Env,
      ifaceId: Identifier,
      cidPos: Position,
      typeRepPos: Position,
  ) =
    let(env, SBUFetchInterface(ifaceId)(env.toSEVar(cidPos), env.toSEVar(typeRepPos))) {
      (payloadPos, env) =>
        let(
          env,
          SBResolveSBUInsertFetchNode(ifaceId)(env.toSEVar(payloadPos), env.toSEVar(cidPos)),
        ) { (_, env) =>
          env.toSEVar(payloadPos)
        }
    }

  private[this] def compileFetchInterface(
      ifaceId: Identifier
  ): (t.SDefinitionRef, SDefinition) =
    topLevelFunction2(t.FetchDefRef(ifaceId)) { (cidPos, _, env) =>
      let(env, s.SEValue(SOptional(None))) { (typeRepPos, env) =>
        compileFetchInterfaceBody(env, ifaceId, cidPos, typeRepPos)
      }
    }

  private[this] def compileInterfacePrecond(
      iface: Identifier,
      param: ExprVarName,
      expr: Expr,
  ) =
    topLevelFunction1(t.InterfacePrecondDefRef(iface))((argPos, env) =>
      translateExp(env.bindExprVar(param, argPos), expr)
    )

  private[this] def compileKey(
      tmplId: Identifier,
      tmpl: Template,
  ): (t.SDefinitionRef, SDefinition) =
    topLevelFunction1(t.KeyDefRef(tmplId)) { (tmplArgPos, env) =>
      translateKeyWithMaintainers(env.bindExprVar(tmpl.param, tmplArgPos), tmpl.key)
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

  // Turn a template value into an interface value. Since interfaces have a
  // toll-free representation (for now), this is just the identity function.
  // But the existence of ImplementsDefRef implies that the template implements
  // the interface, which is useful in itself.
  private[this] def compileImplements(
      tmplId: Identifier,
      ifaceId: Identifier,
  ): (t.SDefinitionRef, SDefinition) =
    t.ImplementsDefRef(tmplId, ifaceId) ->
      SDefinition(pipeline(translateIdentity(Env.Empty)))

  // Compile the implementation of an interface method.
  private[this] def compileImplementsMethod(
      tmplId: Identifier,
      ifaceId: Identifier,
      method: TemplateImplementsMethod,
  ): (t.SDefinitionRef, SDefinition) = {
    val ref = t.ImplementsMethodDefRef(tmplId, ifaceId, method.name)
    ref -> SDefinition(withLabelT(ref, compileExp(method.value)))
  }

  private[this] def translateCreateBody(
      tmplId: Identifier,
      tmpl: Template,
      byInterface: Option[Identifier],
      tmplArgPos: Position,
      env: Env,
  ) = {
    val env2 = env.bindExprVar(tmpl.param, tmplArgPos)
    val implementsPrecondsIterator = tmpl.implements.iterator.map[s.SExpr](impl =>
      // This is because interfaces do not have the same representation as the underlying template
      let(env2, SBToInterface(tmplId)(env2.toSEVar(tmplArgPos))) { (ifaceArgPos, env3) =>
        t.InterfacePrecondDefRef(impl._1)(env3.toSEVar(ifaceArgPos))
      }
    )
    // TODO Clean this up as part of changing how we evaluate these
    // https://github.com/digital-asset/daml/issues/11762
    val precondsArray: ImmArray[s.SExpr] =
      (Iterator(translateExp(env2, tmpl.precond)) ++ implementsPrecondsIterator ++ Iterator(
        s.SEValue.EmptyList
      )).to(ImmArray)
    val preconds = s.SEApp(s.SEBuiltin(SBConsMany(precondsArray.length - 1)), precondsArray.toList)
    // We check precondition in a separated builtin to prevent
    // further evaluation of agreement, signatories, observers and key
    // in case of failed precondition.
    let(env2, SBCheckPrecond(tmplId)(env2.toSEVar(tmplArgPos), preconds)) { (_, env) =>
      SBUCreate(tmplId, byInterface)(
        env.toSEVar(tmplArgPos),
        translateExp(env, tmpl.agreementText),
        translateExp(env, tmpl.signatories),
        translateExp(env, tmpl.observers),
        translateKeyWithMaintainers(env, tmpl.key),
      )
    }
  }

  private[this] def compileCreate(
      tmplId: Identifier,
      tmpl: Template,
  ): (t.SDefinitionRef, SDefinition) = {
    // Translates 'create Foo with <params>' into:
    // CreateDefRef(tmplId) = \ <tmplArg> <token> ->
    //   let _ = $checkPrecond(tmplId)(<tmplArg> [tmpl.precond ++ [precond | precond <- tmpl.implements]]
    //   in $create <tmplArg> [tmpl.agreementText] [tmpl.signatories] [tmpl.observers] [tmpl.key]
    topLevelFunction2(t.CreateDefRef(tmplId))((tmplArgPos, _, env) =>
      translateCreateBody(tmplId, tmpl, None, tmplArgPos, env)
    )
  }

  private[this] def compileCreateByInterface(
      tmplId: Identifier,
      tmpl: Template,
      ifaceId: Identifier,
  ): (t.SDefinitionRef, SDefinition) = {
    // Similar to compileCreate, but sets the 'byInterface' field in the transaction.
    topLevelFunction2(t.CreateByInterfaceDefRef(tmplId, ifaceId))((tmplArgPos, _, env) =>
      translateCreateBody(tmplId, tmpl, Some(ifaceId), tmplArgPos, env)
    )
  }

  private[this] def compileCreateInterface(
      ifaceId: Identifier
  ): (t.SDefinitionRef, SDefinition) = {
    topLevelFunction2(t.CreateDefRef(ifaceId)) { (tmplArgPos, tokenPos, env) =>
      SBResolveCreateByInterface(ifaceId)(
        env.toSEVar(tmplArgPos),
        env.toSEVar(tokenPos),
      )
    }
  }

  private[this] def compileCreateAndExercise(
      env: Env,
      tmplId: Identifier,
      createArg: SValue,
      choiceId: ChoiceName,
      choiceArg: SValue,
  ): s.SExpr =
    labeledUnaryFunction(Profile.CreateAndExerciseLabel(tmplId, choiceId), env) { (tokenPos, env) =>
      let(env, t.CreateDefRef(tmplId)(s.SEValue(createArg), env.toSEVar(tokenPos))) {
        (cidPos, env) =>
          t.ChoiceDefRef(tmplId, choiceId)(
            env.toSEVar(cidPos),
            s.SEValue(choiceArg),
            env.toSEVar(tokenPos),
          )
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
          let(env, compileFetchBody(env, tmplId, tmpl)(cidPos, Some(keyWithMPos), tokenPos)) {
            (contractPos, env) =>
              FetchByKeyResult(env.toSEVar(cidPos), env.toSEVar(contractPos))
          }
        }
      }
    }

  private[this] def compileExerciseByInterface(
      env: Env,
      interfaceId: TypeConName,
      templateId: TypeConName,
      contractId: SValue,
      choiceId: ChoiceName,
      argument: SValue,
  ): s.SExpr =
    unaryFunction(env) { (tokenPos, env) =>
      t.GuardedChoiceDefRef(interfaceId, choiceId)(
        s.SEValue(contractId),
        s.SEValue(argument),
        s.SEValue(SOptional(Some(STypeRep(TTyCon(templateId))))),
        s.SEBuiltin(SBGuardConstTrue),
        env.toSEVar(tokenPos),
      )
    }

  private[this] def compileFetchByInterface(
      env: Env,
      interfaceId: TypeConName,
      templateId: TypeConName,
      contractId: SValue,
  ): s.SExpr =
    unaryFunction(env) { (_, env) =>
      let(env, s.SEValue(contractId)) { (cidPos, env) =>
        let(env, s.SEValue(SOptional(Some(STypeRep(TTyCon(templateId)))))) { (typeRepPos, env) =>
          compileFetchInterfaceBody(env, interfaceId, cidPos, typeRepPos)
        }
      }
    }

  private[this] def translateCommand(env: Env, cmd: Command): s.SExpr = cmd match {
    case Command.Create(templateId, argument) =>
      t.CreateDefRef(templateId)(s.SEValue(argument))
    case Command.CreateByInterface(interfaceId, templateId, argument) =>
      t.CreateByInterfaceDefRef(templateId, interfaceId)(s.SEValue(argument))
    case Command.Exercise(templateId, contractId, choiceId, argument) =>
      t.ChoiceDefRef(templateId, choiceId)(s.SEValue(contractId), s.SEValue(argument))
    case Command.ExerciseByInterface(interfaceId, templateId, contractId, choiceId, argument) =>
      compileExerciseByInterface(env, interfaceId, templateId, contractId, choiceId, argument)
    case Command.ExerciseInterface(interfaceId, contractId, choiceId, argument) =>
      t.ChoiceDefRef(interfaceId, choiceId)(s.SEValue(contractId), s.SEValue(argument))
    case Command.ExerciseByKey(templateId, contractKey, choiceId, argument) =>
      t.ChoiceByKeyDefRef(templateId, choiceId)(s.SEValue(contractKey), s.SEValue(argument))
    case Command.Fetch(templateId, coid) =>
      t.FetchDefRef(templateId)(s.SEValue(coid))
    case Command.FetchByInterface(interfaceId, templateId, coid) =>
      compileFetchByInterface(env, interfaceId, templateId, coid)
    case Command.FetchByKey(templateId, key) =>
      t.FetchByKeyDefRef(templateId)(s.SEValue(key))
    case Command.CreateAndExercise(templateId, createArg, choice, choiceArg) =>
      compileCreateAndExercise(
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
    // commands are compile similarly as update block
    // see compileBlock
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
}
