// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

/** The simplified AST for the speedy interpreter.
  *
  * This reduces the number of binding forms by moving update and scenario
  * expressions into builtins.
  *
  * These are the expression forms which remain at the end of the speedy-compiler
  * pipeline. (after ANF).
  *
  * These are the expressions forms which execute on the Speedy machine.
  */
import java.util

import com.daml.lf.language.Ast._
import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast
import com.daml.lf.value.{Value => V}
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.Speedy._
import com.daml.lf.speedy.SBuiltin._
import com.daml.lf.speedy.{SExpr0 => compileTime}
import com.daml.scalautil.Statement.discard

/** The speedy expression:
  * - variables represented by their runtime location
  * - closure converted.
  * - multi-argument applications and abstractions.
  * - all update and scenario operations converted to builtin functions.
  */

@SuppressWarnings(Array("org.wartremover.warts.Any"))
object SExpr {

  sealed abstract class SExpr extends Product with Serializable {
    def execute(machine: Machine): Control
    @SuppressWarnings(Array("org.wartremover.warts.Any"))
    override def toString: String =
      productPrefix + productIterator.map(prettyPrint).mkString("(", ",", ")")
  }

  sealed abstract class SExprAtomic extends SExpr {
    def lookupValue(machine: Machine): SValue

    final def execute(machine: Machine): Control = {
      Control.Value(lookupValue(machine))
    }
  }

  /** Reference to a value. On first lookup the evaluated expression is
    * stored in 'cached'.
    */
  final case class SEVal(
      ref: SDefinitionRef
  ) extends SExpr {

    // The variable `_cached` is used to cache the evaluation of the
    // LF value defined by `ref` once it has been computed.  Hence we
    // avoid both the lookup in the package definition HashMap and the
    // full reevaluation of the body of the definition.
    // Here we take advantage of the Java memory model
    // (https://docs.oracle.com/javase/specs/jls/se8/html/jls-17.html#jls-17.7)
    // that guarantees the write of `_cache` (in the method
    // `setCached`) is done atomically.
    // This is similar how hashcode evaluation is cached in String
    // http://hg.openjdk.java.net/jdk8/jdk8/jdk/file/tip/src/share/classes/java/lang/String.java
    private var _cached: Option[(SValue, List[Location])] = None

    def cached: Option[(SValue, List[Location])] = _cached

    def setCached(sValue: SValue, stack_trace: List[Location]): Unit =
      _cached = Some((sValue, stack_trace))

    def execute(machine: Machine): Control = {
      machine.lookupVal(this)
    }
  }

  /** Reference to a builtin function */
  final case class SEBuiltin(b: SBuiltin) extends SExprAtomic {
    def lookupValue(machine: Machine): SValue = {
      /* special case for nullary record constructors */
      b match {
        case SBRecCon(id, fields) if b.arity == 0 =>
          SRecord(id, fields, ArrayList.empty)
        case _ =>
          SPAP(PBuiltin(b), ArrayList.empty, b.arity)
      }
    }
  }

  /** A pre-computed value, usually primitive literal, e.g. integer, text, boolean etc. */
  final case class SEValue(v: SValue) extends SExprAtomic {
    def lookupValue(machine: Machine): SValue = {
      v
    }
  }

  object SEValue extends SValueContainer[SEValue]

  /** Function application:
    *    General case: 'fun' and 'args' are any kind of expression
    *    This case *ought* not to exist post-ANF.
    *    Sadly, we have many code paths where this case is constructed.
    */
  final case class SEAppGeneral(fun: SExpr, args: Array[SExpr]) extends SExpr with SomeArrayEquals {
    def execute(machine: Machine): Control = {
      machine.pushKont(KArg(machine, args))
      Control.Expression(fun)
    }
  }

  /** Function application:
    *    Special case: 'fun' is an atomic expression.
    */
  final case class SEAppAtomicFun(fun: SExprAtomic, args: Array[SExpr])
      extends SExpr
      with SomeArrayEquals {
    def execute(machine: Machine): Control = {
      val vfun = fun.lookupValue(machine)
      machine.executeApplication(vfun, args)
    }
  }

  object SEApp {
    def apply(fun: SExpr, args: Array[SExpr]): SExpr = {
      SEAppGeneral(fun, args)
    }
  }

  /** Function application: ANF case: 'fun' and 'args' are atomic expressions */
  final case class SEAppAtomicGeneral(fun: SExprAtomic, args: Array[SExprAtomic])
      extends SExpr
      with SomeArrayEquals {
    def execute(machine: Machine): Control = {
      val vfun = fun.lookupValue(machine)
      machine.enterApplication(vfun, args)
    }
  }

  /** Function application: ANF case: 'fun' is builtin; 'args' are atomic expressions.  Size
    * of `args' matches the builtin arity.
    */
  final case class SEAppAtomicSaturatedBuiltin(builtin: SBuiltin, args: Array[SExprAtomic])
      extends SExpr
      with SomeArrayEquals {
    def execute(machine: Machine): Control = {
      val arity = builtin.arity
      val actuals = new util.ArrayList[SValue](arity)
      var i = 0
      while (i < arity) {
        val arg = args(i)
        val v = arg.lookupValue(machine)
        discard(actuals.add(v))
        i += 1
      }
      builtin.execute(actuals, machine)
    }
  }

  object SEAppAtomic {
    // smart constructor (used in Anf.scala): detect special case of saturated builtin application
    def apply(func: SExprAtomic, args: Array[SExprAtomic]): SExpr = {
      func match {
        case SEBuiltin(builtin) if builtin.arity == args.length =>
          SEAppAtomicSaturatedBuiltin(builtin, args)
        case _ =>
          SEAppAtomicGeneral(func, args) // general case
      }
    }
  }

  /** Closure creation. Create a new closure object storing the free variables
    * in 'body'.
    */
  final case class SEMakeClo(fvs: Array[SELoc], arity: Int, body: SExpr)
      extends SExpr
      with SomeArrayEquals {

    def execute(machine: Machine): Control = {
      val sValues = Array.ofDim[SValue](fvs.length)
      var i = 0
      while (i < fvs.length) {
        sValues(i) = fvs(i).lookupValue(machine)
        i += 1
      }
      val pap = SPAP(PClosure(Profile.LabelUnset, body, sValues), ArrayList.empty, arity)
      Control.Value(pap)
    }
  }

  /** SELoc -- Reference to the runtime location of a variable.
    *
    *    This is the closure-converted form of SEVar. There are three sub-forms, with sufffix:
    *    S/A/F, indicating [S]tack, [A]argument, or [F]ree variable captured by a closure.
    */
  sealed abstract class SELoc extends SExprAtomic

  // SELocS -- variable is located on the stack (SELet & binding forms of SECasePat)
  final case class SELocS(n: Int) extends SELoc {
    def lookupValue(machine: Machine): SValue = {
      machine.getEnvStack(n)
    }
  }

  // SELocS -- variable is located in the args array of the application
  final case class SELocA(n: Int) extends SELoc {
    def lookupValue(machine: Machine): SValue = {
      machine.getEnvArg(n)
    }
  }

  // SELocF -- variable is located in the free-vars array of the closure being applied
  final case class SELocF(n: Int) extends SELoc {
    def lookupValue(machine: Machine): SValue = {
      machine.getEnvFree(n)
    }
  }

  /** (Atomic) Pattern match. */
  final case class SECaseAtomic(scrut: SExprAtomic, alts: Array[SCaseAlt])
      extends SExpr
      with SomeArrayEquals {
    def execute(machine: Machine): Control = {
      val vscrut = scrut.lookupValue(machine)
      executeMatchAlts(machine, alts, vscrut)
    }
  }

  /** A let-expression with a single RHS */
  final case class SELet1General(rhs: SExpr, body: SExpr) extends SExpr with SomeArrayEquals {
    def execute(machine: Machine): Control = {
      machine.pushKont(KPushTo(machine, machine.env, body))
      Control.Expression(rhs)
    }
  }

  /** A (single) let-expression with an unhungry,saturated builtin-application as RHS */
  final case class SELet1Builtin(builtin: SBuiltinPure, args: Array[SExprAtomic], body: SExpr)
      extends SExpr
      with SomeArrayEquals {
    def execute(machine: Machine): Control = {
      val arity = builtin.arity
      val actuals = new util.ArrayList[SValue](arity)
      var i = 0
      while (i < arity) {
        val arg = args(i)
        val v = arg.lookupValue(machine)
        discard(actuals.add(v))
        i += 1
      }
      val v = builtin.executePure(actuals)
      machine.pushEnv(v) // use pushEnv not env.add so instrumentation is updated
      Control.Expression(body)
    }
  }

  /** A (single) let-expression with an unhungry, saturated builtin-application as RHS */
  final case class SELet1BuiltinArithmetic(
      builtin: SBuiltinArithmetic,
      args: Array[SExprAtomic],
      body: SExpr,
  ) extends SExpr
      with SomeArrayEquals {
    def execute(machine: Machine): Control = {
      val arity = builtin.arity
      val actuals = new util.ArrayList[SValue](arity)
      var i = 0
      while (i < arity) {
        val arg = args(i)
        val v = arg.lookupValue(machine)
        discard(actuals.add(v))
        i += 1
      }
      builtin.compute(actuals) match {
        case Some(value) =>
          machine.pushEnv(value) // use pushEnv not env.add so instrumentation is updated
          Control.Expression(body)
        case None =>
          unwindToHandler(machine, builtin.buildException(actuals))
      }
    }
  }

  object SELet1 {
    // smart constructor (used in Anf.scala)
    def apply(rhs: SExpr, body: SExpr): SExpr = {
      rhs match {
        case SEAppAtomicSaturatedBuiltin(builtin: SBuiltinPure, args) =>
          SELet1Builtin(builtin, args, body)
        case SEAppAtomicSaturatedBuiltin(builtin: SBuiltinArithmetic, args) =>
          SELet1BuiltinArithmetic(builtin, args, body)
        case _ => SELet1General(rhs, body)
      }
    }
  }

  /** Location annotation. When encountered the location is stored in the 'lastLocation'
    * variable of the machine. When commit is begun the location is stored in 'commitLocation'.
    */
  final case class SELocation(loc: Location, expr: SExpr) extends SExpr {
    def execute(machine: Machine): Control = {
      machine.pushLocation(loc)
      Control.Expression(expr)
    }
  }

  /** This is used only during profiling. When a package is compiled with
    * profiling enabled, the right hand sides of top-level and let bindings,
    * lambdas and some builtins are wrapped into [[SELabelClosure]]. During
    * runtime, if the value resulting from evaluating [[expr]] is a
    * (partially applied) closure, the label of the closure is set to the
    * [[label]] given here.
    * See [[com.daml.lf.speedy.Profile]] for an explanation why we use
    * [[AnyRef]] for the label.
    */
  final case class SELabelClosure(label: Profile.Label, expr: SExpr) extends SExpr {
    def execute(machine: Machine): Control = {
      machine.pushKont(KLabelClosure(machine, label))
      Control.Expression(expr)
    }
  }

  /** The SEImportValue form is never constructed when compiling user LF.
    * It is only constructed at runtime by certain builtin-ops.
    * Assumes the packages needed to import value of type `typ` is already
    * loaded in `machine`.
    */
  final case class SEImportValue(typ: Ast.Type, value: V) extends SExpr {
    def execute(machine: Machine): Control = {
      machine.importValue(typ, value)
    }
  }

  /** Exception handler */
  final case class SETryCatch(body: SExpr, handler: SExpr) extends SExpr {
    def execute(machine: Machine): Control = {
      machine.pushKont(KTryCatchHandler(machine, handler))
      machine.withOnLedger("SETryCatch") { onLedger =>
        onLedger.ptx = onLedger.ptx.beginTry
      }
      Control.Expression(body)
    }
  }

  /** Exercise scope (begin..end) */
  final case class SEScopeExercise(body: SExpr) extends SExpr {
    def execute(machine: Machine): Control = {
      machine.pushKont(KCloseExercise(machine))
      Control.Expression(body)
    }
  }

  final case class SEPreventCatch(body: SExpr) extends SExpr {
    def execute(machine: Machine): Control = {
      machine.pushKont(KPreventException(machine))
      Control.Expression(body)
    }
  }

  /** Case patterns */
  sealed trait SCasePat {

    private[speedy] def numArgs: Int = this match {
      case _: SCPEnum | _: SCPPrimCon | SCPNil | SCPDefault | SCPNone => 0
      case _: SCPVariant | SCPSome => 1
      case SCPCons => 2
    }
  }

  /** Match on a variant. On match the value is unboxed and pushed to stack. */
  final case class SCPVariant(id: Identifier, variant: Name, constructorRank: Int) extends SCasePat

  /** Match on a variant. On match the value is unboxed and pushed to stack. */
  final case class SCPEnum(id: Identifier, constructor: Name, constructorRank: Int) extends SCasePat

  /** Match on a primitive constructor, that is on true, false or unit. */
  final case class SCPPrimCon(pc: PrimCon) extends SCasePat

  /** Match on an empty list. */
  final case object SCPNil extends SCasePat

  /** Match on a list. On match, the head and tail of the list is pushed to the stack. */
  final case object SCPCons extends SCasePat

  /** Default match case. Always matches. */
  final case object SCPDefault extends SCasePat

  final case object SCPNone extends SCasePat

  final case object SCPSome extends SCasePat

  /** Case alternative. If the 'pattern' matches, then the environment is accordingly
    * extended and 'body' is evaluated.
    */
  final case class SCaseAlt(pattern: SCasePat, body: SExpr)

  sealed abstract class SDefinitionRef extends Product with Serializable {
    def ref: DefinitionRef
    def packageId: PackageId = ref.packageId
    def modName: ModuleName = ref.qualifiedName.module
    // TODO: move this into the speedy compiler code
    private[this] val eval = compileTime.SEVal(this)
    def apply(args: compileTime.SExpr*) = compileTime.SEApp(eval, args.toList)
  }

  // references to definitions that come from the archive
  final case class LfDefRef(ref: DefinitionRef) extends SDefinitionRef
  // references to definitions generated by the Speedy compiler
  final case class TemplateChoiceDefRef(ref: DefinitionRef, choiceName: ChoiceName)
      extends SDefinitionRef
  final case class InterfaceChoiceDefRef(ref: DefinitionRef, choiceName: ChoiceName)
      extends SDefinitionRef
  final case class ChoiceByKeyDefRef(ref: DefinitionRef, choiceName: ChoiceName)
      extends SDefinitionRef
  final case class CreateDefRef(ref: DefinitionRef) extends SDefinitionRef
  final case class FetchTemplateDefRef(ref: DefinitionRef) extends SDefinitionRef
  final case class FetchInterfaceDefRef(ref: DefinitionRef) extends SDefinitionRef

  final case class FetchByKeyDefRef(ref: DefinitionRef) extends SDefinitionRef
  final case class LookupByKeyDefRef(ref: DefinitionRef) extends SDefinitionRef
  final case class ExceptionMessageDefRef(ref: DefinitionRef) extends SDefinitionRef
  final case class SignatoriesDefRef(ref: DefinitionRef) extends SDefinitionRef
  final case class ObserversDefRef(ref: DefinitionRef) extends SDefinitionRef
  final case class ToCachedContractDefRef(ref: DefinitionRef) extends SDefinitionRef

  /** InterfaceInstanceDefRef(parent, interfaceId, templateId)
    * points to the Unit value if 'parent' defines an interface instance
    * of the interface for the template.
    *
    * invariants:
    *   * parent == interfaceId || parent == templateId
    *   * at most one of the following is defined:
    *       InterfaceInstanceDefRef(i, i, t)
    *       InterfaceInstanceDefRef(t, i, t)
    *
    * The parent is used to determine what package and module define
    * the interface instance, which is used to fetch the appropriate
    * package in case it's missing.
    */
  final case class InterfaceInstanceDefRef(
      parent: TypeConName,
      interfaceId: TypeConName,
      templateId: TypeConName,
  ) extends SDefinitionRef {
    override def ref = parent;
  }

  /** InterfaceInstanceMethodDefRef(interfaceInstance, method) invokes
    * the interface instance's implementation of the method.
    */
  final case class InterfaceInstanceMethodDefRef(
      interfaceInstance: InterfaceInstanceDefRef,
      methodName: MethodName,
  ) extends SDefinitionRef {
    override def ref = interfaceInstance.ref;
  }

  /** InterfaceInstanceViewDefRef(interfaceInstance) invokes
    * the interface instance's implementation of the view.
    */
  final case class InterfaceInstanceViewDefRef(
      interfaceInstance: InterfaceInstanceDefRef
  ) extends SDefinitionRef {
    override def ref = interfaceInstance.ref;
  }

  final case object AnonymousClosure

  private def prettyPrint(x: Any): String =
    x match {
      case i: Array[Any] => i.mkString("[", ",", "]")
      case i: Array[Int] => i.mkString("[", ",", "]")
      case i: java.util.ArrayList[_] => i.toArray().mkString("[", ",", "]")
      case other: Any => other.toString
    }

}
