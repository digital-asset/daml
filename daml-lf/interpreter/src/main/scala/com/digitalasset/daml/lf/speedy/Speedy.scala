// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import java.util

import com.daml.lf.data.Ref._
import com.daml.lf.data.{ImmArray, Ref, Time}
import com.daml.lf.language.Ast._
import com.daml.lf.speedy.SError._
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SResult._
import com.daml.lf.speedy.SValue._
import com.daml.lf.value.Value.AbsoluteContractId
import com.daml.lf.value.{Value => V}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.util.control.NoStackTrace

object Speedy {

  /** The speedy CEK machine. */
  final case class Machine(
      /* The control is what the machine should be evaluating. */
      var ctrl: Ctrl,
      /* The enviroment: an array of values */
      var env: Env,
      /* Kont, or continuation specifies what should be done next
       * once the control has been evaluated.
       */
      var kont: util.ArrayList[Kont],
      /* The last encountered location */
      var lastLocation: Option[Location],
      /* The current partial transaction */
      var ptx: PartialTransaction,
      /* Committers of the action. */
      var committers: Set[Party],
      /* Commit location, if a scenario commit is in progress. */
      var commitLocation: Option[Location],
      /* Whether the current submission is validating the transaction, or interpreting
       * it. If this is false, the committers must be a singleton set.
       */
      var validating: Boolean,
      /* The trace log. */
      traceLog: TraceLog,
      /* Compiled packages (DAML-LF ast + compiled speedy expressions). */
      var compiledPackages: CompiledPackages,
      /* Flag to trace usage of get_time builtins */
      var dependsOnTime: Boolean,
  ) {

    def kontPop(): Kont = kont.remove(kont.size - 1)
    def getEnv(i: Int): SValue = env.get(env.size - i)
    def popEnv(count: Int): Unit =
      env.subList(env.size - count, env.size).clear

    /** Push a single location to the continuation stack for the sake of
        maintaining a stack trace. */
    def pushLocation(loc: Location): Unit = {
      lastLocation = Some(loc)
      val last_index = kont.size() - 1
      val last_kont = if (last_index >= 0) Some(kont.get(last_index)) else None
      last_kont match {
        // NOTE(MH): If the top of the continuation stack is the monadic token,
        // we push location information under it to account for the implicit
        // lambda binding the token.
        case Some(KArg(Array(SEValue.Token))) => kont.add(last_index, KLocation(loc))
        // NOTE(MH): When we use a cached top level value, we need to put the
        // stack trace it produced back on the continuation stack to get
        // complete stack trace at the use site. Thus, we store the stack traces
        // of top level values separately during their execution.
        case Some(KCacheVal(v, stack_trace)) =>
          kont.set(last_index, KCacheVal(v, loc :: stack_trace)); ()
        case _ => kont.add(KLocation(loc)); ()
      }
    }

    /** Push an entire stack trace to the continuation stack. The first
        element of the list will be pushed last. */
    def pushStackTrace(locs: List[Location]): Unit =
      locs.reverse.foreach(pushLocation)

    /** Compute a stack trace from the locations in the continuation stack.
        The last seen location will come last. */
    def stackTrace(): ImmArray[Location] = {
      val s = new util.ArrayList[Location]
      kont.forEach { k =>
        k match {
          case KLocation(location) => { s.add(location); () }
          case _ => ()
        }
      }
      ImmArray(s.asScala)
    }

    /** Perform a single step of the machine execution. */
    def step(): SResult =
      try {
        val ctrlToExecute = ctrl
        // Set control to crash as it must be reset after execution. This guards
        // against e.g. buggy builtin operations which do not set control and could
        // then not advance the machine state.
        ctrl = CtrlCrash
        ctrlToExecute.execute(this)
        SResultContinue
      } catch {
        case SpeedyHungry(res: SResult) =>
          res

        case serr: SError =>
          serr match {
            case _: SErrorDamlException if tryHandleException =>
              SResultContinue
            case _ => SResultError(serr)
          }

        case ex: RuntimeException =>
          SResultError(SErrorCrash(s"exception: $ex"))

      }

    /** Try to handle a DAML exception by looking for
      * the catch handler. Returns true if the exception
      * was catched.
      */
    def tryHandleException(): Boolean = {
      val catchIndex =
        kont.asScala.lastIndexWhere(_.isInstanceOf[KCatch])
      if (catchIndex >= 0) {
        val kcatch = kont.get(catchIndex).asInstanceOf[KCatch]
        kont.subList(catchIndex, kont.size).clear()
        env.subList(kcatch.envSize, env.size).clear()
        ctrl = CtrlExpr(kcatch.handler)
        true
      } else
        false
    }

    def lookupVal(eval: SEVal): Ctrl = {
      eval.cached match {
        case Some((v, stack_trace)) =>
          pushStackTrace(stack_trace)
          v

        case None =>
          val ref = eval.ref
          compiledPackages.getDefinition(ref) match {
            case Some(body) =>
              kont.add(KCacheVal(eval, Nil))
              CtrlExpr(body)
            case None =>
              if (compiledPackages.getPackage(ref.packageId).isDefined)
                crash(
                  s"definition $ref not found even after caller provided new set of packages",
                )
              else
                throw SpeedyHungry(
                  SResultNeedPackage(
                    ref.packageId, { packages =>
                      this.compiledPackages = packages
                      // To avoid infinite loop in case the packages are not updated properly by the caller
                      assert(compiledPackages.getPackage(ref.packageId).isDefined)
                      this.ctrl = lookupVal(eval)
                    }
                  ),
                )
          }
      }
    }

    /** Returns true when the machine has finished evaluation.
      * The machine is considered final when the kont stack
      * is empty, and the value is not a fully applied PAP.
      */
    def isFinal(): Boolean =
      if (!kont.isEmpty)
        // Kont stack not empty, can always reduce further.
        false
      else
        ctrl match {
          // control is a PAP, but fully applied so it
          // can be reduced further.
          case pap: SPAP if pap.args.size == pap.arity => false
          case _: SValue => true
          case _ => false
        }

    def toValue: V[V.ContractId] =
      toSValue.toValue

    def toSValue: SValue =
      if (!isFinal) {
        crash("toSValue: machine not final")
      } else {
        ctrl match {
          case v: SValue => v
          case _ => crash("machine did not evaluate to a value")
        }
      }

    def print(count: Int) = {
      println(s"Step: $count")
      println("Control:")
      println(s"  ${ctrl}")
      println("Environment:")
      env.forEach { v =>
        println("  " + v.toString)
      }
      println("Kontinuation:")
      kont.forEach { k =>
        println(s"  " + k.toString)
      }
      println("============================================================")
    }

    // fake participant to generate a new transactionSeed when running scenarios
    private val scenarioServiceParticipant = Ref.ParticipantId.assertFromString("scenario-service")

    // reinitialize the state of the machine with a new fresh submission seed.
    // Should be used only when running scenario
    def clearCommit: Unit = {
      val freshSeed = ptx.context.nextChildrenSeed
        .map(crypto.Hash.deriveTransactionSeed(_, scenarioServiceParticipant, ptx.submissionTime))
      committers = Set.empty
      commitLocation = None
      ptx = PartialTransaction.initial(
        submissionTime = ptx.submissionTime,
        InitialSeeding(freshSeed),
      )
    }

  }

  object Machine {

    private val damlTraceLog = LoggerFactory.getLogger("daml.tracelog")

    private def initial(
        compiledPackages: CompiledPackages,
        submissionTime: Time.Timestamp,
        initialSeeding: InitialSeeding,
    ) =
      Machine(
        ctrl = null,
        env = emptyEnv,
        kont = new util.ArrayList[Kont](128),
        lastLocation = None,
        ptx = PartialTransaction.initial(submissionTime, initialSeeding),
        committers = Set.empty,
        commitLocation = None,
        traceLog = TraceLog(damlTraceLog, 100),
        compiledPackages = compiledPackages,
        validating = false,
        dependsOnTime = false,
      )

    def newBuilder(
        compiledPackages: CompiledPackages,
        submissionTime: Time.Timestamp,
        transactionSeed: Option[crypto.Hash],
    ): Either[SError, Expr => Machine] = {
      val compiler = Compiler(compiledPackages.packages)
      Right(
        (expr: Expr) =>
          fromSExpr(
            SEApp(compiler.unsafeCompile(expr), Array(SEValue.Token)),
            compiledPackages,
            submissionTime,
            InitialSeeding(transactionSeed)
        ))
    }

    def build(
        sexpr: SExpr,
        compiledPackages: CompiledPackages,
        submissionTime: Time.Timestamp,
        seeds: InitialSeeding,
    ): Machine =
      fromSExpr(
        SEApp(sexpr, Array(SEValue.Token)),
        compiledPackages,
        submissionTime,
        seeds,
      )

    // Used from repl.
    def fromExpr(
        expr: Expr,
        compiledPackages: CompiledPackages,
        scenario: Boolean,
        submissionTime: Time.Timestamp,
        transactionSeed: Option[crypto.Hash],
    ): Machine = {
      val compiler = Compiler(compiledPackages.packages)
      val sexpr =
        if (scenario)
          SEApp(compiler.unsafeCompile(expr), Array(SEValue.Token))
        else
          compiler.unsafeCompile(expr)

      fromSExpr(
        sexpr,
        compiledPackages,
        submissionTime,
        InitialSeeding(transactionSeed),
      )
    }

    // Construct a machine from an SExpr. This is useful when you don’t have
    // an update expression and build’s behavior of applying the expression to
    // a token is not appropriate.
    def fromSExpr(
        sexpr: SExpr,
        compiledPackages: CompiledPackages,
        submissionTime: Time.Timestamp,
        seeding: InitialSeeding,
    ): Machine =
      initial(compiledPackages, submissionTime, seeding).copy(ctrl = CtrlExpr(sexpr))
  }

  /** Control specifies the thing that the machine should be reducing.
    * If the control is fully evaluated then the top-most continuation
    * is executed.
    */
  sealed abstract class Ctrl {

    /** Execute a single step to reduce the control */
    def execute(machine: Machine): Unit
  }

  /** A special control object to guard against misbehaving operations.
    * It is set by default, so for example if an action forgets to set the
    * control we won't loop but rather we'll crash.
    */
  final object CtrlCrash extends Ctrl {
    def execute(machine: Machine) =
      crash(s"CtrlCrash: control set to crash after evaluating")
  }

  final case class CtrlExpr(expr: SExpr) extends Ctrl {
    def execute(machine: Machine) =
      machine.ctrl = expr.execute(machine)
  }

  /** When we fetch a contract id from upstream we cannot crash in the
    * that upstream calls. Rather, we set the control to this and then crash
    * when executing.
    */
  final case class CtrlWronglyTypeContractId(
      acoid: AbsoluteContractId,
      expected: TypeConName,
      actual: TypeConName,
  ) extends Ctrl {
    override def execute(machine: Machine): Unit = {
      throw DamlEWronglyTypedContract(acoid, expected, actual)
    }
  }

  object Ctrl {
    def fromPrim(prim: Prim, arity: Int): Ctrl =
      SPAP(prim, new util.ArrayList[SValue](), arity)
  }

  //
  // Environment
  //
  // NOTE(JM): We use ArrayList instead of ArrayBuffer as
  // it is significantly faster.
  type Env = util.ArrayList[SValue]
  def emptyEnv: Env = new util.ArrayList[SValue](512)

  //
  // Kontinuation
  //

  /** Kont, or continuation. Describes the next step for the machine
    * after an expression has been evaluated into a 'SValue'.
    */
  sealed abstract class Kont {

    /** Execute the continuation. */
    def execute(v: SValue, machine: Machine): Unit
  }

  /** Pop 'count' arguments from the environment. */
  final case class KPop(count: Int) extends Kont {
    def execute(v: SValue, machine: Machine) = {
      machine.popEnv(count)
    }
  }

  /** The function has been evaluated to a value, now start evaluating the arguments. */
  final case class KArg(newArgs: Array[SExpr]) extends Kont with SomeArrayEquals {
    def execute(v: SValue, machine: Machine) = {
      v match {
        case SPAP(prim, args, arity) =>
          val missing = arity - args.size
          val newArgsLimit = Math.min(missing, newArgs.length)

          // Keep some space free, because both `KFun` and `KPushTo` will add to the list.
          val extendedArgs = new util.ArrayList[SValue](args.size + newArgsLimit)
          extendedArgs.addAll(args)

          // Stash away over-applied arguments, if any.
          val othersLength = newArgs.length - missing
          if (othersLength > 0) {
            val others = new Array[SExpr](othersLength)
            System.arraycopy(newArgs, missing, others, 0, othersLength)
            machine.kont.add(KArg(others))
          }

          machine.kont.add(KFun(prim, extendedArgs, arity))

          // Start evaluating the arguments.
          var i = 1
          while (i < newArgsLimit) {
            val arg = newArgs(newArgsLimit - i)
            machine.kont.add(KPushTo(extendedArgs, arg))
            i = i + 1
          }
          machine.ctrl = CtrlExpr(newArgs(0))

        case _ =>
          crash(s"Applying non-PAP: $v")
      }
    }
  }

  /** The function and the arguments have been evaluated. Construct a PAP from them.
    * If the PAP is fully applied the machine will push the arguments to the environment
    * and start evaluating the function body. */
  final case class KFun(prim: Prim, args: util.ArrayList[SValue], var arity: Int) extends Kont {
    def execute(v: SValue, machine: Machine) = {
      args.add(v) // Add last argument
      machine.ctrl = SPAP(prim, args, arity)
    }
  }

  final case class KIfThenElse(ifCase: SExpr, elseCase: SExpr) extends Kont {
    def execute(v: SValue, machine: Machine) = {
      machine.ctrl =
        if (v.asInstanceOf[SBool].value)
          ifCase.execute(machine)
        else
          elseCase.execute(machine)
    }
  }

  /** The scrutinee of a match has been evaluated, now match the alternatives against it. */
  final case class KMatch(alts: Array[SCaseAlt]) extends Kont with SomeArrayEquals {
    def execute(v: SValue, machine: Machine) = {
      val altOpt = v match {
        case SBool(b) =>
          alts.find { alt =>
            alt.pattern match {
              case SCPPrimCon(PCTrue) => b
              case SCPPrimCon(PCFalse) => !b
              case SCPDefault => true
              case _ => false
            }
          }
        case SVariant(_, _, rank1, arg) =>
          alts.find { alt =>
            alt.pattern match {
              case SCPVariant(_, _, rank2) if rank1 == rank2 =>
                machine.kont.add(KPop(1))
                machine.env.add(arg)
                true
              case SCPDefault => true
              case _ => false
            }
          }
        case SEnum(_, _, rank1) =>
          alts.find { alt =>
            alt.pattern match {
              case SCPEnum(_, _, rank2) => rank1 == rank2
              case SCPDefault => true
              case _ => false
            }
          }
        case SList(lst) =>
          alts.find { alt =>
            alt.pattern match {
              case SCPNil if lst.isEmpty => true
              case SCPCons if !lst.isEmpty =>
                machine.kont.add(KPop(2))
                val Some((head, tail)) = lst.pop
                machine.env.add(head)
                machine.env.add(SList(tail))
                true
              case SCPDefault => true
              case _ => false
            }
          }
        case SUnit =>
          alts.find { alt =>
            alt.pattern match {
              case SCPPrimCon(PCUnit) => true
              case SCPDefault => true
              case _ => false
            }
          }
        case SOptional(mbVal) =>
          alts.find { alt =>
            alt.pattern match {
              case SCPNone if mbVal.isEmpty => true
              case SCPSome =>
                mbVal match {
                  case None => false
                  case Some(x) =>
                    machine.kont.add(KPop(1))
                    machine.env.add(x)
                    true
                }
              case SCPDefault => true
              case _ => false
            }
          }
        case SContractId(_) | SDate(_) | SNumeric(_) | SInt64(_) | SParty(_) | SText(_) |
            STimestamp(_) | SStruct(_, _) | STextMap(_) | SGenMap(_) | SRecord(_, _, _) |
            SAny(_, _) | STypeRep(_) | STNat(_) | _: SPAP | SToken =>
          crash("Match on non-matchable value")
      }

      machine.ctrl = CtrlExpr(
        altOpt
          .getOrElse(throw DamlEMatchError(s"No match for $v in ${alts.toList}"))
          .body,
      )
    }

  }

  /** Push the evaluated value to the array 'to', and start evaluating the expression 'next'.
    * This continuation is used to implement both function application and lets. In
    * the case of function application the arguments are pushed into the 'args' array of
    * the PAP that is being built, and in the case of lets the evaluated value is pushed
    * direy into the environment.
    */
  final case class KPushTo(to: util.ArrayList[SValue], next: SExpr) extends Kont {
    def execute(v: SValue, machine: Machine) = {
      to.add(v)
      machine.ctrl = CtrlExpr(next)
    }
  }

  /** Store the evaluated value in the 'SEVal' from which the expression came from.
    * This in principle makes top-level values lazy. It is a useful optimization to
    * allow creation of large constants (for example records) that are repeatedly
    * accessed. In older compilers which did not use the builtin record and struct
    * updates this solves the blow-up which would happen when a large record is
    * updated multiple times. */
  final case class KCacheVal(v: SEVal, stack_trace: List[Location]) extends Kont {
    def execute(sv: SValue, machine: Machine) = {
      machine.pushStackTrace(stack_trace)
      v.cached = Some((sv, stack_trace))
    }
  }

  /** A catch frame marks the point to which an exception (of type 'SErrorDamlException')
    * is unwound. The 'envSize' specifies the size to which the environment must be pruned.
    * If an exception is raised and 'KCatch' is found from kont-stack, then 'handler' is
    * evaluated. If 'KCatch' is encountered naturally, then 'fin' is evaluated.
    */
  final case class KCatch(handler: SExpr, fin: SExpr, envSize: Int) extends Kont {

    def execute(v: SValue, machine: Machine) = {
      machine.ctrl = CtrlExpr(fin)
    }
  }

  /** A location frame stores a location annotation found in the AST. */
  final case class KLocation(location: Location) extends Kont {
    def execute(v: SValue, machine: Machine) = {
      machine.ctrl = v
    }
  }

  /** Internal exception thrown when a continuation result needs to be returned. */
  final case class SpeedyHungry(result: SResult) extends RuntimeException with NoStackTrace

  def deriveTransactionSeed(
      submissionSeed: Option[crypto.Hash],
      participant: Ref.ParticipantId,
      submissionTime: Time.Timestamp,
  ): InitialSeeding =
    InitialSeeding(
      submissionSeed.map(crypto.Hash.deriveTransactionSeed(_, participant, submissionTime)))

}

import java.util

import com.daml.lf.data._
import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast
import com.daml.lf.language.Ast._
import com.daml.lf.speedy.SError.{DamlEArithmeticError, SErrorCrash}
import com.daml.lf.speedy.Speedy.{Ctrl, KPop}
import com.daml.lf.value.{Value => V}

import scala.collection.JavaConverters._
import scala.collection.immutable.{HashMap, TreeMap}

/** Speedy values. These are the value types recognized by the
  * machine. In addition to the usual types present in the LF value,
  * this also contains partially applied functions (SPAP).
  */

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
  ),
)
sealed abstract class SValue extends Ctrl {

  import SValue._

  def toValue: V[V.ContractId] =
    this match {
      case SInt64(x) => V.ValueInt64(x)
      case SNumeric(x) => V.ValueNumeric(x)
      case SText(x) => V.ValueText(x)
      case STimestamp(x) => V.ValueTimestamp(x)
      case SParty(x) => V.ValueParty(x)
      case SBool(x) => V.ValueBool(x)
      case SUnit => V.ValueUnit
      case SDate(x) => V.ValueDate(x)
      case SStruct(fields, svalues) =>
        V.ValueStruct(
          ImmArray(
            fields.toSeq
              .zip(svalues.asScala)
              .map { case (fld, sv) => (fld, sv.toValue) },
          ),
        )
      case SRecord(id, fields, svalues) =>
        V.ValueRecord(
          Some(id),
          ImmArray(
            fields.toSeq
              .zip(svalues.asScala)
              .map({ case (fld, sv) => (Some(fld), sv.toValue) }),
          ),
        )
      case SVariant(id, variant, _, sv) =>
        V.ValueVariant(Some(id), variant, sv.toValue)
      case SEnum(id, constructor, _) =>
        V.ValueEnum(Some(id), constructor)
      case SList(lst) =>
        V.ValueList(lst.map(_.toValue))
      case SOptional(mbV) =>
        V.ValueOptional(mbV.map(_.toValue))
      case STextMap(mVal) =>
        V.ValueTextMap(SortedLookupList(mVal).mapValue(_.toValue))
      case SGenMap(values) =>
        V.ValueGenMap(ImmArray(values.map { case (k, v) => k.toValue -> v.toValue }))
      case SContractId(coid) =>
        V.ValueContractId(coid)
      case SAny(_, _) =>
        throw SErrorCrash("SValue.toValue: unexpected SAny")
      case STypeRep(_) =>
        throw SErrorCrash("SValue.toValue: unexpected STypeRep")
      case STNat(_) =>
        throw SErrorCrash("SValue.toValue: unexpected STNat")
      case _: SPAP =>
        throw SErrorCrash("SValue.toValue: unexpected SPAP")
      case SToken =>
        throw SErrorCrash("SValue.toValue: unexpected SToken")
    }

  def mapContractId(f: V.ContractId => V.ContractId): SValue =
    this match {
      case SContractId(coid) => SContractId(f(coid))
      case SEnum(_, _, _) | _: SPrimLit | SToken | STNat(_) | STypeRep(_) => this
      case SPAP(prim, args, arity) =>
        val prim2 = prim match {
          case PClosure(expr, vars) =>
            PClosure(expr, vars.map(_.mapContractId(f)))
          case other => other
        }
        val args2 = mapArrayList(args, _.mapContractId(f))
        SPAP(prim2, args2, arity)
      case SRecord(tycon, fields, values) =>
        SRecord(tycon, fields, mapArrayList(values, v => v.mapContractId(f)))
      case SStruct(fields, values) =>
        SStruct(fields, mapArrayList(values, v => v.mapContractId(f)))
      case SVariant(tycon, variant, rank, value) =>
        SVariant(tycon, variant, rank, value.mapContractId(f))
      case SList(lst) =>
        SList(lst.map(_.mapContractId(f)))
      case SOptional(mbV) =>
        SOptional(mbV.map(_.mapContractId(f)))
      case STextMap(value) =>
        STextMap(value.transform((_, v) => v.mapContractId(f)))
      case SGenMap(value) =>
        SGenMap(
          value.iterator.map { case (k, v) => k.mapContractId(f) -> v.mapContractId(f) }
        )
      case SAny(ty, value) =>
        SAny(ty, value.mapContractId(f))
    }

  override def execute(machine: Speedy.Machine): Unit = {
    machine.ctrl = this
    machine.kontPop.execute(this, machine)
  }
}

object SValue {

  /** "Primitives" that can be applied. */
  sealed trait Prim
  final case class PBuiltin(b: SBuiltin) extends Prim
  final case class PClosure(expr: SExpr, closure: Array[SValue]) extends Prim with SomeArrayEquals {
    override def toString: String = s"PClosure($expr, ${closure.mkString("[", ",", "]")})"
  }

  /** A partially (or fully) applied primitive.
    * This is constructed when an argument is applied. When it becomes fully
    * applied (args.size == arity), the machine will apply the arguments to the primitive.
    * If the primitive is a closure, the arguments are pushed to the environment and the
    * closure body is entered.
    */
  final case class SPAP(prim: Prim, args: util.ArrayList[SValue], arity: Int) extends SValue {
    override def toString: String = s"SPAP($prim, ${args.asScala.mkString("[", ",", "]")}, $arity)"

    override def execute(machine: Speedy.Machine): Unit = {
      if (args.size == arity)
        prim match {
          case PClosure(expr, vars) =>
            // Pop the arguments once we're done evaluating the body.
            machine.kont.add(KPop(arity + vars.size))

            // Add all the variables we closed over
            vars.foreach(machine.env.add)

            // Add the arguments
            machine.env.addAll(args)

            // And start evaluating the body of the closure.
            machine.ctrl = expr.execute(machine)

          case PBuiltin(b) =>
            try {
              b.execute(args, machine)
            } catch {
              // We turn arithmetic exceptions into a daml exception
              // that can be caught.
              case e: ArithmeticException =>
                throw DamlEArithmeticError(e.getMessage)
            }
        } else {
        machine.ctrl = this
        machine.kontPop.execute(this, machine)
      }
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.ArrayEquals"))
  final case class SRecord(id: Identifier, fields: Array[Name], values: util.ArrayList[SValue])
      extends SValue

  @SuppressWarnings(Array("org.wartremover.warts.ArrayEquals"))
  final case class SStruct(fields: Array[Name], values: util.ArrayList[SValue]) extends SValue

  final case class SVariant(
      id: Identifier,
      variant: VariantConName,
      constructorRank: Int,
      value: SValue)
      extends SValue

  final case class SEnum(id: Identifier, constructor: Name, constructorRank: Int) extends SValue

  final case class SOptional(value: Option[SValue]) extends SValue

  final case class SList(list: FrontStack[SValue]) extends SValue

  final case class STextMap(textMap: HashMap[String, SValue]) extends SValue

  final case class SGenMap private (genMap: TreeMap[SValue, SValue]) extends SValue

  object SGenMap {
    implicit def `SGenMap Ordering`: Ordering[SValue] = svalue.Ordering

    @throws[SErrorCrash]
    // crashes if `k` contains type abstraction, function, Partially applied built-in or updates
    def comparable(k: SValue): Unit = {
      `SGenMap Ordering`.compare(k, k)
      ()
    }

    val Empty = SGenMap(TreeMap.empty)

    def apply(xs: Iterator[(SValue, SValue)]): SGenMap = {
      type O[_] = TreeMap[SValue, SValue]
      SGenMap(xs.map { case p @ (k, _) => comparable(k); p }.to[O])
    }

    def apply(xs: (SValue, SValue)*): SGenMap =
      SGenMap(xs.iterator)
  }

  final case class SAny(ty: Type, value: SValue) extends SValue

  // Corresponds to a DAML-LF Nat type reified as a Speedy value.
  // It is currently used to track at runtime the scale of the
  // Numeric builtin's arguments/output. Should never be translated
  // back to DAML-LF expressions / values.
  final case class STNat(n: Numeric.Scale) extends SValue

  // NOTE(JM): We are redefining PrimLit here so it can be unified
  // with SValue and we can remove one layer of indirection.
  sealed trait SPrimLit extends SValue with Equals
  final case class SInt64(value: Long) extends SPrimLit
  final case class SNumeric(value: Numeric) extends SPrimLit
  final case class SText(value: String) extends SPrimLit
  final case class STimestamp(value: Time.Timestamp) extends SPrimLit
  final case class SParty(value: Party) extends SPrimLit
  final case class SBool(value: Boolean) extends SPrimLit
  object SBool {
    def apply(value: Boolean): SBool = if (value) SValue.True else SValue.False
  }
  final case object SUnit extends SPrimLit
  final case class SDate(value: Time.Date) extends SPrimLit
  final case class SContractId(value: V.ContractId) extends SPrimLit
  final case class STypeRep(ty: Type) extends SValue
  // The "effect" token for update or scenario builtin functions.
  final case object SToken extends SValue

  object SValue {
    val Unit = SUnit
    val True = new SBool(true)
    val False = new SBool(false)
    val EmptyList = SList(FrontStack.empty)
    val None = SOptional(Option.empty)
    val EmptyMap = STextMap(HashMap.empty)
    val EmptyGenMap = SGenMap.Empty
    val Token = SToken
  }

  abstract class SValueContainer[X] {
    def apply(value: SValue): X
    val Unit: X = apply(SValue.Unit)
    val True: X = apply(SValue.True)
    val False: X = apply(SValue.False)
    val EmptyList: X = apply(SValue.EmptyList)
    val EmptyMap: X = apply(SValue.EmptyMap)
    val EmptyGenMap: X = apply(SValue.EmptyGenMap)
    val None: X = apply(SValue.None)
    val Token: X = apply(SValue.Token)
    def bool(b: Boolean) = if (b) True else False
  }

  private val entryFields = Name.Array(Ast.keyFieldName, Ast.valueFieldName)

  private def entry(key: SValue, value: SValue) = {
    val args = new util.ArrayList[SValue](2)
    args.add(key)
    args.add(value)
    SStruct(entryFields, args)
  }

  def toList(textMap: STextMap): SList = {
    val entries = SortedLookupList(textMap.textMap).toImmArray
    SList(FrontStack(entries.map { case (k, v) => entry(SText(k), v) }))
  }

  def toList(genMap: SGenMap): SList =
    SList(FrontStack(genMap.genMap.iterator.map { case (k, v) => entry(k, v) }.to[ImmArray]))

  private def mapArrayList(
      as: util.ArrayList[SValue],
      f: SValue => SValue,
  ): util.ArrayList[SValue] = {
    val bs = new util.ArrayList[SValue](as.size)
    as.forEach { a =>
      val _ = bs.add(f(a))
    }
    bs
  }

}
