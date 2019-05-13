// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy

import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.lfpackage.Ast._
import com.digitalasset.daml.lf.speedy.SError._
import com.digitalasset.daml.lf.speedy.SExpr._
import com.digitalasset.daml.lf.speedy.SResult._
import com.digitalasset.daml.lf.speedy.SValue._
import com.digitalasset.daml.lf.transaction.Transaction._
import com.digitalasset.daml.lf.value.{Value => V}

import scala.collection.JavaConverters._
import java.util.ArrayList

import com.digitalasset.daml.lf.CompiledPackages
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId

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
      var kont: ArrayList[Kont],
      /* The last encountered location */
      var lastLocation: Option[Location],
      /* The current partial transaction */
      var ptx: PartialTransaction,
      /* Committer, if a scenario commit is in progress. */
      var committer: Option[Party],
      /* Commit location, if a scenario commit is in progress. */
      var commitLocation: Option[Location],
      /* The trace log. */
      traceLog: TraceLog,
      /* Compiled packages (DAML-LF ast + compiled speedy expressions). */
      var compiledPackages: CompiledPackages,
  ) {

    def kontPop(): Kont = kont.remove(kont.size - 1)
    def getEnv(i: Int): SValue = env.get(env.size - i)
    def popEnv(count: Int): Unit =
      env.subList(env.size - count, env.size).clear

    /** Perform a single step of the machine execution. */
    def step(): SResult =
      try {
        val ctrlToExecute = ctrl
        // Set control to crash as it must be reset after execution. This guards
        // against e.g. buggy builtin operations which do not set control and could
        // then not advance the machine state.
        ctrl = CtrlCrash(ctrlToExecute)
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
      ptx = ptx.markPackage(eval.ref.packageId)
      eval.cached match {
        case Some(v) =>
          CtrlValue(v.asInstanceOf[SValue])
        case None =>
          val ref = eval.ref
          kont.add(KCacheVal(eval))
          compiledPackages.getDefinition(ref) match {
            case Some(body) =>
              CtrlExpr(body)
            case None =>
              throw SpeedyHungry(
                SResultMissingDefinition(
                  ref, { packages =>
                    println(s"reloaded packages")
                    this.compiledPackages = packages
                    compiledPackages.getDefinition(ref) match {
                      case Some(body) =>
                        this.ctrl = CtrlExpr(body)
                      case None =>
                        crash(
                          s"definition $ref not found even after caller provided new set of packages")
                    }
                  }
                ))
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
          case CtrlValue(v) =>
            v match {
              // control is a PAP, but fully applied so it
              // can be reduced further.
              case pap: SPAP if pap.args.size == pap.arity => false
              case _ => true
            }
          case _ =>
            false
        }

    def toValue: V[V.ContractId] =
      toSValue.toValue

    def toSValue: SValue =
      if (!isFinal) {
        crash("toSValue: machine not final")
      } else {
        ctrl match {
          case CtrlValue(v) => v
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
  }

  object Machine {
    private def initial(compiledPackages: CompiledPackages) = Machine(
      ctrl = null,
      env = emptyEnv,
      kont = new ArrayList[Kont](128),
      lastLocation = None,
      ptx = PartialTransaction.initial,
      committer = None,
      commitLocation = None,
      traceLog = TraceLog(100),
      compiledPackages = compiledPackages
    )

    def newBuilder(compiledPackages: CompiledPackages): Either[SError, (Expr => Machine)] = {
      val compiler = Compiler(compiledPackages.packages)
      Right({ (expr: Expr) =>
        initial(compiledPackages).copy(ctrl = CtrlExpr(compiler.compile(expr)(SEValue(SToken))))
      })
    }

    def build(sexpr: SExpr, compiledPackages: CompiledPackages): Machine =
      initial(compiledPackages).copy(
        // apply token
        ctrl = CtrlExpr(sexpr(SEValue(SToken))),
      )

    // Used from repl.
    def fromExpr(expr: Expr, compiledPackages: CompiledPackages, scenario: Boolean): Machine = {
      val compiler = Compiler(compiledPackages.packages)
      val sexpr =
        if (scenario)
          compiler.compile(expr)(SEValue(SToken))
        else
          compiler.compile(expr)

      initial(compiledPackages).copy(
        ctrl = CtrlExpr(sexpr),
      )
    }
  }

  /** Control specifies the thing that the machine should be reducing.
    * If the control is fully evaluated then the top-most continuation
    * is executed.
    */
  sealed trait Ctrl {

    /** Execute a single step to reduce the control */
    def execute(machine: Machine): Unit
  }

  /** A special control object to guard against misbehaving operations.
    * It is set by default, so for example if an action forgets to set the
    * control we won't loop but rather we'll crash.
    */
  final case class CtrlCrash(before: Ctrl) extends Ctrl {
    def execute(machine: Machine) =
      crash(s"CtrlCrash: control set to crash after evaluting: $before")
  }

  final case class CtrlExpr(expr: SExpr) extends Ctrl {
    def execute(machine: Machine) =
      machine.ctrl = expr.execute(machine)
  }

  final case class CtrlValue(value: SValue) extends Ctrl {
    def execute(machine: Machine): Unit = value match {
      case pap: SPAP if pap.args.size == pap.arity =>
        pap.prim match {
          case PClosure(expr, vars) =>
            // Pop the arguments once we're done evaluating the body.
            machine.kont.add(KPop(pap.arity + vars.size))

            // Add all the variables we closed over
            vars.foreach(machine.env.add)

            // Add the arguments
            machine.env.addAll(pap.args)

            // And start evaluating the body of the closure.
            machine.ctrl = CtrlExpr(expr)

          case PBuiltin(b) =>
            try {
              b.execute(pap.args, machine)
            } catch {
              // We turn arithmetic exceptions into a daml exception
              // that can be caught.
              case e: ArithmeticException =>
                throw DamlEArithmeticError(e.getMessage)
            }
        }
      case v =>
        machine.ctrl = CtrlValue(v)
        machine.kontPop.execute(value, machine)
    }
  }

  /** When we fetch a contract id from upstream we cannot crash in the
    * that upstream calls. Rather, we set the control to this and then crash
    * when executing.
    */
  final case class CtrlWronglyTypeContractId(
      acoid: AbsoluteContractId,
      expected: TypeConName,
      actual: TypeConName)
      extends Ctrl {
    override def execute(machine: Machine): Unit = {
      throw DamlEWronglyTypedContract(acoid, expected, actual)
    }
  }

  object Ctrl {
    def fromPrim(prim: Prim, arity: Int): Ctrl =
      CtrlValue(SPAP(prim, new ArrayList[SValue](), arity))
  }

  //
  // Environment
  //
  // NOTE(JM): We use ArrayList instead of ArrayBuffer as
  // it is significantly faster.
  type Env = ArrayList[SValue]
  def emptyEnv(): Env = new ArrayList[SValue](512)

  //
  // Kontinuation
  //

  /** Kont, or continuation. Describes the next step for the machine
    * after an expression has been evaluated into a 'SValue'.
    */
  sealed trait Kont {

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
          val args2 = args.clone.asInstanceOf[ArrayList[SValue]]
          val missing = arity - args2.size

          // Stash away over-applied arguments, if any.
          val othersLength = newArgs.length - missing
          if (othersLength > 0) {
            val others = new Array[SExpr](othersLength)
            System.arraycopy(newArgs, missing, others, 0, othersLength)
            machine.kont.add(KArg(others))
          }

          machine.kont.add(KFun(prim, args2, arity))

          // start evaluating the arguments
          val newArgsLimit = Math.min(missing, newArgs.length)
          for (i <- 1 until newArgsLimit) {
            val arg = newArgs(newArgsLimit - i)
            machine.kont.add(KPushTo(args2, arg))
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
  final case class KFun(prim: Prim, args: ArrayList[SValue], var arity: Int) extends Kont {
    def execute(v: SValue, machine: Machine) = {
      args.add(v) // Add last argument
      machine.ctrl = CtrlValue(SPAP(prim, args, arity))
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
        case SVariant(_, con1, arg) =>
          alts.find { alt =>
            alt.pattern match {
              case SCPVariant(_, con2) if con1 == con2 =>
                machine.kont.add(KPop(1))
                machine.env.add(arg)
                true
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
        case _: SUnit =>
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
        case _ =>
          crash("Match on non-matchable value")
      }

      machine.ctrl = CtrlExpr(
        altOpt
          .getOrElse(throw DamlEMatchError(s"No match for $v in ${alts.toList}"))
          .body
      )
    }
  }

  /** Push the evaluated value to the array 'to', and start evaluating the expression 'next'.
    * This continuation is used to implement both function application and lets. In
    * the case of function application the arguments are pushed into the 'args' array of
    * the PAP that is being built, and in the case of lets the evaluated value is pushed
    * direy into the environment.
    */
  final case class KPushTo(to: ArrayList[SValue], next: SExpr) extends Kont {
    def execute(v: SValue, machine: Machine) = {
      to.add(v)
      machine.ctrl = CtrlExpr(next)
    }
  }

  /** Store the evaluated value in the 'SEVal' from which the expression came from.
    * This in principle makes top-level values lazy. It is a useful optimization to
    * allow creation of large constants (for example records) that are repeatedly
    * accessed. In older compilers which did not use the builtin record and tuple
    * updates this solves the blow-up which would happen when a large record is
    * updated multiple times. */
  final case class KCacheVal(v: SEVal) extends Kont {
    def execute(sv: SValue, machine: Machine) = {
      v.cached = Some(sv)
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

  /** Internal exception thrown when a continuation result needs to be returned. */
  final case class SpeedyHungry(result: SResult) extends RuntimeException(result.toString)

}
