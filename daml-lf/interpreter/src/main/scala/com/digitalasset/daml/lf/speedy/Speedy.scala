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
import com.daml.lf.value.{Value => V}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.util.control.NoStackTrace

object Speedy {

  // Would like these to have zero cost when not enabled. Better still, to be switchable at runtime.
  val enableInstrumentation: Boolean = false
  val enableLightweightStepTracing: Boolean = false

  /** Instrumentation counters. */
  final case class Instrumentation(
      var classifyCounts: Classify.Counts,
      var countPushesKont: Int,
      var countPushesEnv: Int,
      var maxDepthKont: Int,
      var maxDepthEnv: Int,
  ) {
    def print(): Unit = {
      println("--------------------")
      println(s"#steps: ${classifyCounts.steps}")
      println(s"#pushEnv: $countPushesEnv")
      println(s"maxDepthEnv: $maxDepthEnv")
      println(s"#pushKont: $countPushesKont")
      println(s"maxDepthKont: $maxDepthKont")
      println("--------------------")
      println(s"classify:\n${classifyCounts.pp}")
      println("--------------------")
    }
  }

  object Instrumentation {
    def apply(): Instrumentation = {
      Instrumentation(
        classifyCounts = new Classify.Counts(),
        countPushesKont = 0,
        countPushesEnv = 0,
        maxDepthKont = 0,
        maxDepthEnv = 0,
      )
    }
  }

  /*
   Speedy uses a caller-saves strategy for managing the environment.  In a Speedy machine,
   the environment is represented by the `frame` and `env` components.

   Continuations are responsible for restoring their own environment. In the general case,
   an arbitrary amount of computation may have occurred between the continuation being
   pushed and then later entered.

   When we push a continuation which requires it's environment to be preserved, we record
   the current Frame and the current env-stack depth within the continuation. Then, when
   the continuation is entered, it will call `restoreEnv`.

   We do this for KArg, KMatch, KPushTo, KCatch.

   We *dont* need to do this for KFun. Because, when KFun is entered, it immediately
   changes `frame` to point to itself, and there will be no references to existing
   stack-variables within the body of the function. (They will have been translated to
   free-var reference by the compiler).
   */

  /** The speedy CEK machine. */
  final case class Machine(
      /* The control is what the machine should be evaluating. If this is not
       * null, then `returnValue` must be null.
       */
      var ctrl: SExpr,
      /* `returnValue` contains the result once the expression in `ctrl` has
       * been fully evaluated. If this is not null, then `ctrl` must be null.
       */
      var returnValue: SValue,
      /* Frame: to access values for function arguments and closure free-vars. */
      var frame: Frame,
      /* Environment: values pushed to a stack: let-bindings and pattern-matches. */
      var env: Env,
      /* Kont, or continuation specifies what should be done next
       * once the control has been evaluated.
       */
      var kontStack: util.ArrayList[Kont],
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
      // local contracts, that are contracts created in the current transaction)
      var localContracts: Map[V.ContractId, (Ref.TypeConName, SValue)],
      // global contract discriminators, that are discriminators from contract created in previous transactions
      var globalDiscriminators: Set[crypto.Hash],
      /* Used when enableLightweightStepTracing is true */
      var steps: Int,
      /* Used when enableInstrumentation is true */
      var track: Instrumentation,
      /* Profile of the run when the packages haven been compiled with profiling enabled. */
      var profile: Profile
  ) {

    /* kont manipulation... */

    @inline def kontDepth(): Int = kontStack.size()

    @inline def pushKont(k: Kont): Unit = {
      kontStack.add(k)
      if (enableInstrumentation) {
        track.countPushesKont += 1
        if (kontDepth > track.maxDepthKont) track.maxDepthKont = kontDepth
      }
    }

    @inline def popKont(): Kont = {
      kontStack.remove(kontStack.size - 1)
    }

    /* env manipulation... */

    // The environment is partitioned into three locations: Stack, Args, Free
    // The run-time location of a variable is determined (at compile time) by closureConvert
    // And made explicit by a specifc speedy expression node: SELocS/SELocA/SELocF
    // At runtime these different location-node execute by calling the corresponding `getEnv*` function

    // Variables which reside on the stack. Indexed by relative offset from the top of the stack
    @inline def getEnvStack(i: Int): SValue = env.get(env.size - i)

    // Variables which reside in the args array of the current frame. Indexed by absolute offset.
    @inline def getEnvArg(i: Int): SValue = frame.args.get(i)

    // Variables which reside in the free-vars array of the current frame. Indexed by absolute offset.
    @inline def getEnvFree(i: Int): SValue = {
      //TODO(NC) : modify types to avoid this asInstanceOf
      frame.prim.asInstanceOf[PClosure].fvs(i)
    }

    @inline def pushEnv(v: SValue): Unit = {
      env.add(v)
      if (enableInstrumentation) {
        track.countPushesEnv += 1
        if (env.size > track.maxDepthEnv) track.maxDepthEnv = env.size
      }
    }

    @inline def restoreEnv(frameToBeRestored: Frame, envSize: Int): Unit = {
      // Restore the frame pointer captured when the continuation was created.
      frame = frameToBeRestored
      // Pop the env-stack back to the size it was when the continuation was created.
      if (envSize != env.size) {
        val count = env.size - envSize
        if (count < 1) {
          crash(s"restoreEnv, unexpected negative count: $count!")
        }
        env.subList(envSize, env.size).clear
      }
    }

    /** Push a single location to the continuation stack for the sake of
        maintaining a stack trace. */
    def pushLocation(loc: Location): Unit = {
      lastLocation = Some(loc)
      val last_index = kontStack.size() - 1
      val last_kont = if (last_index >= 0) Some(kontStack.get(last_index)) else None
      last_kont match {
        // NOTE(MH): If the top of the continuation stack is the monadic token,
        // we push location information under it to account for the implicit
        // lambda binding the token.
        case Some(KArg(Array(SEValue.Token), _, _)) => {
          // Can't call pushKont here, because we don't push at the top of the stack.
          kontStack.add(last_index, KLocation(loc))
          if (enableInstrumentation) {
            track.countPushesKont += 1
            if (kontDepth > track.maxDepthKont) track.maxDepthKont = kontDepth
          }
        }
        // NOTE(MH): When we use a cached top level value, we need to put the
        // stack trace it produced back on the continuation stack to get
        // complete stack trace at the use site. Thus, we store the stack traces
        // of top level values separately during their execution.
        case Some(KCacheVal(v, stack_trace)) =>
          kontStack.set(last_index, KCacheVal(v, loc :: stack_trace)); ()
        case _ => pushKont(KLocation(loc))
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
      kontStack.forEach { k =>
        k match {
          case KLocation(location) => { s.add(location); () }
          case _ => ()
        }
      }
      ImmArray(s.asScala)
    }

    def addLocalContract(coid: V.ContractId, templateId: Ref.TypeConName, SValue: SValue) =
      coid match {
        case V.AbsoluteContractId.V1(discriminator, _)
            if globalDiscriminators.contains(discriminator) =>
          crash("Conflicting discriminators between a global and local contract ID.")
        case _ =>
          localContracts = localContracts.updated(coid, templateId -> SValue)
      }

    def addGlobalCid(cid: V.ContractId) = cid match {
      case V.AbsoluteContractId.V1(discriminator, _) =>
        if (localContracts.isDefinedAt(V.AbsoluteContractId.V1(discriminator)))
          crash("Conflicting discriminators between a global and local contract ID.")
        else
          globalDiscriminators = globalDiscriminators + discriminator
      case _ =>
    }

    /** Reuse an existing speedy machine to evaluate a new expression.
      Do not use if the machine is partway though an existing evaluation.
      i.e. run() has returned an `SResult` requiring a callback.
      */
    def setExpressionToEvaluate(expr: SExpr): Unit = {
      ctrl = expr
      kontStack = initialKontStack()
      env = emptyEnv
    }

    /** Run a machine until we get a result: either a final-value or a request for data, with a callback */
    def run(): SResult = {
      // Note: We have an outer and inner while loop.
      // An exception handler is wrapped around the inner-loop, but inside the outer-loop.
      // Most iterations are performed by the inner-loop, thus avoiding the work of to
      // wrap the exception handler on each of these steps. This is a performace gain.
      // However, we still need the outer loop because of the case:
      //    case _:SErrorDamlException if tryHandleException =>
      // where we must continue iteration.
      var result: SResult = null
      while (result == null) {
        // note: exception handler is outside while loop
        try {
          // normal exit from this loop is when KFinished.execute throws SpeedyHungry
          while (true) {
            if (enableInstrumentation) {
              Classify.classifyMachine(this, track.classifyCounts)
            }
            if (enableLightweightStepTracing) {
              steps += 1
              println(s"$steps: ${PrettyLightweight.ppMachine(this)}")
            }
            if (returnValue != null) {
              val value = returnValue
              returnValue = null
              popKont.execute(value, this)
            } else {
              val expr = ctrl
              ctrl = null
              expr.execute(this)
            }
          }
        } catch {
          case SpeedyHungry(res: SResult) => result = res //stop
          case serr: SError =>
            serr match {
              case _: SErrorDamlException if tryHandleException => () // outer loop will run again
              case _ => result = SResultError(serr) //stop
            }
          case ex: RuntimeException =>
            result = SResultError(SErrorCrash(s"exception: $ex")) //stop
        }
      }
      result
    }

    /** Try to handle a DAML exception by looking for
      * the catch handler. Returns true if the exception
      * was catched.
      */
    def tryHandleException(): Boolean = {
      val catchIndex =
        kontStack.asScala.lastIndexWhere(_.isInstanceOf[KCatch])
      if (catchIndex >= 0) {
        val kcatch = kontStack.get(catchIndex).asInstanceOf[KCatch]
        kontStack.subList(catchIndex, kontStack.size).clear()
        env.subList(kcatch.envSize, env.size).clear()
        ctrl = kcatch.handler
        true
      } else
        false
    }

    def lookupVal(eval: SEVal): Unit = {
      eval.cached match {
        case Some((v, stack_trace)) =>
          pushStackTrace(stack_trace)
          returnValue = v

        case None =>
          val ref = eval.ref
          compiledPackages.getDefinition(ref) match {
            case Some(body) =>
              pushKont(KCacheVal(eval, Nil))
              ctrl = body
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
                      lookupVal(eval)
                    }
                  ),
                )
          }
      }
    }

    def enterFullyAppliedFunction(prim: Prim, args: util.ArrayList[SValue]): Unit = {
      prim match {
        case PClosure(label, expr, _) =>
          if (label != null) {
            profile.addOpenEvent(label)
            pushKont(KLeaveClosure(label))
          }
          // Start evaluating the body of the closure. (We dont do anything with the
          // function arguments or free-varables, because the frame will have been set to
          // allow their access within the body).
          ctrl = expr

        case PBuiltin(b) =>
          try {
            b.execute(args, this)
          } catch {
            // We turn arithmetic exceptions into a daml exception
            // that can be caught.
            case e: ArithmeticException =>
              throw DamlEArithmeticError(e.getMessage)
          }
      }
    }

    def print(count: Int) = {
      println(s"Step: $count")
      if (returnValue != null) {
        println("Control: null")
        println("Return:")
        println(s"  ${returnValue}")
      } else {
        println("Control:")
        println(s"  ${ctrl}")
        println("Return: null")
      }
      println("Environment:")
      env.forEach { v =>
        println("  " + v.toString)
      }
      println("Kontinuation:")
      kontStack.forEach { k =>
        println(s"  " + k.toString)
      }
      println("============================================================")
    }

    // fake participant to generate a new transactionSeed when running scenarios
    private val scenarioServiceParticipant = Ref.ParticipantId.assertFromString("scenario-service")

    // reinitialize the state of the machine with a new fresh submission seed.
    // Should be used only when running scenario
    def clearCommit: Unit = {
      val freshSeed =
        crypto.Hash.deriveTransactionSeed(
          ptx.context.nextChildrenSeed,
          scenarioServiceParticipant,
          ptx.submissionTime)
      committers = Set.empty
      commitLocation = None
      ptx = PartialTransaction.initial(
        submissionTime = ptx.submissionTime,
        InitialSeeding.TransactionSeed(freshSeed),
      )
    }

    // This translates a well-typed LF value (typically coming from the ledger)
    // to speedy value and set the control of with the result.
    // All the contract IDs contained in the value are considered global.
    // Raises an exception if missing a package.

    def importValue(value: V[V.ContractId]): Unit = {
      def go(value0: V[V.ContractId]): SValue =
        value0 match {
          case V.ValueList(vs) => SList(vs.map[SValue](go))
          case V.ValueContractId(coid) =>
            addGlobalCid(coid)
            SContractId(coid)
          case V.ValueInt64(x) => SInt64(x)
          case V.ValueNumeric(x) => SNumeric(x)
          case V.ValueText(t) => SText(t)
          case V.ValueTimestamp(t) => STimestamp(t)
          case V.ValueParty(p) => SParty(p)
          case V.ValueBool(b) => SBool(b)
          case V.ValueDate(x) => SDate(x)
          case V.ValueUnit => SUnit
          case V.ValueRecord(Some(id), fs) =>
            val fields = Name.Array.ofDim(fs.length)
            val values = new util.ArrayList[SValue](fields.length)
            fs.foreach {
              case (optk, v) =>
                optk match {
                  case None =>
                    crash("SValue.fromValue: record missing field name")
                  case Some(k) =>
                    fields(values.size) = k
                    val _ = values.add(go(v))
                }
            }
            SRecord(id, fields, values)
          case V.ValueRecord(None, _) =>
            crash("SValue.fromValue: record missing identifier")
          case V.ValueStruct(fs) =>
            val fields = Name.Array.ofDim(fs.length)
            val values = new util.ArrayList[SValue](fields.length)
            fs.foreach {
              case (k, v) =>
                fields(values.size) = k
                val _ = values.add(go(v))
            }
            SStruct(fields, values)
          case V.ValueVariant(None, _variant @ _, _value @ _) =>
            crash("SValue.fromValue: variant without identifier")
          case V.ValueEnum(None, constructor @ _) =>
            crash("SValue.fromValue: enum without identifier")
          case V.ValueOptional(mbV) =>
            SOptional(mbV.map(go))
          case V.ValueTextMap(map) =>
            STextMap(map.mapValue(go).toHashMap)
          case V.ValueGenMap(entries) =>
            SGenMap(
              entries.iterator.map { case (k, v) => go(k) -> go(v) }
            )
          case V.ValueVariant(Some(id), variant, arg) =>
            compiledPackages.getPackage(id.packageId) match {
              case Some(pkg) =>
                pkg.lookupIdentifier(id.qualifiedName).fold(crash, identity) match {
                  case DDataType(_, _, data: DataVariant) =>
                    SVariant(id, variant, data.constructorRank(variant), go(arg))
                  case _ =>
                    crash(s"definition for variant $id not found")
                }
              case None =>
                throw SpeedyHungry(
                  SResultNeedPackage(
                    id.packageId,
                    pkg => {
                      compiledPackages = pkg
                      returnValue = go(value)
                    }
                  ))
            }
          case V.ValueEnum(Some(id), constructor) =>
            compiledPackages.getPackage(id.packageId) match {
              case Some(pkg) =>
                pkg.lookupIdentifier(id.qualifiedName).fold(crash, identity) match {
                  case DDataType(_, _, data: DataEnum) =>
                    SEnum(id, constructor, data.constructorRank(constructor))
                  case _ =>
                    crash(s"definition for variant $id not found")
                }
              case None =>
                throw SpeedyHungry(
                  SResultNeedPackage(
                    id.packageId,
                    pkg => {
                      compiledPackages = pkg
                      returnValue = go(value)
                    }
                  ))
            }
        }
      returnValue = go(value)
    }

  }

  object Machine {

    private val damlTraceLog = LoggerFactory.getLogger("daml.tracelog")

    private def initial(
        compiledPackages: CompiledPackages,
        submissionTime: Time.Timestamp,
        initialSeeding: InitialSeeding,
        globalCids: Set[V.AbsoluteContractId]
    ) =
      Machine(
        ctrl = null,
        returnValue = null,
        frame = null,
        env = emptyEnv,
        kontStack = initialKontStack(),
        lastLocation = None,
        ptx = PartialTransaction.initial(submissionTime, initialSeeding),
        committers = Set.empty,
        commitLocation = None,
        traceLog = TraceLog(damlTraceLog, 100),
        compiledPackages = compiledPackages,
        validating = false,
        dependsOnTime = false,
        localContracts = Map.empty,
        globalDiscriminators = globalCids.collect {
          case V.AbsoluteContractId.V1(discriminator, _) => discriminator
        },
        steps = 0,
        track = Instrumentation(),
        profile = new Profile(),
      )

    def newBuilder(
        compiledPackages: CompiledPackages,
        submissionTime: Time.Timestamp,
        submissionSeed: crypto.Hash,
    ): Either[SError, Expr => Machine] = {
      val compiler = compiledPackages.compiler
      Right(
        (expr: Expr) =>
          fromSExpr(
            SEApp(compiler.unsafeCompile(expr), Array(SEValue.Token)),
            compiledPackages,
            submissionTime,
            InitialSeeding.TransactionSeed(submissionSeed),
            Set.empty
        ))
    }

    def build(
        sexpr: SExpr,
        compiledPackages: CompiledPackages,
        submissionTime: Time.Timestamp,
        seeds: InitialSeeding,
        globalCids: Set[V.AbsoluteContractId],
    ): Machine =
      fromSExpr(
        SEApp(sexpr, Array(SEValue.Token)),
        compiledPackages,
        submissionTime,
        seeds,
        globalCids
      )

    // Used from repl.
    def fromExpr(
        expr: Expr,
        compiledPackages: CompiledPackages,
        scenario: Boolean,
        submissionTime: Time.Timestamp,
        initialSeeding: InitialSeeding,
    ): Machine = {
      val compiler = compiledPackages.compiler
      val sexpr =
        if (scenario)
          SEApp(compiler.unsafeCompile(expr), Array(SEValue.Token))
        else
          compiler.unsafeCompile(expr)

      fromSExpr(
        sexpr,
        compiledPackages,
        submissionTime,
        initialSeeding,
        Set.empty,
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
        globalCids: Set[V.AbsoluteContractId],
    ): Machine =
      initial(compiledPackages, submissionTime, seeding, globalCids).copy(ctrl = sexpr)
  }

  //
  // Frame
  //
  // For our frame, we use the KFun continuation directly.  From here
  // we can access both the application arguments, and the values of
  // the free-variables which were stored into the closure.
  type Frame = KFun

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
  // Whilst the machine is running, we ensure the kontStack is *never* empty.
  // We do this by pushing a KFinished continutaion on the initially empty stack, which
  // returns the final result (by raising it as a SpeedyHungry exception).

  def initialKontStack(): util.ArrayList[Kont] = {
    val kontStack = new util.ArrayList[Kont](128)
    kontStack.add(KFinished)
    kontStack
  }

  /** Kont, or continuation. Describes the next step for the machine
    * after an expression has been evaluated into a 'SValue'.
    */
  sealed trait Kont {

    /** Execute the continuation. */
    def execute(v: SValue, machine: Machine): Unit
  }

  /** Final continuation; machine has computed final value */
  final case object KFinished extends Kont {
    def execute(v: SValue, machine: Machine) = {
      if (enableInstrumentation) {
        machine.track.print()
      }
      throw SpeedyHungry(SResultFinalValue(v))
    }
  }

  /** The function has been evaluated to a value, now start evaluating the arguments. */
  final case class KArg(newArgs: Array[SExpr], frame: Frame, envSize: Int)
      extends Kont
      with SomeArrayEquals {
    def execute(v: SValue, machine: Machine) = {
      machine.restoreEnv(frame, envSize)
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
            machine.pushKont(KArg(others, machine.frame, machine.env.size))
          }

          machine.pushKont(KFun(prim, extendedArgs, arity))

          // Start evaluating the arguments.
          var i = 1
          while (i < newArgsLimit) {
            val arg = newArgs(newArgsLimit - i)
            machine.pushKont(KPushTo(extendedArgs, arg, machine.frame, machine.env.size))
            i = i + 1
          }
          machine.ctrl = newArgs(0)

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
      if (args.size == arity) {
        // Set the frame to enable access for Arg/Free variable references
        machine.frame = this
        machine.enterFullyAppliedFunction(prim, args)
      } else {
        // args.size < arity (we already dealt with args.size > args in Karg)
        machine.returnValue = SPAP(prim, args, arity)
      }
    }
  }

  /** The scrutinee of a match has been evaluated, now match the alternatives against it. */
  final case class KMatch(alts: Array[SCaseAlt], frame: Frame, envSize: Int)
      extends Kont
      with SomeArrayEquals {
    def execute(v: SValue, machine: Machine) = {
      machine.restoreEnv(frame, envSize)
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
                machine.pushEnv(arg)
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
                val Some((head, tail)) = lst.pop
                machine.pushEnv(head)
                machine.pushEnv(SList(tail))
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
                    machine.pushEnv(x)
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

      machine.ctrl = altOpt
        .getOrElse(throw DamlEMatchError(s"No match for $v in ${alts.toList}"))
        .body
    }
  }

  /** Push the evaluated value to the array 'to', and start evaluating the expression 'next'.
    * This continuation is used to implement both function application and lets. In
    * the case of function application the arguments are pushed into the 'args' array of
    * the PAP that is being built, and in the case of lets the evaluated value is pushed
    * direy into the environment.
    */
  final case class KPushTo(to: util.ArrayList[SValue], next: SExpr, frame: Frame, envSize: Int)
      extends Kont {
    def execute(v: SValue, machine: Machine) = {
      machine.restoreEnv(frame, envSize)
      to.add(v)
      machine.ctrl = next
    }
  }

  /** Store the evaluated value in the 'SEVal' from which the expression came from.
    * This in principle makes top-level values lazy. It is a useful optimization to
    * allow creation of large constants (for example records) that are repeatedly
    * accessed. In older compilers which did not use the builtin record and struct
    * updates this solves the blow-up which would happen when a large record is
    * updated multiple times. */
  final case class KCacheVal(v: SEVal, stack_trace: List[Location]) extends Kont {
    def execute(sv: SValue, machine: Machine): Unit = {
      machine.pushStackTrace(stack_trace)
      v.setCached(sv, stack_trace)
      machine.returnValue = sv
    }
  }

  /** A catch frame marks the point to which an exception (of type 'SErrorDamlException')
    * is unwound. The 'envSize' specifies the size to which the environment must be pruned.
    * If an exception is raised and 'KCatch' is found from kont-stack, then 'handler' is
    * evaluated. If 'KCatch' is encountered naturally, then 'fin' is evaluated.
    */
  final case class KCatch(handler: SExpr, fin: SExpr, frame: Frame, envSize: Int) extends Kont {
    def execute(v: SValue, machine: Machine) = {
      machine.restoreEnv(frame, envSize)
      machine.ctrl = fin
    }
  }

  /** A location frame stores a location annotation found in the AST. */
  final case class KLocation(location: Location) extends Kont {
    def execute(v: SValue, machine: Machine) = {
      machine.returnValue = v
    }
  }

  /** Continuation produced by [[SELabelClsoure]] expressions. This is only
    * used during profiling. Its purpose is to attach a label to closures such
    * that entering the closure can write an "open event" with that label.
    */
  final case class KLabelClosure(label: AnyRef) extends Kont {
    def execute(v: SValue, machine: Machine) = {
      v match {
        case SPAP(PClosure(_, expr, closure), args, arity) =>
          machine.returnValue = SPAP(PClosure(label, expr, closure), args, arity)
        case _ =>
          machine.returnValue = v
      }
    }
  }

  /** Continuation marking the exit of a closure. This is only used during
    * profiling.
    */
  final case class KLeaveClosure(label: AnyRef) extends Kont {
    def execute(v: SValue, machine: Machine) = {
      machine.profile.addCloseEvent(label)
      machine.returnValue = v
    }
  }

  /** Internal exception thrown when a continuation result needs to be returned.
    Or machine execution has reached a final value. */
  final case class SpeedyHungry(result: SResult) extends RuntimeException with NoStackTrace

  def deriveTransactionSeed(
      submissionSeed: crypto.Hash,
      participant: Ref.ParticipantId,
      submissionTime: Time.Timestamp,
  ): InitialSeeding =
    InitialSeeding.TransactionSeed(
      crypto.Hash.deriveTransactionSeed(submissionSeed, participant, submissionTime))

}
