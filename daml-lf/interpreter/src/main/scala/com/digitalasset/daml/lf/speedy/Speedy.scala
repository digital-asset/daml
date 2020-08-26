// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import java.util

import com.daml.lf.data.Ref._
import com.daml.lf.data.{FrontStack, ImmArray, Ref, Time}
import com.daml.lf.language.Ast._
import com.daml.lf.speedy.Compiler.{CompilationError, PackageNotFound}
import com.daml.lf.speedy.SError._
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SResult._
import com.daml.lf.speedy.SValue._
import com.daml.lf.transaction.{TransactionVersion, TransactionVersions}
import com.daml.lf.value.{ValueVersion, ValueVersions, Value => V}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.util.control.NoStackTrace

private[lf] object Speedy {

  // fake participant to generate a new transactionSeed when running scenarios
  private[this] val scenarioServiceParticipant =
    Ref.ParticipantId.assertFromString("scenario-service")

  // Would like these to have zero cost when not enabled. Better still, to be switchable at runtime.
  private[this] val enableInstrumentation: Boolean = false
  private[this] val enableLightweightStepTracing: Boolean = false

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

  private object Instrumentation {
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
   the environment is represented by the `frame`, `actuals`, and `env` components.

   We use the terminology "frame" for the array of values which correspond to the
   evaluated "free-vars" of a function closure.

   We use the terminology "actuals" for the array of values which correspond to the
   evaluated "args" of a function application. (The args being an array of expressions)

   The environment "env" is now only used for let-bindings and pattern-matches.

   Continuations are responsible for restoring their own environment. In the general case,
   an arbitrary amount of computation may have occurred between the continuation being
   pushed and then later entered.

   When we push a continuation which requires it's environment to be preserved, we record
   the current frame, actuals and env-stack-depth within the continuation. Then, when the
   continuation is entered, it will call `restoreEnv`.

   We do this for KArg, KMatch, KPushTo, KCatch.

   We *dont* need to do this for KFun. Because, when KFun is entered, it immediately
   changes `frame` to point to itself, and there will be no references to existing
   stack-variables within the body of the function. (They will have been translated to
   free-var reference by the compiler).
   */

  private type Frame = Array[SValue]

  private type Actuals = util.ArrayList[SValue]

  /** The speedy CEK machine. */
  final class Machine(
      /* Transaction versions that the machine can read */
      val inputValueVersions: VersionRange[ValueVersion],
      /* Transaction versions that the machine can output */
      val outputTransactionVersions: VersionRange[TransactionVersion],
      /* Whether the current submission is validating the transaction, or interpreting
       * it. If this is false, the committers must be a singleton set.
       */
      val validating: Boolean,
      /* The control is what the machine should be evaluating. If this is not
       * null, then `returnValue` must be null.
       */
      var ctrl: SExpr,
      /* `returnValue` contains the result once the expression in `ctrl` has
       * been fully evaluated. If this is not null, then `ctrl` must be null.
       */
      var returnValue: SValue,
      /* Frame: to access values for a closure's free-vars. */
      var frame: Frame,
      /* Actuals: to access values for a function application's arguments. */
      var actuals: Actuals,
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
      /* The trace log. */
      val traceLog: TraceLog,
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
      var profile: Profile,
      /* True if we are running on ledger building transactions, false if we
         are running off-ledger code, e.g., DAML Script or
         Triggers. It is safe to use on ledger for off ledger code but
         not the other way around.
       */
      val onLedger: Boolean,
  ) {

    /* kont manipulation... */

    @inline
    private[speedy] def kontDepth(): Int = kontStack.size()

    @inline
    private[speedy] def pushKont(k: Kont): Unit = {
      kontStack.add(k)
      if (enableInstrumentation) {
        track.countPushesKont += 1
        if (kontDepth > track.maxDepthKont) track.maxDepthKont = kontDepth
      }
    }

    @inline
    private[speedy] def popKont(): Kont = {
      kontStack.remove(kontStack.size - 1)
    }

    /* env manipulation... */

    // The environment is partitioned into three locations: Stack, Args, Free
    // The run-time location of a variable is determined (at compile time) by closureConvert
    // And made explicit by a specifc speedy expression node: SELocS/SELocA/SELocF
    // At runtime these different location-node execute by calling the corresponding `getEnv*` function

    // Variables which reside on the stack. Indexed by relative offset from the top of the stack
    @inline
    private[speedy] def getEnvStack(i: Int): SValue = env.get(env.size - i)

    // Variables which reside in the args array of the current frame. Indexed by absolute offset.
    @inline
    private[speedy] def getEnvArg(i: Int): SValue = actuals.get(i)

    // Variables which reside in the free-vars array of the current frame. Indexed by absolute offset.
    @inline
    private[speedy] def getEnvFree(i: Int): SValue = frame(i)

    @inline def pushEnv(v: SValue): Unit = {
      env.add(v)
      if (enableInstrumentation) {
        track.countPushesEnv += 1
        if (env.size > track.maxDepthEnv) track.maxDepthEnv = env.size
      }
    }

    @inline
    def restoreEnv(
        frameToBeRestored: Frame,
        actualsToBeRestored: Actuals,
        envSizeToBeRestored: Int,
    ): Unit = {
      // Restore the frame and actuals to there state when the continuation was created.
      frame = frameToBeRestored
      actuals = actualsToBeRestored
      // Pop the env-stack back to the size it was when the continuation was created.
      if (envSizeToBeRestored != env.size) {
        val count = env.size - envSizeToBeRestored
        if (count < 1) {
          crash(s"restoreEnv, unexpected negative count: $count!")
        }
        env.subList(envSizeToBeRestored, env.size).clear
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
        case Some(KArg(Array(SEValue.Token), _, _, _)) => {
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
      val s = ImmArray.newBuilder[Location]
      kontStack.forEach { k =>
        k match {
          case KLocation(location) => s += location
          case _ => ()
        }
      }
      s.result()
    }

    def addLocalContract(coid: V.ContractId, templateId: Ref.TypeConName, SValue: SValue) =
      coid match {
        case V.ContractId.V1(discriminator, _) if globalDiscriminators.contains(discriminator) =>
          crash("Conflicting discriminators between a global and local contract ID.")
        case _ =>
          localContracts = localContracts.updated(coid, templateId -> SValue)
      }

    def addGlobalCid(cid: V.ContractId) = cid match {
      case V.ContractId.V1(discriminator, _) =>
        if (localContracts.isDefinedAt(V.ContractId.V1(discriminator)))
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
      steps = 0
      track = Instrumentation()
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
              case _: SErrorDamlException if tryHandleException() => () // outer loop will run again
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

    /** This function is used to enter an ANF application.  The function has been evaluated to
      a value, and so have the arguments - they just need looking up */
    // TODO: share common code with executeApplication
    private[speedy] def enterApplication(vfun: SValue, newArgs: Array[SExprAtomic]): Unit = {
      vfun match {
        case SPAP(prim, actualsSoFar, arity) =>
          val missing = arity - actualsSoFar.size
          val newArgsLimit = Math.min(missing, newArgs.length)

          val actuals = new util.ArrayList[SValue](actualsSoFar.size + newArgsLimit)
          actuals.addAll(actualsSoFar)

          val othersLength = newArgs.length - missing

          // Evaluate the arguments
          var i = 0
          while (i < newArgsLimit) {
            val newArg = newArgs(i)
            val v = newArg.lookupValue(this)
            actuals.add(v)
            i += 1
          }

          // Not enough arguments. Return a PAP.
          if (othersLength < 0) {
            this.returnValue = SPAP(prim, actuals, arity)

          } else {
            // Too many arguments: Push a continuation to re-apply the over-applied args.
            if (othersLength > 0) {
              val others = new Array[SExprAtomic](othersLength)
              System.arraycopy(newArgs, missing, others, 0, othersLength)
              this.pushKont(KOverApp(others, this.frame, this.actuals, this.env.size))
            }
            // Now the correct number of arguments is ensured. What kind of prim do we have?
            prim match {
              case closure: PClosure =>
                this.frame = closure.frame
                this.actuals = actuals
                // Maybe push a continuation for the profiler
                val label = closure.label
                if (label != null) {
                  this.profile.addOpenEvent(label)
                  this.pushKont(KLeaveClosure(label))
                }
                // Start evaluating the body of the closure.
                this.ctrl = closure.expr

              case PBuiltin(builtin) =>
                this.actuals = actuals
                try {
                  builtin.execute(actuals, this)
                } catch {
                  // We turn arithmetic exceptions into a daml exception that can be caught.
                  case e: ArithmeticException =>
                    throw DamlEArithmeticError(e.getMessage)
                }

            }
          }

        case _ =>
          crash(s"Applying non-PAP: $vfun")
      }
    }

    /** The function has been evaluated to a value, now start evaluating the arguments. */
    private[speedy] def executeApplication(vfun: SValue, newArgs: Array[SExpr]): Unit = {
      vfun match {
        case SPAP(prim, actualsSoFar, arity) =>
          val missing = arity - actualsSoFar.size
          val newArgsLimit = Math.min(missing, newArgs.length)

          val actuals = new util.ArrayList[SValue](actualsSoFar.size + newArgsLimit)
          actuals.addAll(actualsSoFar)

          val othersLength = newArgs.length - missing

          // Not enough arguments. Push a continuation to construct the PAP.
          if (othersLength < 0) {
            this.pushKont(KPap(prim, actuals, arity))
          } else {
            // Too many arguments: Push a continuation to re-apply the over-applied args.
            if (othersLength > 0) {
              val others = new Array[SExpr](othersLength)
              System.arraycopy(newArgs, missing, others, 0, othersLength)
              this.pushKont(KArg(others, this.frame, this.actuals, this.env.size))
            }
            // Now the correct number of arguments is ensured. What kind of prim do we have?
            prim match {
              case closure: PClosure =>
                // Push a continuation to execute the function body when the arguments have been evaluated
                this.pushKont(KFun(closure, actuals, this.env.size))

              case PBuiltin(builtin) =>
                // Push a continuation to execute the builtin when the arguments have been evaluated
                this.pushKont(KBuiltin(builtin, actuals, this.env.size))
            }
          }
          this.evaluateArguments(actuals, newArgs, newArgsLimit)

        case _ =>
          crash(s"Applying non-PAP: $vfun")
      }
    }

    /** Evaluate the first 'n' arguments in 'args'.
      'args' will contain at least 'n' expressions, but it may contain more(!)

      This is because, in the call from 'executeApplication' below, although over-applied
      arguments are pushed into a continuation, they are not removed from the original array
      which is passed here as 'args'.
      */
    private[speedy] def evaluateArguments(
        actuals: util.ArrayList[SValue],
        args: Array[SExpr],
        n: Int) = {
      var i = 1
      while (i < n) {
        val arg = args(n - i)
        this.pushKont(KPushTo(actuals, arg, this.frame, this.actuals, this.env.size))
        i = i + 1
      }
      this.ctrl = args(0)
    }

    private[speedy] def print(count: Int) = {
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

    // reinitialize the state of the machine with a new fresh submission seed.
    // Should be used only when running scenario
    private[lf] def clearCommit: Unit = {
      val freshSeed =
        crypto.Hash.deriveTransactionSeed(
          ptx.context.nextChildrenSeed,
          scenarioServiceParticipant,
          ptx.submissionTime,
        )
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
    private[speedy] def importValue(value: V[V.ContractId]): Unit = {
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
            val values = new util.ArrayList[SValue](fs.length)
            val names = fs.map {
              case (Some(f), v) =>
                values.add(go(v))
                f
              case (None, _) => crash("SValue.fromValue: record missing field name")
            }
            SRecord(id, names, values)
          case V.ValueRecord(None, _) =>
            crash("SValue.fromValue: record missing identifier")
          case V.ValueStruct(fs) =>
            val values = new util.ArrayList[SValue](fs.size)
            fs.values.foreach(v => values.add(go(v)))
            SStruct(fs.names.to[ImmArray], values)
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

    def apply(
        compiledPackages: CompiledPackages,
        submissionTime: Time.Timestamp,
        initialSeeding: InitialSeeding,
        expr: SExpr,
        globalCids: Set[V.ContractId],
        committers: Set[Party],
        inputValueVersions: VersionRange[ValueVersion],
        outputTransactionVersions: VersionRange[TransactionVersion],
        validating: Boolean = false,
        onLedger: Boolean = true,
        traceLog: TraceLog = RingBufferTraceLog(damlTraceLog, 100),
    ): Machine =
      new Machine(
        inputValueVersions = inputValueVersions,
        outputTransactionVersions = outputTransactionVersions,
        validating = validating,
        ctrl = expr,
        returnValue = null,
        frame = null,
        actuals = null,
        env = emptyEnv,
        kontStack = initialKontStack(),
        lastLocation = None,
        ptx = PartialTransaction.initial(submissionTime, initialSeeding),
        committers = committers,
        commitLocation = None,
        traceLog = traceLog,
        compiledPackages = compiledPackages,
        dependsOnTime = false,
        localContracts = Map.empty,
        globalDiscriminators = globalCids.collect {
          case V.ContractId.V1(discriminator, _) => discriminator
        },
        steps = 0,
        track = Instrumentation(),
        profile = new Profile(),
        onLedger = onLedger,
      )

    @throws[PackageNotFound]
    @throws[CompilationError]
    // Construct a machine for running scenario.
    def fromScenarioSExpr(
        compiledPackages: CompiledPackages,
        transactionSeed: crypto.Hash,
        scenario: SExpr,
        inputValueVersions: VersionRange[ValueVersion],
        outputTransactionVersions: VersionRange[TransactionVersion],
    ): Machine = Machine(
      compiledPackages = compiledPackages,
      submissionTime = Time.Timestamp.MinValue,
      initialSeeding = InitialSeeding.TransactionSeed(transactionSeed),
      expr = SEApp(scenario, Array(SEValue.Token)),
      globalCids = Set.empty,
      committers = Set.empty,
      inputValueVersions: VersionRange[ValueVersion],
      outputTransactionVersions = outputTransactionVersions,
    )

    @throws[PackageNotFound]
    @throws[CompilationError]
    // Construct a machine for running scenario.
    def fromScenarioExpr(
        compiledPackages: CompiledPackages,
        transactionSeed: crypto.Hash,
        scenario: Expr,
        inputValueVersions: VersionRange[ValueVersion],
        outputTransactionVersions: VersionRange[TransactionVersion],
    ): Machine =
      fromScenarioSExpr(
        compiledPackages = compiledPackages,
        transactionSeed = transactionSeed,
        scenario = compiledPackages.compiler.unsafeCompile(scenario),
        inputValueVersions = inputValueVersions,
        outputTransactionVersions = outputTransactionVersions,
      )

    @throws[PackageNotFound]
    @throws[CompilationError]
    // Construct a machine for evaluating an expression that is neither an update nor a scenario expression.
    def fromPureSExpr(
        compiledPackages: CompiledPackages,
        expr: SExpr,
        onLedger: Boolean = true,
    ): Machine =
      Machine(
        compiledPackages = compiledPackages,
        submissionTime = Time.Timestamp.MinValue,
        initialSeeding = InitialSeeding.NoSeed,
        expr = expr,
        globalCids = Set.empty,
        committers = Set.empty,
        inputValueVersions = ValueVersions.Empty,
        outputTransactionVersions = TransactionVersions.Empty,
        onLedger = onLedger,
      )

    @throws[PackageNotFound]
    @throws[CompilationError]
    // Construct a machine for evaluating an expression that is neither an update nor a scenario expression.
    def fromPureExpr(
        compiledPackages: CompiledPackages,
        expr: Expr,
        onLedger: Boolean = true,
    ): Machine =
      fromPureSExpr(compiledPackages, compiledPackages.compiler.unsafeCompile(expr), onLedger)

  }

  // Environment
  //
  // NOTE(JM): We use ArrayList instead of ArrayBuffer as
  // it is significantly faster.
  private[speedy] type Env = util.ArrayList[SValue]
  private[speedy] def emptyEnv: Env = new util.ArrayList[SValue](512)

  //
  // Kontinuation
  //
  // Whilst the machine is running, we ensure the kontStack is *never* empty.
  // We do this by pushing a KFinished continutaion on the initially empty stack, which
  // returns the final result (by raising it as a SpeedyHungry exception).

  private[this] def initialKontStack(): util.ArrayList[Kont] = {
    val kontStack = new util.ArrayList[Kont](128)
    kontStack.add(KFinished)
    kontStack
  }

  /** Kont, or continuation. Describes the next step for the machine
    * after an expression has been evaluated into a 'SValue'.
    */
  private[speedy] sealed trait Kont {

    /** Execute the continuation. */
    def execute(v: SValue, machine: Machine): Unit
  }

  /** Final continuation; machine has computed final value */
  private[speedy] final case object KFinished extends Kont {
    def execute(v: SValue, machine: Machine) = {
      if (enableInstrumentation) {
        machine.track.print()
      }
      throw SpeedyHungry(SResultFinalValue(v))
    }
  }

  private[speedy] final case class KOverApp(
      newArgs: Array[SExprAtomic],
      frame: Frame,
      actuals: Actuals,
      envSize: Int)
      extends Kont
      with SomeArrayEquals {
    def execute(vfun: SValue, machine: Machine) = {
      machine.restoreEnv(frame, actuals, envSize)
      machine.enterApplication(vfun, newArgs)
    }
  }

  /** The function has been evaluated to a value. Now restore the environment and execute the application */
  private[speedy] final case class KArg(
      newArgs: Array[SExpr],
      frame: Frame,
      actuals: Actuals,
      envSize: Int)
      extends Kont
      with SomeArrayEquals {
    def execute(vfun: SValue, machine: Machine) = {
      machine.restoreEnv(frame, actuals, envSize)
      machine.executeApplication(vfun, newArgs)
    }
  }

  /** The function-closure and arguments have been evaluated. Now execute the body. */
  private[speedy] final case class KFun(
      closure: PClosure,
      actuals: util.ArrayList[SValue],
      envSize: Int)
      extends Kont
      with SomeArrayEquals {
    def execute(v: SValue, machine: Machine) = {
      actuals.add(v)
      // Set frame/actuals to allow access to the function arguments and closure free-varables.
      machine.restoreEnv(closure.frame, actuals, envSize)
      // Maybe push a continuation for the profiler
      val label = closure.label
      if (label != null) {
        machine.profile.addOpenEvent(label)
        machine.pushKont(KLeaveClosure(label))
      }
      // Start evaluating the body of the closure.
      machine.ctrl = closure.expr
    }
  }

  /** The builtin arguments have been evaluated. Now execute the builtin. */
  private[speedy] final case class KBuiltin(
      builtin: SBuiltin,
      actuals: util.ArrayList[SValue],
      envSize: Int)
      extends Kont {
    def execute(v: SValue, machine: Machine) = {
      actuals.add(v)
      // A builtin has no free-vars, so we set the frame to null.
      machine.restoreEnv(null, actuals, envSize)
      try {
        builtin.execute(actuals, machine)
      } catch {
        // We turn arithmetic exceptions into a daml exception that can be caught.
        case e: ArithmeticException =>
          throw DamlEArithmeticError(e.getMessage)
      }
    }
  }

  /** The function's partial-arguments have been evaluated. Construct and return the PAP */
  private[speedy] final case class KPap(prim: Prim, actuals: util.ArrayList[SValue], arity: Int)
      extends Kont {
    def execute(v: SValue, machine: Machine) = {
      actuals.add(v)
      machine.returnValue = SPAP(prim, actuals, arity)
    }
  }

  /** The scrutinee of a match has been evaluated, now match the alternatives against it. */
  private[speedy] def executeMatchAlts(machine: Machine, alts: Array[SCaseAlt], v: SValue): Unit = {
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
      case SContractId(_) | SDate(_) | SNumeric(_) | SInt64(_) | SParty(_) | SText(_) | STimestamp(
            _) | SStruct(_, _) | STextMap(_) | SGenMap(_) | SRecord(_, _, _) | SAny(_, _) |
          STypeRep(_) | STNat(_) | _: SPAP | SToken =>
        crash("Match on non-matchable value")
    }

    machine.ctrl = altOpt
      .getOrElse(throw DamlEMatchError(s"No match for $v in ${alts.toList}"))
      .body
  }

  private[speedy] final case class KMatch(
      alts: Array[SCaseAlt],
      frame: Frame,
      actuals: Actuals,
      envSize: Int)
      extends Kont
      with SomeArrayEquals {
    def execute(v: SValue, machine: Machine) = {
      machine.restoreEnv(frame, actuals, envSize)
      executeMatchAlts(machine, alts, v);
    }
  }

  /** Push the evaluated value to the array 'to', and start evaluating the expression 'next'.
    * This continuation is used to implement both function application and lets. In
    * the case of function application the arguments are pushed into the 'actuals' array of
    * the PAP that is being built, and in the case of lets the evaluated value is pushed
    * direy into the environment.
    */
  private[speedy] final case class KPushTo(
      to: util.ArrayList[SValue],
      next: SExpr,
      frame: Frame,
      actuals: Actuals,
      envSize: Int)
      extends Kont
      with SomeArrayEquals {
    def execute(v: SValue, machine: Machine) = {
      machine.restoreEnv(frame, actuals, envSize)
      to.add(v)
      machine.ctrl = next
    }
  }

  private[speedy] final case class KFoldl(
      func: SValue,
      var list: FrontStack[SValue],
      frame: Frame,
      actuals: Actuals,
      envSize: Int,
  ) extends Kont
      with SomeArrayEquals {
    def execute(acc: SValue, machine: Machine) = {
      list.pop match {
        case None =>
          machine.returnValue = acc
        case Some((item, rest)) =>
          machine.restoreEnv(frame, actuals, envSize)
          // NOTE: We are "recycling" the current continuation with the
          // remainder of the list to avoid allocating a new continuation.
          list = rest
          machine.pushKont(this)
          machine.enterApplication(func, Array(SEValue(acc), SEValue(item)))
      }
    }
  }

  private[speedy] final case class KFoldr(
      func: SValue,
      list: ImmArray[SValue],
      var lastIndex: Int,
      frame: Frame,
      actuals: Actuals,
      envSize: Int,
  ) extends Kont
      with SomeArrayEquals {
    def execute(acc: SValue, machine: Machine) = {
      if (lastIndex > 0) {
        machine.restoreEnv(frame, actuals, envSize)
        val currentIndex = lastIndex - 1
        val item = list(currentIndex)
        lastIndex = currentIndex
        machine.pushKont(this) // NOTE: We've updated `lastIndex`.
        machine.enterApplication(func, Array(SEValue(item), SEValue(acc)))
      } else {
        machine.returnValue = acc
      }
    }
  }

  // NOTE: See the explanation above the definition of `SBFoldr` on why we need
  // this continuation and what it does.
  private[speedy] final case class KFoldr1Map(
      func: SValue,
      var list: FrontStack[SValue],
      var revClosures: FrontStack[SValue],
      init: SValue,
      frame: Frame,
      actuals: Actuals,
      envSize: Int,
  ) extends Kont
      with SomeArrayEquals {
    def execute(closure: SValue, machine: Machine) = {
      revClosures = closure +: revClosures
      list.pop match {
        case None =>
          machine.pushKont(KFoldr1Reduce(revClosures, frame, actuals, envSize))
          machine.returnValue = init
        case Some((item, rest)) =>
          machine.restoreEnv(frame, actuals, envSize)
          list = rest
          machine.pushKont(this) // NOTE: We've updated `revClosures` and `list`.
          machine.enterApplication(func, Array(SEValue(item)))
      }
    }
  }

  // NOTE: See the explanation above the definition of `SBFoldr` on why we need
  // this continuation and what it does.
  private[speedy] final case class KFoldr1Reduce(
      var revClosures: FrontStack[SValue],
      frame: Frame,
      actuals: Actuals,
      envSize: Int,
  ) extends Kont
      with SomeArrayEquals {
    def execute(acc: SValue, machine: Machine) = {
      revClosures.pop match {
        case None =>
          machine.returnValue = acc
        case Some((closure, rest)) =>
          machine.restoreEnv(frame, actuals, envSize)
          revClosures = rest
          machine.pushKont(this) // NOTE: We've updated `revClosures`.
          machine.enterApplication(closure, Array(SEValue(acc)))
      }
    }
  }

  /** Store the evaluated value in the 'SEVal' from which the expression came from.
    * This in principle makes top-level values lazy. It is a useful optimization to
    * allow creation of large constants (for example records) that are repeatedly
    * accessed. In older compilers which did not use the builtin record and struct
    * updates this solves the blow-up which would happen when a large record is
    * updated multiple times. */
  private[speedy] final case class KCacheVal(v: SEVal, stack_trace: List[Location]) extends Kont {
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
  private[speedy] final case class KCatch(
      handler: SExpr,
      fin: SExpr,
      frame: Frame,
      actuals: Actuals,
      envSize: Int)
      extends Kont
      with SomeArrayEquals {
    def execute(v: SValue, machine: Machine) = {
      machine.restoreEnv(frame, actuals, envSize)
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
  private[speedy] final case class KLabelClosure(label: Profile.Label) extends Kont {
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
  private[speedy] final case class KLeaveClosure(label: Profile.Label) extends Kont {
    def execute(v: SValue, machine: Machine) = {
      machine.profile.addCloseEvent(label)
      machine.returnValue = v
    }
  }

  /** Internal exception thrown when a continuation result needs to be returned.
    Or machine execution has reached a final value. */
  private[speedy] final case class SpeedyHungry(result: SResult)
      extends RuntimeException
      with NoStackTrace

  private[speedy] def deriveTransactionSeed(
      submissionSeed: crypto.Hash,
      participant: Ref.ParticipantId,
      submissionTime: Time.Timestamp,
  ): InitialSeeding =
    InitialSeeding.TransactionSeed(
      crypto.Hash.deriveTransactionSeed(submissionSeed, participant, submissionTime))

}
