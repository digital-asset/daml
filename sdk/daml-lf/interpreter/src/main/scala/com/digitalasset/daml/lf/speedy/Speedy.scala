// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import java.util
import com.daml.lf.data.Ref._
import com.daml.lf.data.{ImmArray, NoCopy, FrontStack, Time, Ref}
import com.daml.lf.interpretation.{Error => IError}
import com.daml.lf.language.Ast._
import com.daml.lf.language.{LookupError, Util => AstUtil}
import com.daml.lf.speedy.Compiler.{CompilationError, PackageNotFound}
import com.daml.lf.speedy.PartialTransaction.NodeSeeds
import com.daml.lf.speedy.SError._
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SResult._
import com.daml.lf.speedy.Speedy.Machine.{newTraceLog, newWarningLog}
import com.daml.lf.transaction.ContractStateMachine.KeyMapping
import com.daml.lf.transaction.GlobalKeyWithMaintainers
import com.daml.lf.transaction.{
  SubmittedTransaction,
  Node,
  ContractKeyUniquenessMode,
  NodeId,
  GlobalKey,
  IncompleteTransaction => IncompleteTx,
  TransactionVersion => TxVersion,
}
import com.daml.lf.value.{Value => V}
import com.daml.nameof.NameOf
import com.daml.scalautil.Statement.discard
import com.daml.logging.{ContextualizedLogger, LoggingContext}

import scala.annotation.{nowarn, tailrec}

private[lf] object Speedy {

  // These have zero cost when not enabled. But they are not switchable at runtime.
  private val enableInstrumentation: Boolean = false

  /** Instrumentation counters. */
  final class Instrumentation() {
    private[this] var countPushesKont: Int = 0
    private[this] var countPushesEnv: Int = 0
    private[this] var maxDepthKont: Int = 0
    private[this] var maxDepthEnv: Int = 0

    val classifyCounts: Classify.Counts = new Classify.Counts()

    def incrPushesKont(): Unit = countPushesKont += 1

    def incrPushesEnv(): Unit = countPushesEnv += 1

    def setDepthKont(depth: Int): Unit = maxDepthKont = maxDepthKont.max(depth)

    def setDepthEnv(depth: Int): Unit = maxDepthEnv = maxDepthEnv.max(depth)

    def reset(): Unit = {
      countPushesKont = 0
      countPushesEnv = 0
      maxDepthKont = 0
      maxDepthEnv = 0
    }

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

  /*
   Speedy uses a caller-saves strategy for managing the environment.  In a Speedy machine,
   the environment is represented by the `frame`, `actuals`, and `env` components.

   We use the terminology "frame" for the array of values which correspond to the
   evaluated "free-vars" of a function closure.

   We use the terminology "actuals" for the array of values which correspond to the
   evaluated "args" of a function application. (The args being an array of expressions)

   The environment "env" is now only used for let-bindings and pattern-matches.

   Continuations are responsible for restoring their own frame/actuals.  On the other
   hand, it is the code which executes a continuation which is responsible for ensuring
   the env-stack of temporaries is popped to the correct height, before the continuation
   is itself executed. See popTempStackToBase.

   When we create/push a continuation which requires it's environment to be preserved, we
   record the current frame and actuals within the continuation. In addition, we call
   markBase to allow the continuation access to temporaries on the env-stack: markBase
   shifts envBase to the current env.size, returning the old envBase, allowing the
   continuation to reset the envBase (calling restoreBase) when it is executed.
   */

  private type Frame = Array[SValue]

  private type Actuals = util.ArrayList[SValue]

  sealed abstract class LedgerMode extends Product with Serializable

  final case class CachedKey(
      globalKeyWithMaintainers: GlobalKeyWithMaintainers,
      key: SValue,
  ) {
    def globalKey: GlobalKey = globalKeyWithMaintainers.globalKey
    def templateId: TypeConName = globalKey.templateId
    def maintainers: Set[Party] = globalKeyWithMaintainers.maintainers
    val lfValue: V = globalKey.key
    def renormalizedGlobalKeyWithMaintainers(version: TxVersion) = {
      globalKeyWithMaintainers.copy(
        globalKey = GlobalKey.assertBuild(templateId, key.toNormalizedValue(version))
      )
    }
  }

  final case class CachedContract(
      version: TxVersion,
      templateId: Ref.TypeConName,
      value: SValue,
      agreementText: String,
      signatories: Set[Party],
      observers: Set[Party],
      keyOpt: Option[CachedKey],
  ) {
    val stakeholders: Set[Party] = signatories union observers

    private[speedy] val any = SValue.SAnyContract(templateId, value)
    private[speedy] def arg = value.toNormalizedValue(version)
    private[speedy] def gkeyOpt: Option[GlobalKey] = keyOpt.map(_.globalKey)
    private[speedy] def toCreateNode(coid: V.ContractId) =
      Node.Create(
        coid = coid,
        templateId = templateId,
        arg = arg,
        agreementText = agreementText,
        signatories = signatories,
        stakeholders = stakeholders,
        keyOpt = keyOpt.map(_.globalKeyWithMaintainers),
        version = version,
      )
  }

  private[speedy] def throwLimitError(location: String, error: IError.Dev.Limit.Error): Nothing =
    throw SError.SErrorDamlException(interpretation.Error.Dev(location, IError.Dev.Limit(error)))

  private[this] def enforceLimit(
      location: String,
      actual: Int,
      limit: Int,
      error: Int => IError.Dev.Limit.Error,
  ): Unit =
    if (actual > limit) throwLimitError(location, error(limit))

  final class UpdateMachine(
      override val sexpr: SExpr,
      override val traceLog: TraceLog,
      override val warningLog: WarningLog,
      override var compiledPackages: CompiledPackages,
      override val profile: Profile,
      override val iterationsBetweenInterruptions: Long,
      val validating: Boolean,
      val submissionTime: Time.Timestamp,
      val contractKeyUniqueness: ContractKeyUniquenessMode,
      /* The current partial transaction */
      private[speedy] var ptx: PartialTransaction,
      /* Committers of the action. */
      val committers: Set[Party],
      /* Additional readers (besides committers) for visibility checks. */
      val readAs: Set[Party],
      /* Commit location, if a scenario commit is in progress. */
      val commitLocation: Option[Location],
      val limits: interpretation.Limits,
  )(implicit loggingContext: LoggingContext)
      extends Machine[Question.Update] {

    private[speedy] override def asUpdateMachine(location: String)(
        f: UpdateMachine => Control[Question.Update]
    ): Control[Question.Update] =
      f(this)

    override private[speedy] def asScenarioMachine(location: String)(
        f: ScenarioMachine => Control[Question.Scenario]
    ): Nothing =
      throw SErrorCrash(location, "unexpected update machine")

    /** unwindToHandler is called when an exception is thrown by the builtin SBThrow or
      * re-thrown by the builtin SBTryHandler. If a catch-handler is found, we initiate
      * execution of the handler code (which might decide to re-throw). Otherwise we call
      * throwUnhandledException to apply the message function to the exception payload,
      * producing a text message.
      */
    private[speedy] override def handleException(excep: SValue.SAny): Control[Nothing] = {
      @tailrec def unwind(): Option[KTryCatchHandler] =
        if (kontDepth() == 0) {
          None
        } else {
          popKont() match {
            case handler: KTryCatchHandler =>
              ptx = ptx.rollbackTry(excep)
              Some(handler)
            case KCloseExercise =>
              ptx = ptx.abortExercises
              unwind()
            case k: KCheckChoiceGuard =>
              // We must abort, because the transaction has failed in a way that is
              // unrecoverable (it depends on the state of an input contract that
              // we may not have the authority to fetch).
              clearKontStack()
              clearEnv()
              k.abort()
            case KPreventException =>
              None
            case _ =>
              unwind()
          }
        }

      unwind() match {
        case Some(kh) =>
          kh.restore(this)
          popTempStackToBase()
          pushEnv(excep) // payload on stack where handler expects it
          Control.Expression(kh.handler)
        case None =>
          unhandledException(excep)
      }
    }

    /* Flag to trace usage of get_time builtins */
    private[this] var dependsOnTime: Boolean = false
    // global contract discriminators, that are discriminators from contract created in previous transactions
    private[this] var cachedContracts_ : Map[V.ContractId, CachedContract] = Map.empty
    private[this] var numInputContracts: Int = 0

    private[this] var disclosedContracts_ = Map.empty[V.ContractId, CachedContract]
    private[speedy] def disclosedContracts: Map[V.ContractId, CachedContract] = disclosedContracts_

    private[this] var disclosedContractKeys_ = Map.empty[GlobalKey, V.ContractId]
    private[speedy] def disclosedContractKeys: Map[GlobalKey, V.ContractId] = disclosedContractKeys_

    private[speedy] def addDisclosedContracts(
        contractId: V.ContractId,
        contract: CachedContract,
    ): Unit = {
      disclosedContracts_ = disclosedContracts.updated(contractId, contract)
      contract.keyOpt.foreach(key =>
        disclosedContractKeys_ = disclosedContractKeys.updated(key.globalKey, contractId)
      )
    }

    private[speedy] def isDisclosedContract(contractId: V.ContractId): Boolean =
      disclosedContracts.isDefinedAt(contractId)

    private[speedy] def setDependsOnTime(): Unit =
      dependsOnTime = true

    def getDependsOnTime: Boolean =
      dependsOnTime

    private[speedy] def cachedContracts: Map[V.ContractId, CachedContract] =
      cachedContracts_

    val visibleToStakeholders: Set[Party] => SVisibleToStakeholders =
      if (validating) { _ => SVisibleToStakeholders.Visible }
      else {
        SVisibleToStakeholders.fromSubmitters(committers, readAs)
      }

    def incompleteTransaction: IncompleteTx = ptx.finishIncomplete
    def nodesToString: String = ptx.nodesToString
    private[speedy] def isLocalContract(contractId: V.ContractId): Boolean = {
      ptx.contractState.locallyCreated.contains(contractId)
    }

    private[speedy] def updateCachedContracts(cid: V.ContractId, contract: CachedContract): Unit = {
      enforceLimit(
        NameOf.qualifiedNameOfCurrentFunc,
        contract.signatories.size,
        limits.contractSignatories,
        IError.Dev.Limit
          .ContractSignatories(
            cid,
            contract.templateId,
            contract.arg,
            contract.signatories,
            _,
          ),
      )
      enforceLimit(
        NameOf.qualifiedNameOfCurrentFunc,
        contract.observers.size,
        limits.contractObservers,
        IError.Dev.Limit
          .ContractObservers(
            cid,
            contract.templateId,
            contract.arg,
            contract.observers,
            _,
          ),
      )
      cachedContracts_ = cachedContracts_.updated(cid, contract)
    }

    private[speedy] def addGlobalContract(coid: V.ContractId, contract: CachedContract): Unit = {
      numInputContracts += 1
      enforceLimit(
        NameOf.qualifiedNameOfCurrentFunc,
        numInputContracts,
        limits.transactionInputContracts,
        IError.Dev.Limit.TransactionInputContracts,
      )
      updateCachedContracts(coid, contract)
    }

    private[speedy] def enforceChoiceControllersLimit(
        controllers: Set[Party],
        cid: V.ContractId,
        templateId: TypeConName,
        choiceName: ChoiceName,
        arg: V,
    ): Unit =
      enforceLimit(
        NameOf.qualifiedNameOfCurrentFunc,
        controllers.size,
        limits.choiceControllers,
        IError.Dev.Limit.ChoiceControllers(cid, templateId, choiceName, arg, controllers, _),
      )

    private[speedy] def enforceChoiceObserversLimit(
        observers: Set[Party],
        cid: V.ContractId,
        templateId: TypeConName,
        choiceName: ChoiceName,
        arg: V,
    ): Unit =
      enforceLimit(
        NameOf.qualifiedNameOfCurrentFunc,
        observers.size,
        limits.choiceObservers,
        IError.Dev.Limit.ChoiceObservers(cid, templateId, choiceName, arg, observers, _),
      )

    private[speedy] def enforceChoiceAuthorizersLimit(
        authorizers: Set[Party],
        cid: V.ContractId,
        templateId: TypeConName,
        choiceName: ChoiceName,
        arg: V,
    ): Unit =
      enforceLimit(
        NameOf.qualifiedNameOfCurrentFunc,
        authorizers.size,
        limits.choiceAuthorizers,
        IError.Dev.Limit.ChoiceAuthorizers(cid, templateId, choiceName, arg, authorizers, _),
      )

    // The set of create events for the disclosed contracts that are used by the generated transaction.
    def disclosedCreateEvents: ImmArray[Node.Create] =
      disclosedContracts.iterator
        .collect {
          case (coid, contract) if cachedContracts_.isDefinedAt(coid) => contract.toCreateNode(coid)
        }
        .to(ImmArray)

    @throws[IllegalArgumentException]
    def zipSameLength[X, Y](xs: ImmArray[X], ys: ImmArray[Y]): ImmArray[(X, Y)] = {
      val n1 = xs.length
      val n2 = ys.length
      if (n1 != n2) {
        throw new IllegalArgumentException(s"sameLengthZip, $n1 /= $n2")
      }
      xs.zip(ys)
    }

    def finish: Either[SErrorCrash, UpdateMachine.Result] = ptx.finish.map { case (tx, seeds) =>
      UpdateMachine.Result(
        tx,
        ptx.locationInfo(),
        zipSameLength(seeds, ptx.actionNodeSeeds.toImmArray),
        ptx.contractState.globalKeyInputs.transform((_, v) => v.toKeyMapping),
        disclosedCreateEvents,
      )
    }

    def checkContractVisibility(
        cid: V.ContractId,
        contract: CachedContract,
    ): Unit = {
      // For disclosed contracts, we do not perform visibility checking
      if (!isDisclosedContract(cid)) {
        visibleToStakeholders(contract.stakeholders) match {
          case SVisibleToStakeholders.Visible =>
            ()

          case SVisibleToStakeholders.NotVisible(actAs, readAs) =>
            val readers = (actAs union readAs).mkString(",")
            val stakeholders = contract.stakeholders.mkString(",")
            this.warningLog.add(
              Warning(
                commitLocation = commitLocation,
                message =
                  s"""Tried to fetch or exercise ${contract.templateId} on contract ${cid.coid}
                     | but none of the reading parties [$readers] are contract stakeholders [$stakeholders].
                     | Use of divulged contracts is deprecated and incompatible with pruning.
                     | To remedy, add one of the readers [$readers] as an observer to the contract.
                     |""".stripMargin.replaceAll("\r|\n", ""),
              )
            )
        }
      }
    }

    @throws[SError]
    def checkKeyVisibility(
        gkey: GlobalKey,
        coid: V.ContractId,
        handleKeyFound: V.ContractId => Control.Value,
    ): Control.Value = {
      // For local and disclosed contracts, we do not perform visibility checking
      if (isLocalContract(coid) || isDisclosedContract(coid)) {
        handleKeyFound(coid)
      } else {
        cachedContracts.get(coid) match {
          case Some(cachedContract) =>
            val stakeholders = cachedContract.signatories union cachedContract.observers
            visibleToStakeholders(stakeholders) match {
              case SVisibleToStakeholders.Visible =>
                handleKeyFound(coid)

              case SVisibleToStakeholders.NotVisible(actAs, readAs) =>
                throw SErrorDamlException(
                  interpretation.Error
                    .ContractKeyNotVisible(coid, gkey, actAs, readAs, stakeholders)
                )
            }

          case None =>
            throw SErrorCrash(
              NameOf.qualifiedNameOfCurrentFunc,
              s"contract ${coid.coid} not in cachedContracts",
            )
        }
      }
    }
  }

  object UpdateMachine {

    private val iterationsBetweenInterruptions: Long = 10000

    @throws[SErrorDamlException]
    def apply(
        compiledPackages: CompiledPackages,
        submissionTime: Time.Timestamp,
        initialSeeding: InitialSeeding,
        expr: SExpr,
        committers: Set[Party],
        readAs: Set[Party],
        authorizationChecker: AuthorizationChecker = DefaultAuthorizationChecker,
        iterationsBetweenInterruptions: Long = UpdateMachine.iterationsBetweenInterruptions,
        validating: Boolean = false,
        traceLog: TraceLog = newTraceLog,
        warningLog: WarningLog = newWarningLog,
        contractKeyUniqueness: ContractKeyUniquenessMode = ContractKeyUniquenessMode.Strict,
        commitLocation: Option[Location] = None,
        limits: interpretation.Limits = interpretation.Limits.Lenient,
    )(implicit loggingContext: LoggingContext): UpdateMachine =
      new UpdateMachine(
        sexpr = expr,
        validating = validating,
        submissionTime = submissionTime,
        ptx = PartialTransaction
          .initial(
            contractKeyUniqueness,
            initialSeeding,
            committers,
            authorizationChecker,
          ),
        committers = committers,
        readAs = readAs,
        commitLocation = commitLocation,
        contractKeyUniqueness = contractKeyUniqueness,
        limits = limits,
        traceLog = traceLog,
        warningLog = warningLog,
        profile = new Profile(),
        iterationsBetweenInterruptions = iterationsBetweenInterruptions,
        compiledPackages = compiledPackages,
      )

    private[lf] final case class Result(
        tx: SubmittedTransaction,
        locationInfo: Map[NodeId, Location],
        seeds: NodeSeeds,
        globalKeyMapping: Map[GlobalKey, KeyMapping],
        disclosedCreateEvent: ImmArray[Node.Create],
    )
  }

  final class ScenarioMachine(
      override val sexpr: SExpr,
      override val traceLog: TraceLog,
      override val warningLog: WarningLog,
      override var compiledPackages: CompiledPackages,
      override val profile: Profile,
      override val iterationsBetweenInterruptions: Long =
        ScenarioMachine.defaultIterationsBetweenInterruptions,
  )(implicit loggingContext: LoggingContext)
      extends Machine[Question.Scenario] {

    private[speedy] override def asUpdateMachine(location: String)(
        f: UpdateMachine => Control[Question.Update]
    ): Nothing =
      throw SErrorCrash(location, "unexpected scenario machine")

    private[speedy] override def asScenarioMachine(location: String)(
        f: ScenarioMachine => Control[Question.Scenario]
    ): Control[Question.Scenario] = f(this)

    /** Scenario Machine does not handle exceptions */
    private[speedy] override def handleException(excep: SValue.SAny): Control.Error =
      unhandledException(excep)
  }

  object ScenarioMachine {
    val defaultIterationsBetweenInterruptions: Long = 10000
  }

  final class PureMachine(
      override val sexpr: SExpr,
      /* The trace log. */
      override val traceLog: TraceLog,
      /* Engine-generated warnings. */
      override val warningLog: WarningLog,
      /* Compiled packages (Daml-LF ast + compiled speedy expressions). */
      override var compiledPackages: CompiledPackages,
      /* Profile of the run when the packages haven been compiled with profiling enabled. */
      override val profile: Profile,
      override val iterationsBetweenInterruptions: Long,
  )(implicit loggingContext: LoggingContext)
      extends Machine[Nothing] {

    private[speedy] override def asUpdateMachine(location: String)(
        f: UpdateMachine => Control[Question.Update]
    ): Nothing =
      throw SErrorCrash(location, "unexpected pure machine")

    private[speedy] override def asScenarioMachine(location: String)(
        f: ScenarioMachine => Control[Question.Scenario]
    ): Nothing =
      throw SErrorCrash(location, "unexpected pure machine")

    /** Pure Machine does not handle exceptions */
    private[speedy] override def handleException(excep: SValue.SAny): Control.Error =
      unhandledException(excep)

    @nowarn("msg=dead code following this construct")
    @tailrec
    def runPure(): Either[SError, SValue] =
      run() match {
        case SResultError(err) => Left(err)
        case SResultFinal(v) => Right(v)
        case SResultInterruption =>
          runPure()
        case SResultQuestion(nothing) => nothing
      }
  }

  /** The speedy CEK machine. */
  private[lf] sealed abstract class Machine[Q](implicit val loggingContext: LoggingContext) {

    val sexpr: SExpr
    /* The trace log. */
    val traceLog: TraceLog
    /* Engine-generated warnings. */
    val warningLog: WarningLog
    /* Compiled packages (Daml-LF ast + compiled speedy expressions). */
    var compiledPackages: CompiledPackages
    /* Profile of the run when the packages haven been compiled with profiling enabled. */
    val profile: Profile

    /* number of iteration between cooperation interruption */
    val iterationsBetweenInterruptions: Long

    private[speedy] def handleException(excep: SValue.SAny): Control[Nothing]

    protected final def unhandledException(excep: SValue.SAny): Control.Error = {
      clearKontStack()
      clearEnv()
      Control.Error(IError.UnhandledException(excep.ty, excep.value.toUnnormalizedValue))
    }

    /* The machine control is either an expression or a value. */
    private[this] var control: Control[Q] = Control.Expression(sexpr)
    /* Frame: to access values for a closure's free-vars. */
    private[this] var frame: Frame = null
    /* Actuals: to access values for a function application's arguments. */
    private[this] var actuals: Actuals = null
    /* [env] is a stack of temporary values for: let-bindings and pattern-matches. */
    private[speedy] final var env: Env = emptyEnv
    /* [envBase] is the depth of the temporaries-stack when the current code-context was
     * begun. We revert to this depth when entering a closure, or returning to the top
     * continuation on the kontStack.
     */
    private[this] var envBase: Int = 0
    /* Kont, or continuation specifies what should be done next
     * once the control has been evaluated.
     */
    private[speedy] final var kontStack: util.ArrayList[Kont] = initialKontStack()
    /* The last encountered location */
    private[this] var lastLocation: Option[Location] = None
    /* Used when enableLightweightStepTracing is true */
    private[this] var interruptionCountDown: Long = iterationsBetweenInterruptions

    /* Used when enableInstrumentation is true */
    private[this] val track: Instrumentation = new Instrumentation

    private[speedy] final def currentControl: Control[Q] = control

    private[speedy] final def currentFrame: Frame = frame

    private[speedy] final def currentActuals: Actuals = actuals

    private[speedy] final def currentEnv: Env = env

    private[speedy] final def currentEnvBase: Int = envBase

    private[speedy] final def currentKontStack: util.ArrayList[Kont] = kontStack

    final def getLastLocation: Option[Location] = lastLocation

    final protected def clearEnv(): Unit = {
      env.clear()
      envBase = 0
    }

    final def tmplId2TxVersion(tmplId: TypeConName): TxVersion =
      TxVersion.assignNodeVersion(
        compiledPackages.pkgInterface.packageLanguageVersion(tmplId.packageId)
      )

    /* kont manipulation... */

    final protected def clearKontStack(): Unit = kontStack.clear()

    @inline
    private[speedy] final def kontDepth(): Int = kontStack.size()

    private[speedy] def asUpdateMachine(location: String)(
        f: UpdateMachine => Control[Question.Update]
    ): Control[Q]

    private[speedy] def asScenarioMachine(location: String)(
        f: ScenarioMachine => Control[Question.Scenario]
    ): Control[Q]

    @inline
    private[speedy] final def pushKont(k: Kont): Unit = {
      discard[Boolean](kontStack.add(k))
      if (enableInstrumentation) {
        track.incrPushesKont()
        track.setDepthKont(kontDepth())
      }
    }

    @inline
    private[speedy] final def popKont(): Kont = {
      kontStack.remove(kontStack.size - 1)
    }

    @inline
    private[speedy] final def peekKontStackEnd(): Kont = {
      kontStack.get(kontStack.size - 1)
    }

    @inline
    private[speedy] final def peekKontStackTop(): Kont = {
      kontStack.get(0)
    }

    /* env manipulation... */

    // The environment is partitioned into three locations: Stack, Args, Free
    // The run-time location of a variable is determined (at compile time) by closureConvert
    // And made explicit by a specifc speedy expression node: SELocS/SELocA/SELocF
    // At runtime these different location-node execute by calling the corresponding `getEnv*` function

    // Variables which reside on the stack. Indexed (from 1) by relative offset from the top of the stack (1 is top!)
    @inline
    private[speedy] final def getEnvStack(i: Int): SValue = env.get(env.size - i)

    // Variables which reside in the args array of the current frame. Indexed by absolute offset.
    @inline
    private[speedy] final def getEnvArg(i: Int): SValue = actuals.get(i)

    // Variables which reside in the free-vars array of the current frame. Indexed by absolute offset.
    @inline
    private[speedy] final def getEnvFree(i: Int): SValue = frame(i)

    @inline
    final def pushEnv(v: SValue): Unit = {
      discard[Boolean](env.add(v))
      if (enableInstrumentation) {
        track.incrPushesEnv()
        track.setDepthEnv(env.size)
      }
    }

    // markBase is called when pushing a continuation which requires access to temporaries
    // currently on the env-stack.  After this call, envBase is set to the current
    // env.size. The old envBase is returned so it can be restored later by the caller.
    @inline
    final def markBase(): Int = {
      val oldBase = this.envBase
      val newBase = this.env.size
      if (newBase < oldBase) {
        throw SErrorCrash(
          NameOf.qualifiedNameOfCurrentFunc,
          s"markBase: $oldBase -> $newBase -- NOT AN INCREASE",
        )
      }
      this.envBase = newBase
      oldBase
    }

    // restoreBase is called when executing a continuation which previously saved the
    // value of envBase (by calling markBase).
    @inline
    final def restoreBase(envBase: Int): Unit = {
      if (this.envBase < envBase) {
        throw SErrorCrash(
          NameOf.qualifiedNameOfCurrentFunc,
          s"restoreBase: ${this.envBase} -> ${envBase} -- NOT A REDUCTION",
        )
      }
      this.envBase = envBase
    }

    // popTempStackToBase is called when we begin a new code-context which does not need
    // to access any temporaries pushed to the stack by the current code-context. This
    // occurs either when returning to the top continuation on the kontStack or when
    // entering (tail-calling) a closure.
    @inline
    final def popTempStackToBase(): Unit = {
      val envSizeToBeRestored = this.envBase
      val count = env.size - envSizeToBeRestored
      if (count < 0) {
        throw SErrorCrash(
          NameOf.qualifiedNameOfCurrentFunc,
          s"popTempStackToBase: ${env.size} --> ${envSizeToBeRestored} -- WRONG DIRECTION",
        )
      }
      if (count > 0) {
        env.subList(envSizeToBeRestored, env.size).clear()
      }
    }

    @inline
    final def restoreFrameAndActuals(frame: Frame, actuals: Actuals): Unit = {
      // Restore the frame and actuals to the state when the continuation was created.
      this.frame = frame
      this.actuals = actuals
    }

    /** Track the location of the expression being evaluated
      */
    final def pushLocation(loc: Location): Unit = {
      lastLocation = Some(loc)
    }

    /** Reuse an existing speedy machine to evaluate a new expression.
      *      Do not use if the machine is partway though an existing evaluation.
      *      i.e. run() has returned an `SResult` requiring a callback.
      */
    final def setExpressionToEvaluate(expr: SExpr): Unit = {
      setControl(Control.Expression(expr))
      kontStack = initialKontStack()
      env = emptyEnv
      envBase = 0
      interruptionCountDown = iterationsBetweenInterruptions
      track.reset()
    }

    final def setControl(x: Control[Q]): Unit = {
      control = x
    }

    /** Run a machine until we get a result: either a final-value or a request for data, with a callback */
    final def run(): SResult[Q] = {
      try {
        @tailrec
        def loop(): SResult[Q] = {
          if (enableInstrumentation)
            Classify.classifyMachine(this, track.classifyCounts)
          if (interruptionCountDown == 0) {
            interruptionCountDown = iterationsBetweenInterruptions
            SResultInterruption
          } else {
            val thisControl = control
            setControl(Control.WeAreUnset)
            interruptionCountDown -= 1
            thisControl match {
              case Control.Value(value) =>
                popTempStackToBase()
                control = popKont().execute(this, value)
                loop()
              case Control.Expression(exp) =>
                control = exp.execute(this)
                loop()
              case Control.Question(res) =>
                SResultQuestion(res)
              case Control.Complete(value: SValue) =>
                if (enableInstrumentation) track.print()
                SResultFinal(value)
              case Control.Error(ie) =>
                SResultError(SErrorDamlException(ie))
              case Control.WeAreUnset =>
                sys.error("**attempt to run a machine with unset control")
            }
          }
        }
        loop()
      } catch {
        case serr: SError => // TODO: prefer Control over throw for SError
          SResultError(serr)
        case ex: RuntimeException =>
          SResultError(SErrorCrash(NameOf.qualifiedNameOfCurrentFunc, s"exception: $ex")) // stop
      }
    }

    final def lookupVal(eval: SEVal): Control[Q] = {
      eval.cached match {
        case Some(v) =>
          Control.Value(v)

        case None =>
          val ref = eval.ref
          compiledPackages.getDefinition(ref) match {
            case Some(defn) =>
              defn.cached match {
                case Some(svalue) =>
                  eval.setCached(svalue)
                  Control.Value(svalue)
                case None =>
                  pushKont(KCacheVal(eval, defn))
                  Control.Expression(defn.body)
              }
            case None =>
              if (compiledPackages.packageIds.contains(ref.packageId))
                throw SErrorCrash(
                  NameOf.qualifiedNameOfCurrentFunc,
                  s"definition $ref not found even after caller provided new set of packages",
                )
              else {
                asUpdateMachine(NameOf.qualifiedNameOfCurrentFunc)(_ =>
                  Control.Question(
                    Question.Update.NeedPackage(
                      ref.packageId,
                      language.Reference.Package(ref.packageId),
                      callback = { packages =>
                        this.compiledPackages = packages
                        // To avoid infinite loop in case the packages are not updated properly by the caller
                        assert(compiledPackages.packageIds.contains(ref.packageId))
                        setControl(Control.Expression(eval))
                      },
                    )
                  )
                )
              }
          }
      }
    }

    /** This function is used to enter an ANF application.  The function has been evaluated to
      *      a value, and so have the arguments - they just need looking up
      */
    // TODO: share common code with executeApplication
    private[speedy] final def enterApplication(
        vfun: SValue,
        newArgs: Array[SExprAtomic],
    ): Control[Q] = {
      vfun match {
        case SValue.SPAP(prim, actualsSoFar, arity) =>
          val missing = arity - actualsSoFar.size
          val newArgsLimit = Math.min(missing, newArgs.length)

          val actuals = new util.ArrayList[SValue](actualsSoFar.size + newArgsLimit)
          discard[Boolean](actuals.addAll(actualsSoFar))

          val othersLength = newArgs.length - missing

          // Evaluate the arguments
          var i = 0
          while (i < newArgsLimit) {
            val newArg = newArgs(i)
            val v = newArg.lookupValue(this)
            discard[Boolean](actuals.add(v))
            i += 1
          }

          // Not enough arguments. Return a PAP.
          if (othersLength < 0) {
            val pap = SValue.SPAP(prim, actuals, arity)
            Control.Value(pap)

          } else {
            // Too many arguments: Push a continuation to re-apply the over-applied args.
            if (othersLength > 0) {
              val others = new Array[SExprAtomic](othersLength)
              System.arraycopy(newArgs, missing, others, 0, othersLength)
              this.pushKont(KOverApp(this, others))
            }
            // Now the correct number of arguments is ensured. What kind of prim do we have?
            prim match {
              case closure: SValue.PClosure =>
                this.frame = closure.frame
                this.actuals = actuals
                // Maybe push a continuation for the profiler
                val label = closure.label
                if (label != null) {
                  this.profile.addOpenEvent(label)
                  this.pushKont(KLeaveClosure(label))
                }
                // Start evaluating the body of the closure.
                popTempStackToBase()
                Control.Expression(closure.expr)

              case SValue.PBuiltin(builtin) =>
                this.actuals = actuals
                builtin.execute(actuals, this)
            }
          }

        case _ =>
          throw SErrorCrash(NameOf.qualifiedNameOfCurrentFunc, s"Applying non-PAP: $vfun")
      }
    }

    /** The function has been evaluated to a value, now start evaluating the arguments. */
    private[speedy] final def executeApplication(
        vfun: SValue,
        newArgs: Array[SExpr],
    ): Control[Nothing] = {
      vfun match {
        case SValue.SPAP(prim, actualsSoFar, arity) =>
          val missing = arity - actualsSoFar.size
          val newArgsLimit = Math.min(missing, newArgs.length)

          val actuals = new util.ArrayList[SValue](actualsSoFar.size + newArgsLimit)
          discard[Boolean](actuals.addAll(actualsSoFar))

          val othersLength = newArgs.length - missing

          // Not enough arguments. Push a continuation to construct the PAP.
          if (othersLength < 0) {
            this.pushKont(KPap(prim, actuals, arity))
          } else {
            // Too many arguments: Push a continuation to re-apply the over-applied args.
            if (othersLength > 0) {
              val others = new Array[SExpr](othersLength)
              System.arraycopy(newArgs, missing, others, 0, othersLength)
              this.pushKont(KArg(this, others))
            }
            // Now the correct number of arguments is ensured. What kind of prim do we have?
            prim match {
              case closure: SValue.PClosure =>
                // Push a continuation to execute the function body when the arguments have been evaluated
                this.pushKont(KFun(this, closure, actuals))

              case SValue.PBuiltin(builtin) =>
                // Push a continuation to execute the builtin when the arguments have been evaluated
                this.pushKont(KBuiltin(this, builtin, actuals))
            }
          }
          this.evaluateArguments(actuals, newArgs, newArgsLimit)

        case _ =>
          throw SErrorCrash(NameOf.qualifiedNameOfCurrentFunc, s"Applying non-PAP: $vfun")
      }
    }

    /** Evaluate the first 'n' arguments in 'args'.
      *      'args' will contain at least 'n' expressions, but it may contain more(!)
      *
      *      This is because, in the call from 'executeApplication' below, although over-applied
      *      arguments are pushed into a continuation, they are not removed from the original array
      *      which is passed here as 'args'.
      */
    private[speedy] final def evaluateArguments(
        actuals: util.ArrayList[SValue],
        args: Array[SExpr],
        n: Int,
    ): Control[Nothing] = {
      var i = 1
      while (i < n) {
        val arg = args(n - i)
        this.pushKont(KPushTo(this, actuals, arg))
        i = i + 1
      }
      Control.Expression(args(0))
    }

    // This translates a well-typed LF value (typically coming from the ledger)
    // to speedy value and set the control of with the result.
    // Note the method does not check the value is well-typed as opposed as
    // com.daml.lf.engine.preprocessing.ValueTranslator.translateValue.
    // All the contract IDs contained in the value are considered global.
    // Raises an exception if missing a package.
    private[speedy] final def importValue(typ0: Type, value0: V): Control.Value = {

      def assertRight[X](x: Either[LookupError, X]): X =
        x match {
          case Right(value) => value
          case Left(error) => throw SErrorCrash(NameOf.qualifiedNameOfCurrentFunc, error.pretty)
        }

      def go(ty: Type, value: V): SValue = {
        def typeMismatch = throw SErrorCrash(
          NameOf.qualifiedNameOfCurrentFunc,
          s"mismatching type: $ty and value: $value",
        )

        val (tyFun, argTypes) = AstUtil.destructApp(ty)
        tyFun match {
          case TBuiltin(_) =>
            argTypes match {
              case Nil =>
                value match {
                  case V.ValueInt64(value) =>
                    SValue.SInt64(value)
                  case V.ValueNumeric(value) =>
                    SValue.SNumeric(value)
                  case V.ValueText(value) =>
                    SValue.SText(value)
                  case V.ValueTimestamp(value) =>
                    SValue.STimestamp(value)
                  case V.ValueDate(value) =>
                    SValue.SDate(value)
                  case V.ValueParty(value) =>
                    SValue.SParty(value)
                  case V.ValueBool(b) =>
                    if (b) SValue.SValue.True else SValue.SValue.False
                  case V.ValueUnit =>
                    SValue.SValue.Unit
                  case _ =>
                    typeMismatch
                }
              case elemType :: Nil =>
                value match {
                  case V.ValueContractId(cid) =>
                    SValue.SContractId(cid)
                  case V.ValueNumeric(d) =>
                    SValue.SNumeric(d)
                  case V.ValueOptional(mb) =>
                    mb match {
                      case Some(value) => SValue.SOptional(Some(go(elemType, value)))
                      case None => SValue.SValue.None
                    }
                  // list
                  case V.ValueList(ls) =>
                    SValue.SList(ls.map(go(elemType, _)))

                  // textMap
                  case V.ValueTextMap(entries) =>
                    SValue.SMap.fromOrderedEntries(
                      isTextMap = true,
                      entries = entries.toImmArray.toSeq.view.map { case (k, v) =>
                        SValue.SText(k) -> go(elemType, v)
                      },
                    )
                  case _ =>
                    typeMismatch
                }
              case keyType :: valueType :: Nil =>
                value match {
                  // genMap
                  case V.ValueGenMap(entries) =>
                    SValue.SMap.fromOrderedEntries(
                      isTextMap = false,
                      entries = entries.toSeq.view.map { case (k, v) =>
                        go(keyType, k) -> go(valueType, v)
                      },
                    )
                  case _ =>
                    typeMismatch
                }
              case _ =>
                typeMismatch
            }
          case TTyCon(tyCon) =>
            value match {
              case V.ValueRecord(_, sourceElements) => { // This _ is the source typecon, which we ignore.
                val lookupResult = assertRight(
                  compiledPackages.pkgInterface.lookupDataRecord(tyCon)
                )
                val targetFieldsAndTypes: ImmArray[(Name, Type)] = lookupResult.dataRecord.fields
                lazy val subst = lookupResult.subst(argTypes)

                // This code implements the compatibility transformation used for up/down-grading
                // And handles the cases:
                // - UPGRADE:   numT > numS : creates a None for each missing fields.
                // - DOWNGRADE: numS > numT : drops each extra field, ensuring it is None.
                //
                // When numS == numT, we wont hit the code marked either as UPGRADE or DOWNGRADE,
                // although it is still possible that the source and target types are different,
                // but since we don't consult the source type (may be unavailable), we wont know.

                val numS: Int = sourceElements.length
                val numT: Int = targetFieldsAndTypes.length

                // traverse the sourceElements, "get"ing the corresponding target type
                // when there is no corresponding type, we must be downgrading, and so we insist the value is None
                val values0: List[SValue] =
                  sourceElements.toSeq.zipWithIndex.flatMap { case ((optName, v), i) =>
                    targetFieldsAndTypes.get(i) match {
                      case Some(x) => {
                        val (targetField, targetFieldType): (Name, Type) = x
                        optName match {
                          case None => ()
                          case Some(sourceField) =>
                            // value is not normalized; check field names match
                            assert(sourceField == targetField)
                        }
                        val typ: Type = AstUtil.substitute(targetFieldType, subst)
                        val sv: SValue = go(typ, v)
                        List(sv)
                      }
                      case None => { // DOWNGRADE
                        // i ranges from 0 to numS-1. So i >= numT implies numS > numT
                        assert((numS > i) && (i >= numT))
                        v match {
                          case V.ValueOptional(None) => List() // ok, drop
                          case V.ValueOptional(Some(_)) =>
                            // TODO: https://github.com/digital-asset/daml/issues/17082
                            // - we need a proper error here
                            throw SErrorDamlException(
                              IError.UserError(
                                "An optional contract field with a value of Some may not be dropped during downgrading."
                              )
                            )
                          case _ =>
                            // TODO: https://github.com/digital-asset/daml/issues/17082
                            // - Impossible (ill typed) case. Ok to crash here?
                            throw SErrorCrash(
                              NameOf.qualifiedNameOfCurrentFunc,
                              "Unexpected non-optional extra contract field encountered during downgrading: something is very wrong.",
                            )
                        }
                      }
                    }
                  }.toList

                val fields: ImmArray[Name] =
                  targetFieldsAndTypes.map { case (name, _) =>
                    name
                  }

                val values: util.ArrayList[SValue] = {
                  if (numT > numS) {
                    values0.padTo(numT, SValue.SOptional(None)) // UPGRADE
                  } else {
                    values0
                  }
                }.to(ArrayList)

                SValue.SRecord(tyCon, fields, values)
              }

              case V.ValueVariant(_, constructor, value) =>
                val info =
                  assertRight(
                    compiledPackages.pkgInterface.lookupVariantConstructor(tyCon, constructor)
                  )
                val valType = info.concreteType(argTypes)
                SValue.SVariant(tyCon, constructor, info.rank, go(valType, value))
              case V.ValueEnum(_, constructor) =>
                val rank =
                  assertRight(
                    compiledPackages.pkgInterface.lookupEnumConstructor(tyCon, constructor)
                  )
                SValue.SEnum(tyCon, constructor, rank)
              case _ =>
                typeMismatch
            }
          case _ =>
            typeMismatch
        }
      }

      Control.Value(go(typ0, value0))
    }

  }

  object Machine {

    private[this] val damlTraceLog = ContextualizedLogger.createFor("daml.tracelog")
    private[this] val damlWarnings = ContextualizedLogger.createFor("daml.warnings")

    def newTraceLog: TraceLog = new RingBufferTraceLog(damlTraceLog, 100)
    def newWarningLog: WarningLog = new WarningLog(damlWarnings)

    @throws[PackageNotFound]
    @throws[CompilationError]
    // Construct a machine for running an update expression (testing -- avoiding scenarios)
    def fromUpdateExpr(
        compiledPackages: CompiledPackages,
        transactionSeed: crypto.Hash,
        updateE: Expr,
        committers: Set[Party],
        authorizationChecker: AuthorizationChecker = DefaultAuthorizationChecker,
        limits: interpretation.Limits = interpretation.Limits.Lenient,
    )(implicit loggingContext: LoggingContext): UpdateMachine = {
      val updateSE: SExpr = compiledPackages.compiler.unsafeCompile(updateE)
      fromUpdateSExpr(
        compiledPackages,
        transactionSeed,
        updateSE,
        committers,
        authorizationChecker,
        limits,
      )
    }

    @throws[PackageNotFound]
    @throws[CompilationError]
    // Construct a machine for running an update expression (testing -- avoiding scenarios)
    private[lf] def fromUpdateSExpr(
        compiledPackages: CompiledPackages,
        transactionSeed: crypto.Hash,
        updateSE: SExpr,
        committers: Set[Party],
        authorizationChecker: AuthorizationChecker = DefaultAuthorizationChecker,
        limits: interpretation.Limits = interpretation.Limits.Lenient,
        traceLog: TraceLog = newTraceLog,
    )(implicit loggingContext: LoggingContext): UpdateMachine = {
      UpdateMachine(
        compiledPackages = compiledPackages,
        submissionTime = Time.Timestamp.MinValue,
        initialSeeding = InitialSeeding.TransactionSeed(transactionSeed),
        expr = SEApp(updateSE, Array(SValue.SToken)),
        committers = committers,
        readAs = Set.empty,
        limits = limits,
        traceLog = traceLog,
        authorizationChecker = authorizationChecker,
        iterationsBetweenInterruptions = 10000,
      )
    }

    @throws[PackageNotFound]
    @throws[CompilationError]
    // Construct an off-ledger machine for running scenario.
    def fromScenarioSExpr(
        compiledPackages: CompiledPackages,
        scenario: SExpr,
        iterationsBetweenInterruptions: Long =
          ScenarioMachine.defaultIterationsBetweenInterruptions,
        traceLog: TraceLog = newTraceLog,
        warningLog: WarningLog = newWarningLog,
    )(implicit loggingContext: LoggingContext): ScenarioMachine =
      new ScenarioMachine(
        sexpr = SEApp(scenario, Array(SValue.SToken)),
        traceLog = traceLog,
        warningLog = warningLog,
        compiledPackages = compiledPackages,
        profile = new Profile(),
        iterationsBetweenInterruptions = iterationsBetweenInterruptions,
      )

    @throws[PackageNotFound]
    @throws[CompilationError]
    // Construct an off-ledger machine for running scenario.
    def fromScenarioExpr(
        compiledPackages: CompiledPackages,
        scenario: Expr,
        iterationsBetweenInterruptions: Long = ScenarioMachine.defaultIterationsBetweenInterruptions,
    )(implicit loggingContext: LoggingContext): ScenarioMachine =
      fromScenarioSExpr(
        compiledPackages = compiledPackages,
        scenario = compiledPackages.compiler.unsafeCompile(scenario),
        iterationsBetweenInterruptions = iterationsBetweenInterruptions,
      )

    @throws[PackageNotFound]
    @throws[CompilationError]
    // Construct an off-ledger machine for evaluating an expression that is neither an update nor a scenario expression.
    def fromPureSExpr(
        compiledPackages: CompiledPackages,
        expr: SExpr,
        iterationsBetweenInterruptions: Long = Long.MaxValue,
        traceLog: TraceLog = newTraceLog,
        warningLog: WarningLog = newWarningLog,
    )(implicit loggingContext: LoggingContext): PureMachine =
      new PureMachine(
        sexpr = expr,
        traceLog = traceLog,
        warningLog = warningLog,
        compiledPackages = compiledPackages,
        profile = new Profile(),
        iterationsBetweenInterruptions = iterationsBetweenInterruptions,
      )

    @throws[PackageNotFound]
    @throws[CompilationError]
    // Construct an off-ledger machine for evaluating an expression that is neither an update nor a scenario expression.
    def fromPureExpr(
        compiledPackages: CompiledPackages,
        expr: Expr,
    )(implicit loggingContext: LoggingContext): PureMachine =
      fromPureSExpr(
        compiledPackages,
        compiledPackages.compiler.unsafeCompile(expr),
      )

    @throws[PackageNotFound]
    @throws[CompilationError]
    def runPureExpr(
        expr: Expr,
        compiledPackages: CompiledPackages = PureCompiledPackages.Empty,
    )(implicit loggingContext: LoggingContext): Either[SError, SValue] =
      fromPureExpr(compiledPackages, expr).runPure()

    @throws[PackageNotFound]
    @throws[CompilationError]
    def runPureSExpr(
        expr: SExpr,
        compiledPackages: CompiledPackages = PureCompiledPackages.Empty,
        iterationsBetweenInterruptions: Long = Long.MaxValue,
    )(implicit loggingContext: LoggingContext): Either[SError, SValue] =
      fromPureSExpr(compiledPackages, expr, iterationsBetweenInterruptions).runPure()

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
  // returns the final result

  private[this] def initialKontStack(): util.ArrayList[Kont] = {
    val kontStack = new util.ArrayList[Kont](128)
    discard[Boolean](kontStack.add(KFinished))
    kontStack
  }

  private[speedy] sealed abstract class Control[+Q]
  object Control {
    final case class Value(v: SValue) extends Control[Nothing]
    final case class Expression(e: SExpr) extends Control[Nothing]
    final case class Question[Q](res: Q) extends Control[Q]
    final case class Complete(res: SValue) extends Control[Nothing]
    final case class Error(err: interpretation.Error) extends Control[Nothing]
    final case object WeAreUnset extends Control[Nothing]
  }

  /** Kont, or continuation. Describes the next step for the machine
    * after an expression has been evaluated into a 'SValue'.
    */
  private[speedy] sealed abstract class Kont {

    /** Execute the continuation. */
    def execute[Q](machine: Machine[Q], v: SValue): Control[Q]
  }

  /** Final continuation; machine has computed final value */
  private[speedy] final case object KFinished extends Kont {
    override def execute[Q](machine: Machine[Q], v: SValue): Control.Complete = Control.Complete(v)
  }

  private[speedy] final case class KOverApp private (
      savedBase: Int,
      frame: Frame,
      actuals: Actuals,
      newArgs: Array[SExprAtomic],
  ) extends Kont
      with SomeArrayEquals
      with NoCopy {
    override def execute[Q](machine: Machine[Q], vfun: SValue): Control[Q] = {
      machine.restoreBase(savedBase);
      machine.restoreFrameAndActuals(frame, actuals)
      machine.enterApplication(vfun, newArgs)
    }
  }

  object KOverApp {
    def apply(machine: Machine[_], newArgs: Array[SExprAtomic]): KOverApp =
      KOverApp(machine.markBase(), machine.currentFrame, machine.currentActuals, newArgs)
  }

  /** The function has been evaluated to a value. Now restore the environment and execute the application */
  private[speedy] final case class KArg private (
      savedBase: Int,
      frame: Frame,
      actuals: Actuals,
      newArgs: Array[SExpr],
  ) extends Kont
      with SomeArrayEquals
      with NoCopy {
    override def execute[Q](machine: Machine[Q], vfun: SValue): Control[Nothing] = {
      machine.restoreBase(savedBase);
      machine.restoreFrameAndActuals(frame, actuals)
      machine.executeApplication(vfun, newArgs)
    }
  }

  object KArg {
    def apply(machine: Machine[_], newArgs: Array[SExpr]): KArg =
      KArg(machine.markBase(), machine.currentFrame, machine.currentActuals, newArgs)
  }

  /** The function-closure and arguments have been evaluated. Now execute the body. */
  private[speedy] final case class KFun private (
      savedBase: Int,
      closure: SValue.PClosure,
      actuals: util.ArrayList[SValue],
  ) extends Kont
      with SomeArrayEquals
      with NoCopy {
    override def execute[Q](machine: Machine[Q], v: SValue): Control.Expression = {
      discard[Boolean](actuals.add(v))
      // Set frame/actuals to allow access to the function arguments and closure free-varables.
      machine.restoreBase(savedBase)
      machine.restoreFrameAndActuals(closure.frame, actuals)
      // Maybe push a continuation for the profiler
      val label = closure.label
      if (label != null) {
        machine.profile.addOpenEvent(label)
        machine.pushKont(KLeaveClosure(label))
      }
      // Start evaluating the body of the closure.
      machine.popTempStackToBase()
      Control.Expression(closure.expr)
    }
  }

  object KFun {
    def apply(
        machine: Machine[_],
        closure: SValue.PClosure,
        actuals: util.ArrayList[SValue],
    ): KFun =
      KFun(machine.markBase(), closure, actuals)
  }

  /** The builtin arguments have been evaluated. Now execute the builtin. */
  private[speedy] final case class KBuiltin private (
      savedBase: Int,
      builtin: SBuiltin,
      actuals: util.ArrayList[SValue],
  ) extends Kont
      with SomeArrayEquals
      with NoCopy {
    override def execute[Q](machine: Machine[Q], v: SValue): Control[Q] = {
      discard[Boolean](actuals.add(v))
      // A builtin has no free-vars, so we set the frame to null.
      machine.restoreBase(savedBase)
      machine.restoreFrameAndActuals(null, actuals)
      builtin.execute(actuals, machine)
    }
  }

  object KBuiltin {
    def apply(machine: Machine[_], builtin: SBuiltin, actuals: util.ArrayList[SValue]): KBuiltin =
      KBuiltin(machine.markBase(), builtin, actuals)
  }

  /** The function's partial-arguments have been evaluated. Construct and return the PAP */
  private[speedy] final case class KPap(
      prim: SValue.Prim,
      actuals: util.ArrayList[SValue],
      arity: Int,
  ) extends Kont {

    override def execute[Q](machine: Machine[Q], v: SValue): Control.Value = {
      discard[Boolean](actuals.add(v))
      val pap = SValue.SPAP(prim, actuals, arity)
      Control.Value(pap)
    }
  }

  /** The scrutinee of a match has been evaluated, now match the alternatives against it. */
  private[speedy] def executeMatchAlts(
      machine: Machine[_],
      alts: Array[SCaseAlt],
      v: SValue,
  ): Control[Nothing] = {
    val altOpt = v match {
      case SValue.SBool(b) =>
        alts.find { alt =>
          alt.pattern match {
            case SCPPrimCon(PCTrue) => b
            case SCPPrimCon(PCFalse) => !b
            case SCPDefault => true
            case _ => false
          }
        }
      case SValue.SVariant(_, _, rank1, arg) =>
        alts.find { alt =>
          alt.pattern match {
            case SCPVariant(_, _, rank2) if rank1 == rank2 =>
              machine.pushEnv(arg)
              true
            case SCPDefault => true
            case _ => false
          }
        }
      case SValue.SEnum(_, _, rank1) =>
        alts.find { alt =>
          alt.pattern match {
            case SCPEnum(_, _, rank2) => rank1 == rank2
            case SCPDefault => true
            case _ => false
          }
        }
      case SValue.SList(lst) =>
        alts.find { alt =>
          alt.pattern match {
            case SCPNil if lst.isEmpty => true
            case SCPCons =>
              lst.pop match {
                case Some((head, tail)) =>
                  machine.pushEnv(head)
                  machine.pushEnv(SValue.SList(tail))
                  true
                case None =>
                  false
              }
            case SCPDefault => true
            case _ => false
          }
        }
      case SValue.SUnit =>
        alts.find { alt =>
          alt.pattern match {
            case SCPPrimCon(PCUnit) => true
            case SCPDefault => true
            case _ => false
          }
        }
      case SValue.SOptional(mbVal) =>
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
      case SValue.SContractId(_) | SValue.SDate(_) | SValue.SNumeric(_) | SValue.SInt64(_) |
          SValue.SParty(_) | SValue.SText(_) | SValue.STimestamp(_) | SValue.SStruct(_, _) |
          SValue.SMap(_, _) | _: SValue.SRecordRep | SValue.SAny(_, _) | SValue.STypeRep(_) |
          SValue.SBigNumeric(_) | _: SValue.SPAP | SValue.SToken =>
        throw SErrorCrash(NameOf.qualifiedNameOfCurrentFunc, "Match on non-matchable value")
    }

    val e = altOpt
      .getOrElse(
        throw SErrorCrash(NameOf.qualifiedNameOfCurrentFunc, s"No match for $v in ${alts.toList}")
      )
      .body
    Control.Expression(e)
  }

  /** Push the evaluated value to the array 'to', and start evaluating the expression 'next'.
    * This continuation is used to implement both function application and lets. In
    * the case of function application the arguments are pushed into the 'actuals' array of
    * the PAP that is being built, and in the case of lets the evaluated value is pushed
    * directly into the environment.
    */
  private[speedy] final case class KPushTo private (
      savedBase: Int,
      frame: Frame,
      actuals: Actuals,
      to: util.ArrayList[SValue],
      next: SExpr,
  ) extends Kont
      with SomeArrayEquals
      with NoCopy {
    override def execute[Q](machine: Machine[Q], v: SValue): Control.Expression = {
      machine.restoreBase(savedBase);
      machine.restoreFrameAndActuals(frame, actuals)
      discard[Boolean](to.add(v))
      Control.Expression(next)
    }
  }

  object KPushTo {
    def apply(machine: Machine[_], to: util.ArrayList[SValue], next: SExpr): KPushTo =
      KPushTo(machine.markBase(), machine.currentFrame, machine.currentActuals, to, next)
  }

  private[speedy] final case class KFoldl private (
      frame: Frame,
      actuals: Actuals,
      func: SValue,
      var list: FrontStack[SValue],
  ) extends Kont
      with SomeArrayEquals
      with NoCopy {
    override def execute[Q](machine: Machine[Q], acc: SValue): Control[Q] = {
      list.pop match {
        case None =>
          Control.Value(acc)
        case Some((item, rest)) =>
          machine.restoreFrameAndActuals(frame, actuals)
          // NOTE: We are "recycling" the current continuation with the
          // remainder of the list to avoid allocating a new continuation.
          list = rest
          machine.pushKont(this)
          machine.enterApplication(func, Array(SEValue(acc), SEValue(item)))
      }
    }
  }

  object KFoldl {
    def apply(machine: Machine[_], func: SValue, list: FrontStack[SValue]): KFoldl =
      KFoldl(machine.currentFrame, machine.currentActuals, func, list)
  }

  private[speedy] final case class KFoldr private (
      frame: Frame,
      actuals: Actuals,
      func: SValue,
      list: ImmArray[SValue],
      var lastIndex: Int,
  ) extends Kont
      with SomeArrayEquals
      with NoCopy {
    override def execute[Q](machine: Machine[Q], acc: SValue): Control[Q] = {
      if (lastIndex > 0) {
        machine.restoreFrameAndActuals(frame, actuals)
        val currentIndex = lastIndex - 1
        val item = list(currentIndex)
        lastIndex = currentIndex
        machine.pushKont(this) // NOTE: We've updated `lastIndex`.
        machine.enterApplication(func, Array(SEValue(item), SEValue(acc)))
      } else {
        Control.Value(acc)
      }
    }
  }

  object KFoldr {
    def apply(machine: Machine[_], func: SValue, list: ImmArray[SValue], lastIndex: Int): KFoldr =
      KFoldr(machine.currentFrame, machine.currentActuals, func, list, lastIndex)
  }

  // NOTE: See the explanation above the definition of `SBFoldr` on why we need
  // this continuation and what it does.
  private[speedy] final case class KFoldr1Map private (
      frame: Frame,
      actuals: Actuals,
      func: SValue,
      var list: FrontStack[SValue],
      var revClosures: FrontStack[SValue],
      init: SValue,
  ) extends Kont
      with SomeArrayEquals
      with NoCopy {
    override def execute[Q](machine: Machine[Q], closure: SValue): Control[Q] = {
      revClosures = closure +: revClosures
      list.pop match {
        case None =>
          machine.pushKont(KFoldr1Reduce(machine, revClosures))
          Control.Value(init)
        case Some((item, rest)) =>
          machine.restoreFrameAndActuals(frame, actuals)
          list = rest
          machine.pushKont(this) // NOTE: We've updated `revClosures` and `list`.
          machine.enterApplication(func, Array(SEValue(item)))
      }
    }
  }

  object KFoldr1Map {
    def apply(
        machine: Machine[_],
        func: SValue,
        list: FrontStack[SValue],
        revClosures: FrontStack[SValue],
        init: SValue,
    ): KFoldr1Map =
      KFoldr1Map(machine.currentFrame, machine.currentActuals, func, list, revClosures, init)
  }

  // NOTE: See the explanation above the definition of `SBFoldr` on why we need
  // this continuation and what it does.
  private[speedy] final case class KFoldr1Reduce private (
      frame: Frame,
      actuals: Actuals,
      var revClosures: FrontStack[SValue],
  ) extends Kont
      with SomeArrayEquals
      with NoCopy {
    override def execute[Q](machine: Machine[Q], acc: SValue): Control[Q] = {
      revClosures.pop match {
        case None =>
          Control.Value(acc)
        case Some((closure, rest)) =>
          machine.restoreFrameAndActuals(frame, actuals)
          revClosures = rest
          machine.pushKont(this) // NOTE: We've updated `revClosures`.
          machine.enterApplication(closure, Array(SEValue(acc)))
      }
    }
  }

  object KFoldr1Reduce {
    def apply(machine: Machine[_], revClosures: FrontStack[SValue]): KFoldr1Reduce =
      KFoldr1Reduce(machine.currentFrame, machine.currentActuals, revClosures)
  }

  /** Store the evaluated value in the definition and in the 'SEVal' from which the
    * expression came from. This in principle makes top-level values lazy. It is a
    * useful optimization to allow creation of large constants (for example records
    * that are repeatedly accessed. In older compilers which did not use the builtin
    * record and struct updates this solves the blow-up which would happen when a
    * large record is updated multiple times.
    */
  private[speedy] final case class KCacheVal(
      v: SEVal,
      defn: SDefinition,
  ) extends Kont {

    override def execute[Q](machine: Machine[Q], sv: SValue): Control.Value = {
      v.setCached(sv)
      defn.setCached(sv)
      Control.Value(sv)
    }
  }

  private[speedy] final case class KCacheContract(cid: V.ContractId) extends Kont {
    override def execute[Q](machine: Machine[Q], sv: SValue): Control[Q] =
      machine.asUpdateMachine(productPrefix) { machine =>
        val cached = SBuiltin.extractCachedContract(machine.tmplId2TxVersion, sv)
        machine.checkContractVisibility(cid, cached)
        machine.addGlobalContract(cid, cached)
        Control.Value(cached.any)
      }
  }

  private[speedy] final case class KCheckKeyVisibility(
      gKey: GlobalKey,
      cid: V.ContractId,
      handleKeyFound: V.ContractId => Control.Value,
  ) extends Kont {
    override def execute[Q](machine: Machine[Q], sv: SValue): Control[Q] =
      machine.asUpdateMachine(productPrefix)(_.checkKeyVisibility(gKey, cid, handleKeyFound))
  }

  /** KCloseExercise. Marks an open-exercise which needs to be closed. Either:
    * (1) by 'endExercises' if this continuation is entered normally, or
    * (2) by 'abortExercises' if we unwind the stack through this continuation
    */
  private[speedy] final case object KCloseExercise extends Kont {

    override def execute[Q](machine: Machine[Q], exerciseResult: SValue): Control[Q] =
      machine.asUpdateMachine(productPrefix) { machine =>
        machine.ptx = machine.ptx.endExercises(exerciseResult.toNormalizedValue)
        Control.Value(exerciseResult)
      }
  }

  /** KTryCatchHandler marks the kont-stack to allow unwinding when throw is executed. If
    * the continuation is entered normally, the environment is restored but the handler is
    * not executed.  When a throw is executed, the kont-stack is unwound to the nearest
    * enclosing KTryCatchHandler (if there is one), and the code for the handler executed.
    */
  private[speedy] final case class KTryCatchHandler private (
      savedBase: Int,
      frame: Frame,
      actuals: Actuals,
      handler: SExpr,
  ) extends Kont
      with SomeArrayEquals
      with NoCopy {
    // we must restore when catching a throw, or for normal execution
    def restore(machine: UpdateMachine): Unit = {
      machine.restoreBase(savedBase)
      machine.restoreFrameAndActuals(frame, actuals)
    }

    override def execute[Q](machine: Machine[Q], v: SValue): Control[Q] =
      machine.asUpdateMachine(productPrefix) { machine =>
        restore(machine)
        machine.ptx = machine.ptx.endTry
        Control.Value(v)
      }
  }

  object KTryCatchHandler {
    def apply(machine: Machine[_], handler: SExpr): KTryCatchHandler =
      KTryCatchHandler(
        machine.markBase(),
        machine.currentFrame,
        machine.currentActuals,
        handler: SExpr,
      )
  }

  private[speedy] final case class KCheckChoiceGuard(
      coid: V.ContractId,
      templateId: TypeConName,
      choiceName: ChoiceName,
      byInterface: Option[TypeConName],
  ) extends Kont {
    def abort(): Nothing =
      throw SErrorDamlException(
        IError.Dev(
          NameOf.qualifiedNameOfCurrentFunc,
          IError.Dev.ChoiceGuardFailed(coid, templateId, choiceName, byInterface),
        )
      )

    override def execute[Q](machine: Machine[Q], v: SValue): Control.Value = {
      v match {
        case SValue.SBool(b) =>
          if (b)
            Control.Value(SValue.SUnit)
          else
            abort()
        case _ =>
          throw SErrorCrash("KCheckChoiceGuard", "Expected SBool value.")
      }
    }
  }

  /** Continuation produced by [[SELabelClosure]] expressions. This is only
    * used during profiling. Its purpose is to attach a label to closures such
    * that entering the closure can write an "open event" with that label.
    */
  private[speedy] final case class KLabelClosure(label: Profile.Label) extends Kont {
    override def execute[Q](machine: Machine[Q], v: SValue): Control.Value = {
      v match {
        case SValue.SPAP(SValue.PClosure(_, expr, closure), args, arity) =>
          val pap = SValue.SPAP(SValue.PClosure(label, expr, closure), args, arity)
          Control.Value(pap)
        case _ =>
          Control.Value(v)
      }
    }
  }

  /** Continuation marking the exit of a closure. This is only used during
    * profiling.
    */
  private[speedy] final case class KLeaveClosure(label: Profile.Label) extends Kont {
    override def execute[Q](machine: Machine[Q], v: SValue): Control.Value = {
      machine.profile.addCloseEvent(label)
      Control.Value(v)
    }
  }

  private[speedy] final case object KPreventException extends Kont {
    override def execute[Q](machine: Machine[Q], v: SValue): Control.Value = {
      Control.Value(v)
    }
  }

  private[speedy] def deriveTransactionSeed(
      submissionSeed: crypto.Hash,
      participant: Ref.ParticipantId,
      submissionTime: Time.Timestamp,
  ): InitialSeeding =
    InitialSeeding.TransactionSeed(
      crypto.Hash.deriveTransactionSeed(submissionSeed, participant, submissionTime)
    )

}
