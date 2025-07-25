// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import java.util
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data.{FrontStack, ImmArray, NoCopy, Ref, Time}
import com.digitalasset.daml.lf.interpretation.{Error => IError}
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.language.{PackageInterface, TypeDestructor}
import com.digitalasset.daml.lf.language.LanguageVersionRangeOps._
import com.digitalasset.daml.lf.speedy.Compiler.{CompilationError, PackageNotFound}
import com.digitalasset.daml.lf.speedy.PartialTransaction.NodeSeeds
import com.digitalasset.daml.lf.speedy.SError._
import com.digitalasset.daml.lf.speedy.SExpr._
import com.digitalasset.daml.lf.speedy.SResult._
import com.digitalasset.daml.lf.speedy.SValue.{SAnyException, SArithmeticError, SRecord, SText}
import com.digitalasset.daml.lf.speedy.Speedy.Machine.{newTraceLog, newWarningLog}
import com.digitalasset.daml.lf.stablepackages.StablePackages
import com.digitalasset.daml.lf.transaction.ContractStateMachine.KeyMapping
import com.digitalasset.daml.lf.transaction.GlobalKeyWithMaintainers
import com.digitalasset.daml.lf.transaction.{
  ContractKeyUniquenessMode,
  GlobalKey,
  Node,
  NodeId,
  SubmittedTransaction,
  IncompleteTransaction => IncompleteTx,
  TransactionVersion => TxVersion,
}
import com.digitalasset.daml.lf.value.Value.ValueArithmeticError
import com.digitalasset.daml.lf.value.{ContractIdVersion, Value => V}
import com.daml.nameof.NameOf
import com.daml.scalautil.Statement.discard
import com.daml.logging.{ContextualizedLogger, LoggingContext}

import scala.annotation.{nowarn, tailrec}
import scala.util.control.NonFatal

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
      packageName: PackageName,
      globalKeyWithMaintainers: GlobalKeyWithMaintainers,
      key: SValue,
  ) {
    def globalKey: GlobalKey = globalKeyWithMaintainers.globalKey
    def templateId: TypeConId = globalKey.templateId
    def maintainers: Set[Party] = globalKeyWithMaintainers.maintainers
    val lfValue: V = globalKey.key
    def renormalizedGlobalKeyWithMaintainers(version: TxVersion) = {
      globalKeyWithMaintainers.copy(
        globalKey = GlobalKey.assertWithRenormalizedValue(globalKey, key.toNormalizedValue(version))
      )
    }
  }

  final case class ContractInfo(
      version: TxVersion,
      packageName: Ref.PackageName,
      templateId: Ref.TypeConId,
      value: SValue,
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
        packageName = packageName,
        templateId = templateId,
        arg = arg,
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
      val packageResolution: Map[Ref.PackageName, Ref.PackageId],
      val validating: Boolean, // TODO: Better: Mode = SubmissionMode | ValidationMode
      val preparationTime: Time.Timestamp,
      val contractKeyUniqueness: ContractKeyUniquenessMode,
      val contractIdVersion: ContractIdVersion,
      /* The current partial transaction */
      private[speedy] var ptx: PartialTransaction,
      /* Committers of the action. */
      val committers: Set[Party],
      /* Additional readers (besides committers) for visibility checks. */
      val readAs: Set[Party],
      /* Commit location, if a script commit is in progress. */
      val commitLocation: Option[Location],
      val limits: interpretation.Limits,
  )(implicit loggingContext: LoggingContext)
      extends Machine[Question.Update] {

    private[this] var contractsCache = Map.empty[V.ContractId, V.ThinContractInstance]

    // To handle continuation exceptions, as continuations run outside the interpreter loop.
    // Here we delay the throw to the interpreter loop, but it would be probably better
    // to delay the whole execution. This would work for all continuation (which have type
    // `X => Unit`)  except `NeedKey` (which have type `X => Bool`) that need to be run
    // strait away.
    private[this] def safelyContinue(
        location: => String,
        question: => String,
        continue: => Control[Question.Update],
    ): Unit = {
      val control =
        try {
          continue
        } catch {
          case NonFatal(e) =>
            Control.Expression(
              SExpr.SEDelayedCrash(
                location = location,
                reason = s"unexpected exception $e when running continuation of question $question",
              )
            )
        }
      setControl(control)
    }

    // The following needXXXX methods take care to emit question while ensuring no exceptions are
    // thrown during the question callbacks execution

    final private[speedy] def needTime(
        continue: Time.Timestamp => Control[Question.Update]
    ): Control[Question.Update] = {
      Control.Question(
        Question.Update.NeedTime { time =>
          safelyContinue(
            NameOf.qualifiedNameOfCurrentFunc,
            "NeedTime", {
              require(
                timeBoundaries.min <= time && time <= timeBoundaries.max,
                s"NeedTime pre-condition failed: time $time lies outside time boundaries $timeBoundaries",
              )

              continue(time).ensuring(
                timeBoundaries.min <= time && time <= timeBoundaries.max,
                s"NeedTime post-condition failed: time $time lies outside time boundaries $timeBoundaries",
              )
            },
          )
        }
      )
    }

    final private[speedy] def needContract(
        location: => String,
        contractId: V.ContractId,
        continue: V.ThinContractInstance => Control[Question.Update],
    ): Control.Question[Question.Update] =
      Control.Question(
        Question.Update.NeedContract(
          contractId,
          committers,
          x => safelyContinue(location, "NeedContract", continue(x)),
        )
      )

    final private[speedy] def needUpgradeVerification(
        location: => String,
        coid: V.ContractId,
        signatories: Set[Party],
        observers: Set[Party],
        keyOpt: Option[GlobalKeyWithMaintainers],
        continue: Option[String] => Control[Question.Update],
    ): Control.Question[Question.Update] =
      Control.Question(
        Question.Update.NeedUpgradeVerification(
          coid,
          signatories,
          observers,
          keyOpt,
          x => safelyContinue(location, "NeedUpgradeVerification", continue(x)),
        )
      )

    final private[speedy] def needPackage(
        location: => String,
        packageId: PackageId,
        context: language.Reference,
        continue: () => Control[Question.Update],
    ): Control.Question[Question.Update] =
      Control.Question(
        Question.Update.NeedPackage(
          packageId,
          context,
          packages =>
            safelyContinue(
              location,
              "NeedPackage", {
                this.compiledPackages = packages
                // To avoid infinite loop in case the packages are not updated properly by the caller
                assert(compiledPackages.contains(packageId))
                continue()
              },
            ),
        )
      )

    final private[speedy] def needKey(
        location: => String,
        key: GlobalKeyWithMaintainers,
        continue: Option[V.ContractId] => (Control[Question.Update], Boolean),
    ): Control.Question[Question.Update] =
      Control.Question(
        Question.Update.NeedKey(
          key,
          committers,
          { result =>
            try {
              val (control, bool) = continue(result)
              setControl(control)
              bool
            } catch {
              case NonFatal(e) =>
                setControl(
                  Control.Expression(
                    SExpr.SEDelayedCrash(
                      location = location,
                      reason =
                        s"unexpected exception $e when running continuation of question NeedKey",
                    )
                  )
                )
                false
            }
          },
        )
      )

    private[speedy] def lookupContract(coid: V.ContractId)(
        f: V.ThinContractInstance => Control[Question.Update]
    ): Control[Question.Update] =
      contractsCache.get(coid) match {
        case Some(res) =>
          f(res)
        case None =>
          disclosedContracts.get(coid) match {
            case Some(contractInfo) =>
              markDisclosedcontractAsUsed(coid)
              f(
                V.ThinContractInstance(
                  contractInfo.packageName,
                  contractInfo.templateId,
                  contractInfo.value.toUnnormalizedValue,
                )
              )
            case None =>
              needContract(
                NameOf.qualifiedNameOfCurrentFunc,
                coid,
                { res =>
                  contractsCache = contractsCache.updated(coid, res)
                  f(res)
                },
              )
          }
      }

    private[speedy] override def asUpdateMachine(location: String)(
        f: UpdateMachine => Control[Question.Update]
    ): Control[Question.Update] =
      f(this)

    /** unwindToHandler is called when an exception is thrown by the builtin SBThrow or
      * re-thrown by the builtin SBTryHandler. If a catch-handler is found, we initiate
      * execution of the handler code (which might decide to re-throw). Otherwise we call
      * throwUnhandledException to apply the message function to the exception payload,
      * producing a text message.
      */
    private[speedy] override def handleException(excep: SValue.SAny): Control[Nothing] = {
      // Return value meaning:
      // None: No handler or exception conversion tag, regular unhandled exception
      // Some(Left): try-catch handler found up the stack, continue using that
      // Some(Right): exception conversion tag found, meaning this exception was thrown during exception -> failure status
      //   conversion, pass this information forward to unhandledException
      @tailrec
      def unwind(
          ptx: PartialTransaction
      ): Option[Either[KTryCatchHandler, KConvertingException[Question.Update]]] =
        if (kontDepth() == 0) {
          None
        } else {
          popKont() match {
            case handler: KTryCatchHandler =>
              // The machine's ptx is updated even if the handler does not catch the exception.
              // This may cause the transaction trace to report the error from the handler's location.
              // Ideally we should embed the trace into the exception directly.
              this.ptx = ptx.rollbackTry()
              Some(Left(handler))
            case _: KCloseExercise =>
              unwind(ptx.abortExercises)
            case k: KCheckChoiceGuard =>
              // We must abort, because the transaction has failed in a way that is
              // unrecoverable (it depends on the state of an input contract that
              // we may not have the authority to fetch).
              abort()
              k.abort()
            case KPreventException() =>
              None
            case converting: KConvertingException[Question.Update] =>
              Some(Right(converting))
            case _ =>
              unwind(ptx)
          }
        }

      unwind(ptx) match {
        case Some(Left(kh)) =>
          kh.restore()
          popTempStackToBase()
          pushEnv(excep) // payload on stack where handler expects it
          Control.Expression(kh.handler)
        case Some(Right(KConvertingException(originalExceptionId))) =>
          unhandledException(excep, Some(originalExceptionId))
        case None =>
          unhandledException(excep)
      }
    }

    /** Tracks the lower and upper bounds on the ledger time for a given Daml interpretation
      * run. At any point during interpretation, the interpretation up to then is invariant
      * for any ledger time within these bounds.
      */
    private[this] var timeBoundaries: Time.Range = Time.Range.unconstrained

    // global contract discriminators, that are discriminators from contract created in previous transactions

    private[this] var numInputContracts: Int = 0

    private[this] var disclosedContracts_ = Map.empty[V.ContractId, ContractInfo]
    private[speedy] def disclosedContracts: Map[V.ContractId, ContractInfo] = disclosedContracts_

    private[this] var disclosedContractKeys_ = Map.empty[GlobalKey, V.ContractId]
    private[speedy] def disclosedContractKeys: Map[GlobalKey, V.ContractId] = disclosedContractKeys_

    private[speedy] def addDisclosedContracts(
        contractId: V.ContractId,
        contract: ContractInfo,
    ): Unit = {
      disclosedContracts_ = disclosedContracts.updated(contractId, contract)
      contract.keyOpt.foreach(key =>
        disclosedContractKeys_ = disclosedContractKeys.updated(key.globalKey, contractId)
      )
    }

    private[speedy] def isDisclosedContract(contractId: V.ContractId): Boolean =
      disclosedContracts.isDefinedAt(contractId)

    def getTimeBoundaries: Time.Range =
      timeBoundaries

    private[speedy] def setTimeBoundaries(newTimeBoundaries: Time.Range): Unit =
      timeBoundaries = newTimeBoundaries

    val visibleToStakeholders: Set[Party] => SVisibleToStakeholders =
      if (validating) { _ => SVisibleToStakeholders.Visible }
      else {
        SVisibleToStakeholders.fromSubmitters(committers, readAs)
      }

    def incompleteTransaction: IncompleteTx = ptx.finishIncomplete
    def nodesToString: String = ptx.nodesToString

    /** Local Contract Store:
      *      Maps contract-id to type+svalue, for LOCALLY-CREATED contracts.
      *      - Consulted (getIfLocalContract) by fetchAny (SBuiltin).
      *      - Updated   (storeLocalContract) by SBUCreate.
      */
    private[speedy] var localContractStore: Map[V.ContractId, (TypeConId, SValue)] = Map.empty
    private[speedy] def getIfLocalContract(coid: V.ContractId): Option[(TypeConId, SValue)] = {
      localContractStore.get(coid)
    }
    private[speedy] def storeLocalContract(
        coid: V.ContractId,
        templateId: TypeConId,
        templateArg: SValue,
    ): Unit = {
      localContractStore = localContractStore + (coid -> (templateId, templateArg))
    }

    /** Contract Info Cache:
      *      Maps contract-id to contract-info, for EVERY referenced contract-id.
      *      - Consulted (lookupContractInfoCache) by getContractInfo (SBuiltin).
      *      - Updated   (insertContractInfoCache) by getContractInfo + SBUCreate.
      */
    // TODO: https://github.com/digital-asset/daml/issues/17082
    // - Must be template-id aware when we support ResultNeedUpgradeVerification
    private[speedy] var contractInfoCache_ : Map[(V.ContractId, PackageId), ContractInfo] =
      Map.empty
    private[speedy] def contractInfoCache: Map[(V.ContractId, PackageId), ContractInfo] =
      contractInfoCache_
    private[speedy] def insertContractInfoCache(
        coid: V.ContractId,
        contract: ContractInfo,
    ): Unit = {
      val pkgId = contract.templateId.packageId
      contractInfoCache_ = contractInfoCache_.updated((coid, pkgId), contract)
    }

    private[speedy] def isLocalContract(contractId: V.ContractId): Boolean = {
      ptx.contractState.locallyCreated.contains(contractId)
    }

    private[speedy] def ensurePackageIsLoaded(
        loc: => String,
        packageId: PackageId,
        ref: => language.Reference,
    )(
        k: () => Control[Question.Update]
    ): Control[Question.Update] =
      if (compiledPackages.contains(packageId))
        k()
      else
        needPackage(loc, packageId, ref, k)

    private[speedy] def enforceLimitSignatoriesAndObservers(
        cid: V.ContractId,
        contract: ContractInfo,
    ): Unit = {
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
    }

    private[speedy] def enforceLimitAddInputContract(): Unit = {
      numInputContracts += 1
      enforceLimit(
        NameOf.qualifiedNameOfCurrentFunc,
        numInputContracts,
        limits.transactionInputContracts,
        IError.Dev.Limit.TransactionInputContracts,
      )
    }

    private[speedy] def enforceChoiceControllersLimit(
        controllers: Set[Party],
        cid: V.ContractId,
        templateId: TypeConId,
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
        templateId: TypeConId,
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
        templateId: TypeConId,
        choiceName: ChoiceName,
        arg: V,
    ): Unit =
      enforceLimit(
        NameOf.qualifiedNameOfCurrentFunc,
        authorizers.size,
        limits.choiceAuthorizers,
        IError.Dev.Limit.ChoiceAuthorizers(cid, templateId, choiceName, arg, authorizers, _),
      )

    // Track which disclosed contracts are used, so we can report events to the ledger
    private[this] var usedDiclosedContracts: Set[V.ContractId] = Set.empty
    private[this] def markDisclosedcontractAsUsed(coid: V.ContractId): Unit = {
      usedDiclosedContracts = usedDiclosedContracts + coid
    }

    // The set of create events for the disclosed contracts that are used by the generated transaction.
    def disclosedCreateEvents: ImmArray[Node.Create] = {
      disclosedContracts.iterator
        .collect {
          case (coid, contract) if usedDiclosedContracts.contains(coid) =>
            contract.toCreateNode(coid)
        }
        .to(ImmArray)
    }

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
        contract: ContractInfo,
    ): Unit = {
      // For disclosed contracts, we do not perform visibility checking
      if (!isDisclosedContract(cid)) {
        visibleToStakeholders(contract.stakeholders) match {
          case SVisibleToStakeholders.Visible =>
            ()

          case SVisibleToStakeholders.NotVisible(actAs, readAs) =>
            val readers = (actAs union readAs).mkString(",")
            val stakeholders = contract.stakeholders.mkString(",")
            // TODO: https://github.com/digital-asset/daml/issues/17082
            //  make this warning an internal error once immutability of meta-data contract is done properly.
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

    private[speedy] var lastCommand: Option[Command] = None

    def transactionTrace(numOfCmds: Int): String = {
      def prettyTypeId(typeId: TypeConId): String =
        s"${typeId.packageId.take(8)}:${typeId.qualifiedName}"
      def prettyCoid(coid: V.ContractId): String = coid.coid.take(10)
      def prettyValue(v: SValue) = Pretty.prettyValue(false)(v.toUnnormalizedValue)
      val stringBuilder = new StringBuilder()
      def addLine(s: String) = {
        val _ = stringBuilder.addAll("    ").addAll(s).addAll("\n")
      }

      val traceIterator = ptx.transactionTrace

      traceIterator
        .take(numOfCmds)
        .map { case (NodeId(nid), exe) =>
          val typeId = prettyTypeId(exe.interfaceId.getOrElse(exe.templateId))
          s"in choice $typeId:${exe.choiceId} on contract ${exe.targetCoid.coid.take(10)} (#$nid)"
        }
        .foreach(addLine)

      if (traceIterator.hasNext) {
        addLine("...")
      }

      lastCommand
        .map {
          case Command.Create(tmplId, _) =>
            s"in create command ${prettyTypeId(tmplId)}."
          case Command.ExerciseTemplate(tmplId, coid, choiceId, _) =>
            s"in exercise command ${prettyTypeId(tmplId)}:$choiceId on contract ${prettyCoid(coid.value)}."
          case Command.ExerciseInterface(ifaceId, coid, choiceId, _) =>
            s"in exercise command ${prettyTypeId(ifaceId)}:$choiceId on contract ${prettyCoid(coid.value)}."
          case Command.ExerciseByKey(tmplId, key, choiceId, _) =>
            s"in exercise-by-key command ${prettyTypeId(tmplId)}:$choiceId on key ${prettyValue(key)}."
          case Command.FetchTemplate(tmplId, coid) =>
            s"in fetch command ${prettyTypeId(tmplId)} on contract ${prettyCoid(coid.value)}."
          case Command.FetchInterface(ifaceId, coid) =>
            s"in fetch-by-interface command ${prettyTypeId(ifaceId)} on contract ${prettyCoid(coid.value)}."
          case Command.FetchByKey(tmplId, key) =>
            s"in fetch-by-key command ${prettyTypeId(tmplId)} on key ${prettyValue(key)}."
          case Command.CreateAndExercise(tmplId, _, choiceId, _) =>
            s"in create-and-exercise command ${prettyTypeId(tmplId)}:$choiceId."
          case Command.LookupByKey(tmplId, key) =>
            s"in lookup-by-key command ${prettyTypeId(tmplId)} on key ${prettyValue(key)}."
        }
        .foreach(addLine)
      stringBuilder.result()
    }
  }

  object UpdateMachine {

    private val iterationsBetweenInterruptions: Long = 10000

    @throws[SErrorDamlException]
    def apply(
        compiledPackages: CompiledPackages,
        preparationTime: Time.Timestamp,
        initialSeeding: InitialSeeding,
        expr: SExpr,
        committers: Set[Party],
        readAs: Set[Party],
        authorizationChecker: AuthorizationChecker = DefaultAuthorizationChecker,
        iterationsBetweenInterruptions: Long = UpdateMachine.iterationsBetweenInterruptions,
        packageResolution: Map[Ref.PackageName, Ref.PackageId] = Map.empty,
        validating: Boolean = false,
        traceLog: TraceLog = newTraceLog,
        warningLog: WarningLog = newWarningLog,
        contractKeyUniqueness: ContractKeyUniquenessMode = ContractKeyUniquenessMode.Strict,
        contractIdVersion: ContractIdVersion = ContractIdVersion.V1,
        commitLocation: Option[Location] = None,
        limits: interpretation.Limits = interpretation.Limits.Lenient,
    )(implicit loggingContext: LoggingContext): UpdateMachine =
      new UpdateMachine(
        sexpr = expr,
        packageResolution = packageResolution,
        validating = validating,
        preparationTime = preparationTime,
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
        contractIdVersion = contractIdVersion,
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
      override val convertLegacyExceptions: Boolean,
  )(implicit loggingContext: LoggingContext)
      extends Machine[Nothing] {

    private[speedy] override def asUpdateMachine(location: String)(
        f: UpdateMachine => Control[Question.Update]
    ): Nothing =
      throw SErrorCrash(location, "unexpected pure machine")

    /** Pure Machine does not handle exceptions */
    private[speedy] override def handleException(excep: SValue.SAny): Control[Nothing] =
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

    /* Should Daml Exceptions be automatically converted to FailureStatus before throwing from the engine
       Daml-script needs to disable this behaviour in 3.3, thus the flag.
     */
    val convertLegacyExceptions: Boolean = true

    private val stablePackages = StablePackages(
      compiledPackages.compilerConfig.allowedLanguageVersions.majorVersion
    )

    /** A constructor/deconstructor of value arithmetic errors. */
    val valueArithmeticError: ValueArithmeticError =
      new ValueArithmeticError(stablePackages)

    /** A constructor/deconstructor of svalue arithmetic errors. */
    val sArithmeticError: SArithmeticError =
      new SArithmeticError(valueArithmeticError)

    private[speedy] def handleException(excep: SValue.SAny): Control[Nothing]

    // Triggers conversion of exception to failure status and throws.
    // if the computation of the exception message also throws an exception, this will be called with
    // originalExceptionId as the original exceptionId, and we won't trigger a conversion
    protected final def unhandledException(
        excep: SValue.SAny,
        originalExceptionId: Option[TypeConId] = None,
    ): Control[Nothing] = {
      abort()
      if (convertLegacyExceptions) {
        val exceptionId = excep.ty match {
          case TTyCon(exceptionId) => exceptionId
          case _ =>
            throw SErrorCrash(
              NameOf.qualifiedNameOfCurrentFunc,
              s"Tried to convert a non-grounded exception type ${excep.ty.pretty} to Failure Status",
            )
        }

        def buildFailureStatus(exceptionId: Identifier, message: String) =
          Control.Error(
            interpretation.Error.FailureStatus(
              "UNHANDLED_EXCEPTION/" + exceptionId.qualifiedName.toString,
              FCInvalidGivenCurrentSystemStateOther.cantonCategoryId,
              message,
              Map(),
            )
          )

        originalExceptionId match {
          case None =>
            (exceptionId, excep) match {
              // Arithmetic error does not need to be loaded into compiledPackages to be thrown (by arithmetic builtins)
              // as such, we can't assume the DefRef for calculating its message or converting to failure
              // status exists. Instead we directly pull out its message field and build a failure status immediately using that.
              case (
                    valueArithmeticError.tyCon,
                    SAnyException(SRecord(_, _, ArrayList(SText(message)))),
                  ) =>
                buildFailureStatus(exceptionId, message)
              case _ =>
                pushKont(KConvertingException(exceptionId))
                Control.Expression(
                  compiledPackages.compiler
                    .throwExceptionAsFailureStatusSExpr(exceptionId, excep.value)
                )
            }
          case Some(originalExceptionId) =>
            buildFailureStatus(
              originalExceptionId,
              s"<Failed to calculate message as ${exceptionId.qualifiedName.toString} was thrown during conversion>",
            )
        }
      } else Control.Error(IError.UnhandledException(excep.ty, excep.value.toUnnormalizedValue))
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
    private[speedy] final var kontStack: util.ArrayList[Kont[Q]] = initialKontStack()
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

    private[speedy] final def currentKontStack: util.ArrayList[Kont[Q]] = kontStack

    final def getLastLocation: Option[Location] = lastLocation

    final protected def clearEnv(): Unit = {
      env.clear()
      envBase = 0
    }

    final def tmplId2TxVersion(tmplId: TypeConId): TxVersion =
      Machine.tmplId2TxVersion(compiledPackages.pkgInterface, tmplId)

    final def tmplId2PackageName(tmplId: TypeConId): PackageName =
      Machine.tmplId2PackageName(compiledPackages.pkgInterface, tmplId)

    private[lf] def abort(): Unit = {
      // We make sure the interpretation cannot be resumed
      // For update machine, this preserves the partial transaction
      clearKontStack()
      clearEnv()
      setControl(Control.WeAreUnset)
    }

    /* kont manipulation... */

    final protected def clearKontStack(): Unit = kontStack.clear()

    @inline
    private[speedy] final def kontDepth(): Int = kontStack.size()

    private[speedy] def asUpdateMachine(location: String)(
        f: UpdateMachine => Control[Question.Update]
    ): Control[Q]

    @inline
    private[speedy] final def pushKont(k: Kont[Q]): Unit = {
      discard[Boolean](kontStack.add(k))
      if (enableInstrumentation) {
        track.incrPushesKont()
        track.setDepthKont(kontDepth())
      }
    }

    @inline
    private[speedy] final def popKont(): Kont[Q] = {
      kontStack.remove(kontStack.size - 1)
    }

    @inline
    private[speedy] final def peekKontStackEnd(): Kont[Q] = {
      kontStack.get(kontStack.size - 1)
    }

    @inline
    private[speedy] final def peekKontStackTop(): Kont[Q] = {
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
                control = popKont().execute(value)
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
                abort()
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
              if (compiledPackages.contains(ref.packageId))
                throw SErrorCrash(
                  NameOf.qualifiedNameOfCurrentFunc,
                  s"definition $ref not found even after caller provided new set of packages",
                )
              else {
                asUpdateMachine(NameOf.qualifiedNameOfCurrentFunc)(
                  _.needPackage(
                    NameOf.qualifiedNameOfCurrentFunc,
                    ref.packageId,
                    language.Reference.Package(ref.packageId),
                    () => Control.Expression(eval),
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
                  this.pushKont(KLeaveClosure(this, label))
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

    // This translates a well-typed LF value (typically coming from the ledger)
    // to speedy value and set the control of with the result.
    // Note the method does not check the value is well-typed as opposed as
    // com.digitalasset.daml.lf.engine.preprocessing.ValueTranslator.translateValue.
    // All the contract IDs contained in the value are considered global.
    // Raises an exception if missing a package.
    private[speedy] final def importValue(typ0: Type, value0: V): Control.Value = {

      import TypeDestructor.SerializableTypeF._
      val Destructor = TypeDestructor(compiledPackages.pkgInterface)

      def go(ty: Type, value: V): SValue = {
        def typeMismatch = throw SErrorCrash(
          NameOf.qualifiedNameOfCurrentFunc,
          s"mismatching type: $ty and value: $value",
        )

        value match {
          case leaf: V.ValueCidlessLeaf =>
            leaf match {
              case V.ValueEnum(_, consName) =>
                Destructor.destruct(ty) match {
                  case Right(enumF: EnumF) =>
                    val rank =
                      enumF
                        .consRank(consName)
                        .getOrElse(
                          throw SErrorDamlException(
                            IError.Upgrade(
                              IError.Upgrade.DowngradeFailed(ty, value)
                            )
                          )
                        )
                    SValue.SEnum(enumF.tyCon, consName, rank)
                  case _ =>
                    typeMismatch
                }
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
              case V.ValueBool(value) =>
                if (value) SValue.SValue.True else SValue.SValue.False
              case V.ValueUnit =>
                SValue.SUnit
            }
          case V.ValueRecord(_, sourceElements) =>
            Destructor.destruct(ty) match {
              case Right(recordF: RecordF[_]) =>
                // This code implements the compatibility transformation used for up/down-grading
                // And handles the cases:
                // - UPGRADE:   numT > numS : creates a None for each missing fields.
                // - DOWNGRADE: numS > numT : drops each extra field, ensuring it is None.
                //
                // When numS == numT, we wont hit the code marked either as UPGRADE or DOWNGRADE,
                // although it is still possible that the source and target types are different,
                // but since we don't consult the source type (may be unavailable), we wont know.

                val numS: Int = sourceElements.length
                val numT: Int = recordF.fieldTypes.length

                // traverse the sourceElements, "get"ing the corresponding target type
                // when there is no corresponding type, we must be downgrading, and so we insist the value is None
                val values0: List[SValue] =
                  sourceElements.toSeq.view.zipWithIndex.flatMap { case ((_, v), i) =>
                    recordF.fieldTypes.lift(i) match {
                      case Some(targetFieldType) =>
                        val sv: SValue = go(targetFieldType, v)
                        List(sv)
                      case None => { // DOWNGRADE
                        // i ranges from 0 to numS-1. So i >= numT implies numS > numT
                        assert((numS > i) && (i >= numT))
                        v match {
                          case V.ValueOptional(None) =>
                            List.empty // ok, drop
                          case V.ValueOptional(Some(_)) =>
                            throw SErrorDamlException(
                              IError.Upgrade(
                                IError.Upgrade.DowngradeDropDefinedField(ty, i.toLong, value)
                              )
                            )
                          case _ =>
                            throw SErrorCrash(
                              NameOf.qualifiedNameOfCurrentFunc,
                              "Unexpected non-optional extra contract field encountered during downgrading.",
                            )
                        }
                      }
                    }
                  }.toList

                val values: util.ArrayList[SValue] = {
                  if (numT > numS) {
                    // UPGRADE

                    recordF.fieldTypes.view.drop(numS).map(Destructor.destruct(_)).foreach {
                      case Right(OptionalF(_)) =>
                      case _ =>
                        throw SErrorCrash(
                          NameOf.qualifiedNameOfCurrentFunc,
                          "Unexpected non-optional extra template field type encountered during upgrading.",
                        )
                    }

                    values0.padTo(numT, SValue.SValue.None)
                  } else {
                    values0
                  }
                }.to(ArrayList)

                SValue.SRecord(recordF.tyCon, recordF.fieldNames.to(ImmArray), values)

              case _ =>
                typeMismatch
            }
          case V.ValueVariant(_, variant, value) =>
            Destructor.destruct(ty) match {
              case Right(variantF: VariantF[_]) =>
                val rank =
                  variantF
                    .consRank(variant)
                    .getOrElse(
                      throw SErrorDamlException(
                        IError.Upgrade(
                          IError.Upgrade.DowngradeFailed(ty, value)
                        )
                      )
                    )
                val a = variantF.consTypes(rank)
                SValue.SVariant(variantF.tyCon, variant, rank, go(a, value))
              case _ =>
                typeMismatch
            }
          case V.ValueContractId(value) =>
            SValue.SContractId(value)
          case V.ValueList(values) =>
            Destructor.destruct(ty) match {
              case Right(ListF(a)) =>
                SValue.SList(values.map(go(a, _)))
              case _ =>
                typeMismatch
            }
          case V.ValueOptional(value) =>
            value match {
              case Some(value) =>
                Destructor.destruct(ty) match {
                  case Right(OptionalF(a)) =>
                    SValue.SOptional(Some(go(a, value)))
                  case _ =>
                    typeMismatch
                }
              case None =>
                SValue.SValue.None
            }
          case V.ValueTextMap(entries) =>
            Destructor.destruct(ty) match {
              case Right(TextMapF(a)) =>
                SValue.SMap.fromOrderedEntries(
                  isTextMap = true,
                  entries = entries.toImmArray.toSeq.view.map { case (k, v) =>
                    SValue.SText(k) -> go(a, v)
                  },
                )
              case _ =>
                typeMismatch
            }
          case V.ValueGenMap(entries) =>
            Destructor.destruct(ty) match {
              case Right(MapF(a, b)) =>
                SValue.SMap.fromOrderedEntries(
                  isTextMap = false,
                  entries = entries.toSeq.view.map { case (k, v) =>
                    go(a, k) -> go(b, v)
                  },
                )
              case _ =>
                typeMismatch
            }
        }
      }
      Control.Value(go(typ0, value0))
    }
  }

  object Machine {

    private[this] val damlTraceLog = ContextualizedLogger.createFor("daml.tracelog")
    private[this] val damlWarnings = ContextualizedLogger.createFor("daml.warnings")

    def newProfile: Profile = new Profile()
    def newTraceLog: TraceLog = new RingBufferTraceLog(damlTraceLog, 100)
    def newWarningLog: WarningLog = new WarningLog(damlWarnings)

    @throws[PackageNotFound]
    @throws[CompilationError]
    // Construct a machine for running an update expression, only used for
    // testing
    def fromUpdateExpr(
        compiledPackages: CompiledPackages,
        transactionSeed: crypto.Hash,
        updateE: Expr,
        committers: Set[Party],
        authorizationChecker: AuthorizationChecker = DefaultAuthorizationChecker,
        packageResolution: Map[Ref.PackageName, Ref.PackageId] = Map.empty,
        limits: interpretation.Limits = interpretation.Limits.Lenient,
    )(implicit loggingContext: LoggingContext): UpdateMachine = {
      val updateSE: SExpr = compiledPackages.compiler.unsafeCompile(updateE)
      fromUpdateSExpr(
        compiledPackages = compiledPackages,
        transactionSeed = transactionSeed,
        updateSE = updateSE,
        committers = committers,
        authorizationChecker = authorizationChecker,
        packageResolution = packageResolution,
        limits = limits,
      )
    }

    @throws[PackageNotFound]
    @throws[CompilationError]
    // Construct a machine for running an update expression, only used for
    // testing
    private[lf] def fromUpdateSExpr(
        compiledPackages: CompiledPackages,
        transactionSeed: crypto.Hash,
        updateSE: SExpr,
        committers: Set[Party],
        readAs: Set[Party] = Set.empty,
        authorizationChecker: AuthorizationChecker = DefaultAuthorizationChecker,
        packageResolution: Map[Ref.PackageName, Ref.PackageId] = Map.empty,
        limits: interpretation.Limits = interpretation.Limits.Lenient,
        traceLog: TraceLog = newTraceLog,
    )(implicit loggingContext: LoggingContext): UpdateMachine = {
      UpdateMachine(
        compiledPackages = compiledPackages,
        preparationTime = Time.Timestamp.MinValue,
        initialSeeding = InitialSeeding.TransactionSeed(transactionSeed),
        expr = SEApp(updateSE, Array(SValue.SToken)),
        committers = committers,
        readAs = readAs,
        packageResolution = packageResolution,
        limits = limits,
        traceLog = traceLog,
        authorizationChecker = authorizationChecker,
        iterationsBetweenInterruptions = 10000,
      )
    }

    @throws[PackageNotFound]
    @throws[CompilationError]
    // Construct an off-ledger machine for evaluating an expression that is not an update
    def fromPureSExpr(
        compiledPackages: CompiledPackages,
        expr: SExpr,
        iterationsBetweenInterruptions: Long = Long.MaxValue,
        traceLog: TraceLog = newTraceLog,
        warningLog: WarningLog = newWarningLog,
        profile: Profile = newProfile,
        convertLegacyExceptions: Boolean = true,
    )(implicit loggingContext: LoggingContext): PureMachine =
      new PureMachine(
        sexpr = expr,
        traceLog = traceLog,
        warningLog = warningLog,
        compiledPackages = compiledPackages,
        profile = profile,
        iterationsBetweenInterruptions = iterationsBetweenInterruptions,
        convertLegacyExceptions = convertLegacyExceptions,
      )

    @throws[PackageNotFound]
    @throws[CompilationError]
    // Construct an off-ledger machine for evaluating an expression that is not an update
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
        compiledPackages: CompiledPackages,
    )(implicit loggingContext: LoggingContext): Either[SError, SValue] =
      fromPureExpr(compiledPackages, expr).runPure()

    @throws[PackageNotFound]
    @throws[CompilationError]
    def runPureSExpr(
        expr: SExpr,
        compiledPackages: CompiledPackages,
        iterationsBetweenInterruptions: Long = Long.MaxValue,
    )(implicit loggingContext: LoggingContext): Either[SError, SValue] =
      fromPureSExpr(compiledPackages, expr, iterationsBetweenInterruptions).runPure()

    def tmplId2TxVersion(pkgInterface: PackageInterface, tmplId: TypeConId): TxVersion =
      pkgInterface.packageLanguageVersion(tmplId.packageId)

    def tmplId2PackageName(
        pkgInterface: PackageInterface,
        tmplId: TypeConId,
    ): PackageName =
      pkgInterface.signatures(tmplId.packageId).pkgName

    private[lf] def globalKey(
        pkgInterface: PackageInterface,
        templateId: Ref.Identifier,
        contractKey: SValue,
    ): Option[GlobalKey] =
      globalKey(
        packageTxVersion = tmplId2TxVersion(pkgInterface, templateId),
        pkgName = tmplId2PackageName(pkgInterface, templateId),
        templateId = templateId,
        keyValue = contractKey,
      )

    private[lf] def globalKey(
        packageTxVersion: TxVersion,
        pkgName: PackageName,
        templateId: TypeConId,
        keyValue: SValue,
    ): Option[GlobalKey] = {
      val lfValue = keyValue.toNormalizedValue(packageTxVersion)
      GlobalKey
        .build(templateId, lfValue, pkgName)
        .toOption
    }

    private[lf] def assertGlobalKey(
        packageTxVersion: TxVersion,
        pkgName: PackageName,
        templateId: TypeConId,
        keyValue: SValue,
    ) =
      globalKey(packageTxVersion, pkgName, templateId, keyValue)
        .getOrElse(
          throw SErrorDamlException(IError.ContractIdInContractKey(keyValue.toUnnormalizedValue))
        )

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
  // We do this by pushing a KPure(Control.Complete) continutaion on the initially
  // empty stack, which returns the final result

  private[this] def initialKontStack[Q](): util.ArrayList[Kont[Q]] = {
    val kontStack = new util.ArrayList[Kont[Q]](128)
    discard[Boolean](kontStack.add(KPure(Control.Complete)))
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
    * Not sealed, so we can define Kont variants in SBuiltin.scala
    */
  private[speedy] sealed abstract class Kont[Q] {

    /** Execute the continuation. */
    def execute(v: SValue): Control[Q]
  }

  private[speedy] final case class KPure[Q](f: SValue => Control[Q]) extends Kont[Q] {
    override def execute(v: SValue): Control[Q] = f(v)
  }

  private[speedy] final case class KOverApp[Q] private (
      machine: Machine[Q],
      savedBase: Int,
      frame: Frame,
      actuals: Actuals,
      newArgs: Array[SExprAtomic],
  ) extends Kont[Q]
      with SomeArrayEquals
      with NoCopy {
    override def execute(vfun: SValue): Control[Q] = {
      machine.restoreBase(savedBase);
      machine.restoreFrameAndActuals(frame, actuals)
      machine.enterApplication(vfun, newArgs)
    }
  }

  object KOverApp {
    def apply[Q](machine: Machine[Q], newArgs: Array[SExprAtomic]): KOverApp[Q] =
      KOverApp(machine, machine.markBase(), machine.currentFrame, machine.currentActuals, newArgs)
  }

  /** The function-closure and arguments have been evaluated. Now execute the body. */
  private[speedy] final case class KFun[Q] private (
      machine: Machine[Q],
      savedBase: Int,
      closure: SValue.PClosure,
      actuals: util.ArrayList[SValue],
  ) extends Kont[Q]
      with SomeArrayEquals
      with NoCopy {
    override def execute(v: SValue): Control.Expression = {
      discard[Boolean](actuals.add(v))
      // Set frame/actuals to allow access to the function arguments and closure free-varables.
      machine.restoreBase(savedBase)
      machine.restoreFrameAndActuals(closure.frame, actuals)
      // Maybe push a continuation for the profiler
      val label = closure.label
      if (label != null) {
        machine.profile.addOpenEvent(label)
        machine.pushKont(KLeaveClosure(machine, label))
      }
      // Start evaluating the body of the closure.
      machine.popTempStackToBase()
      Control.Expression(closure.expr)
    }
  }

  object KFun {
    def apply[Q](
        machine: Machine[Q],
        closure: SValue.PClosure,
        actuals: util.ArrayList[SValue],
    ): KFun[Q] =
      KFun(machine, machine.markBase(), closure, actuals)
  }

  /** The builtin arguments have been evaluated. Now execute the builtin. */
  private[speedy] final case class KBuiltin[Q] private (
      machine: Machine[Q],
      savedBase: Int,
      builtin: SBuiltinFun,
      actuals: util.ArrayList[SValue],
  ) extends Kont[Q]
      with SomeArrayEquals
      with NoCopy {
    override def execute(v: SValue): Control[Q] = {
      discard[Boolean](actuals.add(v))
      // A builtin has no free-vars, so we set the frame to null.
      machine.restoreBase(savedBase)
      machine.restoreFrameAndActuals(null, actuals)
      builtin.execute(actuals, machine)
    }
  }

  object KBuiltin {
    def apply[Q](
        machine: Machine[Q],
        builtin: SBuiltinFun,
        actuals: util.ArrayList[SValue],
    ): KBuiltin[Q] =
      KBuiltin(machine, machine.markBase(), builtin, actuals)
  }

  /** The function's partial-arguments have been evaluated. Construct and return the PAP */
  private[speedy] final case class KPap[Q](
      prim: SValue.Prim,
      actuals: util.ArrayList[SValue],
      arity: Int,
  ) extends Kont[Q] {

    override def execute(v: SValue): Control.Value = {
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
            case SCPBuiltinCon(BCTrue) => b
            case SCPBuiltinCon(BCFalse) => !b
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
            case SCPBuiltinCon(BCUnit) => true
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
          SValue.SMap(_, _) | SValue.SRecord(_, _, _) | SValue.SAny(_, _) | SValue.STypeRep(_) |
          SValue.SBigNumeric(_) | _: SValue.SPAP | SValue.SToken => {
        throw SErrorCrash(NameOf.qualifiedNameOfCurrentFunc, "Match on non-matchable value")
      }
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
  private[speedy] final case class KPushTo[Q] private (
      machine: Machine[Q],
      savedBase: Int,
      frame: Frame,
      actuals: Actuals,
      to: util.ArrayList[SValue],
      next: SExpr,
  ) extends Kont[Q]
      with SomeArrayEquals
      with NoCopy {
    override def execute(v: SValue): Control.Expression = {
      machine.restoreBase(savedBase);
      machine.restoreFrameAndActuals(frame, actuals)
      discard[Boolean](to.add(v))
      Control.Expression(next)
    }
  }

  object KPushTo {
    def apply[Q](machine: Machine[Q], to: util.ArrayList[SValue], next: SExpr): KPushTo[Q] =
      KPushTo(machine, machine.markBase(), machine.currentFrame, machine.currentActuals, to, next)
  }

  private[speedy] final case class KFoldl[Q] private (
      machine: Machine[Q],
      frame: Frame,
      actuals: Actuals,
      func: SValue,
      var list: FrontStack[SValue],
  ) extends Kont[Q]
      with SomeArrayEquals
      with NoCopy {
    override def execute(acc: SValue): Control[Q] = {
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
    def apply[Q](machine: Machine[Q], func: SValue, list: FrontStack[SValue]): KFoldl[Q] =
      KFoldl(machine, machine.currentFrame, machine.currentActuals, func, list)
  }

  private[speedy] final case class KFoldr[Q] private (
      machine: Machine[Q],
      frame: Frame,
      actuals: Actuals,
      func: SValue,
      list: ImmArray[SValue],
      var lastIndex: Int,
  ) extends Kont[Q]
      with SomeArrayEquals
      with NoCopy {
    override def execute(acc: SValue): Control[Q] = {
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
    def apply[Q](
        machine: Machine[Q],
        func: SValue,
        list: ImmArray[SValue],
        lastIndex: Int,
    ): KFoldr[Q] =
      KFoldr(machine, machine.currentFrame, machine.currentActuals, func, list, lastIndex)
  }

  // NOTE: See the explanation above the definition of `SBFoldr` on why we need
  // this continuation and what it does.
  private[speedy] final case class KFoldr1Map[Q] private (
      machine: Machine[Q],
      frame: Frame,
      actuals: Actuals,
      func: SValue,
      var list: FrontStack[SValue],
      var revClosures: FrontStack[SValue],
      init: SValue,
  ) extends Kont[Q]
      with SomeArrayEquals
      with NoCopy {
    override def execute(closure: SValue): Control[Q] = {
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
    def apply[Q](
        machine: Machine[Q],
        func: SValue,
        list: FrontStack[SValue],
        revClosures: FrontStack[SValue],
        init: SValue,
    ): KFoldr1Map[Q] =
      KFoldr1Map(
        machine,
        machine.currentFrame,
        machine.currentActuals,
        func,
        list,
        revClosures,
        init,
      )
  }

  // NOTE: See the explanation above the definition of `SBFoldr` on why we need
  // this continuation and what it does.
  private[speedy] final case class KFoldr1Reduce[Q] private (
      machine: Machine[Q],
      frame: Frame,
      actuals: Actuals,
      var revClosures: FrontStack[SValue],
  ) extends Kont[Q]
      with SomeArrayEquals
      with NoCopy {
    override def execute(acc: SValue): Control[Q] = {
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
    def apply[Q](machine: Machine[Q], revClosures: FrontStack[SValue]): KFoldr1Reduce[Q] =
      KFoldr1Reduce(machine, machine.currentFrame, machine.currentActuals, revClosures)
  }

  /** Store the evaluated value in the definition and in the 'SEVal' from which the
    * expression came from. This in principle makes top-level values lazy. It is a
    * useful optimization to allow creation of large constants (for example records
    * that are repeatedly accessed. In older compilers which did not use the builtin
    * record and struct updates this solves the blow-up which would happen when a
    * large record is updated multiple times.
    */
  private[speedy] final case class KCacheVal[Q](
      v: SEVal,
      defn: SDefinition,
  ) extends Kont[Q] {

    override def execute(sv: SValue): Control.Value = {
      v.setCached(sv)
      defn.setCached(sv)
      Control.Value(sv)
    }
  }

  /** KCloseExercise. Marks an open-exercise which needs to be closed. Either:
    * (1) by 'endExercises' if this continuation is entered normally, or
    * (2) by 'abortExercises' if we unwind the stack through this continuation
    */
  private[speedy] final case class KCloseExercise(machine: UpdateMachine)
      extends Kont[Question.Update] {

    override def execute(exerciseResult: SValue): Control[Question.Update] = {
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
      machine: UpdateMachine,
      savedBase: Int,
      frame: Frame,
      actuals: Actuals,
      handler: SExpr,
  ) extends Kont[Question.Update]
      with SomeArrayEquals
      with NoCopy {
    // we must restore when catching a throw, or for normal execution
    def restore(): Unit = {
      machine.restoreBase(savedBase)
      machine.restoreFrameAndActuals(frame, actuals)
    }

    override def execute(v: SValue): Control[Question.Update] = {
      restore()
      machine.ptx = machine.ptx.endTry
      Control.Value(v)
    }
  }

  object KTryCatchHandler {
    def apply(machine: UpdateMachine, handler: SExpr): KTryCatchHandler =
      KTryCatchHandler(
        machine,
        machine.markBase(),
        machine.currentFrame,
        machine.currentActuals,
        handler: SExpr,
      )
  }

  private[speedy] final case class KCheckChoiceGuard(
      coid: V.ContractId,
      templateId: TypeConId,
      choiceName: ChoiceName,
      byInterface: Option[TypeConId],
  ) extends Kont[Question.Update] {
    def abort(): Nothing =
      throw SErrorDamlException(
        IError.Dev(
          NameOf.qualifiedNameOfCurrentFunc,
          IError.Dev.ChoiceGuardFailed(coid, templateId, choiceName, byInterface),
        )
      )

    override def execute(v: SValue): Control.Value = {
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
  private[speedy] final case class KLabelClosure[Q](label: Profile.Label) extends Kont[Q] {
    override def execute(v: SValue): Control.Value = {
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
  private[speedy] final case class KLeaveClosure[Q](machine: Machine[Q], label: Profile.Label)
      extends Kont[Q] {
    override def execute(v: SValue): Control.Value = {
      machine.profile.addCloseEvent(label)
      Control.Value(v)
    }
  }

  private[speedy] final case class KPreventException[Q]() extends Kont[Q] {
    override def execute(v: SValue): Control.Value = {
      Control.Value(v)
    }
  }

  // For when converting an exception to a failure status
  // if an exception is thrown during that conversion, we need to know to not try to convert that too,
  // but instead give back the original exception with a replacement message
  private[speedy] final case class KConvertingException[Q](exceptionId: TypeConId) extends Kont[Q] {
    override def execute(v: SValue): Control.Value = {
      Control.Value(v)
    }
  }

  private[speedy] def deriveTransactionSeed(
      submissionSeed: crypto.Hash,
      participant: Ref.ParticipantId,
      preparationTime: Time.Timestamp,
  ): InitialSeeding =
    InitialSeeding.TransactionSeed(
      crypto.Hash.deriveTransactionSeed(submissionSeed, participant, preparationTime)
    )

}
