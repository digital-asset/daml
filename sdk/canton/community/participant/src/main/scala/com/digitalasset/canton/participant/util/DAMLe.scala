// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.util

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{LoggingContextUtil, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.PackageService
import com.digitalasset.canton.participant.protocol.EngineController
import com.digitalasset.canton.participant.protocol.EngineController.GetEngineAbortStatus
import com.digitalasset.canton.participant.store.ContractLookupAndVerification
import com.digitalasset.canton.participant.util.DAMLe.{
  ContractWithMetadata,
  HasReinterpret,
  PackageResolver,
  ReInterpretationResult,
  TransactionEnricher,
}
import com.digitalasset.canton.platform.apiserver.configuration.EngineLoggingConfig
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.SerializableContract.LedgerCreateTime
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{LfCommand, LfCreateCommand, LfKeyResolver, LfPartyId, LfVersioned}
import com.digitalasset.daml.lf.VersionRange
import com.digitalasset.daml.lf.data.Ref.{PackageId, PackageName}
import com.digitalasset.daml.lf.data.{ImmArray, Ref, Time}
import com.digitalasset.daml.lf.engine.*
import com.digitalasset.daml.lf.interpretation.Error as LfInterpretationError
import com.digitalasset.daml.lf.language.Ast.Package
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.language.LanguageVersion.v2_dev
import com.digitalasset.daml.lf.transaction.{ContractKeyUniquenessMode, Versioned}

import java.nio.file.Path
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object DAMLe {
  final case class ReInterpretationResult(
      transaction: LfVersionedTransaction,
      metadata: TransactionMetadata,
      keyResolver: LfKeyResolver,
      usedPackages: Set[PackageId],
      usesLedgerTime: Boolean,
  )

  def newEngine(
      enableLfDev: Boolean,
      enableLfBeta: Boolean,
      enableStackTraces: Boolean,
      profileDir: Option[Path] = None,
      iterationsBetweenInterruptions: Long =
        10000, // 10000 is the default value in the engine configuration
  ): Engine =
    new Engine(
      EngineConfig(
        allowedLanguageVersions = VersionRange(
          LanguageVersion.v2_1,
          maxVersion(enableLfDev, enableLfBeta),
        ),
        // The package store contains only validated packages, so we can skip validation upon loading
        packageValidation = false,
        stackTraceMode = enableStackTraces,
        profileDir = profileDir,
        requireSuffixedGlobalContractId = true,
        contractKeyUniqueness = ContractKeyUniquenessMode.Off,
        iterationsBetweenInterruptions = iterationsBetweenInterruptions,
      )
    )

  private def maxVersion(enableLfDev: Boolean, enableLfBeta: Boolean) =
    if (enableLfDev) v2_dev
    else if (enableLfBeta) LanguageVersion.EarlyAccessVersions(LanguageVersion.Major.V2).max
    else LanguageVersion.StableVersions(LanguageVersion.Major.V2).max

  /** Resolves packages by [[com.digitalasset.daml.lf.data.Ref.PackageId]].
    * The returned packages must have been validated
    * so that [[com.digitalasset.daml.lf.engine.Engine]] can skip validation.
    */
  type PackageResolver = PackageId => TraceContext => Future[Option[Package]]
  type TransactionEnricher = LfVersionedTransaction => TraceContext => EitherT[
    FutureUnlessShutdown,
    ReinterpretationError,
    LfVersionedTransaction,
  ]

  sealed trait ReinterpretationError extends PrettyPrinting

  final case class EngineError(cause: Error) extends ReinterpretationError {
    override protected def pretty: Pretty[EngineError] = adHocPrettyInstance
  }

  final case class EngineAborted(reason: String) extends ReinterpretationError {
    override protected def pretty: Pretty[EngineAborted] = prettyOfClass(
      param("reason", _.reason.doubleQuoted)
    )
  }

  final case class ContractWithMetadata(
      instance: LfContractInst,
      signatories: Set[LfPartyId],
      stakeholders: Set[LfPartyId],
      templateId: LfTemplateId,
      keyWithMaintainers: Option[LfGlobalKeyWithMaintainers],
  ) {
    def metadataWithGlobalKey: ContractMetadata =
      ContractMetadata.tryCreate(
        signatories,
        stakeholders,
        keyWithMaintainers.map(LfVersioned(instance.version, _)),
      )
  }

  private val zeroSeed: LfHash =
    LfHash.assertFromByteArray(new Array[Byte](LfHash.underlyingHashLength))

  // Helper to ensure the package service resolver uses the caller's trace context.
  def packageResolver(
      packageService: PackageService
  ): PackageId => TraceContext => Future[Option[Package]] =
    pkgId => traceContext => packageService.getPackage(pkgId)(traceContext)

  trait HasReinterpret {
    def reinterpret(
        contracts: ContractLookupAndVerification,
        submitters: Set[LfPartyId],
        command: LfCommand,
        ledgerTime: CantonTimestamp,
        submissionTime: CantonTimestamp,
        rootSeed: Option[LfHash],
        packageResolution: Map[Ref.PackageName, Ref.PackageId] = Map.empty,
        expectFailure: Boolean,
        getEngineAbortStatus: GetEngineAbortStatus,
    )(implicit traceContext: TraceContext): EitherT[
      Future,
      ReinterpretationError,
      ReInterpretationResult,
    ]
  }

}

/** Represents a Daml runtime instance for interpreting commands. Provides an abstraction for the Daml engine
  * handling requests for contract instance lookup as well as in resolving packages.
  * The recommended execution context is to use a work stealing pool.
  *
  * @param resolvePackage A resolver for resolving packages
  * @param ec The execution context where Daml interpretation and validation are execution
  */
class DAMLe(
    resolvePackage: PackageResolver,
    engine: Engine,
    engineLoggingConfig: EngineLoggingConfig,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with HasReinterpret {

  import DAMLe.{EngineAborted, EngineError, ReinterpretationError}

  logger.debug(engine.info.show)(TraceContext.empty)

  // TODO(i21582) Because we do not hash suffixed CIDs, we need to disable validation of suffixed CIDs otherwise enrichment
  // will fail. Remove this when we hash and sign suffixed CIDs
  private lazy val engineForEnrichment = new Engine(
    engine.config.copy(requireSuffixedGlobalContractId = false)
  )
  private lazy val valueEnricher = new ValueEnricher(engineForEnrichment)

  /** Enrich transaction values by re-hydrating record labels
    */
  val enrichTransaction: TransactionEnricher = { transaction => implicit traceContext =>
    EitherT {
      handleResult(
        ContractLookupAndVerification.noContracts(loggerFactory),
        valueEnricher.enrichVersionedTransaction(transaction),
        // This should not happen as value enrichment should only request lookups
        () =>
          EngineController.EngineAbortStatus(
            Some("Unexpected engine interruption while enriching transaction")
          ),
      )
    }.mapK(FutureUnlessShutdown.outcomeK)
  }

  override def reinterpret(
      contracts: ContractLookupAndVerification,
      submitters: Set[LfPartyId],
      command: LfCommand,
      ledgerTime: CantonTimestamp,
      submissionTime: CantonTimestamp,
      rootSeed: Option[LfHash],
      packageResolution: Map[PackageName, PackageId],
      expectFailure: Boolean,
      getEngineAbortStatus: GetEngineAbortStatus,
  )(implicit traceContext: TraceContext): EitherT[
    Future,
    ReinterpretationError,
    ReInterpretationResult,
  ] = {

    def peelAwayRootLevelRollbackNode(
        tx: LfVersionedTransaction
    ): Either[Error, LfVersionedTransaction] =
      if (!expectFailure) {
        Right(tx)
      } else {
        def err(msg: String): Left[Error, LfVersionedTransaction] =
          Left(
            Error.Interpretation(
              Error.Interpretation.Internal("engine.reinterpret", msg, None),
              transactionTrace = None,
            )
          )

        tx.roots.get(0).map(nid => nid -> tx.nodes(nid)) match {
          case Some((rootNid, LfNodeRollback(children))) =>
            children.toSeq match {
              case Seq(singleChildNodeId) =>
                tx.nodes(singleChildNodeId) match {
                  case _: LfActionNode =>
                    Right(
                      LfVersionedTransaction(
                        tx.version,
                        tx.nodes - rootNid,
                        ImmArray(singleChildNodeId),
                      )
                    )
                  case LfNodeRollback(_) =>
                    err(s"Root-level rollback node not expected to parent another rollback node")
                }
              case Seq() => err(s"Root-level rollback node not expected to have no child node")
              case multipleChildNodes =>
                err(
                  s"Root-level rollback node not expect to have more than one child, but found ${multipleChildNodes.length}"
                )
            }
          case nonRollbackNode =>
            err(
              s"Expected failure to be turned into a root rollback node, but encountered $nonRollbackNode"
            )
        }
      }

    val result = LoggingContextUtil.createLoggingContext(loggerFactory) { implicit loggingContext =>
      engine.reinterpret(
        submitters = submitters,
        command = command,
        nodeSeed = rootSeed,
        submissionTime = submissionTime.toLf,
        ledgerEffectiveTime = ledgerTime.toLf,
        packageResolution = packageResolution,
        engineLogger =
          engineLoggingConfig.toEngineLogger(loggerFactory.append("phase", "validation")),
      )
    }

    for {
      txWithMetadata <- EitherT(handleResult(contracts, result, getEngineAbortStatus))
      (tx, metadata) = txWithMetadata
      peeledTxE = peelAwayRootLevelRollbackNode(tx).leftMap(EngineError.apply)
      txNoRootRollback <- EitherT.fromEither[Future](
        peeledTxE: Either[ReinterpretationError, LfVersionedTransaction]
      )
    } yield ReInterpretationResult(
      txNoRootRollback,
      TransactionMetadata.fromLf(ledgerTime, metadata),
      metadata.globalKeyMapping,
      metadata.usedPackages,
      metadata.dependsOnTime,
    )
  }

  def replayCreate(
      submitters: Set[LfPartyId],
      command: LfCreateCommand,
      ledgerEffectiveTime: LedgerCreateTime,
      getEngineAbortStatus: GetEngineAbortStatus,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, ReinterpretationError, LfNodeCreate] =
    LoggingContextUtil.createLoggingContext(loggerFactory) { implicit loggingContext =>
      val result = engine.reinterpret(
        submitters = submitters,
        command = command,
        nodeSeed = Some(DAMLe.zeroSeed),
        submissionTime = Time.Timestamp.Epoch, // Only used to compute contract ids
        ledgerEffectiveTime = ledgerEffectiveTime.ts.underlying,
        packageResolution = Map.empty,
      )
      for {
        txWithMetadata <- EitherT(
          handleResult(
            ContractLookupAndVerification.noContracts(loggerFactory),
            result,
            getEngineAbortStatus,
          )
        )
        (tx, _) = txWithMetadata
        singleCreate = tx.nodes.values.toList match {
          case (create: LfNodeCreate) :: Nil => create
          case _ =>
            throw new RuntimeException(
              s"DAMLe failed to replay a create $command submitted by $submitters"
            )
        }
        create <- EitherT.pure[Future, ReinterpretationError](singleCreate)
      } yield create
    }

  def contractWithMetadata(
      contractInstance: LfContractInst,
      supersetOfSignatories: Set[LfPartyId],
      getEngineAbortStatus: GetEngineAbortStatus,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, ReinterpretationError, ContractWithMetadata] = {

    val unversionedContractInst = contractInstance.unversioned
    val create = LfCreateCommand(unversionedContractInst.template, unversionedContractInst.arg)

    for {
      transactionWithMetadata <- reinterpret(
        ContractLookupAndVerification.noContracts(loggerFactory),
        supersetOfSignatories,
        create,
        CantonTimestamp.Epoch,
        CantonTimestamp.Epoch,
        Some(DAMLe.zeroSeed),
        expectFailure = false,
        getEngineAbortStatus = getEngineAbortStatus,
      )
      ReInterpretationResult(transaction, _, _, _, _) = transactionWithMetadata
      md = transaction.nodes(transaction.roots(0)) match {
        case nc: LfNodeCreate =>
          ContractWithMetadata(
            LfContractInst(
              template = nc.templateId,
              arg = Versioned(nc.version, nc.arg),
              packageName = nc.packageName,
              packageVersion = nc.packageVersion,
            ),
            nc.signatories,
            nc.stakeholders,
            nc.templateId,
            nc.keyOpt,
          )
        case node => throw new RuntimeException(s"DAMLe reinterpreted a create node as $node")
      }
    } yield md
  }

  def contractMetadata(
      contractInstance: LfContractInst,
      supersetOfSignatories: Set[LfPartyId],
      getEngineAbortStatus: GetEngineAbortStatus,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, ReinterpretationError, ContractMetadata] =
    for {
      contractAndMetadata <- contractWithMetadata(
        contractInstance,
        supersetOfSignatories,
        getEngineAbortStatus,
      )
    } yield contractAndMetadata.metadataWithGlobalKey

  private[this] def handleResult[A](
      contracts: ContractLookupAndVerification,
      result: Result[A],
      getEngineAbortStatus: GetEngineAbortStatus,
  )(implicit
      traceContext: TraceContext
  ): Future[Either[ReinterpretationError, A]] = {
    def handleResultInternal(contracts: ContractLookupAndVerification, result: Result[A])(implicit
        traceContext: TraceContext
    ): Future[Either[ReinterpretationError, A]] = {
      @tailrec
      def iterateOverInterrupts(
          continue: () => Result[A]
      ): Either[EngineAborted, Result[A]] =
        continue() match {
          case ResultInterruption(continue, _) =>
            getEngineAbortStatus().reasonO match {
              case Some(reason) =>
                logger.warn(s"Aborting engine computation, reason = $reason")
                Left(EngineAborted(reason))
              case None => iterateOverInterrupts(continue)
            }

          case otherResult => Right(otherResult)
        }

      result match {
        case ResultNeedPackage(packageId, resume) =>
          resolvePackage(packageId)(traceContext).transformWith {
            case Success(pkg) =>
              handleResultInternal(contracts, resume(pkg))
            case Failure(ex) =>
              logger.error(s"Package resolution failed for [$packageId]", ex)
              Future.failed(ex)
          }
        case ResultDone(completedResult) => Future.successful(Right(completedResult))
        case ResultNeedKey(key, resume) =>
          val gk = key.globalKey
          contracts
            .lookupKey(gk)
            .toRight(
              EngineError(
                Error.Interpretation(
                  Error.Interpretation.DamlException(LfInterpretationError.ContractKeyNotFound(gk)),
                  None,
                )
              )
            )
            .flatMap(optCid => EitherT(handleResultInternal(contracts, resume(optCid))))
            .value
        case ResultNeedContract(acoid, resume) =>
          contracts
            .lookupLfInstance(acoid)
            .value
            .flatMap(optInst => handleResultInternal(contracts, resume(optInst)))
        case ResultError(err) => Future.successful(Left(EngineError(err)))
        case ResultInterruption(continue, _) =>
          // Run the interruption loop asynchronously to avoid blocking the calling thread.
          // Using a `Future` as a trampoline also makes the recursive call to `handleResult` stack safe.
          Future(iterateOverInterrupts(continue)).flatMap {
            case Left(abort) => Future.successful(Left(abort))
            case Right(result) => handleResultInternal(contracts, result)
          }
        case ResultNeedUpgradeVerification(coid, signatories, observers, keyOpt, resume) =>
          val unusedTxVersion = LfLanguageVersion.StableVersions(LfLanguageVersion.Major.V2).max
          val metadata = ContractMetadata.tryCreate(
            signatories = signatories,
            stakeholders = signatories ++ observers,
            maybeKeyWithMaintainersVersioned = keyOpt.map(k => Versioned(unusedTxVersion, k)),
          )
          contracts.verifyMetadata(coid, metadata).value.flatMap { verification =>
            handleResultInternal(contracts, resume(verification))
          }
        case ResultPrefetch(_, resume) => handleResultInternal(contracts, resume())
      }
    }

    handleResultInternal(contracts, result)
  }

}
