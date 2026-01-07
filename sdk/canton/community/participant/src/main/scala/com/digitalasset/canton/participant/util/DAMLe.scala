// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.util

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.data.{CantonTimestamp, LedgerTimeBoundaries}
import com.digitalasset.canton.interactive.InteractiveSubmissionEnricher
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{LoggingContextUtil, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.EngineController.GetEngineAbortStatus
import com.digitalasset.canton.participant.store.ContractAndKeyLookup
import com.digitalasset.canton.participant.util.DAMLe.*
import com.digitalasset.canton.platform.apiserver.configuration.EngineLoggingConfig
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ContractValidator.ContractAuthenticatorFn
import com.digitalasset.canton.util.PackageConsumer.PackageResolver
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.{LfCommand, LfCreateCommand, LfKeyResolver, LfPackageId, LfPartyId}
import com.digitalasset.daml.lf.VersionRange
import com.digitalasset.daml.lf.data.Ref.{PackageId, PackageName}
import com.digitalasset.daml.lf.data.{ImmArray, Ref, Time}
import com.digitalasset.daml.lf.engine.ResultNeedContract.Response
import com.digitalasset.daml.lf.engine.{Enricher as _, *}
import com.digitalasset.daml.lf.interpretation.Error as LfInterpretationError
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.language.LanguageVersion.v2_dev
import com.digitalasset.daml.lf.transaction.{ContractKeyUniquenessMode, FatContractInstance}
import com.digitalasset.daml.lf.value.ContractIdVersion

import java.nio.file.Path
import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

object DAMLe {
  final case class ReInterpretationResult(
      transaction: LfVersionedTransaction,
      metadata: TransactionMetadata,
      keyResolver: LfKeyResolver,
      usedPackages: Set[PackageId],
      timeBoundaries: LedgerTimeBoundaries,
  )

  def newEngine(
      enableLfDev: Boolean,
      enableLfBeta: Boolean,
      enableStackTraces: Boolean,
      profileDir: Option[Path] = None,
      snapshotDir: Option[Path] = None,
      iterationsBetweenInterruptions: Long =
        10000, // 10000 is the default value in the engine configuration,
      paranoidMode: Boolean,
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
        snapshotDir = snapshotDir,
        forbidLocalContractIds = true,
        contractKeyUniqueness = ContractKeyUniquenessMode.Off,
        iterationsBetweenInterruptions = iterationsBetweenInterruptions,
        paranoid = paranoidMode,
      )
    )

  private def maxVersion(enableLfDev: Boolean, enableLfBeta: Boolean) =
    if (enableLfDev) v2_dev
    else if (enableLfBeta) LanguageVersion.earlyAccessLfVersionsRange.max
    else LanguageVersion.stableLfVersionsRange.max

  /** Resolves packages by [[com.digitalasset.daml.lf.data.Ref.PackageId]]. The returned packages
    * must have been validated so that [[com.digitalasset.daml.lf.engine.Engine]] can skip
    * validation.
    */
  private type Enricher[I, O] = I => TraceContext => EitherT[
    FutureUnlessShutdown,
    ReinterpretationError,
    O,
  ]
  type TransactionEnricher = Enricher[LfVersionedTransaction, LfVersionedTransaction]
  type ContractEnricher = Enricher[(FatContractInstance, Set[LfPackageId]), FatContractInstance]

  sealed trait ReinterpretationError extends PrettyPrinting

  final case class EngineError(cause: Error) extends ReinterpretationError {
    override protected def pretty: Pretty[EngineError] = adHocPrettyInstance
  }

  final case class EngineAborted(reason: String) extends ReinterpretationError {
    override protected def pretty: Pretty[EngineAborted] = prettyOfClass(
      param("reason", _.reason.doubleQuoted)
    )
  }

  final case class EnrichmentError(reason: String) extends ReinterpretationError {
    override protected def pretty: Pretty[EnrichmentError] = adHocPrettyInstance
  }

  private val zeroSeed: LfHash =
    LfHash.assertFromByteArray(new Array[Byte](LfHash.underlyingHashLength))

  trait HasReinterpret {
    def reinterpret(
        contracts: ContractAndKeyLookup,
        contractAuthenticator: ContractAuthenticatorFn,
        submitters: Set[LfPartyId],
        command: LfCommand,
        ledgerTime: CantonTimestamp,
        preparationTime: CantonTimestamp,
        rootSeed: Option[LfHash],
        packageResolution: Map[Ref.PackageName, Ref.PackageId],
        expectFailure: Boolean,
        getEngineAbortStatus: GetEngineAbortStatus,
    )(implicit traceContext: TraceContext): EitherT[
      FutureUnlessShutdown,
      ReinterpretationError,
      ReInterpretationResult,
    ]
  }

}

/** Represents a Daml runtime instance for interpreting commands. Provides an abstraction for the
  * Daml engine handling requests for contract instance lookup as well as in resolving packages. The
  * recommended execution context is to use a work stealing pool.
  *
  * @param resolvePackage
  *   A resolver for resolving packages
  * @param ec
  *   The execution context where Daml interpretation and validation are execution
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
    engine.config.copy(forbidLocalContractIds = false)
  )
  private lazy val interactiveSubmissionEnricher = new InteractiveSubmissionEnricher(
    engineForEnrichment,
    packageId =>
      implicit traceContext => {
        resolvePackage(packageId)(traceContext)
          .thereafter {
            case Failure(ex) =>
              logger.error(s"Package resolution failed for [$packageId]", ex)
            case _ => ()
          }
      },
  )

  /** Enrich transaction values by re-hydrating record labels and identifiers
    */
  val enrichTransaction: TransactionEnricher = { transaction => implicit traceContext =>
    EitherT.liftF(interactiveSubmissionEnricher.enrichVersionedTransaction(transaction))
  }

  /** Enrich create node values by re-hydrating record labels and identifiers
    */
  val enrichContract: ContractEnricher = { case (createNode, targetPackageIds) =>
    implicit traceContext =>
      interactiveSubmissionEnricher
        .enrichContract(createNode, targetPackageIds)
        .leftFlatMap(err =>
          EitherT.leftT[FutureUnlessShutdown, FatContractInstance](
            EnrichmentError(err): ReinterpretationError
          )
        )
  }

  override def reinterpret(
      contracts: ContractAndKeyLookup,
      contractAuthenticator: ContractAuthenticatorFn,
      submitters: Set[LfPartyId],
      command: LfCommand,
      ledgerTime: CantonTimestamp,
      preparationTime: CantonTimestamp,
      rootSeed: Option[LfHash],
      packageResolution: Map[PackageName, PackageId],
      expectFailure: Boolean,
      getEngineAbortStatus: GetEngineAbortStatus,
  )(implicit traceContext: TraceContext): EitherT[
    FutureUnlessShutdown,
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
        preparationTime = preparationTime.toLf,
        ledgerEffectiveTime = ledgerTime.toLf,
        packageResolution = packageResolution,
        engineLogger =
          engineLoggingConfig.toEngineLogger(loggerFactory.append("phase", "validation")),
        contractIdVersion = ContractIdVersion.V1,
      )
    }

    for {
      txWithMetadata <- EitherT(
        handleResult(contracts, contractAuthenticator, result, getEngineAbortStatus)
      )
      (tx, metadata) = txWithMetadata
      peeledTxE = peelAwayRootLevelRollbackNode(tx).leftMap(EngineError.apply)
      txNoRootRollback <- EitherT.fromEither[FutureUnlessShutdown](
        peeledTxE: Either[ReinterpretationError, LfVersionedTransaction]
      )
    } yield ReInterpretationResult(
      txNoRootRollback,
      TransactionMetadata.fromLf(ledgerTime, metadata),
      metadata.globalKeyMapping,
      metadata.usedPackages,
      LedgerTimeBoundaries(metadata.timeBoundaries),
    )
  }

  def replayCreate(
      submitters: Set[LfPartyId],
      command: LfCreateCommand,
      getEngineAbortStatus: GetEngineAbortStatus,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReinterpretationError, LfNodeCreate] =
    LoggingContextUtil.createLoggingContext(loggerFactory) { implicit loggingContext =>
      val result = engine.reinterpret(
        submitters = submitters,
        command = command,
        nodeSeed = Some(DAMLe.zeroSeed),
        preparationTime = Time.Timestamp.Epoch, // Only used to compute contract ids
        // Only used in Updates, but a create command does not go through Updates.
        ledgerEffectiveTime = Time.Timestamp.Epoch,
        packageResolution = Map.empty,
        contractIdVersion = ContractIdVersion.V1,
      )
      for {
        txWithMetadata <- EitherT(
          handleResult(
            ContractAndKeyLookup.noContracts(loggerFactory),
            (_, _) => Left("Unexpected contract authenticator when replaying a create command"),
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
        create <- EitherT.pure[FutureUnlessShutdown, ReinterpretationError](singleCreate)
      } yield create
    }

  private[this] def handleResult[A](
      contracts: ContractAndKeyLookup,
      contractAuthenticator: ContractAuthenticatorFn,
      result: Result[A],
      getEngineAbortStatus: GetEngineAbortStatus,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[ReinterpretationError, A]] = {
    def handleResultInternal(contracts: ContractAndKeyLookup, result: Result[A])(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Either[ReinterpretationError, A]] = {
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
          resolvePackage(packageId)(traceContext).transformWithHandledAborted {
            case Success(pkg) =>
              handleResultInternal(contracts, resume(pkg))
            case Failure(ex) =>
              logger.error(s"Package resolution failed for [$packageId]", ex)
              FutureUnlessShutdown.failed(ex)
          }
        case ResultDone(completedResult) => FutureUnlessShutdown.pure(Right(completedResult))
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
          (CantonContractIdVersion.extractCantonContractIdVersion(acoid) match {
            case Right(version) =>
              contracts.lookupFatContract(acoid).value.map[Response] {
                case Some(contract) =>
                  Response.ContractFound(
                    contract,
                    version.contractHashingMethod,
                    hash => contractAuthenticator(contract, hash).isRight,
                  )
                case None => Response.ContractNotFound
              }
            case Left(_) =>
              FutureUnlessShutdown.pure[Response](Response.UnsupportedContractIdVersion)
          }).flatMap(response =>
            handleResultInternal(
              contracts,
              resume(response),
            )
          )

        case ResultError(err) => FutureUnlessShutdown.pure(Left(EngineError(err)))
        case ResultInterruption(continue, _) =>
          // Run the interruption loop asynchronously to avoid blocking the calling thread.
          // Using a `Future` as a trampoline also makes the recursive call to `handleResult` stack safe.
          FutureUnlessShutdown.pure(iterateOverInterrupts(continue)).flatMap {
            case Left(abort) => FutureUnlessShutdown.pure(Left(abort))
            case Right(result) => handleResultInternal(contracts, result)
          }
        case ResultPrefetch(_, _, resume) =>
          // we do not need to prefetch here as Canton includes the keys as a static map in Phase 3
          handleResultInternal(contracts, resume())
      }
    }

    handleResultInternal(contracts, result)
  }

}
