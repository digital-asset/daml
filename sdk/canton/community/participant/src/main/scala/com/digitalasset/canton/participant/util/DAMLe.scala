// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.util

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.lf.VersionRange
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.data.{ImmArray, Ref, Time}
import com.daml.lf.engine.*
import com.daml.lf.interpretation.Error as LfInterpretationError
import com.daml.lf.language.Ast.Package
import com.daml.lf.language.{LanguageMajorVersion, LanguageVersion}
import com.daml.lf.transaction.{ContractKeyUniquenessMode, TransactionVersion, Versioned}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{LoggingContextUtil, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.PackageService
import com.digitalasset.canton.participant.protocol.EngineController.GetEngineAbortStatus
import com.digitalasset.canton.participant.store.ContractLookupAndVerification
import com.digitalasset.canton.participant.util.DAMLe.{ContractWithMetadata, PackageResolver}
import com.digitalasset.canton.platform.apiserver.configuration.EngineLoggingConfig
import com.digitalasset.canton.platform.apiserver.execution.AuthorityResolver
import com.digitalasset.canton.protocol.SerializableContract.LedgerCreateTime
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.{LfCommand, LfCreateCommand, LfKeyResolver, LfPartyId, LfVersioned}

import java.nio.file.Path
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object DAMLe {
  def newEngine(
      enableLfDev: Boolean,
      enableStackTraces: Boolean,
      profileDir: Option[Path] = None,
      iterationsBetweenInterruptions: Long =
        10000, // 10000 is the default value in the engine configuration
  ): Engine =
    new Engine(
      EngineConfig(
        allowedLanguageVersions =
          if (enableLfDev)
            LanguageVersion.AllVersions(LanguageMajorVersion.V2)
          else
            VersionRange(
              LanguageVersion.v2_1,
              LanguageVersion.StableVersions(LanguageMajorVersion.V2).max,
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

  /** Resolves packages by [[com.daml.lf.data.Ref.PackageId]].
    * The returned packages must have been validated
    * so that [[com.daml.lf.engine.Engine]] can skip validation.
    */
  type PackageResolver = PackageId => TraceContext => Future[Option[Package]]

  sealed trait ReinterpretationError extends PrettyPrinting

  final case class EngineError(cause: Error) extends ReinterpretationError {
    override def pretty: Pretty[EngineError] = adHocPrettyInstance
  }

  final case class EngineAborted(reason: String) extends ReinterpretationError {
    override def pretty: Pretty[EngineAborted] = prettyOfClass(
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
    authorityResolver: AuthorityResolver,
    domainId: Option[DomainId],
    engine: Engine,
    engineLoggingConfig: EngineLoggingConfig,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {
  import DAMLe.{ReinterpretationError, EngineError, EngineAborted}

  logger.debug(engine.info.show)(TraceContext.empty)

  def reinterpret(
      contracts: ContractLookupAndVerification,
      submitters: Set[LfPartyId],
      command: LfCommand,
      ledgerTime: CantonTimestamp,
      submissionTime: CantonTimestamp,
      rootSeed: Option[LfHash],
      expectFailure: Boolean,
      packageResolution: Map[Ref.PackageName, Ref.PackageId] = Map.empty,
      getEngineAbortStatus: GetEngineAbortStatus,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    Future,
    ReinterpretationError,
    (LfVersionedTransaction, TransactionMetadata, LfKeyResolver),
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
              detailMessage = None,
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
      peeledTxE = peelAwayRootLevelRollbackNode(tx).leftMap(EngineError)
      txNoRootRollback <- EitherT.fromEither[Future](
        peeledTxE: Either[ReinterpretationError, LfVersionedTransaction]
      )
    } yield (
      txNoRootRollback,
      TransactionMetadata.fromLf(ledgerTime, metadata),
      metadata.globalKeyMapping,
    )
  }

  def replayCreate(
      submitters: Set[LfPartyId],
      command: LfCreateCommand,
      ledgerEffectiveTime: LedgerCreateTime,
      getEngineAbortStatus: GetEngineAbortStatus,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, ReinterpretationError, LfNodeCreate] = {
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
      (transaction, _, _) = transactionWithMetadata
      md = transaction.nodes(transaction.roots(0)) match {
        case nc: LfNodeCreate =>
          ContractWithMetadata(
            LfContractInst(
              template = nc.templateId,
              arg = Versioned(nc.version, nc.arg),
              packageName = nc.packageName,
              packageVersion = None,
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
      ): Either[EngineAborted, Result[A]] = {
        continue() match {
          case ResultInterruption(continue) =>
            getEngineAbortStatus().reasonO match {
              case Some(reason) =>
                logger.warn(s"Aborting engine computation, reason = $reason")
                Left(EngineAborted(reason))
              case None => iterateOverInterrupts(continue)
            }

          case otherResult => Right(otherResult)
        }
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
        case ResultInterruption(continue) =>
          // Run the interruption loop asynchronously to avoid blocking the calling thread
          Future(iterateOverInterrupts(continue)).flatMap {
            case Left(abort) => Future.successful(Left(abort))
            case Right(result) => handleResultInternal(contracts, result)
          }
        case ResultNeedAuthority(holding, requesting, resume) =>
          authorityResolver
            .resolve(
              AuthorityResolver
                .AuthorityRequest(holding, requesting, domainId)
            )
            .flatMap {
              case AuthorityResolver.AuthorityResponse.Authorized =>
                handleResultInternal(contracts, resume(true))
              case AuthorityResolver.AuthorityResponse.MissingAuthorisation(parties) =>
                val receivedAuthorityFor = parties -- requesting
                logger.debug(
                  show"Authorisation failed. Missing authority: [$parties]. Received authority: [$receivedAuthorityFor]"
                )
                handleResultInternal(contracts, resume(false))
            }

        case ResultNeedUpgradeVerification(coid, signatories, observers, keyOpt, resume) =>
          val unusedTxVersion = TransactionVersion.StableVersions.max
          val metadata = ContractMetadata.tryCreate(
            signatories = signatories,
            stakeholders = signatories ++ observers,
            maybeKeyWithMaintainersVersioned = keyOpt.map(k => Versioned(unusedTxVersion, k)),
          )
          contracts.verifyMetadata(coid, metadata).value.flatMap { verification =>
            handleResultInternal(contracts, resume(verification))
          }
      }
    }

    handleResultInternal(contracts, result)
  }
}
