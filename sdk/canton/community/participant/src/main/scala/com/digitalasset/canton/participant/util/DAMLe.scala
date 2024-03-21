// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.util

import cats.data.EitherT
import com.daml.lf.VersionRange
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.data.{ImmArray, Ref, Time}
import com.daml.lf.engine.*
import com.daml.lf.interpretation.Error as LfInterpretationError
import com.daml.lf.language.Ast.Package
import com.daml.lf.language.LanguageVersion
import com.daml.lf.transaction.{ContractKeyUniquenessMode, TransactionVersion, Versioned}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{LoggingContextUtil, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.PackageService
import com.digitalasset.canton.participant.store.ContractLookupAndVerification
import com.digitalasset.canton.participant.util.DAMLe.{ContractWithMetadata, PackageResolver}
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
      uniqueContractKeys: Boolean,
      enableLfDev: Boolean,
      enableStackTraces: Boolean,
      profileDir: Option[Path] = None,
      iterationsBetweenInterruptions: Long =
        10000, // 10000 is the default value in the engine configuration
  ): Engine =
    new Engine(
      EngineConfig(
        allowedLanguageVersions = VersionRange(
          LanguageVersion.v1_14,
          if (enableLfDev) LanguageVersion.v1_dev else LanguageVersion.StableVersions.max,
        ),
        // The package store contains only validated packages, so we can skip validation upon loading
        packageValidation = false,
        stackTraceMode = enableStackTraces,
        profileDir = profileDir,
        requireSuffixedGlobalContractId = true,
        contractKeyUniqueness =
          if (uniqueContractKeys) ContractKeyUniquenessMode.Strict
          else ContractKeyUniquenessMode.Off,
        iterationsBetweenInterruptions = iterationsBetweenInterruptions,
      )
    )

  /** Resolves packages by [[com.daml.lf.data.Ref.PackageId]].
    * The returned packages must have been validated
    * so that [[com.daml.lf.engine.Engine]] can skip validation.
    */
  type PackageResolver = PackageId => TraceContext => Future[Option[Package]]

  final case class ContractWithMetadata(
      instance: LfContractInst,
      signatories: Set[LfPartyId],
      stakeholders: Set[LfPartyId],
      templateId: LfTemplateId,
      keyWithMaintainers: Option[LfGlobalKeyWithMaintainers],
      agreementText: AgreementText,
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
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

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
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    Future,
    Error,
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
      )
    }

    for {
      txWithMetadata <- EitherT(handleResult(contracts, result))
      (tx, metadata) = txWithMetadata
      txNoRootRollback <- EitherT.fromEither[Future](peelAwayRootLevelRollbackNode(tx))
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
  )(implicit traceContext: TraceContext): EitherT[Future, Error, LfNodeCreate] = {
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
          handleResult(ContractLookupAndVerification.noContracts(loggerFactory), result)
        )
        (tx, _) = txWithMetadata
        singleCreate = tx.nodes.values.toList match {
          case (create: LfNodeCreate) :: Nil => create
          case _ =>
            throw new RuntimeException(
              s"DAMLe failed to replay a create $command submitted by $submitters"
            )
        }
        create <- EitherT.pure[Future, Error](singleCreate)
      } yield create
    }
  }

  def contractWithMetadata(contractInstance: LfContractInst, supersetOfSignatories: Set[LfPartyId])(
      implicit traceContext: TraceContext
  ): EitherT[Future, Error, ContractWithMetadata] = {

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
      )
      (transaction, _metadata, _resolver) = transactionWithMetadata
      md = transaction.nodes(transaction.roots(0)) match {
        case nc @ LfNodeCreate(
              _cid,
              packageName,
              templateId,
              arg,
              agreementText,
              signatories,
              stakeholders,
              key,
              version,
            ) =>
          ContractWithMetadata(
            LfContractInst(
              template = templateId,
              packageName = packageName,
              arg = Versioned(version, arg),
            ),
            signatories,
            stakeholders,
            nc.templateId,
            key,
            AgreementText(agreementText),
          )
        case node => throw new RuntimeException(s"DAMLe reinterpreted a create node as $node")
      }
    } yield md
  }

  def contractMetadata(contractInstance: LfContractInst, supersetOfSignatories: Set[LfPartyId])(
      implicit traceContext: TraceContext
  ): EitherT[Future, Error, ContractMetadata] =
    for {
      contractAndMetadata <- contractWithMetadata(contractInstance, supersetOfSignatories)
    } yield contractAndMetadata.metadataWithGlobalKey

  private[this] def handleResult[A](contracts: ContractLookupAndVerification, result: Result[A])(
      implicit traceContext: TraceContext
  ): Future[Either[Error, A]] = {
    @tailrec
    def iterateOverInterrupts(continue: () => Result[A]): Result[A] =
      continue() match {
        case ResultInterruption(continue) => iterateOverInterrupts(continue)
        case otherResult => otherResult
      }

    result match {
      case ResultNeedPackage(packageId, resume) =>
        resolvePackage(packageId)(traceContext).transformWith {
          case Success(pkg) =>
            handleResult(contracts, resume(pkg))
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
            Error.Interpretation(
              Error.Interpretation.DamlException(LfInterpretationError.ContractKeyNotFound(gk)),
              None,
            )
          )
          .flatMap(optCid => EitherT(handleResult(contracts, resume(optCid))))
          .value
      case ResultNeedContract(acoid, resume) =>
        contracts
          .lookupLfInstance(acoid)
          .value
          .flatMap(optInst => handleResult(contracts, resume(optInst)))
      case ResultError(err) => Future.successful(Left(err))
      case ResultInterruption(continue) =>
        handleResult(contracts, iterateOverInterrupts(continue))
      case ResultNeedAuthority(holding, requesting, resume) =>
        authorityResolver
          .resolve(
            AuthorityResolver
              .AuthorityRequest(holding, requesting, domainId)
          )
          .flatMap {
            case AuthorityResolver.AuthorityResponse.Authorized =>
              handleResult(contracts, resume(true))
            case AuthorityResolver.AuthorityResponse.MissingAuthorisation(parties) =>
              val receivedAuthorityFor = parties -- requesting
              logger.debug(
                show"Authorisation failed. Missing authority: [$parties]. Received authority: [$receivedAuthorityFor]"
              )
              handleResult(contracts, resume(false))
          }

      case ResultNeedUpgradeVerification(coid, signatories, observers, keyOpt, resume) =>
        val unusedTxVersion = TransactionVersion.StableVersions.max
        val metadata = ContractMetadata.tryCreate(
          signatories = signatories,
          stakeholders = signatories ++ observers,
          maybeKeyWithMaintainers = keyOpt.map(k => Versioned(unusedTxVersion, k)),
        )
        contracts.verifyMetadata(coid, metadata).value.flatMap { verification =>
          handleResult(contracts, resume(verification))
        }
    }
  }

}
