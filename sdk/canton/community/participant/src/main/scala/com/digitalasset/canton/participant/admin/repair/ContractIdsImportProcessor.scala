// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.repair

import cats.Eval
import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.digitalasset.canton.crypto.{HashOps, HmacOps}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.data.*
import com.digitalasset.canton.participant.sync.StaticSynchronizerParametersGetter
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.transaction.Versioned
import com.digitalasset.daml.lf.value.Value.ThinContractInstance

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

sealed abstract class ContractIdsImportProcessor(
    staticParametersGetter: StaticSynchronizerParametersGetter
) extends NamedLogging {
  def process(contracts: Seq[RepairContract])(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[Future, String, (Seq[RepairContract], Map[LfContractId, LfContractId])]

  /*
    In the context of a migration combining ACS import and synchronizer change (such as the one we perform
    as part a major upgrade for early mainnet), the `contract.protocolVersion` and the protocol
    version of the synchronizer will be different. Hence, we need to query it using the getter.
   */
  protected def getMaximumSupportedContractIdVersion(
      synchronizerId: SynchronizerId
  ): Either[String, CantonContractIdVersion] =
    staticParametersGetter
      .latestKnownProtocolVersion(synchronizerId)
      .toRight(
        s"Protocol version for synchronizer with ID $synchronizerId cannot be resolved"
      )
      .flatMap(CantonContractIdVersion.maximumSupportedVersion)
}

object ContractIdsImportProcessor {

  /** Verify that all contract IDs have a version greater or equal to the contract ID version
    * associated with the protocol version of the synchronizer to which the contract is assigned. If
    * any contract ID fails, the whole process fails.
    */
  private final class VerifyContractIdSuffixes(
      staticParametersGetter: StaticSynchronizerParametersGetter,
      override val loggerFactory: NamedLoggerFactory,
  ) extends ContractIdsImportProcessor(staticParametersGetter) {

    private def verifyContractIdSuffix(
        contract: RepairContract
    ): Either[String, RepairContract] =
      for {
        maxSynchronizerVersion <- getMaximumSupportedContractIdVersion(contract.synchronizerId)
        activeContractVersion <- CantonContractIdVersion
          .extractCantonContractIdVersion(contract.contract.contractId)
          .leftMap(_.toString)
        _ <- Either.cond(
          maxSynchronizerVersion >= activeContractVersion,
          (),
          s"Contract ID ${contract.contract.contractId} has version $activeContractVersion but synchronizer ${contract.synchronizerId.toProtoPrimitive} only supports up to $maxSynchronizerVersion",
        )
      } yield contract

    override def process(contracts: Seq[RepairContract])(implicit
        ec: ExecutionContext,
        tc: TraceContext,
    ): EitherT[Future, String, (Seq[RepairContract], Map[LfContractId, LfContractId])] =
      EitherT
        .fromEither[Future](contracts.traverse(verifyContractIdSuffix))
        .map((_, Map.empty))
  }

  private final case class DiscriminatorWithContractId(
      discriminator: Hash,
      contractId: LfContractId,
  )

  /** Recompute the contract IDs of all contracts using the provided cryptoOps. The whole
    * preprocessing will fail if any of the following conditions apply to any contract:
    *   - the contract ID discriminator version is unknown
    *   - any contract ID referenced in a payload is missing from the import
    *   - any contract is referenced by two different IDs (e.g. the ID in the payload is fine but
    *     the one in the contract is not)
    */
  private final class RecomputeContractIdSuffixes(
      staticParametersGetter: StaticSynchronizerParametersGetter,
      cryptoOps: HashOps & HmacOps,
      override val loggerFactory: NamedLoggerFactory,
  ) extends ContractIdsImportProcessor(staticParametersGetter) {

    private val unicumGenerator = new UnicumGenerator(cryptoOps)

    private val fullRemapping =
      TrieMap.empty[LfContractId, Eval[EitherT[Future, String, RepairContract]]]

    private def getDiscriminator(c: LfFatContractInst): Either[String, Hash] =
      c.contractId match {
        case LfContractId.V1(discriminator, _) =>
          Right(discriminator)
        case _ =>
          Left(s"Unknown LF contract ID version, cannot recompute contract ID ${c.contractId.coid}")
      }

    // Recompute the contract ID of a single contract. Any dependency is taken from the `fullRemapping`,
    // which is pre-populated with a lazy reference to the contract ID recomputed here. The evaluation
    // of the `Eval` as part of resolving the (recomputed) contract ID for dependencies will cause the
    // immediate retrieval of the dependency, possibly triggering recomputation, limiting throughput in
    // the presence of dependencies but preventing deadlocks while being stack-safe (`Eval` employs
    // trampolining). If a contract ID is reached for which there is no instance, the recomputation
    // cannot be performed. This is normal, as the dependency might have been archived and pruned. Still,
    // we issue a warning out of caution.
    private def recomputeContractIdSuffix(
        repairContract: RepairContract,
        contractIdVersion: CantonContractIdVersion,
    )(implicit tc: TraceContext, ec: ExecutionContext): EitherT[Future, String, RepairContract] = {
      val contract = repairContract.contract

      for {
        discriminator <- EitherT.fromEither[Future](getDiscriminator(contract))
        depsRemapping <- contract.createArg.cids.toSeq
          // parTraverse use is fine because computation is in-memory only
          .parTraverse { contractId =>
            fullRemapping
              .get(contractId)
              .fold {
                logger.warn(
                  s"Missing dependency with contract ID '${contractId.coid}'. The contract might have been archived. Its contract ID cannot be recomputed."
                )
                EitherT.rightT[Future, String](contractId -> contractId)
              }(_.value.map(contract => contractId -> contract.contract.contractId))
          }
          .map(_.toMap)
        newThinContractInstance = ThinContractInstance(
          contract.packageName,
          contract.templateId,
          contract.createArg.mapCid(depsRemapping),
        )
        contractIdV1Version <- EitherT.fromEither[Future](contractIdVersion match {
          case v1: CantonContractIdV1Version => Right(v1)
          case _ =>
            // TODO(#23971) implement this if possible
            Left(
              s"Contract ID version $contractIdVersion is not supported for recomputation, only V1 versions are supported"
            )
        })
        authenticationData <- EitherT.fromEither[Future](
          ContractAuthenticationData
            .fromLfBytes(contractIdV1Version, contract.authenticationData)
            .leftMap(err =>
              s"Could not parse contract authentication data for contract ID ${contract.contractId}: $err"
            )
        )
        metadata <- EitherT.fromEither[Future](
          ContractMetadata.create(
            signatories = contract.signatories,
            stakeholders = contract.stakeholders,
            maybeKeyWithMaintainersVersioned =
              contract.contractKeyWithMaintainers.map(Versioned(contract.version, _)),
          )
        )
        unicum <- EitherT.fromEither[Future] {
          unicumGenerator.recomputeUnicum(
            authenticationData.salt,
            contract.createdAt,
            metadata,
            newThinContractInstance,
            contractIdV1Version,
          )
        }
        newContractId = contractIdV1Version.fromDiscriminator(discriminator, unicum)
        newFatContractInstance = LfFatContractInst.fromCreateNode(
          contract.toCreateNode.copy(coid = newContractId, arg = newThinContractInstance.arg),
          contract.createdAt,
          contract.authenticationData,
        )
      } yield repairContract.withContractInstance(newFatContractInstance)
    }

    // If the contract ID is already valid return the contract as is, eagerly and synchronously.
    // If the contract ID is not valid it will recompute it, lazily and asynchronously.
    private def recomputeBrokenContractIdSuffix(contract: RepairContract)(implicit
        ec: ExecutionContext,
        tc: TraceContext,
    ): Eval[EitherT[Future, String, RepairContract]] =
      getMaximumSupportedContractIdVersion(contract.synchronizerId).fold(
        error => Eval.now(EitherT.leftT[Future, RepairContract](error)),
        maxContractIdVersion => {
          val contractId = contract.contract.contractId
          val valid = CantonContractIdVersion
            .extractCantonContractIdVersion(contractId)
            .exists(_ <= maxContractIdVersion)
          if (valid) {
            logger.debug(s"Contract ID '${contractId.coid}' is already valid")
            Eval.now(EitherT.rightT[Future, String](contract))
          } else {
            logger.debug(s"Contract ID '${contractId.coid}' needs to be recomputed")
            Eval.later(recomputeContractIdSuffix(contract, maxContractIdVersion))
          }
        },
      )

    private def ensureDiscriminatorUniqueness(
        contracts: Seq[RepairContract]
    ): Either[String, Unit] = {
      val allContractIds = contracts.map(_.contract.contractId)
      val allDependencies = contracts.flatMap(_.contract.createArg.cids)
      (allContractIds ++ allDependencies)
        .traverse {
          case contractId @ LfContractId.V1(discriminator, _) =>
            Right(DiscriminatorWithContractId(discriminator, contractId))
          case unknown =>
            Left(s"Unknown LF contract ID version, cannot recompute contract ID ${unknown.coid}")
        }
        .map(_.groupMapReduce(_.discriminator)(cid => Set(cid.contractId))(_ ++ _))
        .flatMap(
          _.collectFirst { case cid @ (_, contractIds) if contractIds.sizeIs > 1 => cid }
            .toLeft(())
            .leftMap { case (discriminator, contractIds) =>
              s"Duplicate discriminator '${discriminator.bytes.toHexString}' is used by ${contractIds.size} contract IDs, including (showing up to 10): ${contractIds.take(10).map(_.coid).mkString(", ")}..."
            }
        )
    }

    private def recomputeBrokenContractIdSuffixes(contracts: Seq[RepairContract])(implicit
        ec: ExecutionContext,
        tc: TraceContext,
    ): EitherT[Future, String, (Seq[RepairContract], Map[LfContractId, LfContractId])] = {
      // Associate every contract ID with a lazy deferred computation that will recompute the contract ID if necessary
      // It's lazy so that every single contract ID is associated with a computation, before the first one finishes.
      // The assumptions are that every contract ID references in any payload has an associated `RepairContract` in
      // the import, and that there are no cycles in the contract ID references.
      for (contract <- contracts) {
        fullRemapping
          .put(
            contract.contract.contractId,
            recomputeBrokenContractIdSuffix(contract),
          )
          .discard
      }
      for {
        // parTraverse use is fine because computation is in-memory only
        completedRemapping <- fullRemapping.view.valuesIterator.toSeq.parTraverse(_.value)
        contractIdRemapping <- fullRemapping.toSeq.parTraverseFilter { case (cid, v) =>
          v.value.map(c => Option.when(cid != c.contract.contractId)(cid -> c.contract.contractId))
        }
      } yield completedRemapping -> contractIdRemapping.toMap
    }

    override def process(contracts: Seq[RepairContract])(implicit
        ec: ExecutionContext,
        tc: TraceContext,
    ): EitherT[Future, String, (Seq[RepairContract], Map[LfContractId, LfContractId])] =
      for {
        _ <- EitherT.fromEither[Future](ensureDiscriminatorUniqueness(contracts))
        completedRemapping <- recomputeBrokenContractIdSuffixes(contracts)
      } yield completedRemapping
  }

  /** Ensures that all contract IDs comply with the scheme associated to the synchronizer where the
    * contracts are assigned.
    */
  def apply(
      loggerFactory: NamedLoggerFactory,
      staticParametersGetter: StaticSynchronizerParametersGetter,
      cryptoOps: HashOps & HmacOps,
      contractIdImportMode: ContractIdImportMode,
  )(contracts: Seq[RepairContract])(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[Future, String, (Seq[RepairContract], Map[LfContractId, LfContractId])] =
    contractIdImportMode match {
      // Accept contract IDs as they are.
      case ContractIdImportMode.Accept => EitherT.rightT((contracts, Map.empty))
      case ContractIdImportMode.Validation =>
        new VerifyContractIdSuffixes(staticParametersGetter, loggerFactory)
          .process(contracts)
      case ContractIdImportMode.Recomputation =>
        new RecomputeContractIdSuffixes(staticParametersGetter, cryptoOps, loggerFactory)
          .process(contracts)
    }

}
