// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.repair

import cats.Eval
import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.digitalasset.canton.crypto.{HashOps, HmacOps, Salt}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.data.ActiveContract
import com.digitalasset.canton.protocol.{
  CantonContractIdVersion,
  LfContractId,
  SerializableContract,
  SerializableRawContractInstance,
  UnicumGenerator,
}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.daml.lf.crypto.Hash

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

sealed abstract class EnsureValidContractIds(
    protocolVersionGetter: Traced[SynchronizerId] => Option[ProtocolVersion]
) extends NamedLogging {
  def apply(contracts: Seq[ActiveContract])(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[Future, String, (Seq[ActiveContract], Map[LfContractId, LfContractId])]

  /*
    In the context of a migration combining ACS import and synchronizer change (such as the one we perform
    as part a major upgrade for early mainnet), the `contract.protocolVersion` and the protocol
    version of the synchronizer will be different. Hence, we need to query it using the getter.
   */
  protected def getExpectedContractIdVersion(
      contract: ActiveContract
  )(implicit tc: TraceContext): Either[String, CantonContractIdVersion] =
    protocolVersionGetter(Traced(contract.synchronizerId))
      .toRight(
        s"Protocol version for synchronizer with ID ${contract.synchronizerId} cannot be resolved"
      )
      .flatMap(CantonContractIdVersion.fromProtocolVersion)
}

object EnsureValidContractIds {

  /** Verify that all contract IDs have a version greater or equal to the contract ID version associated
    * with the protocol version of the synchronizer to which the contract is assigned.
    * If any contract ID fails, the whole process fails.
    */
  private final class VerifyContractIdSuffixes(
      protocolVersionGetter: Traced[SynchronizerId] => Option[ProtocolVersion],
      override val loggerFactory: NamedLoggerFactory,
  ) extends EnsureValidContractIds(protocolVersionGetter) {

    private def verifyContractIdSuffix(
        contract: ActiveContract
    )(implicit tc: TraceContext): Either[String, ActiveContract] =
      for {
        contractIdVersion <- getExpectedContractIdVersion(contract)
        _ <- CantonContractIdVersion
          .ensureCantonContractId(contract.contract.contractId)
          .leftMap(_.toString)
          .ensureOr(actualVersion =>
            s"Contract ID ${contract.contract.contractId} has version ${actualVersion.v} but synchronizer ${contract.synchronizerId.toProtoPrimitive} requires ${contractIdVersion.v}"
          )(_ >= contractIdVersion)
      } yield contract

    override def apply(contracts: Seq[ActiveContract])(implicit
        ec: ExecutionContext,
        tc: TraceContext,
    ): EitherT[Future, String, (Seq[ActiveContract], Map[LfContractId, LfContractId])] =
      EitherT
        .fromEither[Future](contracts.traverse(verifyContractIdSuffix))
        .map((_, Map.empty))
  }

  private final case class DiscriminatorWithContractId(
      discriminator: Hash,
      contractId: LfContractId,
  )

  /** Recompute the contract IDs of all contracts using the provided cryptoOps.
    * The whole preprocessing will fail if any of the following conditions apply to any contract:
    * - the contract ID discriminator version is unknown
    * - the contract salt is missing
    * - any contract ID referenced in a payload is missing from the import
    * - any contract is referenced by two different IDs (e.g. the ID in the payload is fine but the one in the contract is not)
    */
  private final class RecomputeContractIdSuffixes(
      protocolVersionGetter: Traced[SynchronizerId] => Option[ProtocolVersion],
      cryptoOps: HashOps with HmacOps,
      override val loggerFactory: NamedLoggerFactory,
  ) extends EnsureValidContractIds(protocolVersionGetter) {

    private val unicumGenerator = new UnicumGenerator(cryptoOps)

    private val fullRemapping =
      TrieMap.empty[LfContractId, Eval[EitherT[Future, String, ActiveContract]]]

    private def getDiscriminator(c: SerializableContract): Either[String, Hash] =
      c.contractId match {
        case LfContractId.V1(discriminator, _) =>
          Right(discriminator)
        case _ =>
          Left(s"Unknown LF contract ID version, cannot recompute contract ID ${c.contractId.coid}")
      }

    private def getSalt(c: SerializableContract): Either[String, Salt] =
      c.contractSalt.toRight(s"Missing salt, cannot recompute contract ID ${c.contractId.coid}")

    // Recompute the contract ID of a single contract. Any dependency is taken from the `fullRemapping`,
    // which is pre-populated with a lazy reference to the contract ID recomputed here. The evaluation
    // of the `Eval` as part of resolving the (recomputed) contract ID for dependencies will cause the
    // immediate retrieval of the dependency, possibly triggering recomputation, limiting throughput in
    // the presence of dependencies but preventing deadlocks while being stack-safe (`Eval` employs
    // trampolining). If a contract ID is reached for which there is no instance, the recomputation
    // cannot be performed. This is normal, as the dependency might have been archived and pruned. Still,
    // we issue a warning out of caution.
    private def recomputeContractIdSuffix(
        activeContract: ActiveContract,
        contractIdVersion: CantonContractIdVersion,
    )(implicit tc: TraceContext, ec: ExecutionContext): EitherT[Future, String, ActiveContract] = {
      val contract = activeContract.contract

      for {
        discriminator <- EitherT.fromEither[Future](getDiscriminator(contract))
        salt <- EitherT.fromEither[Future](getSalt(contract))
        depsRemapping <- contract.contractInstance.unversioned.cids.toSeq
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
        newRawContractInstance <- EitherT
          .fromEither[Future](
            SerializableRawContractInstance.create(
              contract.contractInstance
                .copy(unversioned = contract.contractInstance.unversioned.mapCid(depsRemapping))
            )
          )
          .leftMap(_.errorMessage)
        unicum <- EitherT {
          Future.successful {
            unicumGenerator
              .recomputeUnicum(
                salt,
                contract.ledgerCreateTime,
                contract.metadata,
                newRawContractInstance,
              )
          }
        }
      } yield {
        val newContractId = contractIdVersion.fromDiscriminator(discriminator, unicum)
        activeContract.withSerializableContract(contract =
          contract.copy(
            contractId = newContractId,
            rawContractInstance = newRawContractInstance,
          )
        )
      }
    }

    // If the contract ID is already valid return the contract as is, eagerly and synchronously.
    // If the contract ID is not valid it will recompute it, lazily and asynchronously.
    private def recomputeBrokenContractIdSuffix(contract: ActiveContract)(implicit
        ec: ExecutionContext,
        tc: TraceContext,
    ): Eval[EitherT[Future, String, ActiveContract]] =
      getExpectedContractIdVersion(contract).fold(
        error => Eval.now(EitherT.leftT[Future, ActiveContract](error)),
        contractIdVersion => {
          val contractId = contract.contract.contractId
          val valid = CantonContractIdVersion
            .ensureCantonContractId(contractId)
            .exists(_ >= contractIdVersion)
          if (valid) {
            logger.debug(s"Contract ID '${contractId.coid}' is already valid")
            Eval.now(EitherT.rightT[Future, String](contract))
          } else {
            logger.debug(s"Contract ID '${contractId.coid}' needs to be recomputed")
            Eval.later(recomputeContractIdSuffix(contract, contractIdVersion))
          }
        },
      )

    private def ensureDiscriminatorUniqueness(
        contracts: Seq[ActiveContract]
    ): Either[String, Unit] = {
      val allContractIds = contracts.map(_.contract.contractId)
      val allDependencies = contracts.flatMap(_.contract.contractInstance.unversioned.cids)
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

    private def recomputeBrokenContractIdSuffixes(contracts: Seq[ActiveContract])(implicit
        ec: ExecutionContext,
        tc: TraceContext,
    ): EitherT[Future, String, (Seq[ActiveContract], Map[LfContractId, LfContractId])] = {
      // Associate every contract ID with a lazy deferred computation that will recompute the contract ID if necessary
      // It's lazy so that every single contract ID is associated with a computation, before the first one finishes.
      // The assumptions are that every contract ID references in any payload has an associated `ActiveContract` in
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
        completedRemapping <- fullRemapping.view.valuesIterator.toSeq.parTraverse(_.value)
        contractIdRemapping <- fullRemapping.toSeq.parTraverseFilter { case (cid, v) =>
          v.value.map(c => Option.when(cid != c.contract.contractId)(cid -> c.contract.contractId))
        }
      } yield completedRemapping -> contractIdRemapping.toMap
    }

    override def apply(contracts: Seq[ActiveContract])(implicit
        ec: ExecutionContext,
        tc: TraceContext,
    ): EitherT[Future, String, (Seq[ActiveContract], Map[LfContractId, LfContractId])] =
      for {
        _ <- EitherT.fromEither[Future](ensureDiscriminatorUniqueness(contracts))
        completedRemapping <- recomputeBrokenContractIdSuffixes(contracts)
      } yield completedRemapping
  }

  /** Creates an object that ensures that all contract IDs comply with the scheme associated to the synchronizer where the contracts are assigned.
    * @param cryptoOps If defined, the contract IDs will be recomputed using the provided cryptoOps. Else, the contract IDs will only be verified.
    */
  def apply(
      loggerFactory: NamedLoggerFactory,
      protocolVersionGetter: Traced[SynchronizerId] => Option[ProtocolVersion],
      cryptoOps: Option[HashOps with HmacOps],
  ): EnsureValidContractIds =
    cryptoOps.fold[EnsureValidContractIds](
      new VerifyContractIdSuffixes(protocolVersionGetter, loggerFactory)
    )(
      new RecomputeContractIdSuffixes(protocolVersionGetter, _, loggerFactory)
    )

}
