// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.repair

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.logging.LoggingContext
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.data.*
import com.digitalasset.canton.participant.sync.SyncPersistentStateLookup
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ContractValidator, MonadUtil}

import scala.concurrent.ExecutionContext

sealed abstract class ContractAuthenticationImportProcessor(
    syncPersistentStateLookup: SyncPersistentStateLookup
) extends NamedLogging {
  protected implicit def executionContext: ExecutionContext

  def validate(contracts: Seq[RepairContract])(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit]

  /*
    In the context of a migration combining ACS import and synchronizer change (such as the one we perform
    as part a major upgrade for early mainnet), the `contract.protocolVersion` and the protocol
    version of the synchronizer will be different. Hence, we need to query it using the syncPersistentStateLookup.
   */
  protected def getMaximumSupportedContractIdVersion(
      synchronizerId: SynchronizerId
  ): Either[String, CantonContractIdVersion] =
    syncPersistentStateLookup
      .latestKnownProtocolVersion(synchronizerId)
      .toRight(
        s"Protocol version for synchronizer with ID $synchronizerId cannot be resolved"
      )
      .flatMap(CantonContractIdVersion.maximumSupportedVersion)
}

object ContractAuthenticationImportProcessor {

  /** Verify that all contract IDs have a version greater or equal to the contract ID version
    * associated with the protocol version of the synchronizer to which the contract is assigned.
    * Furthermore, perform full contract validation. If these checks fail for any contract, the
    * whole import is aborted.
    */
  private final class ValidateContracts(
      syncPersistentStateLookup: SyncPersistentStateLookup,
      contractValidator: ContractValidator,
      override val loggerFactory: NamedLoggerFactory,
  )(protected implicit val executionContext: ExecutionContext)
      extends ContractAuthenticationImportProcessor(syncPersistentStateLookup) {

    private def validateContract(
        contract: RepairContract
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, Unit] = {
      val validatedContractIdVersionE = for {
        maxSynchronizerVersion <- getMaximumSupportedContractIdVersion(contract.synchronizerId)
        activeContractVersion <- CantonContractIdVersion
          .extractCantonContractIdVersion(contract.contract.contractId)
        _ <- Either.cond(
          maxSynchronizerVersion >= activeContractVersion,
          (),
          s"Contract ID ${contract.contract.contractId} has version $activeContractVersion but synchronizer ${contract.synchronizerId.toProtoPrimitive} only supports up to $maxSynchronizerVersion",
        )
      } yield contract

      for {
        _ <- validatedContractIdVersionE.toEitherT[FutureUnlessShutdown]
        _ <- {
          implicit val loggingContext: LoggingContext = LoggingContext.empty
          contractValidator
            .authenticate(contract.contract, contract.representativePackageId)
            .leftMap { e =>
              s"Failed to authenticate contract with id: ${contract.contract.contractId}: $e"
            }
        }
      } yield ()
    }

    override def validate(contracts: Seq[RepairContract])(implicit
        tc: TraceContext
    ): EitherT[FutureUnlessShutdown, String, Unit] = MonadUtil
      .sequentialTraverse_(contracts)(validateContract)
  }

  /** Ensures that all contracts are validated and their contract IDs comply with the scheme
    * associated to the synchronizer where the contracts are assigned.
    */
  def validate(
      loggerFactory: NamedLoggerFactory,
      syncPersistentStateLookup: SyncPersistentStateLookup,
      contractValidator: ContractValidator,
      contractImportMode: ContractImportMode,
  )(contracts: Seq[RepairContract])(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    contractImportMode match {
      // Accept contracts as they are.
      case ContractImportMode.Accept => EitherT.rightT(())
      case ContractImportMode.Validation =>
        new ValidateContracts(syncPersistentStateLookup, contractValidator, loggerFactory)
          .validate(contracts)
    }

}
