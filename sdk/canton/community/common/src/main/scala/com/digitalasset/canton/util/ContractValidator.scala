// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.data.EitherT
import cats.implicits.toBifunctorOps
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.crypto.{HashOps, HmacOps}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.PackageConsumer.PackageResolver
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.engine.Engine
import com.digitalasset.daml.lf.transaction.{CreationTime, FatContractInstance, Versioned}

import scala.concurrent.ExecutionContext

trait ContractValidator {

  /** Perform full contract authentication against the target package:
    *   - type check the contract against the target package
    *   - authenticate that the contract id is consistent with the contract contents
    *   - verify the ensures clause of the target package template is satisfied
    *   - verify that the contract metadata calculated by the target template matches that in the
    *     contract
    */
  def authenticate(
      contract: FatContractInstance,
      targetPackageId: Ref.PackageId,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, Unit]

  /** Authenticate the contract hash by recomputing the contract id suffix and checking it the one
    * provided in the contract
    */
  def authenticateHash(contract: FatContractInstance, contractHash: LfHash): Either[String, Unit]

}

object ContractValidator {

  def apply(
      cryptoOps: HashOps & HmacOps,
      engine: Engine,
      packageResolver: PackageResolver,
  ): ContractValidator =
    new ContractValidatorImpl(
      new UnicumGenerator(cryptoOps),
      LfContractValidation(engine, packageResolver),
    )

  // TODO(#23971) add support for V2 contract ids
  private class ContractValidatorImpl(
      unicumGenerator: UnicumGenerator,
      lfContractValidation: LfContractValidation,
  ) extends ContractValidator {

    def authenticate(contract: FatContractInstance, targetPackageId: LfPackageId)(implicit
        ec: ExecutionContext,
        traceContext: TraceContext,
    ): EitherT[FutureUnlessShutdown, String, Unit] =
      for {
        contractIdVersion <- EitherT.fromEither[FutureUnlessShutdown](
          CantonContractIdVersion.extractCantonContractIdV1Version(contract.contractId)
        )
        result <- lfContractValidation.validate(
          contract,
          targetPackageId,
          contractIdVersion.contractHashingMethod,
          hash => authenticateHashInternal(contract, hash, contractIdVersion).isRight,
        )
      } yield result

    override def authenticateHash(
        contract: FatContractInstance,
        contractHash: LfHash,
    ): Either[String, Unit] =
      for {
        contractIdVersion <- CantonContractIdVersion.extractCantonContractIdV1Version(
          contract.contractId
        )
        _ <- authenticateHashInternal(contract, contractHash, contractIdVersion)
      } yield ()

    private def authenticateHashInternal(
        contract: FatContractInstance,
        contractHash: LfHash,
        contractIdVersion: CantonContractIdV1Version,
    ): Either[String, Unit] = {
      val gk = contract.contractKeyWithMaintainers.map(Versioned(contract.version, _))
      for {
        metadata <- ContractMetadata.create(contract.signatories, contract.stakeholders, gk)
        authenticationData <- ContractAuthenticationData
          .fromLfBytes(contractIdVersion, contract.authenticationData)
          .leftMap(_.toString)
        cantonContractSuffix <- contract.contractId match {
          case cid: LfContractId.V1 => Right(cid.suffix)
          case _ => Left("ContractId V2 are not supported")
        }
        createdAt <- contract.createdAt match {
          case t: CreationTime.CreatedAt => Right(t)
          case _ => Left("Cannot authenticate contract with creation time Now")
        }
        recomputedUnicum <- unicumGenerator.recomputeUnicum(
          authenticationData.salt,
          createdAt,
          metadata,
          contractHash,
        )
        recomputedSuffix = recomputedUnicum.toContractIdSuffix(contractIdVersion)
        _ <- Either.cond(
          recomputedSuffix == cantonContractSuffix,
          (),
          s"Mismatching contract id suffixes. Expected: $recomputedSuffix vs actual: $cantonContractSuffix",
        )
      } yield ()
    }
  }
}
