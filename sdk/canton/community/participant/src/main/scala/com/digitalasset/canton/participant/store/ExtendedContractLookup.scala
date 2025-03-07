// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.{EitherT, OptionT}
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.ContractAuthenticator
import com.digitalasset.canton.protocol.{
  ContractMetadata,
  LfContractId,
  LfGlobalKey,
  SerializableContract,
}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

/** A contract lookup that adds some convenience methods
  *
  * @param contracts
  *   Contracts in this map take precedence over contracts in `backingContractLookup`
  * @throws java.lang.IllegalArgumentException
  *   if `contracts` stores a contract under a wrong id
  */
class ExtendedContractLookup(
    private val contracts: Map[LfContractId, SerializableContract],
    private val keys: Map[LfGlobalKey, Option[LfContractId]],
    private val authenticator: ContractAuthenticator,
)(protected implicit val ec: ExecutionContext)
    extends ContractLookupAndVerification {

  contracts.foreach { case (id, contract) =>
    require(
      contract.contractId == id,
      s"Tried to store contract $contract under the wrong id $id",
    )
  }

  override def verifyMetadata(coid: LfContractId, metadata: ContractMetadata)(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, String] =
    lookup(coid).transform {
      case Some(contract) =>
        authenticator.verifyMetadata(contract, metadata).left.toOption
      case None =>
        Some(s"Failed to find contract $coid")
    }

  override def lookup(
      id: LfContractId
  )(implicit traceContext: TraceContext): OptionT[FutureUnlessShutdown, SerializableContract] =
    OptionT.fromOption[FutureUnlessShutdown](contracts.get(id))

  override def lookupKey(key: LfGlobalKey)(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, Option[LfContractId]] =
    OptionT.fromOption[FutureUnlessShutdown](keys.get(key))

  // This lookup is fairly inefficient in the additional stakeholders, but this function is currently not really
  // used anywhere
  override def lookupStakeholders(ids: Set[LfContractId])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, UnknownContracts, Map[LfContractId, Set[LfPartyId]]] =
    lookupMetadata(ids).map(_.view.mapValues(_.stakeholders).toMap)

  // This lookup is fairly inefficient in the additional signatories, but this function is currently not really
  // used anywhere
  override def lookupSignatories(ids: Set[LfContractId])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, UnknownContracts, Map[LfContractId, Set[LfPartyId]]] =
    lookupMetadata(ids).map(_.view.mapValues(_.signatories).toMap)

  private def lookupMetadata(
      ids: Set[LfContractId]
  ): EitherT[FutureUnlessShutdown, UnknownContracts, Map[LfContractId, ContractMetadata]] = {
    val (unknown, known) = ids.partitionMap(id => contracts.get(id).toRight(id))

    if (unknown.isEmpty)
      EitherT.pure(known.map(c => c.contractId -> c.metadata).toMap)
    else
      EitherT.leftT(UnknownContracts(unknown))
  }
}
