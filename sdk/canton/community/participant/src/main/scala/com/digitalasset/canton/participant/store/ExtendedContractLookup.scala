// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.{EitherT, OptionT}
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.participant.protocol.SerializableContractAuthenticator
import com.digitalasset.canton.protocol.{ContractMetadata, LfContractId, LfGlobalKey}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

/** A contract lookup that adds some convenience methods
  *
  * @param contracts Contracts in this map take precedence over contracts in `backingContractLookup`
  * @throws java.lang.IllegalArgumentException if `contracts` stores a contract under a wrong id
  */
class ExtendedContractLookup(
    private val contracts: Map[LfContractId, StoredContract],
    private val keys: Map[LfGlobalKey, Option[LfContractId]],
    private val authenticator: SerializableContractAuthenticator,
)(protected implicit val ec: ExecutionContext)
    extends ContractLookupAndVerification {

  contracts.foreach { case (id, storedContract) =>
    require(
      storedContract.contractId == id,
      s"Tried to store contract $storedContract under the wrong id $id",
    )
  }

  override def verifyMetadata(coid: LfContractId, metadata: ContractMetadata)(implicit
      traceContext: TraceContext
  ): OptionT[Future, String] =
    lookup(coid).transform {
      case Some(storedContract: StoredContract) =>
        authenticator.verifyMetadata(storedContract.contract, metadata).left.toOption
      case None =>
        Some(s"Failed to find contract $coid")
    }

  override def lookup(
      id: LfContractId
  )(implicit traceContext: TraceContext): OptionT[Future, StoredContract] =
    OptionT.fromOption[Future](contracts.get(id))

  override def lookupKey(key: LfGlobalKey)(implicit
      traceContext: TraceContext
  ): OptionT[Future, Option[LfContractId]] =
    OptionT.fromOption[Future](keys.get(key))

  // This lookup is fairly inefficient in the additional stakeholders, but this function is currently not really
  // used anywhere
  override def lookupStakeholders(ids: Set[LfContractId])(implicit
      traceContext: TraceContext
  ): EitherT[Future, UnknownContracts, Map[LfContractId, Set[LfPartyId]]] = {
    val (unknown, known) = ids.partitionMap(id => contracts.get(id).toRight(id))

    if (unknown.isEmpty)
      EitherT.pure(known.map(c => c.contractId -> c.contract.metadata.stakeholders).toMap)
    else
      EitherT.leftT(UnknownContracts(unknown))
  }
}
