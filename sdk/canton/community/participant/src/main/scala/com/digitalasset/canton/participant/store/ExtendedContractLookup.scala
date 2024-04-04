// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.{EitherT, OptionT}
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.participant.protocol.SerializableContractAuthenticator
import com.digitalasset.canton.protocol.{ContractMetadata, LfContractId, LfGlobalKey}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

/** A contract lookup that adds a fixed set of contracts to a `backingContractLookup`.
  *
  * @param backingContractLookup The [[ContractLookup]] to default to if no overwrite is given in `additionalContracts`
  * @param additionalContracts   Contracts in this map take precedence over contracts in `backingContractLookup`
  * @throws java.lang.IllegalArgumentException if `additionalContracts` stores a contract under a wrong id
  */
class ExtendedContractLookup(
    private val backingContractLookup: ContractLookup,
    private val additionalContracts: Map[LfContractId, StoredContract],
    private val keys: Map[LfGlobalKey, Option[LfContractId]],
    private val authenticator: SerializableContractAuthenticator,
)(protected implicit val ec: ExecutionContext)
    extends ContractLookupAndVerification {

  additionalContracts.foreach { case (id, storedContract) =>
    require(
      storedContract.contractId == id,
      s"Tried to store contract $storedContract under the wrong id $id",
    )
  }

  override def authenticateForUpgradeValidation(coid: LfContractId, metadata: ContractMetadata)(
      implicit traceContext: TraceContext
  ): OptionT[Future, String] =
    lookup(coid).transform {
      case Some(storedContract: StoredContract) =>
        val contractWithRecomputedMetadata = storedContract.contract.copy(metadata = metadata)
        authenticator.authenticateUpgradableContract(contractWithRecomputedMetadata).left.toOption
      case None =>
        Some(s"Failed to find contract $coid")
    }

  override def lookup(
      id: LfContractId
  )(implicit traceContext: TraceContext): OptionT[Future, StoredContract] =
    additionalContracts.get(id) match {
      case None => backingContractLookup.lookup(id)
      case Some(inFlightContract) => OptionT.some(inFlightContract)
    }

  override def lookupKey(key: LfGlobalKey)(implicit
      traceContext: TraceContext
  ): OptionT[Future, Option[LfContractId]] =
    OptionT.fromOption[Future](keys.get(key))

  // This lookup is fairly inefficient in the additional stakeholders, but this function is currently not really
  // used anywhere
  override def lookupStakeholders(ids: Set[LfContractId])(implicit
      traceContext: TraceContext
  ): EitherT[Future, UnknownContracts, Map[LfContractId, Set[LfPartyId]]] = {
    val (inAdditional, notInAdditional) = ids.partition(cid => additionalContracts.contains(cid))
    for {
      m <- backingContractLookup.lookupStakeholders(notInAdditional)
    } yield {
      m ++ additionalContracts.filter { case (cid, _) => inAdditional.contains(cid) }.map {
        case (cid, c) => (cid, c.contract.metadata.stakeholders)
      }
    }

  }

}
