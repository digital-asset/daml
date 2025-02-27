// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.{EitherT, OptionT}
import cats.syntax.parallel.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

trait ContractLookup {

  protected implicit def ec: ExecutionContext

  def lookup(id: LfContractId)(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, SerializableContract]

  def lookupManyExistingUncached(
      ids: Seq[LfContractId]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, LfContractId, List[SerializableContract]] =
    ids.toList.parTraverse(id => lookup(id).toRight(id))

  def lookupManyUncached(
      ids: Seq[LfContractId]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[List[Option[SerializableContract]]] =
    ids.toList.parTraverse(id => lookup(id).value)

  def lookupE(id: LfContractId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, UnknownContract, SerializableContract] =
    lookup(id).toRight(UnknownContract(id))

  /** Yields `None` (embedded in a Future) if the contract instance has not been stored or the id
    * cannot be parsed.
    *
    * Discards the serialization.
    */
  def lookupLfInstance(lfId: LfContractId)(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, LfContractInst] =
    lookup(lfId).map(_.contractInstance)

  def lookupContract(id: LfContractId)(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, SerializableContract] =
    lookup(id)

  def lookupContractE(id: LfContractId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, UnknownContract, SerializableContract] =
    lookupE(id)

  def lookupStakeholders(ids: Set[LfContractId])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, UnknownContracts, Map[LfContractId, Set[LfPartyId]]]
}

trait ContractAndKeyLookup extends ContractLookup {

  /** Find a contract with the given key. Typically used for Daml interpretation in Phase 3, where
    * the key resolution is provided by the submitter. Returns [[scala.None$]] if the key is not
    * supposed to be resolved, e.g., during reinterpretation by Daml Engine. Returns
    * [[scala.Some$]]`(`[[scala.None$]]`)` if no contract with the given key can be found.
    */
  def lookupKey(key: LfGlobalKey)(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, Option[LfContractId]]
}

trait ContractLookupAndVerification extends ContractAndKeyLookup {

  /** Verify that the contract metadata associated with the contract id is consistent with the
    * provided metadata
    */
  def verifyMetadata(coid: LfContractId, metadata: ContractMetadata)(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, String]

}

object ContractLookupAndVerification {

  /** An empty contract and key lookup interface that fails to find any contracts and keys when
    * asked, but allows any key to be asked
    */
  def noContracts(namedLoggerFactory: NamedLoggerFactory): ContractLookupAndVerification =
    new ContractLookupAndVerification with NamedLogging {

      val loggerFactory: NamedLoggerFactory = namedLoggerFactory
      implicit val ec: ExecutionContext = DirectExecutionContext(noTracingLogger)

      override def lookup(id: LfContractId)(implicit
          traceContext: TraceContext
      ): OptionT[FutureUnlessShutdown, SerializableContract] =
        OptionT.none[FutureUnlessShutdown, SerializableContract]

      override def lookupManyExistingUncached(ids: Seq[LfContractId])(implicit
          traceContext: TraceContext
      ): EitherT[FutureUnlessShutdown, LfContractId, List[SerializableContract]] =
        EitherT.rightT(Nil)

      override def lookupKey(key: LfGlobalKey)(implicit
          traceContext: TraceContext
      ): OptionT[FutureUnlessShutdown, Option[LfContractId]] =
        OptionT.pure[FutureUnlessShutdown](None)

      override def lookupStakeholders(ids: Set[LfContractId])(implicit
          traceContext: TraceContext
      ): EitherT[FutureUnlessShutdown, UnknownContracts, Map[LfContractId, Set[LfPartyId]]] =
        EitherT.cond(ids.isEmpty, Map.empty, UnknownContracts(ids))

      override def verifyMetadata(coid: LfContractId, metadata: ContractMetadata)(implicit
          traceContext: TraceContext
      ): OptionT[FutureUnlessShutdown, String] =
        OptionT.pure[FutureUnlessShutdown]("Not expecting call to verifyMetadata")

    }

}
