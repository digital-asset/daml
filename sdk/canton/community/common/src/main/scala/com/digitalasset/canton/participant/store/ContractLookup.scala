// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.digitalasset.daml.lf.transaction.{CreationTime, FatContractInstance}

import scala.concurrent.ExecutionContext

trait ContractLookup {

  type ContractsCreatedAtTime <: CreationTime

  protected implicit def ec: ExecutionContext

  def lookup(id: LfContractId)(implicit
      traceContext: TraceContext
  ): OptionT[
    FutureUnlessShutdown,
    GenContractInstance { type InstCreatedAtTime <: ContractsCreatedAtTime },
  ]

  def lookupFatContract(id: LfContractId)(implicit
      traceContext: TraceContext
  ): OptionT[
    FutureUnlessShutdown,
    FatContractInstance { type CreatedAtTime <: ContractsCreatedAtTime },
  ] = lookup(id).map(_.inst)

  def lookupManyExistingUncached(
      ids: Seq[LfContractId]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, LfContractId, List[
    GenContractInstance { type InstCreatedAtTime <: ContractsCreatedAtTime }
  ]] =
    ids.toList.parTraverse(id => lookup(id).toRight(id))

  def lookupManyUncached(
      ids: Seq[LfContractId]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[
    List[Option[GenContractInstance { type InstCreatedAtTime <: ContractsCreatedAtTime }]]
  ] = ids.toList.parTraverse(id => lookup(id).value)

  def lookupE(id: LfContractId)(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    UnknownContract,
    GenContractInstance { type InstCreatedAtTime <: ContractsCreatedAtTime },
  ] = lookup(id).toRight(UnknownContract(id))

  def lookupStakeholders(ids: Set[LfContractId])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, UnknownContracts, Map[LfContractId, Set[LfPartyId]]]

  def lookupSignatories(ids: Set[LfContractId])(implicit
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

object ContractAndKeyLookup {

  /** An empty contract and key lookup interface that fails to find any contracts and keys when
    * asked, but allows any key to be asked
    */
  def noContracts(namedLoggerFactory: NamedLoggerFactory): ContractAndKeyLookup =
    new ContractAndKeyLookup with NamedLogging {

      override type ContractsCreatedAtTime = CreationTime.CreatedAt

      override protected val loggerFactory: NamedLoggerFactory = namedLoggerFactory
      implicit val ec: ExecutionContext = DirectExecutionContext(noTracingLogger)

      override def lookup(id: LfContractId)(implicit
          traceContext: TraceContext
      ): OptionT[FutureUnlessShutdown, ContractInstance] =
        OptionT.none[FutureUnlessShutdown, ContractInstance]

      override def lookupManyExistingUncached(ids: Seq[LfContractId])(implicit
          traceContext: TraceContext
      ): EitherT[FutureUnlessShutdown, LfContractId, List[ContractInstance]] =
        EitherT.rightT(Nil)

      override def lookupKey(key: LfGlobalKey)(implicit
          traceContext: TraceContext
      ): OptionT[FutureUnlessShutdown, Option[LfContractId]] =
        OptionT.pure[FutureUnlessShutdown](None)

      override def lookupStakeholders(ids: Set[LfContractId])(implicit
          traceContext: TraceContext
      ): EitherT[FutureUnlessShutdown, UnknownContracts, Map[LfContractId, Set[LfPartyId]]] =
        EitherT.cond(ids.isEmpty, Map.empty, UnknownContracts(ids))

      override def lookupSignatories(ids: Set[LfContractId])(implicit
          traceContext: TraceContext
      ): EitherT[FutureUnlessShutdown, UnknownContracts, Map[LfContractId, Set[LfPartyId]]] =
        EitherT.cond(ids.isEmpty, Map.empty, UnknownContracts(ids))

    }
}
