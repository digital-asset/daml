// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.Id
import cats.data.{EitherT, OptionT}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MapsUtil
import com.digitalasset.canton.{LfPartyId, RequestCounter}

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

/** An in-memory contract store. This class is thread-safe. */
class InMemoryContractStore(protected val loggerFactory: NamedLoggerFactory)(
    protected implicit val ec: ExecutionContext
) extends ContractStore
    with NamedLogging {

  override protected[store] def logger: TracedLogger = super.logger

  /** Invariants:
    * <ol>
    *   <li>Every [[StoredContract]] is stored under [[StoredContract.contract.contractId]].</li>
    * </ol>
    */
  private[this] val contracts = TrieMap.empty[LfContractId, StoredContract]

  /** Debug find utility to search pcs
    */
  override def find(
      filterId: Option[String],
      filterPackage: Option[String],
      filterTemplate: Option[String],
      limit: Int,
  )(implicit traceContext: TraceContext): Future[List[SerializableContract]] = {
    def search(needle: String, accessor: StoredContract => String): StoredContract => Boolean =
      needle match {
        case rs if rs.startsWith("!") => accessor(_) == needle.drop(1)
        case rs if rs.startsWith("^") => accessor(_).startsWith(needle.drop(1))
        case _ => accessor(_).contains(needle)
      }
    val flt1 =
      filterPackage.map(search(_, _.contract.contractInstance.unversioned.template.packageId))
    val flt2 = filterTemplate.map(
      search(_, _.contract.contractInstance.unversioned.template.qualifiedName.qualifiedName)
    )
    val flt3 = filterId.map(search(_, _.contractId.coid))

    def conjunctiveFilter(sc: StoredContract): Boolean = {
      flt1.forall(_(sc)) && flt2.forall(_(sc)) && flt3.forall(_(sc))
    }
    Future.successful(contracts.values.filter(conjunctiveFilter).take(limit).map(_.contract).toList)
  }

  override def lookup(
      id: LfContractId
  )(implicit traceContext: TraceContext): OptionT[Future, StoredContract] = {
    logger.debug(s"Looking up contract: $id")
    OptionT(Future.successful {
      val result = contracts.get(id)
      result.fold(logger.debug(s"Contract $id not found"))(contract =>
        logger.debug(
          s"Found contract $id of type ${contract.contract.contractInstance.unversioned.template.qualifiedName.qualifiedName}"
        )
      )
      result
    })
  }

  override def storeCreatedContracts(
      requestCounter: RequestCounter,
      creations: Seq[WithTransactionId[SerializableContract]],
  )(implicit traceContext: TraceContext): Future[Unit] = {
    creations.foreach { case WithTransactionId(creation, transactionId) =>
      store(StoredContract.fromCreatedContract(creation, requestCounter, transactionId))
    }
    Future.unit
  }

  private def store(storedContract: StoredContract): Unit = {
    val _ = MapsUtil.modifyWithConcurrentlyM[Id, LfContractId, StoredContract](
      contracts,
      storedContract.contractId,
      Some(storedContract),
      existing => Some(existing.mergeWith(storedContract)),
    )
  }

  override def storeDivulgedContracts(
      requestCounter: RequestCounter,
      divulgences: Seq[SerializableContract],
  )(implicit traceContext: TraceContext): Future[Unit] = {
    divulgences.foreach { divulgence =>
      store(StoredContract.fromDivulgedContract(divulgence, requestCounter))
    }
    Future.unit
  }

  override def deleteContract(
      id: LfContractId
  )(implicit traceContext: TraceContext): EitherT[Future, UnknownContract, Unit] =
    EitherT.fromEither {
      contracts.remove(id).toRight(UnknownContract(id)).map(_ => ())
    }

  override def deleteIgnoringUnknown(
      ids: Iterable[LfContractId]
  )(implicit traceContext: TraceContext): Future[Unit] = {
    ids.foreach(id => contracts.remove(id).discard[Option[StoredContract]])
    Future.unit
  }

  override def deleteDivulged(
      upTo: RequestCounter
  )(implicit traceContext: TraceContext): Future[Unit] = {
    contracts.filterInPlace { case (_, contract) =>
      contract.creatingTransactionIdO.isDefined || contract.requestCounter > upTo
    }
    Future.unit
  }

  override def lookupStakeholders(ids: Set[LfContractId])(implicit
      traceContext: TraceContext
  ): EitherT[Future, UnknownContracts, Map[LfContractId, Set[LfPartyId]]] = {
    val res = contracts.filter { case (cid, _) => ids.contains(cid) }.map { case (cid, c) =>
      (cid, c.contract.metadata.stakeholders)
    }
    EitherT.cond(res.sizeCompare(ids) == 0, res.toMap, UnknownContracts(ids -- res.keySet))
  }

  override def contractCount()(implicit traceContext: TraceContext): Future[Int] =
    Future.successful(contracts.size)
}
