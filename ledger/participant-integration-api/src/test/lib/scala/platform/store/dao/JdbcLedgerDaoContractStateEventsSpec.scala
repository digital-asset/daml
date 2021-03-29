// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import akka.NotUsed
import akka.stream.scaladsl.{Sink, Source}
import com.daml.ledger.participant.state.v1.Offset
import com.daml.platform.store.dao.events.ContractStateEventsReader
import com.daml.platform.store.dao.events.ContractStateEventsReader.ContractStateEvent.{
  Archived,
  Created,
  LedgerEndMarker,
}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inside, LoneElement, OptionValues}

import scala.collection.immutable
import scala.concurrent.Future

trait JdbcLedgerDaoContractEventsStreamSpec extends LoneElement with Inside with OptionValues {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  behavior of "JdbcLedgerDao (contract stream events)"

  it should "return the expected contracts event stream for the specified offset range" in {
    for {
      beforeOffset <- ledgerDao.lookupLedgerEnd()
      beforeSequentialId <- ledgerDao.lookupLedgerEndSequentialId()
      (offset1, t1) <- store(singleCreate)
      (offset2, t2) <- store(singleCreate)
      (offset3, _) <- store(singleExercise(nonTransient(t2).loneElement))
      (offset4, t4) <- store(fullyTransient)
      (offset5, t5) <- store(singleCreate)
      (offset6, t6) <- store(singleCreate)
      afterOffset <- ledgerDao.lookupLedgerEnd()
      afterSequentialId <- ledgerDao.lookupLedgerEndSequentialId()
      contractStateEvents <- contractEventsOf(
        ledgerDao.transactionsReader.getContractStateEvents(
          startExclusive = (beforeOffset, beforeSequentialId),
          endInclusive = (afterOffset, afterSequentialId),
        )
      )
    } yield {
      val first = contractStateEvents.head
      val sequentialIdState = new SequentialIdState(first.eventSequentialId)
      val contract = first.asInstanceOf[Created].contract

      contractStateEvents should contain theSameElementsInOrderAs Seq(
        Created(
          nonTransient(t1).loneElement,
          contract,
          None,
          Set(alice, bob),
          offset1,
          sequentialIdState.currentId,
        ),
        Created(
          nonTransient(t2).loneElement,
          contract,
          None,
          Set(alice, bob),
          offset2,
          sequentialIdState.nextId(),
        ),
        Archived(
          nonTransient(t2).loneElement,
          Set(alice, bob),
          offset3,
          sequentialIdState.nextId(),
        ),
        Created(
          createdContractId(t4).loneElement,
          contract,
          None,
          Set(alice, bob),
          offset4,
          sequentialIdState.nextId(),
        ),
        Archived(
          createdContractId(t4).loneElement,
          Set(alice, bob),
          offset4,
          sequentialIdState.nextId(),
        ),
        Created(
          nonTransient(t5).loneElement,
          contract,
          None,
          Set(alice, bob),
          offset5,
          sequentialIdState.nextId(),
        ),
        Created(
          nonTransient(t6).loneElement,
          contract,
          None,
          Set(alice, bob),
          offset6,
          sequentialIdState.nextId(),
        ),
        LedgerEndMarker(offset6, sequentialIdState.currentId),
      )
    }
  }

  // TODO: Temporary helper - change it when the append-only schema gets in.
  private class SequentialIdState(initial: Long) {
    private var id: Long = initial

    def currentId: Long = id

    def nextId(): Long = {
      id += 1
      id
    }
  }

  private def contractEventsOf(
      source: Source[((Offset, Long), ContractStateEventsReader.ContractStateEvent), NotUsed]
  ): Future[immutable.Seq[ContractStateEventsReader.ContractStateEvent]] =
    source
      .runWith(Sink.seq)
      .map(_.map(_._2))
}
