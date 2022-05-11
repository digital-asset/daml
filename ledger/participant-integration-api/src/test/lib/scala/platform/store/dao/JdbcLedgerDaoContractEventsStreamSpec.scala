// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import java.util.concurrent.atomic.AtomicLong
import akka.NotUsed
import akka.stream.scaladsl.{Sink, Source}
import com.daml.ledger.offset.Offset
import com.daml.lf.data.ImmArray
import com.daml.lf.value.{Value => LfValue}
import com.daml.platform.{Contract, ContractId}
import com.daml.platform.store.dao.events.ContractStateEvent
import com.daml.platform.store.dao.events.ContractStateEvent.{Archived, Created, LedgerEndMarker}
import org.scalatest.LoneElement
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

/** This test can only be run successfully against the append-only schema on PostgreSQL.
  *
  * The asserted logic in this test, the [[LedgerDaoTransactionsReader.getContractStateEvents]] event stream,
  * uses the `event_kind` column for SQL events filtering, which is not available in the old mutating schema.
  */
trait JdbcLedgerDaoContractEventsStreamSpec extends LoneElement {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  behavior of "JdbcLedgerDao (contract stream events)"

  it should "return the expected contracts event stream for the specified offset range" in {
    val contractArg = (arg: String) =>
      LfValue.ValueRecord(
        Some(someTemplateId),
        ImmArray(
          recordFieldName("text") -> LfValue.ValueText(arg)
        ),
      )

    for {
      before <- ledgerDao.lookupLedgerEnd()
      (offset1, t1) <- store(
        singleCreate(cid => create(absCid = cid, contractArgument = contractArg("t1")))
      )

      (key2, globalKey2) = createTestKey(Set(alice, bob))
      (offset2, t2) <- createAndStoreContract(
        submittingParties = Set(alice),
        signatories = Set(alice, bob),
        stakeholders = Set(alice, bob),
        key = Some(key2),
        contractArgument = contractArg("t2"),
      )

      (offset3, _) <- store(singleExercise(nonTransient(t2).loneElement, Some(key2)))

      (offset4, t4) <- store(
        fullyTransient(cid => create(absCid = cid, contractArgument = contractArg("t4")))
      )

      (offset5, t5) <- store(
        singleCreate(cid => create(absCid = cid, contractArgument = contractArg("t5")))
      )

      (offset6, t6) <- store(
        singleCreate(cid => create(absCid = cid, contractArgument = contractArg("t6")))
      )

      after <- ledgerDao.lookupLedgerEnd()

      contractStateEvents <- contractEventsOf(
        ledgerDao.transactionsReader.getContractStateEvents(
          startExclusive = before.lastOffset -> before.lastEventSeqId,
          endInclusive = after.lastOffset -> after.lastEventSeqId,
        )
      )
    } yield {
      val first = contractStateEvents.head
      val sequentialIdState = new AtomicLong(first._2.head.eventSequentialId)

      contractStateEvents should contain theSameElementsInOrderAs Seq(
        offset1 -> Vector(
          Created(
            nonTransient(t1).loneElement,
            contract(created(t1).loneElement, contractArg("t1")),
            None,
            t1.ledgerEffectiveTime,
            Set(alice, bob),
            offset1,
            sequentialIdState.getAndIncrement(),
          )
        ),
        offset2 -> Vector(
          Created(
            nonTransient(t2).loneElement,
            contract(created(t2).loneElement, contractArg("t2")),
            Some(globalKey2),
            t2.ledgerEffectiveTime,
            Set(alice, bob),
            offset2,
            sequentialIdState.getAndIncrement(),
          )
        ),
        offset3 -> Vector(
          Archived(
            nonTransient(t2).loneElement,
            Some(globalKey2),
            Set(alice, bob),
            offset3,
            sequentialIdState.getAndIncrement(),
          )
        ),
        offset4 -> Vector(
          Created(
            created(t4).loneElement,
            contract(created(t4).loneElement, contractArg("t4")),
            None,
            t4.ledgerEffectiveTime,
            Set(alice, bob),
            offset4,
            sequentialIdState.getAndIncrement(),
          ),
          Archived(
            created(t4).loneElement,
            None,
            Set(alice, bob),
            offset4,
            sequentialIdState.getAndIncrement(),
          ),
        ),
        offset5 -> Vector(
          Created(
            nonTransient(t5).loneElement,
            contract(created(t5).loneElement, contractArg("t5")),
            None,
            t5.ledgerEffectiveTime,
            Set(alice, bob),
            offset5,
            sequentialIdState.getAndIncrement(),
          )
        ),
        offset6 -> Vector(
          Created(
            nonTransient(t6).loneElement,
            contract(created(t6).loneElement, contractArg("t6")),
            None,
            t6.ledgerEffectiveTime,
            Set(alice, bob),
            offset6,
            sequentialIdState.get(),
          )
        ),
        offset6 -> Vector(LedgerEndMarker(offset6, sequentialIdState.get())),
      )
    }
  }

  private def contractEventsOf(
      source: Source[((Offset, Long), Vector[ContractStateEvent]), NotUsed]
  ): Future[Seq[(Offset, Vector[ContractStateEvent])]] =
    source
      .runWith(Sink.seq)
      .map(_.map { case ((offset, _), events) => offset -> events })

  private def contract(cid: ContractId, contractArgument: LfValue): Contract =
    createNode(cid, Set.empty, Set.empty, contractArgument = contractArgument)
      .copy(agreementText = "")
      .versionedCoinst
}
