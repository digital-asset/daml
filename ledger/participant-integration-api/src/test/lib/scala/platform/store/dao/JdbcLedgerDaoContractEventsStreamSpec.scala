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

  it should "return the expected contracts event stream for the specified offset range" in {
    for {
      before <- ledgerDao.lookupLedgerEndAndEventSequentialId()
      (offset1, t1) <- store(singleCreate)
      (offset2, t2) <- store(singleCreate)
      (offset3, _) <- store(singleExercise(nonTransient(t2).loneElement))
      (offset4, t4) <- store(fullyTransient)
      (offset5, t5) <- store(singleCreate)
      (offset6, t6) <- store(singleCreate)
      after <- ledgerDao.lookupLedgerEndAndEventSequentialId()
      contractStateEvents <- contractEventsOf(
        ledgerDao.transactionsReader.getContractStateEvents(
          startExclusive = before,
          endInclusive = after,
        )
      )
    } yield {
      println(contractStateEvents)
      val first = contractStateEvents.head
      val firstEventSeqId = first.eventSequentialId
      val contract = first.asInstanceOf[Created].contract
      contractStateEvents should contain theSameElementsInOrderAs Seq(
        Created(
          nonTransient(t1).loneElement,
          contract,
          None,
          Set(alice, bob),
          offset1,
          firstEventSeqId,
        ),
        Created(
          nonTransient(t2).loneElement,
          contract,
          None,
          Set(alice, bob),
          offset2,
          firstEventSeqId + 2, // TDT do something more revealing about the event seq id assertions
        ),
        Archived(
          nonTransient(t2).loneElement,
          contract,
          None,
          Set(alice, bob),
          firstEventSeqId + 2,
          offset3,
          firstEventSeqId + 4,
        ),
        Created(
          transient(t4).loneElement,
          contract,
          None,
          Set(alice, bob),
          offset4,
          firstEventSeqId + 7,
        ),
        Archived(
          transient(t4).loneElement,
          contract,
          None,
          Set(alice, bob),
          firstEventSeqId + 7,
          offset4,
          firstEventSeqId + 8,
        ),
        Created(
          nonTransient(t5).loneElement,
          contract,
          None,
          Set(alice, bob),
          offset5,
          firstEventSeqId + 11,
        ),
        Created(
          nonTransient(t6).loneElement,
          contract,
          None,
          Set(alice, bob),
          offset6,
          firstEventSeqId + 13,
        ),
        LedgerEndMarker(offset6, firstEventSeqId + 13),
      )
    }
  }

  private def contractEventsOf(
      source: Source[((Offset, Long), ContractStateEventsReader.ContractStateEvent), NotUsed]
  ): Future[immutable.Seq[ContractStateEventsReader.ContractStateEvent]] =
    source
      .runWith(Sink.seq)
      .map(_.map(_._2))
}
