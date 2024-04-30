// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client

import com.daml.error.NoLogging
import com.daml.ledger.api.v2.participant_offset.ParticipantOffset
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.javaapi.data.Party
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.client.ResilientLedgerSubscriptionTest.SubscriptionState
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.ParticipantPrunedDataAccessed
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import io.grpc.{Status, StatusRuntimeException}
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Flow, Source}
import org.reactivestreams.{Publisher, Subscriber, Subscription}
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.AtomicReference

class ResilientLedgerSubscriptionTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  private implicit lazy val actorSystem: ActorSystem = ActorSystem(
    "ResilientLedgerSubscriptionTest"
  )
  private implicit lazy val materializer: Materializer = Materializer(actorSystem)

  "restartable subscription" should {
    "receive txs successfully" when {
      "items are dispatched" in new TestContext {
        runTest { sut =>
          val sub = getSubscriberWhenReady()
          sub.subscriber.onNext(tx)
          eventually() {
            received.get() shouldBe Seq(tx)
          }
          sut.close()
          succeed
        }(expectedWarningMessages = Seq.empty)
      }
    }

    "complete with the last error" when {
      "closed after a failed subscription" in new TestContext {
        runTest { sut =>
          val sub = getSubscriberWhenReady()
          val exception = new StatusRuntimeException(Status.UNAVAILABLE)
          sub.subscriber.onError(exception)
          sut.close()
          sut.subscriptionF.failed.futureValue shouldBe exception
        }(
          expectedWarningMessages = Seq(
            s"Ledger subscription ${subscriptionName} failed with an error",
            s"wait-for-${subscriptionName}-completed finished with an error",
          )
        )
      }
    }

    "resubscribe from the latest observed offset" when {
      "the current subscription fails" in new TestContext {
        runTest { sut =>
          val sub = getSubscriberWhenReady()
          sub.subscriber.onNext(tx)
          val exception = new StatusRuntimeException(Status.UNAVAILABLE)
          sub.subscriber.onError(exception)
          eventually() {
            val next = getSubscriberWhenReady()
            next.offset shouldBe reSubscriptionOffset
          }
          sut.close()
          succeed
        }(
          expectedWarningMessages = Seq(
            s"Ledger subscription ${subscriptionName} failed with an error"
          )
        )
      }
    }

    "resubscribe from the latest unpruned offset" when {
      s"the current subscription fails with a ${ParticipantPrunedDataAccessed.id} error" in new TestContext {

        private val nextOffsetAfterPruned =
          ParticipantOffset(ParticipantOffset.Value.Absolute("17"))
        runTest { sut =>
          val sub = getSubscriberWhenReady()
          val reject =
            ParticipantPrunedDataAccessed.Reject("some cause", "17")(NoLogging).asGrpcError
          sub.subscriber.onError(reject)

          eventually() {
            val next = getSubscriberWhenReady()
            next.offset shouldBe nextOffsetAfterPruned
          }

          sut.close()
          succeed
        }(
          expectedWarningMessages = Seq(
            s"due to pruning. Some commands might timeout or events might become stale.",
            s"Ledger subscription $subscriptionName failed with an error",
          )
        )
      }
    }
  }

  private[client] trait TestContext {
    val serviceName = "TestServiceForResilientTransactionSubscription"
    val subscriptionName = "SubscriptionForTestService"
    val sender = new Party("alice")

    val initialOffset: ParticipantOffset = ParticipantOffset(ParticipantOffset.Value.Absolute("00"))
    val reSubscriptionOffset = ParticipantOffset(ParticipantOffset.Value.Absolute("07"))
    val tx = Transaction(offset = "07")

    private[client] val subscriber = new AtomicReference[Option[SubscriptionState]](
      None
    )

    def makeSource(offset: ParticipantOffset): Source[Transaction, NotUsed] = {
      Source.fromPublisher[Transaction](new Publisher[Transaction] {
        override def subscribe(s: Subscriber[_ >: Transaction]): Unit = {
          subscriber.updateAndGet { cur =>
            Some(
              SubscriptionState(
                index = cur.map(x => x.index + 1).getOrElse(1),
                offset,
                s,
                request = -1,
                cancel = false,
              )
            )
          }

          s.onSubscribe(new Subscription {
            override def request(n: Long): Unit =
              subscriber.updateAndGet(_.map(_.copy(request = n)))
            override def cancel(): Unit = subscriber.updateAndGet(_.map(_.copy(cancel = true)))
          })
        }
      })
    }
    val received = new AtomicReference[Seq[Transaction]](Seq.empty)

    def getSubscriberWhenReady(): SubscriptionState = {
      eventually() {
        val sub = subscriber.get().valueOrFail("subscriber not set")
        assert(sub.request > 0)
        sub
      }
    }

    def runTest(test: ResilientLedgerSubscription[Transaction, Unit] => Assertion)(
        expectedWarningMessages: Seq[String]
    ): Assertion =
      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        {
          val sut = new ResilientLedgerSubscription[Transaction, Unit](
            makeSource = makeSource,
            consumingFlow = Flow[Transaction].map { tx =>
              received.updateAndGet(_ :+ tx).discard
            },
            subscriptionName = subscriptionName,
            startOffset = initialOffset,
            extractOffset =
              tx => Some(ParticipantOffset(ParticipantOffset.Value.Absolute(tx.offset))),
            timeouts = timeouts,
            loggerFactory = loggerFactory,
            resubscribeIfPruned = true,
          )
          val result = test(sut)
          sut.close()
          result
        },
        LogEntry.assertLogSeq(
          expectedWarningMessages.map(expected =>
            (_.warningMessage should include(expected), expected)
          )
        ),
      )
  }
}

object ResilientLedgerSubscriptionTest {
  private[client] final case class SubscriptionState(
      index: Int,
      offset: ParticipantOffset,
      subscriber: Subscriber[_ >: Transaction],
      request: Long,
      cancel: Boolean,
  )
}
