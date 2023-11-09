// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import akka.Done
import com.daml.error.NoLogging
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction.Transaction
import com.daml.ledger.api.v1.transaction_filter.TransactionFilter
import com.daml.ledger.javaapi.data.Party
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.ParticipantPrunedDataAccessed
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.participant.ledger.api.client.{LedgerConnection, LedgerSubscription}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.mockito.captor.ArgCaptor
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Promise
import scala.util.Success

class ResilientTransactionsSubscriptionTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext {

  "restartable subscription" should {
    "complete successfully" when {
      "ledger connection subscription completes successfully" in new TestContext {
        runTest { sut =>
          s1_promise.complete(Success(Done))
          sut.close()
          succeed
        }(expectedWarningMessages = Seq.empty)
      }
    }

    "complete with the last error" when {
      "closed after a failed subscription" in new TestContext {
        runTest { sut =>
          val exception = new RuntimeException("some error")
          s1_promise.failure(exception)
          sut.close()
          sut.subscriptionF.failed.futureValue shouldBe exception
        }(
          expectedWarningMessages = Seq(
            s"wait-for-$serviceName-$subscriptionName-completed finished with an error",
            s"Ledger subscription $subscriptionName for $serviceName failed with an error",
          )
        )
      }
    }

    "resubscribe from the latest observed offset" when {
      "the current subscription fails" in new TestContext {
        runTest { sut =>
          s1_promise.tryFailure(new RuntimeException("some failure"))
          s2_promise.success(Done)

          // Wait for restart
          sut.subscriptionF.futureValue shouldBe ()
          verify(connection).subscribe(any[String], eqTo(initialOffset), transactionFilterMatcher)(
            any[Transaction => Unit]
          )
          verify(connection).subscribe(
            any[String],
            eqTo(reSubscriptionOffset),
            transactionFilterMatcher,
          )(any[Transaction => Unit])

          sut.close()
          succeed
        }(
          expectedWarningMessages = Seq(
            s"Ledger subscription $subscriptionName for $serviceName failed with an error"
          )
        )
      }
    }

    "resubscribe from the latest unpruned offset" when {
      s"the current subscription fails with a ${ParticipantPrunedDataAccessed.id} error" in new TestContext {
        private val nextOffsetAfterPruned = LedgerOffset(LedgerOffset.Value.Absolute("17"))
        when(
          connection.subscribe(
            subscriptionName = eqTo("SubscriptionForTestService"),
            offset = eqTo(nextOffsetAfterPruned),
            filter = transactionFilterMatcher,
          )(any[Transaction => Unit])
        ).thenReturn(s2)

        runTest { sut =>
          s1_promise.tryFailure(
            ParticipantPrunedDataAccessed.Reject("some cause", "17")(NoLogging).asGrpcError
          )
          s2_promise.complete(Success(Done))

          // Wait for restart
          sut.subscriptionF.futureValue shouldBe ()
          verify(connection).subscribe(any[String], eqTo(initialOffset), transactionFilterMatcher)(
            any[Transaction => Unit]
          )
          verify(connection).subscribe(
            any[String],
            eqTo(nextOffsetAfterPruned),
            transactionFilterMatcher,
          )(any[Transaction => Unit])

          sut.close()
          succeed
        }(
          expectedWarningMessages = Seq(
            s"Setting the ledger subscription SubscriptionForTestService for TestServiceForResilientTransactionSubscription offset to a later offset [17] due to pruning. Some commands might timeout or events might become stale.",
            s"Ledger subscription $subscriptionName for $serviceName failed with an error",
          )
        )
      }
    }
  }

  private[admin] trait TestContext {
    val serviceName = "TestServiceForResilientTransactionSubscription"
    val subscriptionName = "SubscriptionForTestService"
    val connection = mock[LedgerConnection]
    val sender = new Party("alice")
    val argCaptor = ArgCaptor[Transaction => Unit]
    val initialOffset: LedgerOffset = LedgerOffset(LedgerOffset.Value.Absolute("00"))
    val reSubscriptionOffset = LedgerOffset(LedgerOffset.Value.Absolute("07"))
    val tx = Transaction(offset = "07")
    val s1, s2 = mock[LedgerSubscription]

    // If used with specific argument matching, this throws a NPE when run with Java 17
    // protected def transactionFilterMatcher: TransactionFilter = eqTo(LedgerConnection.transactionFilter(sender))

    // Since the test and implementation doesn't interact with the transactionFilter,
    // avoid the exception by using any[TransactionFilter] as below
    protected def transactionFilterMatcher: TransactionFilter = any[TransactionFilter]

    when(connection.sender).thenReturn(sender)
    when(
      connection.subscribe(
        subscriptionName = eqTo("SubscriptionForTestService"),
        offset = eqTo(initialOffset),
        filter = transactionFilterMatcher,
      )(argCaptor.capture)
    ).thenReturn(s1)
    when(
      connection.subscribe(
        subscriptionName = eqTo("SubscriptionForTestService"),
        offset = eqTo(reSubscriptionOffset),
        filter = transactionFilterMatcher,
      )(any[Transaction => Unit])
    ).thenReturn(s2)

    val s1_promise, s2_promise = Promise[Done]()

    when(s1.completed).thenAnswer {
      argCaptor.value(tx)
      s1_promise.future
    }
    when(s2.completed).thenReturn(s2_promise.future)

    val processTransaction = mock[(Transaction, TraceContext) => Unit]
    when(processTransaction.apply(eqTo(tx), anyTraceContext)).thenReturn(())

    def runTest(test: ResilientTransactionsSubscription => Assertion)(
        expectedWarningMessages: Seq[String]
    ): Assertion =
      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        {
          val sut = new ResilientTransactionsSubscription(
            connection = connection,
            serviceName = serviceName,
            initialOffset = initialOffset,
            subscriptionName = subscriptionName,
            timeouts = timeouts,
            loggerFactory = loggerFactory,
          )(processTransaction)
          val result = test(sut)
          sut.close()
          result
        },
        LogEntry.assertLogSeq(
          expectedWarningMessages.map(expected => (_.warningMessage shouldBe expected, expected))
        ),
      )
  }
}
