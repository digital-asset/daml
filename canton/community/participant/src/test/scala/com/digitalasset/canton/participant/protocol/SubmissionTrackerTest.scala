// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.syntax.parallel.*
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.crypto.TestHash
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.store.memory.InMemorySubmissionTrackerStore
import com.digitalasset.canton.protocol.{RequestId, RootHash}
import com.digitalasset.canton.topology.{ParticipantId, UniqueIdentifier}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.FutureUtil
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, HasExecutionContext, ProtocolVersionChecksAnyWordSpec}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap
import scala.concurrent.Future
import scala.util.Random

class SubmissionTrackerTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with ProtocolVersionChecksAnyWordSpec
    with BeforeAndAfterEach {
  lazy val participantId = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("participant::participant")
  )
  lazy val otherParticipantId = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("participant::other-participant")
  )

  lazy val rootHash: RootHash = RootHash(TestHash.digest(1))
  lazy val requestId: RequestId = RequestId(CantonTimestamp.Epoch)
  lazy val requestIds: Seq[RequestId] =
    (1 to 1000).map(i => RequestId(CantonTimestamp.Epoch.plusSeconds(i.toLong)))

  lazy val submissionTrackerStore = new InMemorySubmissionTrackerStore(loggerFactory)
  lazy val submissionTracker: SubmissionTracker = SubmissionTracker(testedProtocolVersion)(
    participantId,
    submissionTrackerStore,
    futureSupervisor,
    timeouts,
    loggerFactory,
  )

  private def getSubmissionData(
      requestId: RequestId,
      participantId: ParticipantId = participantId,
      maxSeqTimeOffset: Long = 10,
  ): SubmissionTracker.SubmissionData =
    SubmissionTracker.SubmissionData(
      participantId,
      Some(requestId.unwrap.plusSeconds(maxSeqTimeOffset)),
    )
  override def beforeEach(): Unit = {
    submissionTrackerStore.clear()
  }

  override def afterEach(): Unit = {
    // Internal structures should be clean
    submissionTracker match {
      case st: SubmissionTrackerImpl =>
        st.internalSize shouldBe 0
      case _ => fail("Unexpected SubmissionTracker instance")
    }
  }

  "submission tracker" should {
    "successfully process a normal request" in {
      val resultFUS = submissionTracker.register(rootHash, requestId)
      submissionTracker.provideSubmissionData(
        rootHash,
        requestId,
        getSubmissionData(requestId),
      )

      resultFUS.failOnShutdown.futureValue shouldBe true
    }

    "fail a request with expired maxSequencingTime" onlyRunWithOrGreaterThan ProtocolVersion.v5 in {
      val resultFUS = submissionTracker.register(rootHash, requestId)
      submissionTracker.provideSubmissionData(
        rootHash,
        requestId,
        getSubmissionData(requestId, maxSeqTimeOffset = -1),
      )

      resultFUS.failOnShutdown.futureValue shouldBe false
    }

    "fail a request for a different participant" in {
      val resultFUS = submissionTracker.register(rootHash, requestId)
      submissionTracker.provideSubmissionData(
        rootHash,
        requestId,
        getSubmissionData(requestId, participantId = otherParticipantId),
      )

      resultFUS.failOnShutdown.futureValue shouldBe false
    }

    "fail replayed requests" onlyRunWithOrGreaterThan ProtocolVersion.v5 in {
      val resultFUS1 = submissionTracker.register(rootHash, requestIds(0))
      val resultFUS2 = submissionTracker.register(rootHash, requestIds(1))

      submissionTracker.provideSubmissionData(
        rootHash,
        requestIds(0),
        getSubmissionData(requestIds(0)),
      )
      submissionTracker.provideSubmissionData(
        rootHash,
        requestIds(1),
        getSubmissionData(requestIds(1)),
      )

      resultFUS1.failOnShutdown.futureValue shouldBe true
      resultFUS2.failOnShutdown.futureValue shouldBe false

      val resultFUS3 = submissionTracker.register(rootHash, requestIds(2))

      submissionTracker.provideSubmissionData(
        rootHash,
        requestIds(2),
        getSubmissionData(requestIds(2)),
      )

      resultFUS3.failOnShutdown.futureValue shouldBe false
    }

    "follow registration order" in {
      val resultFUS1 = submissionTracker.register(rootHash, requestIds(0))
      val resultFUS2 = submissionTracker.register(rootHash, requestIds(1))

      submissionTracker.provideSubmissionData(
        rootHash,
        requestIds(1),
        getSubmissionData(requestIds(1)),
      )
      submissionTracker.provideSubmissionData(
        rootHash,
        requestIds(0),
        getSubmissionData(requestIds(0)),
      )

      resultFUS1.failOnShutdown.futureValue shouldBe true
      resultFUS2.failOnShutdown.futureValue shouldBe false
    }

    "succeed a request when previous ones cancelled" in {
      val resultFUS1 = submissionTracker.register(rootHash, requestIds(0))
      val resultFUS2 = submissionTracker.register(rootHash, requestIds(1))
      val resultFUS3 = submissionTracker.register(rootHash, requestIds(2))

      Seq(0, 1).foreach(index =>
        submissionTracker.cancelRegistration(
          rootHash,
          requestIds(index),
        )
      )
      submissionTracker.provideSubmissionData(
        rootHash,
        requestIds(2),
        getSubmissionData(requestIds(2)),
      )

      resultFUS1.failOnShutdown.futureValue shouldBe false
      resultFUS2.failOnShutdown.futureValue shouldBe false
      resultFUS3.failOnShutdown.futureValue shouldBe true
    }

    "survive smoke tests" onlyRunWithOrGreaterThan ProtocolVersion.v5 in {
      // Try to make some concurrent scenarios
      val seed = Random.nextLong()
      val rand = new Random(seed)
      logger.debug(s"Seed for smoke tests = $seed")

      val requestIdsWithIndices = requestIds.zipWithIndex

      (1 to 100).foreach { iteration =>
        val rootHash: RootHash = RootHash(TestHash.digest(iteration))

        // Pick the first request to be validated
        val n = rand.nextInt(requestIds.size)

        // Pick the requests to validate (true -> validate, false -> cancel)
        val requestIdToValidate = requestIds.indices.map {
          // Cancel all requests before the picked one
          case index if index < n => false
          // Validate the picked request
          case index if index == n => true
          // Randomly cancel or validate requests after the picked one
          case index if index > n => rand.nextInt(2) == 0
        }
        requestIdToValidate.indexWhere(identity) shouldBe n

        val registeredRequestIds = TrieMap[Int, RequestId]()
        val allRequestsRegistered = new AtomicReference[Boolean](false)

        // Register all requests
        val resultsF = Future {
          requestIdsWithIndices.map { case (reqId, index) =>
            val f = submissionTracker.register(rootHash, reqId)
            registeredRequestIds += (index -> reqId)
            f
          }
        }.thereafter { _ => allRequestsRegistered.set(true) }

        @tailrec
        def processRegisteredRequests(): Unit =
          if (!allRequestsRegistered.get) {
            // Give some time to accumulate requests
            Threading.sleep(1)
            handleRequests()
            processRegisteredRequests()
          } else {
            handleRequests()
            registeredRequestIds shouldBe empty
          }

        def handleRequests(): Unit = {
          // Take a snapshot
          val reqIds = registeredRequestIds.toSeq

          FutureUtil.doNotAwait(
            reqIds.parTraverse_ { case (index, reqId) =>
              Future {
                if (requestIdToValidate(index))
                  submissionTracker
                    .provideSubmissionData(rootHash, reqId, getSubmissionData(reqId))
                else
                  submissionTracker.cancelRegistration(rootHash, reqId)
              }
            },
            "processing",
          )
          registeredRequestIds --= reqIds.map(_._1)
        }

        processRegisteredRequests()

        // Only the picked request should succeed
        val succeeded =
          resultsF.futureValue.map(_.failOnShutdown.futureValue).zipWithIndex.collect {
            case (true, index) => index
          }
        succeeded shouldBe Seq(n)
      }
    }
  }
}
