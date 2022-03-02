// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter.client.rs

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.{ExecutorService, Executors}
import java.util.function.BiConsumer

import com.daml.grpc.adapter.TestExecutionSequencerFactory
import com.daml.grpc.adapter.server.rs.MockClientCallStreamObserver
import io.grpc.stub.{ClientResponseObserver, StreamObserver}
import org.reactivestreams.Publisher
import org.reactivestreams.tck.PublisherVerification
import org.slf4j.{Logger, LoggerFactory}
import org.testng.annotations.{AfterMethod, BeforeMethod}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NoStackTrace

trait PublisherCreation {
  self: PublisherVerification[Long] =>

  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)

  // For test cases that signal high demand, emitting Int.MAX_VALUE elements  blocks the calling thread for too long.
  // To avoid this, the publisher emits asynchronously, and it uses a private managed execution context to make sure
  // that the Runnables that were scheduled earlier do not affect the runtime characteristics of following tests
  // (which would be the case if we'd use the `global` context.

  private var es: ExecutorService = _
  implicit private var ec: ExecutionContext = _
  var testRunning = false

  @BeforeMethod
  def beforeMethod(): Unit = {
    setUp()
    es = Executors.newSingleThreadExecutor()
    ec = ExecutionContext.fromExecutor(es)
    testRunning = true
  }

  @AfterMethod
  def afterMethod(): Unit = {
    testRunning = false
    es.shutdownNow()
    es = null
    ec = null
  }

  def createPublisher(elemCount: Long): Publisher[Long] = {
    val stub: BiConsumer[Long, StreamObserver[Long]] = { (_, responseObserver) =>
      logger.trace("PUBLISHER WILL EMIT {} elements.", elemCount)
      val completed = new AtomicBoolean(false)

      def complete(): Unit = {
        completed.set(true)
        responseObserver.onCompleted()
        logger.trace("COMPLETED.")
      }

      if (elemCount >= 1) {
        val emittedElements = new AtomicLong(0L)
        val completeAfterFirst = if (elemCount == 1) {
          responseObserver
            .asInstanceOf[ClientResponseObserver[Long, Long]]
            .beforeStart(new MockClientCallStreamObserver[Long](_ => ()))
          true
        } else {
          responseObserver
            .asInstanceOf[ClientResponseObserver[Long, Long]]
            .beforeStart(new MockClientCallStreamObserver[Long](integer => {
              logger.trace("RECEIVED DEMAND FOR {} elements", integer)
              Future {
                1.to(integer)
                  .foreach(_ => {
                    if (testRunning) {
                      if (!completed.get()) {
                        val emitted = emittedElements.getAndIncrement()
                        responseObserver.onNext(emitted)
                        logger.trace("EMITTED {}", emitted)
                        if (emitted + 1 == elemCount) {
                          complete()
                        }
                      }
                    } else {
                      throw new RuntimeException(
                        "Stopping emission of elements due to test termination"
                      ) with NoStackTrace
                    }
                  })
              }(ec)
              ()

            }))
          false
        }
        val firstEmitted = emittedElements.getAndIncrement()
        responseObserver.onNext(firstEmitted)
        logger.trace("EMITTED FIRST {}", firstEmitted)
        if (completeAfterFirst) complete()

      } else {
        complete()
      }
    }
    new ClientPublisher[Long, Long](elemCount, stub, TestExecutionSequencerFactory.instance)
  }

  def createFailedPublisher(): Publisher[Long] = {

    val stub: BiConsumer[Long, StreamObserver[Long]] = { (_, responseObserver) =>
      responseObserver.onError(
        new RuntimeException("Exception created to test failed Publisher") with NoStackTrace
      )
    }
    new ClientPublisher[Long, Long](1L, stub, TestExecutionSequencerFactory.instance)
  }
}
