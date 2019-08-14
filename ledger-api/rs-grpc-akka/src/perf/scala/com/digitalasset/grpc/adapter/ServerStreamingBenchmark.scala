// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.grpc.adapter

import akka.Done
import akka.stream.scaladsl.Sink
import com.digitalasset.grpc.adapter.client.akka.ClientAdapter
import com.digitalasset.grpc.adapter.operation.AkkaServiceFixture
import com.digitalasset.ledger.api.perf.util.AkkaStreamPerformanceTest
import com.digitalasset.ledger.api.testing.utils.Resource
import com.digitalasset.platform.hello.{HelloRequest, HelloServiceGrpc}
import io.grpc.ManagedChannel
import org.scalameter.api.Gen
import org.scalameter.picklers.noPickler._

import scala.concurrent.Future
import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

object ServerStreamingBenchmark extends AkkaStreamPerformanceTest {

  override type ResourceType = () => ManagedChannel

  @transient override protected lazy val resource: Resource[() => ManagedChannel] =
    AkkaServiceFixture.getResource(Some(new InetSocketAddress(0))).map(_._2.channel)

  private val sizes = for {
    totalElements <- Gen.range("numResponses")(50000, 100000, 50000)
    clients <- Gen.enumeration("numClients")(1, 10)
    callsPerClient <- Gen.enumeration("numCals")(1, 10)
  } yield (totalElements, clients, callsPerClient)

  performance of "Akka-Stream server" config (daConfig: _*) in {
    measure method "server streaming" in {
      using(sizes).withLifecycleManagement() in {
        case (totalElements, clients, callsPerClient) =>
          val eventualDones = for {
            (channel, schedulerPool) <- 1
              .to(clients)
              .map(i => resource.value() -> new AkkaExecutionSequencerPool(s"client-$i")(system))
            _ <- 1.to(callsPerClient)
          } yield {
            serverStreamingCall(totalElements / clients / callsPerClient, channel)(schedulerPool)
              .map(_ => channel -> schedulerPool)
          }
          val eventualTuples = Future.sequence(eventualDones)
          await(eventualTuples).foreach {
            case (channel, pool) =>
              channel.shutdown()
              channel.awaitTermination(5, TimeUnit.SECONDS)
              pool.close()
          }

      }
    }
  }

  private def serverStreamingCall(streamedElements: Int, managedChannel: ManagedChannel)(
      implicit
      executionSequencerFactory: ExecutionSequencerFactory): Future[Done] = {
    ClientAdapter
      .serverStreaming(
        HelloRequest(streamedElements),
        HelloServiceGrpc.stub(managedChannel).serverStreaming)
      .runWith(Sink.ignore)(materializer)
  }
}
