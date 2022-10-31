// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter

import akka.Done
import akka.stream.scaladsl.Sink
import com.daml.grpc.adapter.client.akka.ClientAdapter
import com.daml.grpc.sampleservice.implementations.HelloServiceReferenceImplementation
import com.daml.ledger.api.perf.util.AkkaStreamPerformanceTest
import com.daml.ledger.api.testing.utils.{AkkaStreamGrpcServerResource, Resource}
import com.daml.platform.hello._
import io.grpc.ManagedChannel
import org.scalameter.api.Gen
import org.scalameter.picklers.noPickler._

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit
import scala.concurrent.Future
import scala.util.Random

object ServerStreamingBenchmark extends AkkaStreamPerformanceTest {
  private implicit val esf: ExecutionSequencerFactory = TestExecutionSequencerFactory.instance
  override type ResourceType = () => ManagedChannel

  @transient override protected lazy val resource: Resource[() => ManagedChannel] =
    AkkaStreamGrpcServerResource(
      implicit m => List(new HelloServiceReferenceImplementation()),
      "server",
      Some(new InetSocketAddress(0)),
    ).map(_._2.channel)

  private val sizesSimpleRequest = for {
    totalElements <- Gen.enumeration("numResponses")(10000, 50000)
    clients <- Gen.enumeration("numClients")(1, 10)
    callsPerClient <- Gen.enumeration("numCals")(1, 10)
  } yield (totalElements, clients, callsPerClient)

  private val sizesHeavyRequest = for {
    depth <- Gen.enumeration("nestingLevel")(4)
    request = generatedRequest(depth, 5, 64, 100)
  } yield request

  performance of "Akka-Stream server" config (daConfig: _*) in {
    measure method "server streaming" in {
      using(sizesSimpleRequest).withLifecycleManagement() in {
        case (totalElements, clients, callsPerClient) =>
          val eventualDones = 1 to clients map { i =>
            val pool = new AkkaExecutionSequencerPool(s"client-$i")(system)
            val channel = resource.value()
            Future
              .sequence(
                1 to callsPerClient map (_ =>
                  serverStreamingCall(totalElements / clients / callsPerClient, channel, pool)
                )
              )
              .map(_ => channel -> pool)
          }
          val eventualTuples = Future.sequence(eventualDones)
          await(eventualTuples).foreach { case (channel, pool) =>
            channel.shutdown()
            channel.awaitTermination(5, TimeUnit.SECONDS)
            pool.close()
          }
      }
    }

    measure method "server streaming (heavy payload)" in {
      using(sizesHeavyRequest).withLifecycleManagement() in { request =>
        val pool = new AkkaExecutionSequencerPool(s"client")(system)
        val channel = resource.value()

        val doneF = ClientAdapter
          .serverStreaming(
            request,
            HelloServiceGrpc.stub(channel).serverStreamingHeavy,
          )(pool)
          .runWith(Sink.ignore)(materializer)

        await(doneF)

        channel.shutdown()
        channel.awaitTermination(5, TimeUnit.SECONDS)
        pool.close()
      }
    }
  }

  private def generatedRequest(
      depth: Int,
      breadth: Int,
      payloadSize: Int,
      noResponses: Int,
  ): HelloRequestHeavy = {
    def generatedNestedMapNode(depth: Int): NestedMapNode =
      if (depth == 0) NestedMapNode(kind = NestedMapNode.Kind.Leaf(Random.nextString(payloadSize)))
      else
        NestedMapNode(kind =
          NestedMapNode.Kind.Node(
            NestedMap(
              (1 to breadth).view
                .map(_ => Random.nextString(payloadSize) -> generatedNestedMapNode(depth - 1))
                .toMap
            )
          )
        )

    HelloRequestHeavy(
      responses = (1 to noResponses).map(_ => generatedNestedMapNode(depth))
    )
  }

  def serverStreamingCall(
      streamedElements: Int,
      managedChannel: ManagedChannel,
      executionSequencerFactory: ExecutionSequencerFactory,
  ): Future[Done] = {
    ClientAdapter
      .serverStreaming(
        HelloRequest(streamedElements),
        HelloServiceGrpc.stub(managedChannel).serverStreaming,
      )(executionSequencerFactory)
      .runWith(Sink.ignore)(materializer)
  }
}
