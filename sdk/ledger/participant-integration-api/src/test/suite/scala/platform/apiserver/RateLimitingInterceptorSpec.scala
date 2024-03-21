// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import akka.actor.ActorSystem
import com.codahale.metrics.MetricRegistry
import com.daml.grpc.adapter.utils.implementations.HelloServiceAkkaImplementation
import com.daml.ledger.api.health.HealthChecks.ComponentName
import com.daml.ledger.api.health.{HealthChecks, ReportsHealth}
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner, TestResourceContext}
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.apiserver.TenuredMemoryPool.findTenuredMemoryPool
import com.daml.platform.apiserver.configuration.RateLimitingConfig
import com.daml.platform.apiserver.services.GrpcClientResource
import com.daml.platform.configuration.ServerRole
import com.daml.platform.hello.{HelloRequest, HelloServiceGrpc}
import com.daml.platform.server.api.services.grpc.GrpcHealthService
import com.daml.ports.Port
import com.daml.resources.akka.ActorSystemResourceOwner
import io.grpc._
import io.grpc.health.v1.health.{HealthCheckRequest, HealthCheckResponse, HealthGrpc}
import io.grpc.netty.NettyServerBuilder
import io.grpc.protobuf.services.ProtoReflectionService
import io.grpc.reflection.v1alpha.{
  ServerReflectionGrpc,
  ServerReflectionRequest,
  ServerReflectionResponse,
}
import io.grpc.stub.StreamObserver
import org.mockito.MockitoSugar
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Second, Span}

import java.lang.management._
import java.net.{InetAddress, InetSocketAddress}
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Future, Promise}

final class RateLimitingInterceptorSpec
    extends AsyncFlatSpec
    with AkkaBeforeAndAfterAll
    with Matchers
    with Eventually
    with TestResourceContext
    with MockitoSugar {

  import RateLimitingInterceptorSpec._

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(1, Second)))

  private val config = RateLimitingConfig(100, 10, 75, 100 * RateLimitingConfig.Megabyte)

  behavior of "RateLimitingInterceptor"

  it should "limit calls when apiServices executor service is over limit" in {
    val metrics = new Metrics(new MetricRegistry)

    withChannel(metrics, new HelloServiceAkkaImplementation, config).use { channel: Channel =>
      val helloService = HelloServiceGrpc.stub(channel)
      val submitted = metrics.registry.meter(
        MetricRegistry.name(metrics.daml.lapi.threadpool.apiServices, "submitted")
      )
      for {
        _ <- helloService.single(HelloRequest(1))
        _ = submitted.mark(config.maxApiServicesQueueSize.toLong + 1)
        exception <- helloService.single(HelloRequest(2)).failed
        _ = submitted.mark(-config.maxApiServicesQueueSize.toLong - 1)
        _ <- helloService.single(HelloRequest(3))
      } yield {
        exception.getMessage should include(metrics.daml.lapi.threadpool.apiServices)
      }
    }
  }

  it should "limit calls when apiServices DB thread pool executor service is over limit" in {
    val metrics = new Metrics(new MetricRegistry)
    withChannel(metrics, new HelloServiceAkkaImplementation, config).use { channel: Channel =>
      val helloService = HelloServiceGrpc.stub(channel)
      val submitted = metrics.registry.meter(
        MetricRegistry.name(
          metrics.daml.index.db.threadpool.connection,
          ServerRole.ApiServer.threadPoolSuffix,
          "submitted",
        )
      )
      for {
        _ <- helloService.single(HelloRequest(1))
        _ = submitted.mark(config.maxApiServicesIndexDbQueueSize.toLong + 1)
        exception <- helloService.single(HelloRequest(2)).failed
        _ = submitted.mark(-config.maxApiServicesIndexDbQueueSize.toLong - 1)
        _ <- helloService.single(HelloRequest(3))
      } yield {
        exception.getMessage should include(metrics.daml.index.db.threadpool.connection)
      }
    }
  }

  /** Allowing metadata requests allows grpcurl to be used to debug problems */
  it should "allow metadata requests even when over limit" in {
    val metrics = new Metrics(new MetricRegistry)
    metrics.registry
      .meter(MetricRegistry.name(metrics.daml.lapi.threadpool.apiServices, "submitted"))
      .mark(config.maxApiServicesQueueSize.toLong + 1) // Over limit

    val protoService = ProtoReflectionService.newInstance()

    withChannel(metrics, protoService, config).use { channel: Channel =>
      val methodDescriptor: MethodDescriptor[ServerReflectionRequest, ServerReflectionResponse] =
        ServerReflectionGrpc.getServerReflectionInfoMethod
      val call = channel.newCall(methodDescriptor, CallOptions.DEFAULT)
      val promise = Promise[Status]()
      val listener = new ClientCall.Listener[ServerReflectionResponse]() {
        override def onReady(): Unit = {
          call.request(1)
        }
        override def onClose(status: Status, trailers: Metadata): Unit = {
          promise.success(status)
        }
      }
      call.start(listener, new Metadata())
      val request = ServerReflectionRequest
        .newBuilder()
        .setListServices("services")
        .setHost("localhost")
        .build()
      call.sendMessage(ServerReflectionRequest.newBuilder(request).build())
      call.halfClose()
      promise.future.map(status => status shouldBe Status.OK)
    }
  }

  it should "allow health checks event when over limit" in {
    val metrics = new Metrics(new MetricRegistry)
    metrics.registry
      .meter(MetricRegistry.name(metrics.daml.lapi.threadpool.apiServices, "submitted"))
      .mark(config.maxApiServicesQueueSize.toLong + 1) // Over limit

    val healthService = new GrpcHealthService(healthChecks)(
      executionSequencerFactory,
      materializer,
      executionContext,
      LoggingContext.ForTesting,
    )

    withChannel(metrics, healthService, config).use { channel: Channel =>
      val healthStub = HealthGrpc.stub(channel)
      val promise = Promise[Unit]()
      for {
        _ <- healthStub.check(HealthCheckRequest())
        _ = healthStub.watch(
          HealthCheckRequest(),
          new StreamObserver[HealthCheckResponse] {
            override def onNext(value: HealthCheckResponse): Unit = {
              promise.success(())
            }
            override def onError(t: Throwable): Unit = {}
            override def onCompleted(): Unit = {}
          },
        )
        _ <- promise.future
      } yield {
        succeed
      }
    }
  }

  it should "limit calls when there is a danger of running out of heap space" in {
    val poolName = "Tenured_Gen"
    val maxMemory = 100000L

    // Based on a combination of JvmMetricSet and MemoryUsageGaugeSet
    val expectedMetric = s"jvm_memory_usage_pools_$poolName"
    val metrics = new Metrics(new MetricRegistry)

    val memoryBean = mock[MemoryMXBean]

    val memoryPoolBean = mock[MemoryPoolMXBean]
    when(memoryPoolBean.getType).thenReturn(MemoryType.HEAP)
    when(memoryPoolBean.getName).thenReturn(poolName)
    when(memoryPoolBean.getCollectionUsage).thenReturn(new MemoryUsage(0, 0, 0, maxMemory))
    when(memoryPoolBean.isCollectionUsageThresholdSupported).thenReturn(true)
    when(memoryPoolBean.isCollectionUsageThresholdExceeded).thenReturn(false, true, false)
    when(memoryPoolBean.getCollectionUsageThreshold).thenReturn(
      config.calculateCollectionUsageThreshold(maxMemory)
    )

    val nonCollectableBean = mock[MemoryPoolMXBean]
    when(nonCollectableBean.getType).thenReturn(MemoryType.HEAP)
    when(nonCollectableBean.isCollectionUsageThresholdSupported).thenReturn(false)

    val nonHeapBean = mock[MemoryPoolMXBean]
    when(nonHeapBean.getType).thenReturn(MemoryType.NON_HEAP)
    when(nonHeapBean.isCollectionUsageThresholdSupported).thenReturn(true)

    val pool = List(nonCollectableBean, nonHeapBean, memoryPoolBean)

    withChannel(metrics, new HelloServiceAkkaImplementation, config, pool, memoryBean).use {
      channel: Channel =>
        val helloService = HelloServiceGrpc.stub(channel)
        for {
          _ <- helloService.single(HelloRequest(1))
          exception <- helloService.single(HelloRequest(2)).failed
          _ <- helloService.single(HelloRequest(3))
        } yield {
          verify(memoryPoolBean).setCollectionUsageThreshold(
            config.calculateCollectionUsageThreshold(maxMemory)
          )
          verify(memoryBean).gc()
          exception.getMessage should include(expectedMetric)
        }
    }
  }

  it should "should reset limit if a change of max memory is detected" in {

    val initMemory = 100000L
    val increasedMemory = 200000L

    val metrics = new Metrics(new MetricRegistry)

    val memoryBean = mock[MemoryMXBean]

    val memoryPoolBean = mock[MemoryPoolMXBean]
    when(memoryPoolBean.getType).thenReturn(MemoryType.HEAP)
    when(memoryPoolBean.getName).thenReturn("Tenured_Gen")
    when(memoryPoolBean.getCollectionUsage).thenReturn(
      new MemoryUsage(0, 0, 0, initMemory),
      new MemoryUsage(0, 0, 0, increasedMemory),
    )
    when(memoryPoolBean.isCollectionUsageThresholdSupported).thenReturn(true)
    when(memoryPoolBean.isCollectionUsageThresholdExceeded).thenReturn(true)

    val pool = List(memoryPoolBean)

    withChannel(metrics, new HelloServiceAkkaImplementation, config, pool, memoryBean).use {
      channel: Channel =>
        val helloService = HelloServiceGrpc.stub(channel)
        for {
          _ <- helloService.single(HelloRequest(1))
        } yield {
          verify(memoryPoolBean).setCollectionUsageThreshold(
            config.calculateCollectionUsageThreshold(initMemory)
          )
          verify(memoryPoolBean).setCollectionUsageThreshold(
            config.calculateCollectionUsageThreshold(increasedMemory)
          )
          succeed
        }
    }
  }

  it should "calculate the collection threshold zone size" in {
    // The actual threshold used would be max(maxHeapSpacePercentage * maxHeapSize / 100, maxHeapSize - maxOverThresholdZoneSize)
    val underTest =
      RateLimitingConfig.Default.copy(maxUsedHeapSpacePercentage = 90, minFreeHeapSpaceBytes = 1000)
    underTest.calculateCollectionUsageThreshold(3000) shouldBe 2700 // 90%
    underTest.calculateCollectionUsageThreshold(101000) shouldBe 100000 // 101000 - 1000
  }

  it should "use largest tenured pool as rate limiting pool" in {
    val expected = underLimitMemoryPoolMXBean()
    when(expected.getCollectionUsage).thenReturn(new MemoryUsage(0, 0, 0, 100))
    findTenuredMemoryPool(config, Nil) shouldBe None
    findTenuredMemoryPool(
      config,
      List(
        underLimitMemoryPoolMXBean(),
        expected,
        underLimitMemoryPoolMXBean(),
      ),
    ) shouldBe Some(expected)
  }

  it should "throttle calls to GC" in {
    val delegate = mock[MemoryMXBean]
    val delayBetweenCalls = 100.milliseconds
    val underTest = new GcThrottledMemoryBean(delegate, delayBetweenCalls)
    underTest.gc()
    underTest.gc()
    verify(delegate, times(1)).gc()
    Thread.sleep(2 * delayBetweenCalls.toMillis)
    underTest.gc()
    verify(delegate, times(2)).gc()
    succeed
  }

}

object RateLimitingInterceptorSpec extends MockitoSugar {

  val healthChecks = new HealthChecks(Map.empty[ComponentName, ReportsHealth])
  val systemOwner: ResourceOwner[ActorSystem] = new ActorSystemResourceOwner(() =>
    ActorSystem("RateLimitingInterceptorSpec")
  )

  // For tests that do not involve memory
  def underLimitMemoryPoolMXBean(): MemoryPoolMXBean = {
    val memoryPoolBean = mock[MemoryPoolMXBean]
    when(memoryPoolBean.getType).thenReturn(MemoryType.HEAP)
    when(memoryPoolBean.getName).thenReturn("UnderLimitPool")
    when(memoryPoolBean.getCollectionUsage).thenReturn(new MemoryUsage(0, 0, 0, 0))
    when(memoryPoolBean.isCollectionUsageThresholdSupported).thenReturn(true)
    when(memoryPoolBean.isCollectionUsageThresholdExceeded).thenReturn(false)
  }

  def withChannel(
      metrics: Metrics,
      service: BindableService,
      config: RateLimitingConfig,
      pool: List[MemoryPoolMXBean] = List(underLimitMemoryPoolMXBean()),
      memoryBean: MemoryMXBean = ManagementFactory.getMemoryMXBean,
  ): ResourceOwner[Channel] =
    for {
      server <- serverOwner(RateLimitingInterceptor(metrics, config, pool, memoryBean), service)
      channel <- GrpcClientResource.owner(Port(server.getPort))
    } yield channel

  def serverOwner(
      interceptor: ServerInterceptor,
      service: BindableService,
  ): ResourceOwner[Server] =
    new ResourceOwner[Server] {
      def acquire()(implicit context: ResourceContext): Resource[Server] =
        Resource(Future {
          val server =
            NettyServerBuilder
              .forAddress(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
              .directExecutor()
              .intercept(interceptor)
              .addService(service)
              .build()
          server.start()
          server
        })(server => Future(server.shutdown().awaitTermination()))
    }

}
