// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins

import cats.implicits.catsSyntaxOptionId
import com.daml.metrics.api.MetricsContext
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.metrics.grpc.DamlGrpcServerMetrics
import com.daml.tracing.NoOpTelemetry
import com.digitalasset.canton.auth.AsyncForwardingListener
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.config.{
  AdminServerConfig,
  ApiLoggingConfig,
  CantonConfig,
  TlsServerConfig,
}
import com.digitalasset.canton.integration.{EnvironmentSetupPlugin, TestConsoleEnvironment}
import com.digitalasset.canton.lifecycle.LifeCycle.{CloseableServer, toCloseableServer}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.ActiveRequestsMetrics
import com.digitalasset.canton.networking.grpc.CantonServerBuilder
import com.digitalasset.canton.tracing.TracingConfig.{BatchSpanProcessor, Exporter}
import com.digitalasset.canton.tracing.{TraceContext, TracingConfig}
import com.google.common.io.BaseEncoding
import io.grpc.stub.StreamObserver
import io.grpc.{Context, Contexts, Metadata, ServerCall, ServerCallHandler, ServerInterceptor}
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc.TraceServiceImplBase
import io.opentelemetry.proto.collector.trace.v1.{
  ExportTraceServiceRequest,
  ExportTraceServiceResponse,
}
import io.opentelemetry.proto.trace.v1.Span
import monocle.macros.syntax.lens.*

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, blocking}
import scala.jdk.CollectionConverters.ListHasAsScala

final case class OtlpSpan(traceId: String, spanId: String, parentSpanId: String, name: String)

class OtlpGrpcServer(protected val loggerFactory: NamedLoggerFactory)
    extends TraceServiceImplBase
    with NamedLogging {

  logger.info("Created Otlp Server")(TraceContext.empty)
  private val traceSpans: ListBuffer[Span] = ListBuffer[Span]()

  def getSpans: Seq[OtlpSpan] = blocking(synchronized(traceSpans.toSeq)).map(span =>
    OtlpSpan(
      traceId = BaseEncoding.base16().lowerCase().encode(span.getTraceId.toByteArray),
      spanId = BaseEncoding.base16().lowerCase().encode(span.getSpanId.toByteArray),
      parentSpanId = BaseEncoding.base16().lowerCase().encode(span.getParentSpanId.toByteArray),
      name = span.getName,
    )
  )

  override def `export`(
      request: ExportTraceServiceRequest,
      responseObserver: StreamObserver[ExportTraceServiceResponse],
  ): Unit = {
    blocking(
      synchronized(
        traceSpans.addAll(
          request.getResourceSpansList.asScala
            .flatMap(_.getScopeSpansList.asScala)
            .flatMap(_.getSpansList.asScala)
        )
      )
    )
    responseObserver.onNext(ExportTraceServiceResponse.getDefaultInstance)
    responseObserver.onCompleted()
  }
}

class HeaderPrinter(
    val loggerFactory: NamedLoggerFactory
)(implicit val ec: ExecutionContext)
    extends ServerInterceptor
    with NamedLogging {
  override def interceptCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      headers: Metadata,
      nextListener: ServerCallHandler[ReqT, RespT],
  ): ServerCall.Listener[ReqT] =
    new AsyncForwardingListener[ReqT] {
      logger.info(s"Intercepted call: ${headers.toString}")(TraceContext.empty)
      setNextListener(Contexts.interceptCall(Context.current, call, headers, nextListener))
    }

}

/** Integration test plugin for setting up OTLP server in process
  *
  * @param port
  *   defines the port at which the OTLP server will be listening for connections.
  * @param loggerFactory
  *   defines the ambient logger in canton tests.
  * @param trustCollectionPath
  *   defines the path to the ca crt file that the trace exporter should use on the client side when
  *   connecting to the OTLP server.
  * @param tls
  *   configuration of the tls to be used by the OTLP server spun up by the plugin.
  * @param otlpHeaders
  *   headers to be used by the trace exporter on all grpc calls to the OTLP server.
  */
class UseOtlp(
    protected val port: Port,
    protected val loggerFactory: NamedLoggerFactory,
    protected val trustCollectionPath: Option[String] = None,
    protected val tls: Option[TlsServerConfig] = None,
    protected val otlpHeaders: Map[String, String] = Map.empty,
) extends EnvironmentSetupPlugin
    with AutoCloseable {

  private var otlpServer: OtlpGrpcServer = _
  private var grpcServer: CloseableServer = _

  private def transformConfig(config: CantonConfig): CantonConfig =
    config
      .focus(_.monitoring.tracing.tracer.exporter)
      .replace(
        Exporter.Otlp(
          port = port.unwrap,
          trustCollectionPath = trustCollectionPath,
          additionalHeaders = otlpHeaders,
        )
      )
      .focus(_.monitoring.tracing.tracer.batchSpanProcessor)
      .replace(BatchSpanProcessor(batchSize = Some(64), scheduleDelay = Some(50.millis)))

  private def startServer(implicit
      env: TestConsoleEnvironment
  ): CloseableServer = {
    import env.*

    val serverConfig = AdminServerConfig(internalPort = port.some, tls = tls)
    otlpServer = new OtlpGrpcServer(loggerFactory)

    val serverBuilder = CantonServerBuilder
      .forConfig(
        serverConfig,
        None,
        executionContext,
        loggerFactory,
        apiLoggingConfig = ApiLoggingConfig(messagePayloads = false),
        TracingConfig(),
        (
          new DamlGrpcServerMetrics(NoOpMetricsFactory, "test"),
          new ActiveRequestsMetrics(NoOpMetricsFactory, "test")(MetricsContext.Empty),
        ),
        NoOpTelemetry,
        Seq(new HeaderPrinter(loggerFactory)),
      )
      .addService(otlpServer.bindService)
    val server = serverBuilder.build.start()
    logger.info(
      s"Otlp Server started listening on ${serverConfig.address}:${serverConfig.internalPort.toString}"
    )(TraceContext.empty)
    toCloseableServer(server, loggerFactory.getTracedLogger(this.getClass), "TimeServer")
  }

  def getSpans: Seq[OtlpSpan] = otlpServer.getSpans

  override def beforeEnvironmentCreated(config: CantonConfig): CantonConfig =
    transformConfig(config)

  override def afterEnvironmentCreated(
      config: CantonConfig,
      environment: TestConsoleEnvironment,
  ): Unit =
    grpcServer = startServer(environment)

  override def beforeEnvironmentDestroyed(
      environment: TestConsoleEnvironment
  ): Unit = {}

  override def afterEnvironmentDestroyed(config: CantonConfig): Unit =
    // Server must be shutdown manually after all the closeables from the environment
    // to make sure that the GrpcSpanExporter from the configured open telemetry is shutdown first
    grpcServer.close()

  override def close(): Unit = {}

  override def afterTests(): Unit =
    close()
}
