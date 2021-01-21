package com.daml.platform.apiserver.tls

import java.io.File
import java.util.concurrent.Executors

import com.codahale.metrics.MetricRegistry
import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.grpc.sampleservice.implementations.ReferenceImplementation
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.client.GrpcChannel
import com.daml.ledger.client.configuration.{CommandClientConfiguration, LedgerClientConfiguration, LedgerIdRequirement}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner, TestResourceContext}
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.apiserver.{ApiServer, ApiServices, LedgerApiServer}
import com.daml.platform.hello.{HelloRequest, HelloResponse, HelloServiceGrpc}
import com.daml.ports.Port
import io.grpc.{BindableService, ManagedChannel}
import io.netty.handler.ssl.ClientAuth
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AsyncWordSpec

import scala.collection.immutable
import scala.concurrent.Future

class TlsSpec extends AsyncWordSpec with Matchers with TableDrivenPropertyChecks with TestResourceContext {
  import TlsSpec.{TlsFixture, resource}

  val serverCrt = resource("server.crt")
  val serverKey = resource("server.pem")
  val caCrt = resource("ca.crt")
  val clientCrt = resource("client.crt")
  val clientKey = resource("client.pem")

  def tlsEnabledFixture(clientCrt: Option[File], clientKey: Option[File], clientAuth: ClientAuth) =
    TlsFixture(tlsEnabled = true, serverCrt, serverKey, caCrt, clientCrt, clientKey, clientAuth)

  classOf[LedgerApiServer].getSimpleName when {
    "client authorization is set to none" should {
      "allow TLS connections with valid certificates" in {
        tlsEnabledFixture(Some(clientCrt), Some(clientKey), ClientAuth.NONE)
          .makeARequest()
          .map(_ => succeed)
      }

      "allow TLS connections without certificates" in {
        tlsEnabledFixture(None, None, ClientAuth.NONE)
          .makeARequest()
          .map(_ => succeed)
      }
    }

    "client authorization is set to optional" should {
      "allow TLS connections with valid certificates" in {
        tlsEnabledFixture(Some(clientCrt), Some(clientKey), ClientAuth.OPTIONAL)
          .makeARequest()
          .map(_ => succeed)
      }

      "allow TLS connections without certificates" in {
        tlsEnabledFixture(None, None, ClientAuth.OPTIONAL)
          .makeARequest()
          .map(_ => succeed)
      }
    }

    "client authorization is set to require" should {
      "allow TLS connections with valid certificates" in {
        tlsEnabledFixture(Some(clientCrt), Some(clientKey), ClientAuth.REQUIRE)
          .makeARequest()
          .map(_ => succeed)
      }

      "block TLS connections without certificates" in {
        tlsEnabledFixture(None, None, ClientAuth.REQUIRE)
          .makeARequest()
          .failed
          .collect {
            case com.daml.grpc.GrpcException.UNAVAILABLE() =>
              succeed
            case ex =>
              fail(s"Invalid exception: ${ex.getClass.getCanonicalName}: ${ex.getMessage}")
          }
      }
    }
  }
}

object TlsSpec {

  protected final case class TlsFixture(
                                         tlsEnabled: Boolean,
                                         serverCrt: File,
                                         serverKey: File,
                                         caCrt: File,
                                         clientCrt: Option[File],
                                         clientKey: Option[File],
                                         clientAuth: ClientAuth = ClientAuth.REQUIRE,
                                         certRevocationChecking: Boolean = false,
                                       )(implicit rc: ResourceContext) {

    def makeARequest(): Future[HelloResponse] =
      resources().use { channel =>
        val testRequest = HelloRequest(1)
        HelloServiceGrpc
          .stub(channel)
          .single(testRequest)
      }

    private val DefaultMaxInboundMessageSize: Int = 4 * 1024 * 1024 // taken from the Sandbox config

    private final class MockApiServices(apiServices: ApiServices)
      extends ResourceOwner[ApiServices] {
      override def acquire()(implicit context: ResourceContext): Resource[ApiServices] = {
        Resource(Future.successful(apiServices))(_ => Future.successful(()))(context)
      }
    }

    private final class EmptyApiServices extends ApiServices {
      override val services: Iterable[BindableService] = List(new ReferenceImplementation)
      override def withServices(otherServices: immutable.Seq[BindableService]): ApiServices = this
    }

    private val serverTlsConfiguration = TlsConfiguration(
      enabled = tlsEnabled,
      keyCertChainFile = Some(serverCrt),
      keyFile = Some(serverKey),
      trustCertCollectionFile = Some(caCrt),
      clientAuth = clientAuth,
      enableCertRevocationChecking = certRevocationChecking,
    )

    private def apiServerOwner(): ResourceOwner[ApiServer] = {
      val apiServices = new EmptyApiServices
      val owner = new MockApiServices(apiServices)

      LoggingContext.newLoggingContext { implicit loggingContext =>
        ResourceOwner
          .forExecutorService(() => Executors.newCachedThreadPool())
          .flatMap(servicesExecutor =>
            new LedgerApiServer(
              apiServicesOwner = owner,
              desiredPort = Port.Dynamic,
              maxInboundMessageSize = DefaultMaxInboundMessageSize,
              address = None,
              tlsConfiguration = Some(serverTlsConfiguration),
              servicesExecutor = servicesExecutor,
              metrics = new Metrics(new MetricRegistry),
            )
          )
      }
    }

    private val clientTlsConfiguration =
      TlsConfiguration(
        enabled = tlsEnabled,
        // TODO: this is different
        keyCertChainFile = clientCrt,
        keyFile = clientKey,
        trustCertCollectionFile = Some(caCrt),
      )

    private val ledgerClientConfiguration = LedgerClientConfiguration(
      applicationId = s"TlsCertificates-app",
      ledgerIdRequirement = LedgerIdRequirement.none,
      commandClient = CommandClientConfiguration.default,
      sslContext = clientTlsConfiguration.client,
      token = None,
    )

    private def resources(): ResourceOwner[ManagedChannel] =
      for {
        apiServer <- apiServerOwner()
        channel <- new GrpcChannel.Owner(apiServer.port, ledgerClientConfiguration)
      } yield channel

  }

  protected def resource(src: String) =
    new File(rlocation("ledger/test-common/test-certificates/" + src))
}
