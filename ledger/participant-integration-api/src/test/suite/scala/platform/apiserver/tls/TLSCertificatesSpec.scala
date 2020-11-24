package com.daml.platform.apiserver.tls

import java.io.File

import akka.actor.ActorSystem
import com.codahale.metrics.MetricRegistry
import com.daml.bazeltools.BazelRunfiles
import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.grpc.sampleservice.implementations.ReferenceImplementation
import com.daml.ledger.api.testing.utils.{AkkaBeforeAndAfterAll, OwnedResource}
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner, TestResourceContext}
import com.daml.platform.apiserver.{ApiServer, ApiServices, LedgerApiServer}
import io.grpc.{BindableService, ManagedChannel}
import org.scalatest.{AsyncWordSpec, Matchers, Suite}
import com.daml.ledger.client.GrpcChannel
import com.daml.ledger.client.configuration.{CommandClientConfiguration, LedgerClientConfiguration, LedgerIdRequirement}
import com.daml.metrics.Metrics
import com.daml.ports.Port
import org.mockito.MockitoSugar
import com.daml.logging.LoggingContext
import com.daml.platform.hello.{HelloRequest, HelloResponse, HelloServiceGrpc}
import com.daml.timer.RetryStrategy

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}
import scala.sys.process.Process

final class TLSCertificatesSpec extends AsyncWordSpec with Matchers with MockitoSugar with AkkaBeforeAndAfterAll with TestResourceContext with OCSPResponderFixture {
  import TLSCertificatesSpec.{ TLSFixture, resource }

  val serverCrt = resource("server.crt")
  val serverKey = resource("server.pem")
  val caCrt = resource("ca.crt")
  val clientCrt = resource("client.crt")
  val clientKey = resource("client.pem")
  val clientRevokedCrt = resource("client-revoked.crt")
  val clientRevokedKey = resource("client-revoked.pem")
  val ocspCrt = resource("ocsp.crt")
  val ocspKey = resource("ocsp.key.pem")
  val index = resource("index.txt")

  override protected def indexPath: String = index.getAbsolutePath
  override protected def caCertPath: String = caCrt.getAbsolutePath
  override protected def ocspKeyPath: String = ocspKey.getAbsolutePath
  override protected def ocspCertPath: String = ocspCrt.getAbsolutePath
  override protected def ocspTestCertificate: String = clientCrt.getAbsolutePath

  classOf[LedgerApiServer].getSimpleName when {
    "certificate revocation checking is enabled" should {
      "allow TLS connections with valid certificates" in {
        TLSFixture(tlsEnabled = true, serverCrt, serverKey, caCrt, clientCrt, clientKey)
          .makeARequest()
          .map(_ => succeed)
      }

      "block TLS connections with revoked certificates" in {
        TLSFixture(tlsEnabled = true, serverCrt, serverKey, caCrt, clientRevokedCrt, clientRevokedKey)
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

    "certificate revocation checking is not enabled" should {
      "allow TLS connections with valid certificates" in {
        TLSFixture(tlsEnabled = false, serverCrt, serverKey, caCrt, clientCrt, clientKey)
          .makeARequest()
          .map(_ => succeed)
      }

      "allow TLS connections with revoked certificates" in {
        TLSFixture(tlsEnabled = false, serverCrt, serverKey, caCrt, clientRevokedCrt, clientRevokedKey)
          .makeARequest()
          .map(_ => succeed)
      }
    }
  }
}

object TLSCertificatesSpec {

  protected final case class TLSFixture(
                          tlsEnabled: Boolean,
                          serverCrt: File,
                          serverKey: File,
                          caCrt: File,
                          clientCrt: File,
                          clientKey: File,
                          )(implicit rc: ResourceContext, actorSystem: ActorSystem) {

    def makeARequest(): Future[HelloResponse] =
      resources().use { channel =>
        val testRequest = HelloRequest(1)
        HelloServiceGrpc.stub(channel)
          .single(testRequest)
      }

    private val DefaultMaxInboundMessageSize: Int = 4 * 1024 * 1024 // taken from the Sandbox config

    private final class MockApiServices(apiServices: ApiServices) extends ResourceOwner[ApiServices] {
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
      revocationChecks = true
    )

    private def apiServerOwner(): ResourceOwner[ApiServer] = {
      val apiServices = new EmptyApiServices
      val owner = new MockApiServices(apiServices)

      LoggingContext.newLoggingContext { implicit loggingContext =>
        new LedgerApiServer(
          apiServicesOwner = owner,
          desiredPort = Port.Dynamic,
          maxInboundMessageSize = DefaultMaxInboundMessageSize,
          address = None,
          tlsConfiguration = Some(serverTlsConfiguration),
          metrics = new Metrics(new MetricRegistry),
        )
      }
    }

    private val clientTlsConfiguration =
      TlsConfiguration(
        enabled = tlsEnabled,
        keyCertChainFile = Some(clientCrt),
        keyFile = Some(clientKey),
        trustCertCollectionFile = Some(caCrt)
      )

    private val ledgerClientConfiguration = LedgerClientConfiguration(
      applicationId = s"TLSCertificates-app",
      ledgerIdRequirement = LedgerIdRequirement.none,
      commandClient = CommandClientConfiguration.default,
      sslContext = clientTlsConfiguration.client,
      token = None
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

trait OCSPResponderFixture extends AkkaBeforeAndAfterAll { this: Suite =>
  import scala.concurrent.duration._

  private val ec: ExecutionContext = system.dispatcher

  private val RESPONDER_HOST: String = "127.0.0.1"
  private val RESPONDER_PORT: Int = 2560

  protected def indexPath: String
  protected def caCertPath: String
  protected def ocspKeyPath: String
  protected def ocspCertPath: String
  protected def ocspTestCertificate: String

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    responderResource.setup()
  }

  override protected def afterAll(): Unit = {
    responderResource.close()
    super.afterAll()
  }

  private val isWindows: Boolean = sys.props("os.name").toLowerCase.contains("windows")

  private val opensslExecutable: String =
    if (!isWindows) BazelRunfiles.rlocation("external/openssl_dev_env/bin/openssl")
    else BazelRunfiles.rlocation("external/openssl_dev_env/usr/bin/openssl.exe")

  lazy val responderResource = {
    implicit val resourceContext: ResourceContext = ResourceContext(ec)
    new OwnedResource[ResourceContext, Process](
      owner = responderResourceOwner,
      acquisitionTimeout = 10.seconds,
      releaseTimeout = 5.seconds
    )
  }

  private def responderResourceOwner: ResourceOwner[Process] =
    new ResourceOwner[Process] {

      override def acquire()(implicit context: ResourceContext): Resource[Process] = {
        def start(): Future[Process] =
          for {
            process <- startResponderProcess()
            _ <- verifyResponderReady()
          } yield process

        def stop(responderProcess: Process): Future[Unit] =
          Future {
            responderProcess.destroy()
          }.map(_ => ())

        Resource(start())(stop)
      }
    }

  private def startResponderProcess()(implicit ec: ExecutionContext): Future[Process] =
    Future(Process(ocspServerCommand).run())

  private def verifyResponderReady()(implicit ec: ExecutionContext): Future[String] =
    RetryStrategy.constant(attempts = 3, waitTime = 5.seconds) { (_, _) =>
      Future(Process(testOCSPRequestCommand).!!)
    }

  private def ocspServerCommand = List(
    opensslExecutable,
    "ocsp",
    "-port",
    RESPONDER_PORT.toString,
    "-text",
    "-index",
    indexPath,
    "-CA",
    caCertPath,
    "-rkey",
    ocspKeyPath,
    "-rsigner",
    ocspCertPath
  )

  private def testOCSPRequestCommand = List(
    opensslExecutable,
    "ocsp",
    "-CAfile",
    caCertPath,
    "-url",
    s"http://$RESPONDER_HOST:$RESPONDER_PORT",
    "-resp_text",
    "-issuer",
    caCertPath,
    "-cert",
    ocspTestCertificate
  )
}
