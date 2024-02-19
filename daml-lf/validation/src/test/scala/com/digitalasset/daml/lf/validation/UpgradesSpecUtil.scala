// Temporary stand-in for the real admin api clients defined in canton. Needed only for upgrades testing
// We should intend to replace this as soon as possible

package com.daml.lf.validation

import com.digitalasset.canton.ledger.client.LedgerCallCredentials.authenticatingStub
import com.digitalasset.canton.ledger.client.configuration.LedgerClientChannelConfiguration
import com.digitalasset.canton.ledger.client.GrpcChannel
import com.digitalasset.canton.admin.participant.{v30 => admin_package_service}
import io.grpc.Channel
import io.grpc.netty.NettyChannelBuilder
import io.grpc.stub.AbstractStub
import java.io.Closeable
import scala.concurrent.{ExecutionContext, Future}
import com.google.protobuf.ByteString
import io.netty.handler.ssl.SslContext

private[validation] final class AdminLedgerClient (
    val channel: Channel,
    token: Option[String],
)(implicit ec: ExecutionContext)
    extends Closeable {

  private val packageServiceStub =
    AdminLedgerClient.stub(admin_package_service.PackageServiceGrpc.stub(channel), token)

  def uploadDar(bytes: ByteString, filename: String): Future[Unit] = {
    packageServiceStub.uploadDar(
      admin_package_service.UploadDarRequest(
        bytes, filename, false, false
      ))
      .map(_ => ())
  }

  override def close(): Unit = GrpcChannel.close(channel)
}

object AdminLedgerClient {
  def stub[A <: AbstractStub[A]](stub: A, token: Option[String]): A =
    token.fold(stub)(authenticatingStub(stub, _))

  def singleHost(
      hostIp: String,
      port: Int,
      token: Option[String] = None,
      sslContext: Option[SslContext] = None,
      maxInboundMessageSize: Int = 64 * 1024 * 1024,
  )(implicit
      ec: ExecutionContext
  ): AdminLedgerClient =
    fromBuilder(
      LedgerClientChannelConfiguration(sslContext, maxInboundMessageSize).builderFor(hostIp, port),
      token
    )

  def fromBuilder(
      builder: NettyChannelBuilder,
      token: Option[String] = None,
  )(implicit ec: ExecutionContext): AdminLedgerClient =
    new AdminLedgerClient(
      GrpcChannel.withShutdownHook(builder),
      token,
    )
}
