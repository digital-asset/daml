package com.digitalasset.platform.apitesting

import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.lfpackage.Ast
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.testing.utils.Resource

object SingleRemoteServerResource {
  def fromHostAndPort(host: String, port: Int, packages: Map[PackageId, Ast.Package], _esf: ExecutionSequencerFactory): SingleRemoteServerResource =
    new SingleRemoteServerResource(RemoteServerResource(host, port, None), packages, _esf)
}

class SingleRemoteServerResource(val ledgerResource: RemoteServerResource,
                                 val packages: Map[PackageId, Ast.Package],
                                 implicit val _esf: ExecutionSequencerFactory) extends Resource[LedgerContext] {
  @volatile
  private var ledgerContext: LedgerContext = _

  /**
    * Access the resource.
    */
  override def value: LedgerContext = ledgerContext

  /**
    * Initialize the resource.
    */
  override def setup(): Unit = {
    ledgerResource.setup()
    ledgerContext = ledgerResource.map {
            case PlatformChannels(channel) =>
              LedgerContext.SingleChannelContext(channel, None, packages.keys)
          }.value
  }

  /** Dispose of the resource */
  override def close(): Unit = {
    ledgerResource.close()
  }

}
