// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apitesting

import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.testing.utils.Resource
import com.digitalasset.platform.PlatformApplications.RemoteApiEndpoint
import com.digitalasset.platform.apitesting.LedgerContext.MultiChannelContext
import com.digitalasset.platform.common.LedgerIdMode

object MultiRemoteServerResource {
  def fromMapping(
      mapping: Map[Option[String], RemoteApiEndpoint],
      configuredLedgerId: LedgerIdMode,
      packageIds: List[PackageId])(
      implicit esf: ExecutionSequencerFactory): MultiRemoteServerResource = {
    val hostMapping = mapping
      .foldRight[Map[Option[String], RemoteServerResource]](Map.empty) {
        case ((party, endpoint), map) =>
          map + (party -> RemoteServerResource(endpoint.host, endpoint.port, None))
      }
    new MultiRemoteServerResource(hostMapping, configuredLedgerId, packageIds)
  }
}

class MultiRemoteServerResource(
    val mapping: Map[Option[String], RemoteServerResource], // maps party name to a remote Ledger API endpoint resource
    val configuredLedgerId: LedgerIdMode,
    val packageIds: List[PackageId])(implicit val esf: ExecutionSequencerFactory)
    extends Resource[LedgerContext] {

  @volatile
  private var multiLedgerContext: MultiChannelContext = _

  /**
    * Access the resource.
    */
  override def value: LedgerContext = multiLedgerContext

  /**
    * Initialize the resource.
    */
  override def setup(): Unit = {
    val m: Map[Option[String], LedgerContext] = mapping.transform { (_, server) =>
      {
        server.setup()
        server.value match {
          case PlatformChannels(channel) =>
            LedgerContext.SingleChannelContext(channel, configuredLedgerId, packageIds)
        }
      }
    }
    multiLedgerContext = new MultiChannelContext(m)
  }

  /** Dispose of the resource */
  override def close(): Unit = {
    multiLedgerContext = null
    mapping.foreach(_._2.close())
  }

}
