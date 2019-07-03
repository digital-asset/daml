// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apitesting

import java.io.File

import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.testing.utils.Resource
import com.digitalasset.platform.apitesting.LedgerContext.MultiChannelContext
import com.digitalasset.platform.common.LedgerIdMode
import com.typesafe.config.{ConfigFactory, ConfigObject}

import scala.collection.JavaConverters._

object MultiRemoteServerResource {
  def fromConfig(
      file: File,
      defaultParty: String,
      configuredLedgerId: LedgerIdMode,
      packageIds: List[PackageId])(
      implicit esf: ExecutionSequencerFactory): MultiRemoteServerResource = {
    val hostMappingCfg = ConfigFactory.parseFile(file)
    val hostMapping = hostMappingCfg
      .root()
      .entrySet()
      .asScala
      .foldRight[Map[String, RemoteServerResource]](Map.empty) {
        case (entry, map) =>
          // using this config API, cannot see any other way to get inner object
          val cfg = entry.getValue.asInstanceOf[ConfigObject].toConfig
          val host = cfg.getString("host")
          val port = cfg.getInt("port")
          map + (entry.getKey -> RemoteServerResource(host, port, None))
      }
    new MultiRemoteServerResource(hostMapping, defaultParty, configuredLedgerId, packageIds)
  }
}

class MultiRemoteServerResource(
    val mapping: Map[String, RemoteServerResource],
    val defaultParty: String,
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
    multiLedgerContext = new MultiChannelContext(
      mapping.mapValues { server =>
        {
          server.setup()
          server.value match {
            case PlatformChannels(channel) =>
              LedgerContext.SingleChannelContext(channel, configuredLedgerId, packageIds)
          }
        }
      },
      defaultParty
    )
  }

  /** Dispose of the resource */
  override def close(): Unit = {
    multiLedgerContext = null
    mapping.foreach {
      case (_, server) =>
        server.close()
    }
  }

}
