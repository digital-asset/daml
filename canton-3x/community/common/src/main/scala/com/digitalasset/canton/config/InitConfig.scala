// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.config.InitConfigBase.Identity

import java.util.UUID

object InitConfigBase {
  sealed trait NodeIdentifierConfig {
    def identifierName: Option[String]
  }
  object NodeIdentifierConfig {

    /** Generates a random UUID as a name for the node identifier
      */
    case object Random extends NodeIdentifierConfig {
      lazy val identifierName: Option[String] = Some(UUID.randomUUID().toString)
    }

    /** Sets an explicit name for the node identifier
      * @param name name to use as identifier
      */
    final case class Explicit(name: String) extends NodeIdentifierConfig {
      override val identifierName: Option[String] = Some(name)
    }

    /** Uses the node name from the configuration as identifier (default)
      */
    case object Config extends NodeIdentifierConfig {
      override val identifierName: Option[String] = None
    }
  }

  /** Identity related init config
    * If set, the node will automatically initialize itself.
    *   In particular, it will create a new namespace, and initialize its member id and its keys for signing and encryption.
    * If not set, the user has to manually perform these steps.
    * @param generateLegalIdentityCertificate If true create a signing key and self-signed certificate that is submitted as legal identity to the domain.
    * @param nodeIdentifier Controls the identifier that will be assigned to the node during auto-init
    */
  final case class Identity(
      generateLegalIdentityCertificate: Boolean = false,
      nodeIdentifier: NodeIdentifierConfig = NodeIdentifierConfig.Config,
  )
}

trait InitConfigBase {
  def identity: Option[Identity]
  def autoInit: Boolean = identity.isDefined
}

/** Configuration for the node's init process
  * @param identity Controls how the node identity (prefix of the unique identifier) is determined
  */
final case class InitConfig(
    identity: Option[Identity] = None
) extends InitConfigBase
