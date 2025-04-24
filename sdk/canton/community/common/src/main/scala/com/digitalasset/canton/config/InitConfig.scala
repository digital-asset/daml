// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.config.InitConfigBase.NodeIdentifierConfig
import com.digitalasset.canton.config.manual.CantonConfigValidatorDerivation

import java.io.File
import java.util.UUID

object InitConfigBase {

  sealed trait NodeIdentifierConfig {
    def identifierName: Option[String]
  }
  object NodeIdentifierConfig {
    implicit val nodeIdentifierConfigCantonConfigValidator
        : CantonConfigValidator[NodeIdentifierConfig] =
      CantonConfigValidatorDerivation[NodeIdentifierConfig]

    /** Generates a random UUID as a name for the node identifier
      */
    case object Random extends NodeIdentifierConfig with UniformCantonConfigValidation {
      lazy val identifierName: Option[String] = Some(UUID.randomUUID().toString)
    }

    /** Sets an explicit name for the node identifier
      * @param name
      *   name to use as identifier
      */
    final case class Explicit(name: String)
        extends NodeIdentifierConfig
        with UniformCantonConfigValidation {
      override val identifierName: Option[String] = Some(name)
    }

    /** Uses the node name from the configuration as identifier (default)
      */
    case object Config extends NodeIdentifierConfig with UniformCantonConfigValidation {
      override val identifierName: Option[String] = None
    }
  }

}

/** Control how the identity of the node is determined.
  *
  * At startup, we need to determine the node's identity. We support various modes of operations for
  * different deployments.
  *
  * We distinguish between setting the node-id and subsequently generating the topology transactions
  * and the remaining keys. As such it is possible to manually set the node-id but let the system
  * automatically generate the remaining topology transactions.
  */
sealed trait IdentityConfig {
  def isManual: Boolean
}

object IdentityConfig {

  /** Read the node identifier from the config file
    *
    * @param identifier
    *   the string to use as identifier of the node
    * @param namespace
    *   optional fingerprint to use as the namespace (default is to extract it from the first
    *   certificate)
    * @param certificates
    *   optional certificates to use (in case we have external root keys)
    */
  final case class External(
      identifier: String,
      namespace: Option[String] = None,
      certificates: Seq[File] = Seq.empty,
  ) extends IdentityConfig
      with UniformCantonConfigValidation {
    def isManual: Boolean = false
  }

  /** Automatically generate a namespace key and initialize the node id */
  final case class Auto(
      identifier: NodeIdentifierConfig = NodeIdentifierConfig.Config
  ) extends IdentityConfig
      with UniformCantonConfigValidation {
    def isManual: Boolean = false
  }

  /** Manually wait for the node-id to be configured via API */
  final case object Manual extends IdentityConfig with UniformCantonConfigValidation {
    def isManual: Boolean = true
  }

  implicit val identityConfigCantonConfigValidator: CantonConfigValidator[IdentityConfig] =
    CantonConfigValidatorDerivation[IdentityConfig]

}

trait InitConfigBase {
  def identity: IdentityConfig
  def generateIntermediateKey: Boolean
  def generateTopologyTransactionsAndKeys: Boolean
}

/** Configuration for the node's init process
  * @param identity
  *   Controls how the node identity (prefix of the unique identifier) is determined
  * @param generateIntermediateKey
  *   If true (default false), then the node will generate an additional intermediate key. This
  *   allows to turn off access to the root key for the node.
  * @param generateTopologyTransactionsAndKeys
  *   If true (default), then the node will generate automatically the topology transactions and
  *   keys once it has the necessary namespace delegations to do so.
  */
final case class InitConfig(
    identity: IdentityConfig = IdentityConfig.Auto(),
    generateIntermediateKey: Boolean = false,
    generateTopologyTransactionsAndKeys: Boolean = true,
) extends InitConfigBase
    with UniformCantonConfigValidation

object InitConfig {
  implicit val initConfigCantonConfigValidator: CantonConfigValidator[InitConfig] =
    CantonConfigValidatorDerivation[InitConfig]
}
