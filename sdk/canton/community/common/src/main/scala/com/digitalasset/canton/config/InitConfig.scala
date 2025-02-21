// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.config
import com.digitalasset.canton.config.InitConfigBase.Identity
import com.digitalasset.canton.config.manual.CantonConfigValidatorDerivation
import com.typesafe.config.ConfigValueFactory
import pureconfig.ConfigWriter

import java.io.File
import java.util.UUID
import scala.jdk.CollectionConverters.MapHasAsJava

object InitConfigBase {
  def writerForSubtype[A <: InitConfigBase](configWriter: ConfigWriter[A]): ConfigWriter[A] =
    ConfigWriter.fromFunction[A]({ initConfig =>
      configWriter
        .mapConfig {
          // Write auto-init explicitly, as it's not an attribute of the InitConfigBase class anymore
          case configValue if !initConfig.autoInit =>
            configValue.withFallback(
              ConfigValueFactory.fromMap(
                Map("auto-init" -> ConfigValueFactory.fromAnyRef(false)).asJava
              )
            )
          case configValue => configValue
        }
        .to(initConfig)
    })

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

  /** Identity related init config If set, the node will automatically initialize itself. In
    * particular, it will create a new namespace, and initialize its member id and its keys for
    * signing and encryption. If not set, the user has to manually perform these steps.
    * @param generateLegalIdentityCertificate
    *   If true create a signing key and self-signed certificate that is submitted as legal identity
    *   to the synchronizer.
    * @param nodeIdentifier
    *   Controls the identifier that will be assigned to the node during auto-init
    */
  final case class Identity(
      generateLegalIdentityCertificate: Boolean = false,
      nodeIdentifier: NodeIdentifierConfig = NodeIdentifierConfig.Config,
  ) extends UniformCantonConfigValidation

  object Identity {
    implicit val identityCantonConfigValidator: CantonConfigValidator[Identity] =
      CantonConfigValidatorDerivation[Identity]
  }
}

/** Control the dynamic state of the node through a state configuration file
  *
  * @param file
  *   which file to read the state from
  * @param refreshInterval
  *   how often to check the file for changes
  * @param consistencyTimeout
  *   how long to wait for the changes to be successfully applied
  */
final case class StateConfig(
    file: File,
    refreshInterval: config.NonNegativeFiniteDuration =
      config.NonNegativeFiniteDuration.ofSeconds(5),
    consistencyTimeout: config.NonNegativeDuration = config.NonNegativeDuration.ofMinutes(1),
) extends UniformCantonConfigValidation

object StateConfig {
  implicit val stateConfigCantonConfigValidator: CantonConfigValidator[StateConfig] =
    CantonConfigValidatorDerivation[StateConfig]
}

trait InitConfigBase {
  def identity: Option[Identity]
  def autoInit: Boolean = identity.isDefined
  def state: Option[StateConfig]
}

/** Configuration for the node's init process
  * @param identity
  *   Controls how the node identity (prefix of the unique identifier) is determined
  */
final case class InitConfig(
    identity: Option[Identity] = Some(Identity()),
    state: Option[StateConfig] = None,
) extends InitConfigBase
    with UniformCantonConfigValidation

object InitConfig {
  implicit val initConfigCantonConfigValidator: CantonConfigValidator[InitConfig] =
    CantonConfigValidatorDerivation[InitConfig]
}
