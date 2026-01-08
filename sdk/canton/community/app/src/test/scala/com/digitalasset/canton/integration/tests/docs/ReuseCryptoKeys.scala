// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.docs

import cats.implicits.catsSyntaxOptionId
import com.digitalasset.canton.config.IdentityConfig
import com.digitalasset.canton.console.LocalInstanceReference
import com.digitalasset.canton.crypto.SigningKeyUsage
import com.digitalasset.canton.integration.{
  ConfigTransform,
  ConfigTransforms,
  EnvironmentDefinition,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.topology.UniqueIdentifier
import monocle.macros.syntax.lens.*

/** Helper utility to allow reusing crypto keys for documentation purposes
  *
  * When editing docs, we often generate new crypto keys. This method here allows to manually
  * configure the keys so that the identity remains unchanged.
  */
object ReuseCryptoKeys {

  def transformEnvironmentDefinition(
      definition: EnvironmentDefinition
  )(implicit errorLoggingContext: ErrorLoggingContext): EnvironmentDefinition =
    definition
      .addConfigTransform(ReuseCryptoKeys.configTransform)
      .withManualStart
      .withSetup(setupAllNodes(_, errorLoggingContext))

  /** Config transforms to set to manual init */
  def configTransform: ConfigTransform =
    ConfigTransforms.updateAllMediatorConfigs_(
      _.focus(_.init.identity).replace(IdentityConfig.Manual)
    ) compose
      ConfigTransforms.updateAllSequencerConfigs_(
        _.focus(_.init.identity).replace(IdentityConfig.Manual)
      ) compose
      ConfigTransforms.updateAllParticipantConfigs_(
        _.focus(_.init.identity).replace(IdentityConfig.Manual)
      )

  def setupNode(
      node: LocalInstanceReference
  )(implicit errorLoggingContext: ErrorLoggingContext): Unit = {
    val logger = errorLoggingContext.noTracingLogger
    node.start()
    if (!node.is_initialized) {
      // check if key of this node already exists
      val file =
        new java.io.File("community/app/src/test/resources/docs/keys/" + node.name + ".docs.key")
      val keyName = s"${node.name}-${SigningKeyUsage.Namespace.identifier}"
      val namespace = if (!file.exists()) {
        logger.info("Generating new key for " + node.name + " as " + keyName)
        val key = node.keys.secret.generate_signing_key(
          keyName,
          SigningKeyUsage.NamespaceOnly,
        )
        node.keys.secret.download_to(key.fingerprint, file.getPath)
        key.fingerprint
      } else {
        logger.info("Uploading key for " + node.name + " as " + keyName + " from " + file)
        node.keys.secret.upload_from(file.getPath, keyName.some)
        val key = node.keys.secret
          .list(filterName = keyName)
          .headOption
          .getOrElse(throw new Exception("Key not found " + keyName))
        key.publicKey.fingerprint
      }
      val prefix =
        node.config.init.identity match {
          case IdentityConfig.Manual =>
            node.name
          case IdentityConfig.External(identifier, _, _) =>
            identifier
          case IdentityConfig.Auto(identifier) =>
            identifier.identifierName.getOrElse(node.name)
        }
      node.topology.init_id_from_uid(UniqueIdentifier.tryCreate(prefix, namespace))
    }
  }

  def setupAllNodes(implicit
      env: TestConsoleEnvironment,
      errorLoggingContext: ErrorLoggingContext,
  ): Unit = {
    env.sequencers.local.foreach(setupNode)
    env.mediators.local.foreach(setupNode)
    env.participants.local.foreach(setupNode)
  }

}
