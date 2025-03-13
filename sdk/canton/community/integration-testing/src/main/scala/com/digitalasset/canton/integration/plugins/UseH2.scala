// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins

import com.digitalasset.canton.config.{CantonConfig, DbConfig}
import com.digitalasset.canton.integration.{
  ConfigTransforms,
  EnvironmentSetupPlugin,
  StorageConfigTransform,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.store.db.DbStorageSetup.DbBasicConfig
import org.h2.tools.Server

/** Integration test plugin for using a H2 database. Updates the storage config of all members to
  * use H2.
  */
@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
class UseH2(protected val loggerFactory: NamedLoggerFactory) extends EnvironmentSetupPlugin {

  var server: Server = _

  override def beforeEnvironmentCreated(config: CantonConfig): CantonConfig =
    ConfigTransforms.modifyAllStorageConfigs { case (nodeType, name, storage) =>
      UseH2.h2Transform(nodeType, name, storage)
    }(config)
}

object UseH2 {
  private val h2Transform: StorageConfigTransform = { (_, nodeName, storage) =>
    val dbName = ConfigTransforms.generateUniqueH2DatabaseName(nodeName)
    DbConfig.H2(
      config =
        DbBasicConfig(nodeName, "pass", dbName, "", 0, connectionPoolEnabled = true).toH2Config,
      parameters = storage.parameters,
    )
  }
}
