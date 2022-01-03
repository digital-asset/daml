// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation.app

import java.net.InetSocketAddress

private[app] object OptionParser extends scopt.OptionParser[Configuration](NonRepudiationApp.Name) {

  head(NonRepudiationApp.Name)

  opt[String]("ledger-host")
    .required()
    .action(setLedgerHost)
    .text("The host address of the participant. Required.")

  opt[String]("ledger-port")
    .required()
    .action(setLedgerPort)
    .text("The port of the participant. Required.")

  opt[Map[String, String]]("jdbc")
    .required()
    .action(setJdbcConfiguration)
    .text(
      """Contains comma-separated key-value pairs. Where:
      |        url -- JDBC connection URL, beginning with jdbc:postgresql,
      |        user -- user name for database user with permissions to create tables,
      |        password -- password of database user,
      |Example: "url=jdbc:postgresql://localhost:5432/nonrepudiation,user=nonrepudiation,password=secret"
      |""".stripMargin
    )

  opt[String]("proxy-host")
    .action(setProxyHost)
    .text(
      s"The interface over which the proxy will be exposed. Defaults to ${Configuration.Default.proxyAddress.getAddress}."
    )

  opt[String]("proxy-port")
    .action(setProxyPort)
    .text(
      s"The port over which the proxy will be exposed. Defaults to ${Configuration.Default.proxyAddress.getPort}."
    )

  opt[String]("api-host")
    .action(setApiHost)
    .text(
      s"The interface over which the non-repudiation API will be exposed. Defaults to ${Configuration.Default.apiAddress.getAddress}."
    )

  opt[String]("api-port")
    .action(setApiPort)
    .text(
      s"The port over which the non-repudiation API will be exposed. Defaults to ${Configuration.Default.apiAddress.getPort}."
    )

  opt[Int]("database-max-pool-size")
    .action((size, configuration) => configuration.copy(databaseMaxPoolSize = size))
    .text(
      s"The maximum number of pooled connections to the non-repudiation database. Defaults to ${Configuration.Default.databaseMaxPoolSize}."
    )

  // TODO Add metrics reporting configuration once there is an alternative to SLF4J

  private def setLedgerHost(host: String, configuration: Configuration): Configuration =
    configuration.copy(participantAddress = setHost(host, configuration.participantAddress))

  private def setLedgerPort(port: String, configuration: Configuration): Configuration =
    configuration.copy(participantAddress = setPort(port, configuration.participantAddress))

  private def setProxyHost(host: String, configuration: Configuration): Configuration =
    configuration.copy(proxyAddress = setHost(host, configuration.proxyAddress))

  private def setProxyPort(port: String, configuration: Configuration): Configuration =
    configuration.copy(proxyAddress = setPort(port, configuration.proxyAddress))

  private def setApiHost(host: String, configuration: Configuration): Configuration =
    configuration.copy(apiAddress = setHost(host, configuration.apiAddress))

  private def setApiPort(port: String, configuration: Configuration): Configuration =
    configuration.copy(apiAddress = setPort(port, configuration.apiAddress))

  private def setHost(host: String, address: InetSocketAddress): InetSocketAddress =
    new InetSocketAddress(host, address.getPort)

  private def setPort(port: String, address: InetSocketAddress): InetSocketAddress =
    new InetSocketAddress(address.getAddress, port.toInt)

  private def setJdbcConfiguration(
      jdbc: Map[String, String],
      configuration: Configuration,
  ): Configuration =
    configuration.copy(
      databaseJdbcUrl = jdbc("url"),
      databaseJdbcUsername = jdbc("user"),
      databaseJdbcPassword = jdbc("password"),
    )

}
