// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api

import cats.syntax.functor.*
import com.digitalasset.canton.config.{DbConfig, H2DbConfig, PostgresDbConfig}
import com.digitalasset.canton.participant.ledger.api.CantonLedgerApiServerWrapper.FailedToConfigureLedgerApiStorage
import com.digitalasset.canton.participant.ledger.api.LedgerApiStorage.ledgerApiSchemaName
import com.typesafe.config.{Config, ConfigObject, ConfigValueType}

import java.net.URLEncoder
import scala.jdk.CollectionConverters.SetHasAsScala

/** Canton's storage is configured using Slick's configuration.
  * This offers an expansive set of options allowing you to configure a connection pool, data source, or JDBC driver.
  * The ledger-api configuration however only takes a JDBC url.
  * LedgerApiJdbcUrl generator attempts to scrape together enough from Slick's configuration to generate
  * a jdbc url for the targeted database (currently either H2 or Postgres).
  * It also adds a schema specification to point the ledger-api server at a distinct schema from canton allowing
  * it to be managed separately.
  * Although it is expected a JDBC url will be generated for almost all circumstances, if this process fails or an
  * advanced configuration is required the [[com.digitalasset.canton.config.DbParametersConfig.ledgerApiJdbcUrl]] configuration can be explicitly set
  * and this will be used instead. This manually configured url **must** specify using the schema of `ledger_api`.
  */
object LedgerApiJdbcUrl {

  // DB property keys not passed as url parameter in the jdbc url
  private val serverNameKey = "serverName"
  private val portNumberKey = "portNumber"
  private val userKey = "user"
  private val passwordKey = "password"
  private val databaseNameKey = "databaseName"
  private val currentSchema = "currentSchema"

  private val nonParametersProperties = Set(
    serverNameKey,
    portNumberKey,
    userKey,
    passwordKey,
    databaseNameKey,
  )

  def fromDbConfig(
      dbConfig: DbConfig
  ): Either[FailedToConfigureLedgerApiStorage, LedgerApiJdbcUrl] = {

    def generate =
      (dbConfig match {
        case h2: H2DbConfig => reuseH2(h2.config)
        case postgres: PostgresDbConfig => reusePostgres(postgres.config)
        case other: DbConfig =>
          other.parameters.ledgerApiJdbcUrl.map(CustomLedgerApiUrl).toRight("No URL specified")
      }).left.map(FailedToConfigureLedgerApiStorage)

    // In the unlikely event we've explicitly specified the jdbc url for the ledger-api just use that.
    // Otherwise generate an appropriate one for the datastore.
    dbConfig.parameters.ledgerApiJdbcUrl.fold(generate)(url => Right(CustomLedgerApiUrl(url)))
  }

  /** Extensions to [[com.typesafe.config.Config]] to make config extraction more concise for our purposes. */
  private implicit class ConfigExtensions(config: Config) {

    /** Read a string value from either the main config or the properties config within it. */
    def getDbConfig(key: String): Option[String] =
      config
        .getOptionalString(key)
        .orElse(getDbProperties.flatMap(_.getOptionalString(key)))

    /** Read the properties configuration */
    def getDbProperties: Option[Config] = config.getOptionalConfig("properties")

    /** Read the properties configuration as an object */
    def getDbPropertiesObject: Option[ConfigObject] = config.getOptionalConfigObject("properties")

    /** Read a string value only from the driver properties */
    def getDbProperty(key: String): Option[String] =
      config.getDbProperties.flatMap(_.getOptionalString(key))

    def getOptionalInt(key: String): Option[Int] =
      if (config.hasPath(key)) Some(config.getInt(key))
      else None

    def getOptionalString(key: String): Option[String] =
      if (config.hasPath(key)) Some(config.getString(key))
      else None

    def getOptionalConfigObject(key: String): Option[ConfigObject] =
      if (config.hasPath(key)) Some(config.getObject(key))
      else None

    def getOptionalConfig(key: String): Option[Config] =
      if (config.hasPath(key)) Some(config.getConfig(key))
      else None
  }

  private def reuseH2(h2Config: Config): Either[String, LedgerApiJdbcUrl] =
    for {
      cantonUrl <- DbConfig
        .writeH2UrlIfNotSet(h2Config)
        .getDbConfig(
          "url"
        ) toRight "h2 configuration url not found or generated."
    } yield {
      val h2CantonUrl = UrlBuilder
        .forH2(cantonUrl)
        .addIfMissing(userKey, h2Config.getDbConfig(userKey))
        .addIfMissing(passwordKey, h2Config.getDbConfig(passwordKey))
        .build

      val ledgerApiUrl = UrlBuilder
        .forH2(cantonUrl)
        .addIfMissing(userKey, h2Config.getDbConfig(userKey))
        .addIfMissing(passwordKey, h2Config.getDbConfig(passwordKey))
        .replace("schema", ledgerApiSchemaName)
        .build

      ReuseCantonDb(ledgerApiUrl, h2CantonUrl)
    }

  def reusePostgres(pgConfig: Config): Either[String, LedgerApiJdbcUrl] =
    for {
      cantonUrl <- pgConfig
        .getDbConfig("url")
        .fold(generatePostgresUrl(pgConfig))(Right(_))
    } yield {

      val additionalProperties = pgConfig.getDbPropertiesObject
        .map(
          _.entrySet().asScala
            .filterNot(e => nonParametersProperties.contains(e.getKey))
            .foldLeft(Map.empty[String, String])({
              // All properties should be string anyways, but to avoid bad surprises, filter for them here
              case (parametersMap, configEntry)
                  if configEntry.getValue.valueType() == ConfigValueType.STRING =>
                parametersMap.updated(configEntry.getKey, configEntry.getValue.unwrapped.toString)
              case (parametersMap, _) => parametersMap
            })
        )
        .getOrElse(Map.empty[String, String])

      val pgCantonUrl = UrlBuilder
        .forPostgres(cantonUrl)
        .addIfMissing(userKey, pgConfig.getDbConfig(userKey))
        .addIfMissing(passwordKey, pgConfig.getDbConfig(passwordKey))
        .addAll(additionalProperties)
        .build

      val ledgerApiUrl = UrlBuilder
        .forPostgres(cantonUrl)
        .addIfMissing(userKey, pgConfig.getDbConfig(userKey))
        .addIfMissing(passwordKey, pgConfig.getDbConfig(passwordKey))
        .addAll(additionalProperties)
        .replace(currentSchema, ledgerApiSchemaName)
        .build

      ReuseCantonDb(ledgerApiUrl, pgCantonUrl)
    }

  /** Generate the simplest postgres url we can create */
  private def generatePostgresUrl(pgConfig: Config): Either[String, String] = {
    // details on jdbc urls can be found here: https://jdbc.postgresql.org/documentation/head/connect.html
    val host = pgConfig.getDbProperty(serverNameKey)
    val port = pgConfig.getDbProperties
      .flatMap(_.getOptionalInt(portNumberKey))
    val database = pgConfig.getDbProperty(databaseNameKey)

    /* Mirror generating the base jdbc url using the required forms from the docs above:
      - jdbc:postgresql:database
      - jdbc:postgresql:/
      - jdbc:postgresql://host/database
      - jdbc:postgresql://host/
      - jdbc:postgresql://host:port/database
      - jdbc:postgresql://host:port/
     */
    ((host, port, database) match {
      case (None, None, Some(database)) => Right(database)
      case (None, None, None) => Right("/")
      case (Some(host), None, Some(database)) => Right(s"//$host/$database")
      case (Some(host), None, None) => Right(s"//$host/")
      case (Some(host), Some(port), Some(database)) => Right(s"//$host:$port/$database")
      case (Some(host), Some(port), None) => Right(s"//$host:$port/")
      case _ =>
        val specifiedFields = Seq(
          host.map(_ => "host").toList, // toList is added to avoid a wart. pfft.
          port.map(_ => "port").toList,
          database.map(_ => "database").toList,
        ).flatten
        Left(
          s"Could not generate a postgres jdbc url from the specified fields: [${specifiedFields.mkString(",")}]"
        )
    }) map ("jdbc:postgresql:" + _) // add the common prefix to all successfully generated urls
  }

  private final case class UrlBuilder(
      baseUrl: String,
      options: Map[String, String],
      format: JdbcUrlFormat,
  ) {
    // these options are case in-sensitive
    def isDefined(key: String): Boolean =
      options.keySet.find(_.equalsIgnoreCase(key)).fold(false)(_ => true)

    def replace(key: String, value: String): UrlBuilder =
      // remove any existing values with this key and add the provided
      copy(options = options.filterNot(_._1.equalsIgnoreCase(key)) + (key -> value))

    def addIfMissing(key: String, defaultValue: => Option[String]): UrlBuilder = {
      if (isDefined(key)) this
      else
        defaultValue
          // JDBC is just an URL, so we need to URL encode any option we get here
          // https://stackoverflow.com/questions/13984567/how-to-escape-special-characters-in-mysql-jdbc-connection-string
          .map(v => copy(options = options + (key -> URLEncoder.encode(v, "utf-8"))))
          .getOrElse(this)
    }

    def addAll(values: Map[String, String]): UrlBuilder = {
      copy(options = options ++ values.fmap(URLEncoder.encode(_, "utf-8")))
    }

    def build: String = {
      val JdbcUrlFormat(queryStringSeparator, parameterSeparator) = format
      val newOptionsText =
        options.map { case (key, value) => s"$key=$value" }.mkString(format.parameterSeparator)
      if (baseUrl.contains(queryStringSeparator)) {
        if (baseUrl.endsWith(queryStringSeparator) || baseUrl.endsWith(parameterSeparator))
          s"$baseUrl$newOptionsText"
        else s"$baseUrl$parameterSeparator$newOptionsText"
      } else s"$baseUrl$queryStringSeparator$newOptionsText"
    }
  }

  private final case class JdbcUrlFormat(queryStringSeparator: String, parameterSeparator: String)
  private object JdbcUrlFormat {
    val h2: JdbcUrlFormat = JdbcUrlFormat(";", ";")
    val postgres: JdbcUrlFormat = JdbcUrlFormat("?", "&")
  }

  private object UrlParser {
    def parse(url: String, format: JdbcUrlFormat): (String, Map[String, String]) = {
      val queryStringIndex = url.indexOf(format.queryStringSeparator)

      if (queryStringIndex >= 0)
        (
          url.substring(0, queryStringIndex),
          parseQueryString(url.substring(queryStringIndex + 1), format),
        )
      else (url, Map.empty)
    }

    /** parse out the options currently set.
      * Based on the simple postgres parsing in org.postgresql.Driver.parseURL.
      * The h2 parsing is similar but performs validation as it goes (org.h2.engine.ConnectionInfo.readSettingsFromURL),
      * however we won't mirror this given it'll be loaded by the driver eventually anyway.
      */
    private def parseQueryString(
        queryString: String,
        format: JdbcUrlFormat,
    ): Map[String, String] = {
      queryString
        .split(format.parameterSeparator)
        .map(param => {
          val eqIndex = param.indexOf('=')

          if (eqIndex >= 0) param.substring(0, eqIndex) -> param.substring(eqIndex + 1)
          else param -> ""
        })
        .toMap // if a key is duplicated the last one will win, this matches the behavior of the postgres driver
    }
  }

  private object UrlBuilder {
    def forH2(url: String): UrlBuilder =
      mkBuilder(url, JdbcUrlFormat.h2)

    def forPostgres(url: String): UrlBuilder =
      mkBuilder(url, JdbcUrlFormat.postgres)

    private def mkBuilder(url: String, format: JdbcUrlFormat): UrlBuilder = {
      val (baseUrl, options) = UrlParser.parse(url, format)
      UrlBuilder(baseUrl, options, format)
    }
  }

  private final case class CustomLedgerApiUrl(override val url: String) extends LedgerApiJdbcUrl {
    override def createLedgerApiSchemaIfNotExists(): Unit = ()
  }

  private final case class ReuseCantonDb(override val url: String, private val cantonUrl: String)
      extends LedgerApiJdbcUrl {
    override def createLedgerApiSchemaIfNotExists(): Unit =
      DbActions.createSchemaIfNotExists(cantonUrl)
  }
}

sealed trait LedgerApiJdbcUrl {
  def url: String
  def createLedgerApiSchemaIfNotExists(): Unit
}
