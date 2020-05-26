// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.test

import java.net.URL

import com.daml.navigator.test.config.Arguments
import com.typesafe.scalalogging.LazyLogging
import io.circe.parser.decode
import io.circe.generic.auto._
import org.apache.commons.codec.binary.Base64

import scala.io.Source
import scala.util.{Failure, Success, Try}

case class BuildInfo(
    automation_build: AutomationBuildInfo
)
case class AutomationBuildInfo(
    name: String,
    hashed_id: String,
    duration: Option[Int],
    status: Option[String]
)
case class SessionInfo(
    automation_session: AutomationSessionInfo
)
case class AutomationSessionInfo(
    name: String,
    hashed_id: String,
    duration: Option[Int],
    os: Option[String],
    os_version: Option[String],
    browser_version: Option[String],
    browser: Option[String],
    device: Option[String],
    status: Option[String],
    reason: Option[String],
    build_name: Option[String],
    project_name: Option[String],
    logs: Option[String],
    browser_url: Option[String],
    public_url: Option[String],
    appium_logs_url: Option[String],
    video_url: Option[String],
    browser_console_logs_url: Option[String],
    har_logs_url: Option[String]
)

object HttpBasicAuth {
  val BASIC = "Basic";
  val AUTHORIZATION = "Authorization";

  def encodeCredentials(username: String, password: String): String = {
    new String(Base64.encodeBase64String((username + ":" + password).getBytes));
  };

  def getHeader(username: String, password: String): String =
    BASIC + " " + encodeCredentials(username, password);
};

object BrowserStack extends LazyLogging {

  private val apiBase = "https://api.browserstack.com/automate"

  private def getBuildId(builds: List[BuildInfo], buildName: String): Try[String] =
    builds
      .find(bi => bi.automation_build.name == buildName)
      .map(bi => bi.automation_build.hashed_id)
      .map(Success(_))
      .getOrElse(Failure(new Exception(s"Can't find build info '$buildName'")))

  private def getSession(sessions: List[SessionInfo], sessionName: String): Try[SessionInfo] =
    sessions
      .find(si => si.automation_session.name == sessionName)
      .map(Success(_))
      .getOrElse(Failure(new Exception(s"Can't find session info '$sessionName'")))

  private def downloadWithAuth(url: String, username: String, password: String): Try[String] = Try {
    val connection = new URL(url).openConnection
    connection
      .setRequestProperty(HttpBasicAuth.AUTHORIZATION, HttpBasicAuth.getHeader(username, password))
    val result = Source.fromInputStream(connection.getInputStream).mkString
    logger.debug(result)
    result
  }

  def printLog(args: Arguments, buildName: String, sessionName: String): Unit = {
    logger.info("Getting BrowserStack log")
    val username = args.browserStackUser
    val password = args.browserStackKey

    val public_url = for {
      buildsJson <- downloadWithAuth(s"$apiBase/builds.json", username, password)
      builds <- decode[List[BuildInfo]](buildsJson).toTry
      buildId <- getBuildId(builds, buildName)
      sessionsJson <- downloadWithAuth(
        s"$apiBase/builds/$buildId/sessions.json",
        username,
        password)
      sessions <- decode[List[SessionInfo]](sessionsJson).toTry
      session <- getSession(sessions, sessionName)
    } yield {
      session.automation_session.public_url.getOrElse("N/A")
    }

    public_url.fold(
      e => logger.error(s"Failed to get BrowserStack log: '$e'"),
      url => logger.info(s"BrowserStack log for this session: $url")
    )
  }
}
