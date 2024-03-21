// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.auth.middleware.oauth2

import java.nio.file.Path
import java.util.UUID

import org.apache.pekko.http.scaladsl.model.Uri
import com.daml.auth.middleware.api.Request
import com.daml.auth.middleware.api.Tagged.RefreshToken

import scala.collection.concurrent.TrieMap
import scala.io.{BufferedSource, Source}
import scala.util.Try

private[oauth2] class RequestTemplates(
    clientId: String,
    clientSecret: SecretString,
    authTemplate: Option[Path],
    tokenTemplate: Option[Path],
    refreshTemplate: Option[Path],
) {

  private val authResourcePath: String = "auth0_request_authorization.jsonnet"
  private val tokenResourcePath: String = "auth0_request_token.jsonnet"
  private val refreshResourcePath: String = "auth0_request_refresh.jsonnet"

  /** Load a Jsonnet source file.
    * @param optFilePath Load from this file path, if provided.
    * @param resourcePath Load from this JAR resource, if no file is provided.
    * @return Content and file path (for error reporting) of the loaded Jsonnet file.
    */
  private def jsonnetSource(
      optFilePath: Option[Path],
      resourcePath: String,
  ): (String, sjsonnet.Path) = {
    def readSource(source: BufferedSource): String = {
      try { source.mkString }
      finally { source.close() }
    }
    optFilePath match {
      case Some(filePath) =>
        val content: String = readSource(Source.fromFile(filePath.toString))
        val path: sjsonnet.Path = sjsonnet.OsPath(os.Path(filePath.toAbsolutePath))
        (content, path)
      case None =>
        val resource = getClass.getResource(resourcePath)
        val content: String = readSource(Source.fromInputStream(resource.openStream()))
        // This path is only used for error reporting and a builtin template should not raise any errors.
        // However, if it does it should be clear that the path refers to a builtin file.
        // Paths are reported relative to `$PWD`, we prefix `$PWD` to avoid `../../` noise.
        val path: sjsonnet.Path =
          sjsonnet.OsPath(os.RelPath(s"BUILTIN/$resourcePath").resolveFrom(os.pwd))
        (content, path)
    }
  }

  private val jsonnetParseCache
      : TrieMap[String, fastparse.Parsed[(sjsonnet.Expr, Map[String, Int])]] = TrieMap.empty

  /** Interpret the given Jsonnet code.
    * @param source The Jsonnet source code.
    * @param sourcePath The Jsonnet source file path (for error reporting).
    * @param arguments Top-level arguments to pass to the Jsonnet code.
    * @return The resulting JSON value.
    */
  private def interpretJsonnet(
      source: String,
      sourcePath: sjsonnet.Path,
      arguments: Map[String, ujson.Value],
  ): Try[ujson.Value] = {
    val interp = new sjsonnet.Interpreter(
      jsonnetParseCache,
      Map(),
      arguments,
      sjsonnet.OsPath(os.pwd),
      importer = sjsonnet.SjsonnetMain.resolveImport(Nil, None),
    )
    interp
      .interpret(source, sourcePath)
      .left
      .map(new RequestTemplates.InterpretTemplateException(_))
      .toTry
  }

  /** Convert a JSON value to a string mapping representing request parameters.
    */
  private def toRequestParams(value: ujson.Value): Try[Map[String, String]] =
    Try(value.obj.view.mapValues(_.str).toMap)

  private def createRequest(
      template: (String, sjsonnet.Path),
      args: Map[String, ujson.Value],
  ): Try[Map[String, String]] = {
    val (jsonnet_src, jsonnet_path) = template
    interpretJsonnet(jsonnet_src, jsonnet_path, args).flatMap(toRequestParams)
  }

  private lazy val config: ujson.Value = ujson.Obj(
    "clientId" -> clientId,
    "clientSecret" -> clientSecret.value,
  )

  private lazy val authJsonnetSource: (String, sjsonnet.Path) =
    jsonnetSource(authTemplate, authResourcePath)
  private def authArguments(
      claims: Request.Claims,
      requestId: UUID,
      redirectUri: Uri,
  ): Map[String, ujson.Value] =
    Map(
      "config" -> config,
      "request" -> ujson.Obj(
        "claims" -> ujson.Obj(
          "admin" -> claims.admin,
          "applicationId" -> (claims.applicationId match {
            case Some(appId) => appId
            case None => ujson.Null
          }),
          "actAs" -> claims.actAs,
          "readAs" -> claims.readAs,
        ),
        "redirectUri" -> redirectUri.toString,
        "state" -> requestId.toString,
      ),
    )
  def createAuthRequest(
      claims: Request.Claims,
      requestId: UUID,
      redirectUri: Uri,
  ): Try[Map[String, String]] = {
    createRequest(authJsonnetSource, authArguments(claims, requestId, redirectUri))
  }

  private lazy val tokenJsonnetSource: (String, sjsonnet.Path) =
    jsonnetSource(tokenTemplate, tokenResourcePath)
  private def tokenArguments(code: String, redirectUri: Uri): Map[String, ujson.Value] = Map(
    "config" -> config,
    "request" -> ujson.Obj(
      "code" -> code,
      "redirectUri" -> redirectUri.toString,
    ),
  )
  def createTokenRequest(code: String, redirectUri: Uri): Try[Map[String, String]] =
    createRequest(tokenJsonnetSource, tokenArguments(code, redirectUri))

  private lazy val refreshJsonnetSource: (String, sjsonnet.Path) =
    jsonnetSource(refreshTemplate, refreshResourcePath)
  private def refreshArguments(refreshToken: RefreshToken): Map[String, ujson.Value] = Map(
    "config" -> config,
    "request" -> ujson.Obj(
      "refreshToken" -> RefreshToken.unwrap(refreshToken)
    ),
  )
  def createRefreshRequest(refreshToken: RefreshToken): Try[Map[String, String]] =
    createRequest(refreshJsonnetSource, refreshArguments(refreshToken))
}

object RequestTemplates {
  class InterpretTemplateException(msg: String) extends RuntimeException(msg)

  def apply(
      clientId: String,
      clientSecret: SecretString,
      authTemplate: Option[Path],
      tokenTemplate: Option[Path],
      refreshTemplate: Option[Path],
  ): RequestTemplates =
    new RequestTemplates(clientId, clientSecret, authTemplate, tokenTemplate, refreshTemplate)
}
