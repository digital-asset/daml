// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.digitalasset.canton.http.json.v2.damldefinitionsservice.DamlDefinitionsView
import com.digitalasset.canton.http.json.v2.damldefinitionsservice.Schema.{AllTemplatesResponse, TemplateDefinition, TypeSig}
import com.digitalasset.canton.http.json.v2.damldefinitionsservice.Schema.Codecs.*
import com.digitalasset.canton.logging.NamedLoggerFactory
import sttp.tapir.generic.auto.*
import sttp.tapir.path

import scala.concurrent.Future

class JsDamlDefinitionsService(
    damlDefinitionsView: DamlDefinitionsView,
    val loggerFactory: NamedLoggerFactory,
) extends Endpoints {
  private val definitions = v2Endpoint.in("definitions")
  private val packageDefinitions = definitions.in("packages")
  private val templateDefinitions = definitions.in("templates")
  private val packageSignatureSelectorPath = "package-signature"
  private val templateSelectorPath = "template-id"

  // TODO(#21695): Enrich endpoints with more dimensions on which we can query the definitions
  //               e.g. all templates by package-id; interface definitions; etc.
  def endpoints() =
    List(
      json(
        packageDefinitions.get
          .in(path[String](packageSignatureSelectorPath))
          .description("Get the package signature"),
        getPackageSignature,
      ),
      json(
        templateDefinitions.get
          .description("Get all the templates pertaining to packages uploaded on the participant"),
        getAllTemplates,
      ),
      json(
        templateDefinitions.get
          .in(path[String](templateSelectorPath))
          .description("Get the template definition"),
        getTemplateDefinition,
      ),
    )

  // TODO(#21695): Propagate TraceContext
  private def getPackageSignature(_callerContext: CallerContext)(
      req: TracedInput[String]
  ): Future[Either[JsSchema.JsCantonError, Option[TypeSig]]] =
    Future.successful(Right(damlDefinitionsView.packageSignature(req.in)))

  private def getAllTemplates(_callerContext: CallerContext)(
      req: TracedInput[Unit]
  ): Future[Either[JsSchema.JsCantonError, AllTemplatesResponse]] =
    Future.successful(Right(damlDefinitionsView.allTemplates()))

  private def getTemplateDefinition(_callerContext: CallerContext)(
      req: TracedInput[String]
  ): Future[Either[JsSchema.JsCantonError, Option[TemplateDefinition]]] =
    Future.successful(Right(damlDefinitionsView.templateDefinition(req.in)))
}
