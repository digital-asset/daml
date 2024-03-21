// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator

import akka.actor.ActorRef
import akka.http.scaladsl.model.StatusCode
import akka.http.scaladsl.model.StatusCodes._
import com.daml.navigator.graphql._
import com.daml.navigator.graphql.SprayMarshallers._
import com.daml.navigator.model.PartyState
import com.daml.navigator.store.Store.StoreException
import com.typesafe.scalalogging.LazyLogging
import sangria.ast.Document
import sangria.execution._
import sangria.parser.QueryParser
import sangria.renderer.SchemaRenderer
import sangria.schema.Schema
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

case class ParseResult(ast: Document, operationName: Option[String], variables: JsValue)

/** Provides a way of executing GraphQL queries.
  */
trait GraphQLHandler {
  def schema: Schema[GraphQLContext, Unit]
  def parse(request: String): Try[ParseResult]
  def parse(request: JsValue): Try[ParseResult]
  def executeQuery(parsed: ParseResult, party: PartyState): Future[(StatusCode, JsValue)]
  def renderSchema: String
}

object GraphQLHandler {
  type ParseQuery = JsValue => Try[ParseResult]
  type ExecuteQuery = (ParseResult, PartyState) => Future[(StatusCode, JsValue)]
  type CustomEndpoints = Set[CustomEndpoint[_]]
}

case class DefaultGraphQLHandler(
    customEndpoints: GraphQLHandler.CustomEndpoints,
    platformStore: Option[ActorRef],
)(implicit
    executionContext: ExecutionContext
) extends GraphQLHandler
    with LazyLogging {

  def schema: Schema[GraphQLContext, Unit] = new GraphQLSchema(customEndpoints).QuerySchema

  def parse(request: String): Try[ParseResult] =
    Try(request.parseJson).flatMap(parse)

  def parse(request: JsValue): Try[ParseResult] =
    for {
      fields <- Try(request.asJsObject.fields)
      JsString(query) <- Try(fields("query"))
      operationName = fields.get("operationName").collect { case JsString(value) =>
        value
      }
      vars: JsValue = fields.get("variables") match {
        case Some(obj: JsObject) => obj
        case _ => JsObject.empty
      }
      ast <- QueryParser.parse(query)
    } yield ParseResult(ast, operationName, vars)

  def executeQuery(parsed: ParseResult, party: PartyState): Future[(StatusCode, JsValue)] = {
    platformStore.fold[Future[(StatusCode, JsValue)]](
      Future.successful(InternalServerError -> JsString("Platform store not available"))
    )(store => {
      val context = GraphQLContext(party, store)
      Executor
        .execute(
          schema,
          parsed.ast,
          context,
          variables = parsed.variables,
          operationName = parsed.operationName,
          exceptionHandler = ExceptionHandler { case (_, StoreException(message)) =>
            HandledException(message)
          },
        )
        .map(OK -> _)
        .recover {
          case error: QueryAnalysisError =>
            logger.warn(s"GraphQL analysis error ${error.getMessage}.")
            BadRequest -> error.resolveError
          case error: ErrorWithResolver =>
            logger.error("Failed to execute GraphQL query", error)
            InternalServerError -> error.resolveError
        }
    })
  }

  def renderSchema: String = SchemaRenderer.renderSchema(schema)
}
