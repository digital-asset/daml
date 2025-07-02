// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json

import com.daml.ledger.api.v2 as lapi
import com.daml.ledger.api.v2.value.{Identifier, Record, Value}
import com.digitalasset.canton.http.json.v2.{
  ProtocolConverter,
  ProtocolConverters,
  SchemaProcessors,
}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import com.digitalasset.daml.lf.data.Ref.IdString
import magnolify.scalacheck.semiauto.ArbitraryDerivation
import org.scalacheck.Arbitrary
import org.scalatest.wordspec.AnyWordSpec

import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

// TODO(#23504) remove
@nowarn("cat=deprecation")
class ProtocolConvertersTest extends AnyWordSpec with BaseTest with HasExecutionContext {
  import StdGenerators.*
  import Arbitraries.*

  private val randomSamplesPerMappedClass = 2

  "check all Js mirrors mapping" in {
    forAll(mappings) { mapping =>
      (1 to randomSamplesPerMappedClass).foreach(_ => mapping.check())
    }
  }

  private val mockSchemaProcessor = new MockSchemaProcessor()
  private val converters = new ProtocolConverters(mockSchemaProcessor)

  import magnolify.scalacheck.auto.*
  private val mappings: Seq[JsMapping[_, _]] = Seq(
    JsMapping(converters.Commands),
    JsMapping(converters.InterfaceView),
    JsMapping(converters.Event),
    JsMapping(converters.Transaction),
    JsMapping(converters.TransactionTree),
    JsMapping(converters.SubmitAndWaitTransactionTreeResponse),
    JsMapping(converters.SubmitAndWaitTransactionResponse),
    JsMapping(converters.SubmitAndWaitForReassignmentResponse),
    JsMapping(converters.SubmitAndWaitForTransactionRequest),
    JsMapping(converters.GetEventsByContractIdResponse),
    JsMapping(converters.CreatedEvent),
    JsMapping(converters.AssignedEvent),
    JsMapping(converters.ContractEntry),
    JsMapping(converters.GetActiveContractsResponse),
    JsMapping(converters.ReassignmentEvent),
    JsMapping(converters.Reassignment),
    JsMapping(converters.GetUpdatesResponse),
    JsMapping(converters.GetUpdateTreesResponse),
    JsMapping(converters.GetTransactionResponse),
//    JsMapping(converters.PrepareSubmissionRequest),//we only need toJson
//    JsMapping(converters.PrepareSubmissionResponse), // we only need toJson
//    JsMapping(converters.ExecuteSubmissionRequest), // we only need fromJson
    JsMapping(converters.AllocatePartyRequest),
    JsMapping(converters.PrefetchContractKey),
  )
}

// TODO(#23504) remove suppression of deprecation warnings
@nowarn("cat=deprecation")
object Arbitraries {
  import StdGenerators.*
  import magnolify.scalacheck.auto.*

  val defaultJsValue: ujson.Value = ujson.Obj("key" -> ujson.Str("value"))
  val defaultLapiRecord: Value = Value(sum =
    Value.Sum.Record(value =
      Record(fields =
        Seq(
          com.daml.ledger.api.v2.value.RecordField(value =
            Some(
              com.daml.ledger.api.v2.value
                .Value(com.daml.ledger.api.v2.value.Value.Sum.Text("quantumly-random"))
            )
          )
        )
      )
    )
  )

  implicit val arbJsValue: Arbitrary[ujson.Value] = Arbitrary {
    defaultJsValue
  }
  implicit val arbLapiRecord: Arbitrary[Value] = Arbitrary {
    defaultLapiRecord
  }

  def retryUntilSome[T](f: => Option[T], maxAttempts: Int = 100): Option[T] =
    Iterator.continually(f).take(maxAttempts).collectFirst { case Some(v) => v }

  def nonEmptyScalaPbOneOf[T <: scalapb.GeneratedOneof](arb: Arbitrary[T]): Arbitrary[T] =
    Arbitrary {
      retryUntilSome(
        arb.arbitrary.sample.filter(_.isDefined)
      ).getOrElse(throw new RuntimeException("Failed to generate non-empty ScalaPB OneOf"))
    }

  implicit val arbCommand: Arbitrary[com.daml.ledger.api.v2.commands.Command.Command] =
    nonEmptyScalaPbOneOf[com.daml.ledger.api.v2.commands.Command.Command](
      ArbitraryDerivation[com.daml.ledger.api.v2.commands.Command.Command]
    )
  implicit val arbEvent: Arbitrary[lapi.event.Event.Event] =
    nonEmptyScalaPbOneOf(
      ArbitraryDerivation[lapi.event.Event.Event]
    )
  implicit val arbTopologyEvent: Arbitrary[lapi.topology_transaction.TopologyEvent.Event] =
    nonEmptyScalaPbOneOf(
      ArbitraryDerivation[lapi.topology_transaction.TopologyEvent.Event]
    )
  implicit val arbTreeEventKind: Arbitrary[lapi.transaction.TreeEvent.Kind] =
    nonEmptyScalaPbOneOf(
      ArbitraryDerivation[lapi.transaction.TreeEvent.Kind]
    )
  implicit val arbReassignmentEventEvent: Arbitrary[lapi.reassignment.ReassignmentEvent.Event] =
    nonEmptyScalaPbOneOf(
      ArbitraryDerivation[lapi.reassignment.ReassignmentEvent.Event]
    )
  implicit val arbGetUpdatesResponseUpdate
      : Arbitrary[lapi.update_service.GetUpdatesResponse.Update] =
    nonEmptyScalaPbOneOf(
      ArbitraryDerivation[lapi.update_service.GetUpdatesResponse.Update]
    )
  implicit val arbGetUpdatesTreesResponseUpdate
      : Arbitrary[lapi.update_service.GetUpdateTreesResponse.Update] =
    nonEmptyScalaPbOneOf(
      ArbitraryDerivation[lapi.update_service.GetUpdateTreesResponse.Update]
    )
}

class MockSchemaProcessor()(implicit val executionContext: ExecutionContext)
    extends SchemaProcessors {

  val simpleJsValue = Future.successful(Arbitraries.defaultJsValue)
  val simpleLapiValue =
    Future.successful(Arbitraries.defaultLapiRecord)
  override def contractArgFromJsonToProto(template: Identifier, jsonArgsValue: ujson.Value)(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Future[Value] = simpleLapiValue

  override def contractArgFromProtoToJson(template: Identifier, protoArgs: Record)(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Future[ujson.Value] = simpleJsValue

  override def choiceArgsFromJsonToProto(
      template: Identifier,
      choiceName: IdString.Name,
      jsonArgsValue: ujson.Value,
  )(implicit errorLoggingContext: ErrorLoggingContext): Future[Value] = simpleLapiValue

  override def choiceArgsFromProtoToJson(
      template: Identifier,
      choiceName: IdString.Name,
      protoArgs: Value,
  )(implicit errorLoggingContext: ErrorLoggingContext): Future[ujson.Value] = simpleJsValue

  override def keyArgFromProtoToJson(template: Identifier, protoArgs: Value)(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Future[ujson.Value] = simpleJsValue

  override def keyArgFromJsonToProto(template: Identifier, protoArgs: ujson.Value)(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Future[Value] = simpleLapiValue

  override def exerciseResultFromProtoToJson(
      template: Identifier,
      choiceName: IdString.Name,
      v: Value,
  )(implicit errorLoggingContext: ErrorLoggingContext): Future[ujson.Value] = simpleJsValue

  override def exerciseResultFromJsonToProto(
      template: Identifier,
      choiceName: IdString.Name,
      value: ujson.Value,
  )(implicit errorLoggingContext: ErrorLoggingContext): Future[Option[Value]] =
    simpleLapiValue.map(Some(_))
}
final case class JsMapping[LAPI, JS](converter: ProtocolConverter[LAPI, JS])(implicit
    arb: Arbitrary[LAPI],
    lapClassTag: ClassTag[LAPI],
) {
  def check()(implicit
      errorLoggingContext: ErrorLoggingContext,
      executionContext: ExecutionContext,
  ): Unit =
    arb.arbitrary.sample
      .map { lapiValue =>
        for {
          jsValue <- converter.toJson(lapiValue)
          lapiValueReconstructed <- converter.fromJson(jsValue)
        } yield {
          assert(
            lapiValue == lapiValueReconstructed,
            s"Mapping for $lapClassTag failed: $lapiValue != $lapiValueReconstructed",
          )
        }
      }
      .getOrElse(throw new RuntimeException("Failed to generate LAPI value"))
}
