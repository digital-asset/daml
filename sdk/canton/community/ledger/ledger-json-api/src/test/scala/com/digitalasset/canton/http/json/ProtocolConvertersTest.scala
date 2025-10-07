// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json

import com.daml.ledger.api.v2 as lapi
import com.daml.ledger.api.v2.value.{Identifier, Record, Value}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.http.json.v2.LegacyDTOs.GetUpdateTreesResponse.Update
import com.digitalasset.canton.http.json.v2.LegacyDTOs.TreeEvent.Kind
import com.digitalasset.canton.http.json.v2.{
  LegacyDTOs,
  ProtocolConverter,
  ProtocolConverters,
  SchemaProcessors,
  TranscodePackageIdResolver,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{
  BaseTest,
  HasExecutionContext,
  LfPackageId,
  LfPackageName,
  LfPartyId,
}
import com.digitalasset.daml.lf.data.Ref.IdString
import magnolify.scalacheck.semiauto.ArbitraryDerivation
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

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
  private val packageNameResolver = new TranscodePackageIdResolver {
    implicit def ec: ExecutionContext = executorService
    override def loggerFactory: NamedLoggerFactory = ProtocolConvertersTest.this.loggerFactory
    override def resolvePackageNamesInternal(
        packageNames: NonEmpty[Set[LfPackageName]],
        party: LfPartyId,
        packageIdSelectionPreferences: Set[LfPackageId],
        synchronizerIdO: Option[String],
    )(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Map[LfPackageName, LfPackageId]] =
      FutureUnlessShutdown.pure(Map.empty)
  }
  private val converters = new ProtocolConverters(mockSchemaProcessor, packageNameResolver)

  import magnolify.scalacheck.auto.*
  private val mappings: Seq[JsMapping[_, _]] = Seq(
    JsMapping(converters.Commands),
    JsMapping(converters.InterfaceView),
    JsMapping(converters.Event),
    JsMapping(converters.Transaction),
    JsMapping(converters.SubmitAndWaitTransactionTreeResponseLegacy),
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
    JsMapping(converters.GetUpdateTreesResponseLegacy),
    JsMapping(converters.GetTransactionResponseLegacy),
//    JsMapping(converters.PrepareSubmissionRequest),//we only need toJson
//    JsMapping(converters.PrepareSubmissionResponse), // we only need toJson
//    JsMapping(converters.ExecuteSubmissionRequest), // we only need fromJson
    JsMapping(converters.AllocatePartyRequest),
    JsMapping(converters.PrefetchContractKey),
  )
}

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

  implicit val arbIdentifier: Arbitrary[Option[Identifier]] = Arbitrary(for {
    packageId <- Gen.stringOfN(8, Gen.alphaChar)
    moduleName <- Gen.stringOfN(8, Gen.alphaChar)
    scriptName <- Gen.stringOfN(8, Gen.alphaChar)
  } yield Some(Identifier(packageId, moduleName, scriptName)))

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
  implicit val arbTreeEventLegacyKind: Arbitrary[LegacyDTOs.TreeEvent.Kind] = {
    val arb = ArbitraryDerivation[LegacyDTOs.TreeEvent.Kind]
    Arbitrary {
      retryUntilSome(
        arb.arbitrary.sample.filter(_ != Kind.Empty)
      ).getOrElse(throw new RuntimeException("Failed to generate non-empty TreeEvent.Kind"))
    }
  }

  implicit val arbReassignmentEventEvent: Arbitrary[lapi.reassignment.ReassignmentEvent.Event] =
    nonEmptyScalaPbOneOf(
      ArbitraryDerivation[lapi.reassignment.ReassignmentEvent.Event]
    )
  implicit val arbGetUpdatesResponseUpdate
      : Arbitrary[lapi.update_service.GetUpdatesResponse.Update] =
    nonEmptyScalaPbOneOf(
      ArbitraryDerivation[lapi.update_service.GetUpdatesResponse.Update]
    )
  implicit val arbGetUpdatesTreesResponseLegacyUpdate
      : Arbitrary[LegacyDTOs.GetUpdateTreesResponse.Update] = {
    val arb = ArbitraryDerivation[LegacyDTOs.GetUpdateTreesResponse.Update]
    Arbitrary {
      retryUntilSome(
        arb.arbitrary.sample.filter(_ != Update.Empty)
      ).getOrElse(
        throw new RuntimeException("Failed to generate non-empty GetUpdateTreesResponse.Update")
      )
    }
  }
  implicit val arbSubmitAndWaitTransactionTreeResponseLegacy
      : Arbitrary[LegacyDTOs.SubmitAndWaitForTransactionTreeResponse] = {
    val arb = ArbitraryDerivation[LegacyDTOs.SubmitAndWaitForTransactionTreeResponse]
    Arbitrary {
      retryUntilSome(
        arb.arbitrary.sample.filter(_.transaction.isDefined)
      ).getOrElse(
        throw new RuntimeException(
          "Failed to generate non-empty SubmitAndWaitForTransactionTreeResponse"
        )
      )
    }
  }
  implicit val arbGetTransactionResponseLegacy: Arbitrary[LegacyDTOs.GetTransactionResponse] = {
    val arb = ArbitraryDerivation[LegacyDTOs.GetTransactionResponse]
    Arbitrary {
      retryUntilSome(
        arb.arbitrary.sample.filter(_.transaction.isDefined)
      ).getOrElse(
        throw new RuntimeException("Failed to generate non-empty GetTransactionResponse")
      )
    }
  }
}

class MockSchemaProcessor()(implicit val executionContext: ExecutionContext)
    extends SchemaProcessors {

  val simpleJsValue = Future.successful(Arbitraries.defaultJsValue)
  val simpleLapiValue =
    Future.successful(Arbitraries.defaultLapiRecord)
  override def contractArgFromJsonToProto(template: Identifier, jsonArgsValue: ujson.Value)(implicit
      traceContext: TraceContext
  ): Future[Value] = simpleLapiValue

  override def contractArgFromProtoToJson(template: Identifier, protoArgs: Record)(implicit
      traceContext: TraceContext
  ): Future[ujson.Value] = simpleJsValue

  override def choiceArgsFromJsonToProto(
      template: Identifier,
      choiceName: IdString.Name,
      jsonArgsValue: ujson.Value,
  )(implicit traceContext: TraceContext): Future[Value] = simpleLapiValue

  override def choiceArgsFromProtoToJson(
      template: Identifier,
      choiceName: IdString.Name,
      protoArgs: Value,
  )(implicit traceContext: TraceContext): Future[ujson.Value] = simpleJsValue

  override def keyArgFromProtoToJson(template: Identifier, protoArgs: Value)(implicit
      traceContext: TraceContext
  ): Future[ujson.Value] = simpleJsValue

  override def keyArgFromJsonToProto(template: Identifier, protoArgs: ujson.Value)(implicit
      traceContext: TraceContext
  ): Future[Value] = simpleLapiValue

  override def exerciseResultFromProtoToJson(
      template: Identifier,
      choiceName: IdString.Name,
      v: Value,
  )(implicit traceContext: TraceContext): Future[ujson.Value] = simpleJsValue

  override def exerciseResultFromJsonToProto(
      template: Identifier,
      choiceName: IdString.Name,
      value: ujson.Value,
  )(implicit traceContext: TraceContext): Future[Option[Value]] =
    simpleLapiValue.map(Some(_))
}
final case class JsMapping[LAPI, JS](converter: ProtocolConverter[LAPI, JS])(implicit
    arb: Arbitrary[LAPI],
    lapClassTag: ClassTag[LAPI],
) {
  def check()(implicit
      traceContext: TraceContext,
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
