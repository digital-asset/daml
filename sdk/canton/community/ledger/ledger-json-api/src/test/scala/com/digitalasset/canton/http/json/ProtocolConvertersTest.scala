// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json

import cats.implicits.toFunctorOps
import com.daml.ledger.api.v2 as lapi
import com.daml.ledger.api.v2.value.{Identifier, Record, Value}
import com.daml.nonempty.NonEmpty
import com.daml.scalatest.Equalz.convertToAnyShouldWrapper
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
import com.google.protobuf.ByteString
import com.google.rpc.Code
import magnolify.scalacheck.semiauto.ArbitraryDerivation
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.AppendedClues.convertToClueful
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

class ProtocolConvertersTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  import Arbitraries.*

  private val randomSamplesPerMappedClass = 10

  "check all Js mirrors mapping" in {
    forAll(mappings) { mapping =>
      mapping.check(randomSamplesPerMappedClass).futureValue
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

  private lazy val mappings: Seq[JsMapping[?, ?]] = Seq(
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
    JsMapping(converters.PrefetchContractKey),
  )
}

object Arbitraries {

  import StdGenerators.*
  import magnolify.scalacheck.auto.*

  val defaultJsValue: ujson.Value = ujson.Obj("key" -> ujson.Str("value"))
  val defaultLapiRecord: com.daml.ledger.api.v2.value.Record = Record(fields =
    Seq(
      com.daml.ledger.api.v2.value.RecordField(value =
        Some(
          com.daml.ledger.api.v2.value
            .Value(com.daml.ledger.api.v2.value.Value.Sum.Text("quantumly-random"))
        )
      )
    )
  )
  val defaultLapiValue: com.daml.ledger.api.v2.value.Value =
    Value(sum = Value.Sum.Record(value = defaultLapiRecord))

  def smallSeqArbitrary[T](implicit arbT: Arbitrary[T]): Arbitrary[Seq[T]] =
    Arbitrary(Gen.choose(0, 5).flatMap(n => Gen.listOfN(n, arbT.arbitrary)))

  implicit val arbStringSeq: Arbitrary[Seq[String]] =
    smallSeqArbitrary

  implicit val arbIdentifier: Arbitrary[Option[Identifier]] = Arbitrary(for {
    packageId <- Gen.stringOfN(8, Gen.alphaChar)
    moduleName <- Gen.stringOfN(8, Gen.alphaChar)
    scriptName <- Gen.stringOfN(8, Gen.alphaChar)
  } yield Some(Identifier(packageId, moduleName, scriptName)))

  implicit val arbJsValue: Arbitrary[ujson.Value] = Arbitrary {
    defaultJsValue
  }
  implicit val arbLapiValue: Arbitrary[Value] = Arbitrary {
    defaultLapiValue
  }

  implicit val arbLapiRecord: Arbitrary[Record] = Arbitrary {
    defaultLapiRecord
  }

  implicit val arbOptionLapiRecord: Arbitrary[Option[Record]] = Arbitrary {
    Some(defaultLapiRecord)
  }

  implicit val arbOptionLapiValue: Arbitrary[Option[Value]] = Arbitrary {
    Some(defaultLapiValue)
  }

  def nonEmptyScalaPbOneOf[T <: scalapb.GeneratedOneof](
      arb: Arbitrary[T]
  ): Arbitrary[T] =
    Arbitrary {
      arb.arbitrary.retryUntil(_.isDefined)
    }

  implicit val arbCommand: Arbitrary[com.daml.ledger.api.v2.commands.Command.Command] =
    nonEmptyScalaPbOneOf[com.daml.ledger.api.v2.commands.Command.Command](
      ArbitraryDerivation[com.daml.ledger.api.v2.commands.Command.Command]
    )

  implicit val arbEventEvent: Arbitrary[lapi.event.Event.Event] =
    nonEmptyScalaPbOneOf(
      ArbitraryDerivation[lapi.event.Event.Event]
    )

  implicit val arbEventSeq: Arbitrary[Seq[lapi.event.Event]] =
    smallSeqArbitrary[lapi.event.Event]

  implicit val arbTopologyEvent: Arbitrary[lapi.topology_transaction.TopologyEvent.Event] =
    nonEmptyScalaPbOneOf(
      ArbitraryDerivation[lapi.topology_transaction.TopologyEvent.Event]
    )
  implicit val arbTreeEventLegacyKind: Arbitrary[LegacyDTOs.TreeEvent.Kind] = {
    val arb = ArbitraryDerivation[LegacyDTOs.TreeEvent.Kind]
    Arbitrary {
      arb.arbitrary.retryUntil(_ != Kind.Empty)
    }
  }

  implicit val arbPrefetchContractKey: Arbitrary[lapi.commands.PrefetchContractKey] = {
    val arb = ArbitraryDerivation[lapi.commands.PrefetchContractKey]
    Arbitrary {
      arb.arbitrary.retryUntil(pk => pk.contractKey.zip(pk.templateId).isDefined)
    }
  }

  implicit val arbTransaction: Arbitrary[lapi.transaction.Transaction] = {
    val arb = ArbitraryDerivation[lapi.transaction.Transaction]
    Arbitrary {
      arb.arbitrary.retryUntil(v => v.effectiveAt.isDefined && v.recordTime.isDefined).map { tx =>
        val hash32Bytes =
          ByteString.copyFrom(Array.fill(32)(scala.util.Random.nextInt(256).toByte))
        tx.copy(externalTransactionHash = Some(hash32Bytes))
      }
    }
  }

  implicit val arbTransactionTree: Arbitrary[LegacyDTOs.TransactionTree] = {
    val arb = ArbitraryDerivation[LegacyDTOs.TransactionTree]
    Arbitrary {
      arb.arbitrary.retryUntil(v => v.effectiveAt.isDefined && v.recordTime.isDefined)
    }
  }

  implicit val arbCommandSeq: Arbitrary[Seq[lapi.commands.Command]] =
    smallSeqArbitrary

  implicit val arbPrefContractKeySeq: Arbitrary[Seq[lapi.commands.PrefetchContractKey]] =
    smallSeqArbitrary

  implicit val arbCommands: Arbitrary[lapi.commands.Commands] = {
    val arb = ArbitraryDerivation[lapi.commands.Commands]
    Arbitrary {
      arb.arbitrary.retryUntil(_.actAs.nonEmpty)
    }
  }

  implicit val arbOptCommands: Arbitrary[Option[lapi.commands.Commands]] = Arbitrary {
    arbCommands.arbitrary.map(Some(_))
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
      arb.arbitrary.retryUntil(_ != Update.Empty)
    }
  }
  implicit val arbSubmitAndWaitTransactionTreeResponseLegacy
      : Arbitrary[LegacyDTOs.SubmitAndWaitForTransactionTreeResponse] = {
    val arb = ArbitraryDerivation[LegacyDTOs.SubmitAndWaitForTransactionTreeResponse]
    Arbitrary {
      arb.arbitrary.retryUntil(_.transaction.isDefined)
    }
  }
  implicit val arbSubmitAndWaitTransactionResponse
      : Arbitrary[lapi.command_service.SubmitAndWaitForTransactionResponse] = {
    val arb = ArbitraryDerivation[lapi.command_service.SubmitAndWaitForTransactionResponse]
    Arbitrary {
      arb.arbitrary.retryUntil(_.transaction.isDefined)
    }
  }
  implicit val arbGetTransactionResponseLegacy: Arbitrary[LegacyDTOs.GetTransactionResponse] = {
    val arb = ArbitraryDerivation[LegacyDTOs.GetTransactionResponse]
    Arbitrary {
      arb.arbitrary.retryUntil(_.transaction.isDefined)
    }
  }

  implicit val arbReassignment: Arbitrary[lapi.reassignment.Reassignment] = {
    val arb = ArbitraryDerivation[lapi.reassignment.Reassignment]
    Arbitrary {
      arb.arbitrary.retryUntil(v => v.recordTime.isDefined)
    }
  }

  implicit val arbSubmitAndReassignmentResponse
      : Arbitrary[lapi.command_service.SubmitAndWaitForReassignmentResponse] = {
    val arb = ArbitraryDerivation[lapi.command_service.SubmitAndWaitForReassignmentResponse]
    Arbitrary {
      arb.arbitrary.retryUntil(_.reassignment.isDefined)
    }
  }

  implicit val arbInterfaceView: Arbitrary[lapi.event.InterfaceView] = {
    val arb = ArbitraryDerivation[lapi.event.InterfaceView]
    Arbitrary {
      arb.arbitrary.retryUntil(v =>
        v.viewStatus match {
          case Some(status) if status.code != Code.OK.getNumber =>
            // if status is an error, viewValue should be empty
            v.viewValue.isEmpty
          case Some(_) => true
          case None => false
        }
      )
    }
  }

  implicit val arbCreatedEvent: Arbitrary[lapi.event.CreatedEvent] = {
    val arb = ArbitraryDerivation[lapi.event.CreatedEvent]
    Arbitrary {
      arb.arbitrary.retryUntil(v => v.createdAt.isDefined)
    }
  }

  implicit val arbOptCreatedEvent: Arbitrary[Option[lapi.event.CreatedEvent]] =
    Arbitrary {
      arbCreatedEvent.arbitrary.map(Some(_))
    }

  implicit val arbOptArchivedEvent: Arbitrary[Option[lapi.event.ArchivedEvent]] = {
    implicit val arb: Arbitrary[lapi.event.ArchivedEvent] =
      ArbitraryDerivation[lapi.event.ArchivedEvent]
    Arbitrary {
      arb.arbitrary.map(Some(_))
    }
  }

  implicit val arbIncompleteAssigned: Arbitrary[lapi.state_service.IncompleteAssigned] = {
    val arb = ArbitraryDerivation[lapi.state_service.IncompleteAssigned]
    Arbitrary {
      arb.arbitrary.retryUntil(_.assignedEvent.isDefined)
    }
  }

  implicit val arbOptUnAssignedEvent: Arbitrary[Option[lapi.reassignment.UnassignedEvent]] =
    Arbitrary {
      ArbitraryDerivation[lapi.reassignment.UnassignedEvent].arbitrary.map(Some(_))
    }

  implicit class OptM[T](val v: Option[T]) {
    def getValue()(implicit ct: ClassTag[T]): T =
      v.getOrElse(
        throw new RuntimeException(s"Could not create arbitrary for ${ct.runtimeClass.getName}")
      )
  }

}

class MockSchemaProcessor()(implicit val executionContext: ExecutionContext)
    extends SchemaProcessors {

  val simpleJsValue = Future.successful(Arbitraries.defaultJsValue)
  val simpleLapiValue =
    Future.successful(Arbitraries.defaultLapiValue)

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
  def check(samples: Int)(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): Future[Unit] =
    Future
      .traverse(Vector.from(1 to samples)) { _ =>
        arb.arbitrary.sample
          .map { lapiValue =>
            for {
              jsValue <- converter.toJson(lapiValue)
              lapiValueReconstructed <- converter.fromJson(jsValue)
            } yield lapiValue shouldBe lapiValueReconstructed withClue s"Mapping for $lapClassTag"
          }
          .getOrElse(throw new RuntimeException(s"Failed to generate LAPI value for $lapClassTag"))
      }
      .void
}
