// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.openapi

import com.daml.ledger.api.v2
import com.digitalasset.canton.http.json.v2 as json
import com.digitalasset.canton.http.json.v2.LegacyDTOs
import com.digitalasset.canton.openapi.json.{JSON, model as openapi}
import io.circe.{Decoder, Encoder}
import io.swagger.parser.OpenAPIParser
import io.swagger.v3.parser.core.models.ParseOptions
import org.scalacheck.Arbitrary
import org.scalatest.Assertion
import org.scalatest.Inspectors.forAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters.*
import scala.reflect.ClassTag
import scala.util.Using
import scala.util.control.NonFatal

/** This tests checks that Openapi generated classes match with json serialization that we use.
  *
  * The reason for this test is that tapir can generate openapi that is not in sync with circe
  * codec, we are trying to detect such cases and fix them.
  *
  * To recap:
  *   - Codec = Encoder + Decoder by Circe to translate Scala object to Json and back
  *   - Schema = Information to Tapir how to represent Scala object in Openapi. The schema is then
  *     used by tapir to generate the openapi spec (openapi.yaml).
  *
  * Also existing code generators are often buggy -> we are trying to detect such cases at least for
  * java (here).
  *
  * Test generates multiple samples, unfortunately no seed is used so every time examples will be
  * different. (Introduction of a seed would complicate the code a lot)
  *
  * If you have issues:
  *   - check how the type is represented in openapi.yaml
  *   - try to serialize an example instance
  *   - inspect the string.
  *   - try to deserialize the string with openapi generated class
  *
  * Do not forget to regenerate the openapi definitions (openapi.yaml). See GenerateJSONApiDocs.
  */
class OpenapiTypesTest extends AnyWordSpec with Matchers {
  // this can be increased locally
  // with 100 examples tests take 5 minutes on my machine
  // 20 is a modest value to ensure CI is not overloaded
  private val randomSamplesPerMappedClass = 20
  private val allMappingExamples = Mappings.allMappings
  private val logger = LoggerFactory.getLogger(getClass)

  def checkType[T, V](
      fromJson: (String) => V
  )(implicit
      arb: Arbitrary[T],
      encoder: Encoder[T],
      decoder: Decoder[T],
      classTag: ClassTag[T],
  ): Assertion = {

    val sample = arb.arbitrary.sample
    val initialCirceJson = sample.map(encoder(_)).map(x => x.toString()).toRight("-- no sample --")
    val javaObject =
      try {
        initialCirceJson.map(fromJson)
      } catch {
        case NonFatal(error) =>
          logger.error(
            s"Parse error detected for class $classTag when attempting to parse the generated json.\n  json-error: $error\n  sample: $sample\n  encoded-json: $initialCirceJson",
            error,
          )
          throw new RuntimeException(
            s"parse error, class $classTag json: $error\n $initialCirceJson",
            error,
          )
      }
    val javaBasedJson = javaObject.map(JSON.getGson.toJson(_))
    val circeBack = javaBasedJson.flatMap(io.circe.parser.decode[T](_))
    val circeBackJson = circeBack.map(encoder(_).toString())
    assert(
      circeBackJson === initialCirceJson,
      s"reconstructed json should match initial for $classTag",
    )

  }

  def checkTypeN[T, V](
      fromJson: (String) => V
  )(implicit arb: Arbitrary[T], encoder: Encoder[T], decoder: Decoder[T], classTag: ClassTag[T]) =
    (1 to randomSamplesPerMappedClass).foreach(_ => checkType(fromJson))

  "mappings" should {
    "have openapi spec matching used circe serialization " in {
      forAll(allMappingExamples) { mapping =>
        mapping.check()
      }
    }

    "exhaust schema definitions in openapi" in {
      val resourcePath = "/json-api-docs/openapi.yaml"
      val content = Using(getClass.getResourceAsStream(resourcePath)) { stream =>
        scala.io.Source.fromInputStream(stream).mkString
      }.getOrElse(throw new IllegalStateException(s"Failed to load openapi from: $resourcePath"))

      val parser = new OpenAPIParser()
      val openApi = parser.readContents(content, Seq.empty.asJava, new ParseOptions())
      val schemas = openApi.getOpenAPI().getComponents().getSchemas().asScala.toMap
      // it makes no sense to check empty schemas
      val nonEmptySchemas = schemas.filter { case (_, schema) =>
        schema.getProperties() != null
      }
      // `Identifier` is included in openapi but never used and definition is actually wrong
      val validSchemas = nonEmptySchemas.removed("Identifier")
      val allOpenApiSchemaNames = validSchemas.keys.toSeq.sorted
      val allTestedModelClasses = allMappingExamples.map(_.openapiClass().getSimpleName).sorted

      forAll(allOpenApiSchemaNames) { definedSchema =>
        assert(allTestedModelClasses.contains(definedSchema))
      }
    }
  }

  case class Mapping[T, V](
      fromJson: (String) => V
  )(implicit
      arb: Arbitrary[T],
      encoder: Encoder[T],
      decoder: Decoder[T],
      classTag: ClassTag[T],
      openapiClassTag: ClassTag[V],
  ) {
    def check(): Unit = checkTypeN[T, V](fromJson)

    def openapiClass(): Class[_] = openapiClassTag.runtimeClass
  }

  // This object is added to split mappings initialization, in order to prevent MethodTooLarge compilation error
  // reason is that magnolia generates multiple lines of code for arbitrary instances generation
  object Mappings {

    import com.digitalasset.canton.http.json.StdGenerators.*
    import CantonGenerators.*
    import com.digitalasset.canton.http.json.v2.JsSchema.DirectScalaPbRwImplicits.*
    import com.digitalasset.canton.http.json.v2.JsCommandServiceCodecs.*
    import com.digitalasset.canton.http.json.v2.JsStateServiceCodecs.*
    import com.digitalasset.canton.http.json.v2.JsEventServiceCodecs.*
    import com.digitalasset.canton.http.json.v2.JsUpdateServiceCodecs.*
    import com.digitalasset.canton.http.json.v2.JsUserManagementCodecs.*
    import com.digitalasset.canton.http.json.v2.JsPartyManagementCodecs.*
    import com.digitalasset.canton.http.json.v2.JsPackageCodecs.*
    import com.digitalasset.canton.http.json.v2.JsSchema.JsServicesCommonCodecs.*
    import com.digitalasset.canton.http.json.v2.JsInteractiveSubmissionServiceCodecs.*
    import com.digitalasset.canton.http.json.v2.JsIdentityProviderCodecs.*
    import com.digitalasset.canton.http.json.v2.JsVersionServiceCodecs.*
    import com.digitalasset.canton.http.json.v2.JsSchema.Crypto.*

    import magnolify.scalacheck.auto.*

    // as stated above this split is needed to ensure that mappings initialization do not exceed max 64kB method size
    val allMappings =
      JsMappings1.value ++ JsMappings2.value ++ GrpcMappings1.value ++ GrpcMappings2.value ++ GrpcMappings3.value ++ GrpcMappings4.value

    object GrpcMappings1 {
      val value: Seq[Mapping[_, _]] = Seq(
        Mapping[
          v2.admin.party_management_service.AllocatePartyResponse,
          openapi.AllocatePartyResponse,
        ](
          openapi.AllocatePartyResponse.fromJson
        ),
        Mapping[v2.event.ArchivedEvent, openapi.ArchivedEvent](
          openapi.ArchivedEvent.fromJson
        ),
        Mapping[v2.reassignment_commands.AssignCommand, openapi.AssignCommand1](
          openapi.AssignCommand1.fromJson
        ),
        Mapping[
          v2.reassignment_commands.ReassignmentCommand.Command.AssignCommand,
          openapi.AssignCommand,
        ](
          openapi.AssignCommand.fromJson
        ),
        Mapping[v2.admin.user_management_service.Right.CanActAs, openapi.CanActAs1](
          openapi.CanActAs1.fromJson
        ),
        Mapping[v2.admin.user_management_service.Right.Kind.CanActAs, openapi.CanActAs](
          openapi.CanActAs.fromJson
        ),
        Mapping[v2.admin.user_management_service.Right.CanExecuteAs, openapi.CanExecuteAs1](
          openapi.CanExecuteAs1.fromJson
        ),
        Mapping[v2.admin.user_management_service.Right.Kind.CanExecuteAs, openapi.CanExecuteAs](
          openapi.CanExecuteAs.fromJson
        ),
        Mapping[
          v2.admin.user_management_service.Right.Kind.CanExecuteAsAnyParty,
          openapi.CanExecuteAsAnyParty,
        ](
          openapi.CanExecuteAsAnyParty.fromJson
        ),
        Mapping[v2.admin.user_management_service.Right.CanReadAs, openapi.CanReadAs1](
          openapi.CanReadAs1.fromJson
        ),
        Mapping[v2.admin.user_management_service.Right.Kind.CanReadAs, openapi.CanReadAs](
          openapi.CanReadAs.fromJson
        ),
        Mapping[
          v2.admin.user_management_service.Right.Kind.CanReadAsAnyParty,
          openapi.CanReadAsAnyParty,
        ](
          openapi.CanReadAsAnyParty.fromJson
        ),
        Mapping[v2.completion.Completion, openapi.Completion1](
          openapi.Completion1.fromJson
        ),
        Mapping[
          v2.command_completion_service.CompletionStreamResponse.CompletionResponse.Completion,
          openapi.Completion,
        ](
          openapi.Completion.fromJson
        ),
        Mapping[
          v2.command_completion_service.CompletionStreamRequest,
          openapi.CompletionStreamRequest,
        ](
          openapi.CompletionStreamRequest.fromJson
        ),
        Mapping[
          v2.command_completion_service.CompletionStreamResponse,
          openapi.CompletionStreamResponse,
        ](
          openapi.CompletionStreamResponse.fromJson
        ),
        Mapping[
          v2.state_service.GetConnectedSynchronizersResponse.ConnectedSynchronizer,
          openapi.ConnectedSynchronizer,
        ](
          openapi.ConnectedSynchronizer.fromJson
        ),
        Mapping[json.JsCommand.CreateAndExerciseCommand, openapi.CreateAndExerciseCommand](
          openapi.CreateAndExerciseCommand.fromJson
        ),
        Mapping[json.JsCommand.CreateCommand, openapi.CreateCommand](
          openapi.CreateCommand.fromJson
        ),
        Mapping[json.JsSchema.JsEvent.CreatedEvent, openapi.CreatedEvent](
          openapi.CreatedEvent.fromJson
        ),
        Mapping[json.JsSchema.JsTreeEvent.CreatedTreeEvent, openapi.CreatedTreeEvent](
          openapi.CreatedTreeEvent.fromJson
        ),
        Mapping[
          v2.admin.identity_provider_config_service.CreateIdentityProviderConfigRequest,
          openapi.CreateIdentityProviderConfigRequest,
        ](
          openapi.CreateIdentityProviderConfigRequest.fromJson
        ),
        Mapping[
          v2.admin.identity_provider_config_service.CreateIdentityProviderConfigResponse,
          openapi.CreateIdentityProviderConfigResponse,
        ](
          openapi.CreateIdentityProviderConfigResponse.fromJson
        ),
        Mapping[v2.admin.user_management_service.CreateUserRequest, openapi.CreateUserRequest](
          openapi.CreateUserRequest.fromJson
        ),
        Mapping[v2.admin.user_management_service.CreateUserResponse, openapi.CreateUserResponse](
          openapi.CreateUserResponse.fromJson
        ),
        Mapping[v2.transaction_filter.CumulativeFilter, openapi.CumulativeFilter](
          openapi.CumulativeFilter.fromJson
        ),
        Mapping[
          v2.completion.Completion.DeduplicationPeriod.DeduplicationDuration,
          openapi.DeduplicationDuration1,
        ](
          openapi.DeduplicationDuration1.fromJson
        ),
        Mapping[
          v2.commands.Commands.DeduplicationPeriod.DeduplicationDuration,
          openapi.DeduplicationDuration2,
        ](
          openapi.DeduplicationDuration2.fromJson
        ),
        Mapping[
          v2.interactive.interactive_submission_service.ExecuteSubmissionRequest.DeduplicationPeriod.DeduplicationDuration,
          openapi.DeduplicationDuration,
        ](
          openapi.DeduplicationDuration.fromJson
        ),
        Mapping[
          v2.completion.Completion.DeduplicationPeriod.DeduplicationOffset,
          openapi.DeduplicationOffset1,
        ](
          openapi.DeduplicationOffset1.fromJson
        ),
        Mapping[
          v2.commands.Commands.DeduplicationPeriod.DeduplicationOffset,
          openapi.DeduplicationOffset2,
        ](
          openapi.DeduplicationOffset2.fromJson
        ),
        Mapping[
          v2.interactive.interactive_submission_service.ExecuteSubmissionRequest.DeduplicationPeriod.DeduplicationOffset,
          openapi.DeduplicationOffset,
        ](
          openapi.DeduplicationOffset.fromJson
        ),
        Mapping[v2.commands.DisclosedContract, openapi.DisclosedContract](
          openapi.DisclosedContract.fromJson
        ),
        Mapping[com.google.protobuf.duration.Duration, openapi.Duration](
          openapi.Duration.fromJson
        ),
        Mapping[v2.transaction_filter.EventFormat, openapi.EventFormat](
          openapi.EventFormat.fromJson
        ),
        Mapping[json.JsCommand.ExerciseByKeyCommand, openapi.ExerciseByKeyCommand](
          openapi.ExerciseByKeyCommand.fromJson
        ),
        Mapping[json.JsCommand.ExerciseCommand, openapi.ExerciseCommand](
          openapi.ExerciseCommand.fromJson
        ),
        Mapping[json.JsSchema.JsEvent.ExercisedEvent, openapi.ExercisedEvent](
          openapi.ExercisedEvent.fromJson
        ),
        Mapping[json.JsSchema.JsTreeEvent.ExercisedTreeEvent, openapi.ExercisedTreeEvent](
          openapi.ExercisedTreeEvent.fromJson
        ),
        Mapping[
          v2.experimental_features.ExperimentalCommandInspectionService,
          openapi.ExperimentalCommandInspectionService,
        ](
          openapi.ExperimentalCommandInspectionService.fromJson
        ),
        Mapping[v2.experimental_features.ExperimentalFeatures, openapi.ExperimentalFeatures](
          openapi.ExperimentalFeatures.fromJson
        ),
        Mapping[v2.experimental_features.ExperimentalStaticTime, openapi.ExperimentalStaticTime](
          openapi.ExperimentalStaticTime.fromJson
        ),
        Mapping[v2.version_service.FeaturesDescriptor, openapi.FeaturesDescriptor](
          openapi.FeaturesDescriptor.fromJson
        ),
        Mapping[scalapb.UnknownFieldSet.Field, openapi.Field](
          openapi.Field.fromJson
        ),
        Mapping[com.google.protobuf.field_mask.FieldMask, openapi.FieldMask](
          openapi.FieldMask.fromJson
        ),
        Mapping[v2.transaction_filter.Filters, openapi.Filters](
          openapi.Filters.fromJson
        ),
        Mapping[LegacyDTOs.GetActiveContractsRequest, openapi.GetActiveContractsRequest](
          openapi.GetActiveContractsRequest.fromJson
        ),
        Mapping[
          v2.state_service.GetConnectedSynchronizersResponse,
          openapi.GetConnectedSynchronizersResponse,
        ](
          openapi.GetConnectedSynchronizersResponse.fromJson
        ),
        Mapping[
          v2.event_query_service.GetEventsByContractIdRequest,
          openapi.GetEventsByContractIdRequest,
        ](
          openapi.GetEventsByContractIdRequest.fromJson
        ),
        Mapping[
          v2.admin.identity_provider_config_service.GetIdentityProviderConfigResponse,
          openapi.GetIdentityProviderConfigResponse,
        ](
          openapi.GetIdentityProviderConfigResponse.fromJson
        ),
        Mapping[
          v2.state_service.GetLatestPrunedOffsetsResponse,
          openapi.GetLatestPrunedOffsetsResponse,
        ](
          openapi.GetLatestPrunedOffsetsResponse.fromJson
        ),
        Mapping[
          v2.version_service.GetLedgerApiVersionResponse,
          openapi.GetLedgerApiVersionResponse,
        ](
          openapi.GetLedgerApiVersionResponse.fromJson
        ),
        Mapping[v2.state_service.GetLedgerEndResponse, openapi.GetLedgerEndResponse](
          openapi.GetLedgerEndResponse.fromJson
        ),
        Mapping[v2.package_service.GetPackageStatusResponse, openapi.GetPackageStatusResponse](
          openapi.GetPackageStatusResponse.fromJson
        ),
        Mapping[
          v2.admin.party_management_service.GetParticipantIdResponse,
          openapi.GetParticipantIdResponse,
        ](
          openapi.GetParticipantIdResponse.fromJson
        ),
        Mapping[v2.admin.party_management_service.GetPartiesResponse, openapi.GetPartiesResponse](
          openapi.GetPartiesResponse.fromJson
        ),
        Mapping[
          v2.interactive.interactive_submission_service.PackageVettingRequirement,
          openapi.PackageVettingRequirement,
        ](
          openapi.PackageVettingRequirement.fromJson
        ),
        Mapping[
          v2.interactive.interactive_submission_service.GetPreferredPackagesRequest,
          openapi.GetPreferredPackagesRequest,
        ](
          openapi.GetPreferredPackagesRequest.fromJson
        ),
        Mapping[
          v2.interactive.interactive_submission_service.GetPreferredPackagesResponse,
          openapi.GetPreferredPackagesResponse,
        ](
          openapi.GetPreferredPackagesResponse.fromJson
        ),
        Mapping[
          v2.interactive.interactive_submission_service.GetPreferredPackageVersionResponse,
          openapi.GetPreferredPackageVersionResponse,
        ](
          openapi.GetPreferredPackageVersionResponse.fromJson
        ),
      )
    }

    object GrpcMappings2 {
      val value: Seq[Mapping[_, _]] = Seq(
        Mapping[LegacyDTOs.GetTransactionByIdRequest, openapi.GetTransactionByIdRequest](
          openapi.GetTransactionByIdRequest.fromJson
        ),
        Mapping[
          LegacyDTOs.GetTransactionByOffsetRequest,
          openapi.GetTransactionByOffsetRequest,
        ](
          openapi.GetTransactionByOffsetRequest.fromJson
        ),
        Mapping[v2.update_service.GetUpdateByIdRequest, openapi.GetUpdateByIdRequest](
          openapi.GetUpdateByIdRequest.fromJson
        ),
        Mapping[v2.update_service.GetUpdateByOffsetRequest, openapi.GetUpdateByOffsetRequest](
          openapi.GetUpdateByOffsetRequest.fromJson
        ),
        Mapping[LegacyDTOs.GetUpdatesRequest, openapi.GetUpdatesRequest](
          openapi.GetUpdatesRequest.fromJson
        ),
        Mapping[v2.admin.user_management_service.GetUserResponse, openapi.GetUserResponse](
          openapi.GetUserResponse.fromJson
        ),
        Mapping[
          v2.admin.user_management_service.GrantUserRightsRequest,
          openapi.GrantUserRightsRequest,
        ](
          openapi.GrantUserRightsRequest.fromJson
        ),
        Mapping[
          v2.admin.user_management_service.GrantUserRightsResponse,
          openapi.GrantUserRightsResponse,
        ](
          openapi.GrantUserRightsResponse.fromJson
        ),
        // Strange case: should be mapped to string - seems to be not used anywhere, but somehow included in openapi
        //       Mapping[v2.value.Identifier,openapi.Identifier](
        //        openapi.Identifier.fromJson
        //      ),
        Mapping[
          v2.admin.user_management_service.Right.Kind.IdentityProviderAdmin,
          openapi.IdentityProviderAdmin,
        ](
          openapi.IdentityProviderAdmin.fromJson
        ),
        Mapping[
          v2.admin.identity_provider_config_service.IdentityProviderConfig,
          openapi.IdentityProviderConfig,
        ](
          openapi.IdentityProviderConfig.fromJson
        ),
        Mapping[v2.transaction_filter.InterfaceFilter, openapi.InterfaceFilter1](
          openapi.InterfaceFilter1.fromJson
        ),
        Mapping[
          v2.transaction_filter.CumulativeFilter.IdentifierFilter.InterfaceFilter,
          openapi.InterfaceFilter,
        ](
          openapi.InterfaceFilter.fromJson
        ),
        Mapping[
          v2.admin.identity_provider_config_service.ListIdentityProviderConfigsResponse,
          openapi.ListIdentityProviderConfigsResponse,
        ](
          openapi.ListIdentityProviderConfigsResponse.fromJson
        ),
        Mapping[
          v2.admin.party_management_service.ListKnownPartiesResponse,
          openapi.ListKnownPartiesResponse,
        ](
          openapi.ListKnownPartiesResponse.fromJson
        ),
        Mapping[v2.package_service.ListPackagesResponse, openapi.ListPackagesResponse](
          openapi.ListPackagesResponse.fromJson
        ),
        Mapping[
          v2.admin.user_management_service.ListUserRightsResponse,
          openapi.ListUserRightsResponse,
        ](
          openapi.ListUserRightsResponse.fromJson
        ),
        Mapping[v2.admin.user_management_service.ListUsersResponse, openapi.ListUsersResponse](
          openapi.ListUsersResponse.fromJson
        ),
        Mapping[
          v2.interactive.interactive_submission_service.MinLedgerTime.Time.MinLedgerTimeAbs,
          openapi.MinLedgerTimeAbs,
        ](
          openapi.MinLedgerTimeAbs.fromJson
        ),
        Mapping[v2.interactive.interactive_submission_service.MinLedgerTime, openapi.MinLedgerTime](
          openapi.MinLedgerTime.fromJson
        ),
        Mapping[
          v2.interactive.interactive_submission_service.MinLedgerTime.Time.MinLedgerTimeRel,
          openapi.MinLedgerTimeRel,
        ](
          openapi.MinLedgerTimeRel.fromJson
        ),
        Mapping[v2.admin.object_meta.ObjectMeta, openapi.ObjectMeta](
          openapi.ObjectMeta.fromJson
        ),
        Mapping[v2.offset_checkpoint.OffsetCheckpoint, openapi.OffsetCheckpoint1](
          openapi.OffsetCheckpoint1.fromJson
        ),
        Mapping[json.JsUpdate.OffsetCheckpoint, openapi.OffsetCheckpoint2](
          openapi.OffsetCheckpoint2.fromJson
        ),
        Mapping[json.JsUpdateTree.OffsetCheckpoint, openapi.OffsetCheckpoint3](
          openapi.OffsetCheckpoint3.fromJson
        ),
        Mapping[v2.version_service.OffsetCheckpointFeature, openapi.OffsetCheckpointFeature](
          openapi.OffsetCheckpointFeature.fromJson
        ),
        Mapping[
          v2.command_completion_service.CompletionStreamResponse.CompletionResponse.OffsetCheckpoint,
          openapi.OffsetCheckpoint,
        ](
          openapi.OffsetCheckpoint.fromJson
        ),
        Mapping[v2.package_reference.PackageReference, openapi.PackageReference](
          openapi.PackageReference.fromJson
        ),
        Mapping[
          v2.interactive.interactive_submission_service.PackagePreference,
          openapi.PackagePreference,
        ](
          openapi.PackagePreference.fromJson
        ),
        Mapping[
          v2.admin.user_management_service.Right.Kind.ParticipantAdmin,
          openapi.ParticipantAdmin,
        ](
          openapi.ParticipantAdmin.fromJson
        ),
        Mapping[
          v2.topology_transaction.TopologyEvent.Event.ParticipantAuthorizationAdded,
          openapi.ParticipantAuthorizationAdded,
        ](
          openapi.ParticipantAuthorizationAdded.fromJson
        ),
        Mapping[
          v2.topology_transaction.TopologyEvent.Event.ParticipantAuthorizationChanged,
          openapi.ParticipantAuthorizationChanged,
        ](
          openapi.ParticipantAuthorizationChanged.fromJson
        ),
        Mapping[
          v2.topology_transaction.TopologyEvent.Event.ParticipantAuthorizationRevoked,
          openapi.ParticipantAuthorizationRevoked,
        ](
          openapi.ParticipantAuthorizationRevoked.fromJson
        ),
        Mapping[
          v2.topology_transaction.ParticipantAuthorizationAdded,
          openapi.ParticipantAuthorizationAdded1,
        ](
          openapi.ParticipantAuthorizationAdded1.fromJson
        ),
        Mapping[
          v2.topology_transaction.ParticipantAuthorizationChanged,
          openapi.ParticipantAuthorizationChanged1,
        ](
          openapi.ParticipantAuthorizationChanged1.fromJson
        ),
        Mapping[
          v2.topology_transaction.ParticipantAuthorizationRevoked,
          openapi.ParticipantAuthorizationRevoked1,
        ](
          openapi.ParticipantAuthorizationRevoked1.fromJson
        ),
        Mapping[
          v2.transaction_filter.ParticipantAuthorizationTopologyFormat,
          openapi.ParticipantAuthorizationTopologyFormat,
        ](
          openapi.ParticipantAuthorizationTopologyFormat.fromJson
        ),
        Mapping[v2.admin.party_management_service.PartyDetails, openapi.PartyDetails](
          openapi.PartyDetails.fromJson
        ),
        Mapping[v2.version_service.PartyManagementFeature, openapi.PartyManagementFeature](
          openapi.PartyManagementFeature.fromJson
        ),
        Mapping[
          v2.interactive.interactive_submission_service.PartySignatures,
          openapi.PartySignatures,
        ](
          openapi.PartySignatures.fromJson
        ),
        Mapping[com.google.protobuf.any.Any, openapi.ProtoAny](
          openapi.ProtoAny.fromJson
        ),
        Mapping[json.js.PrefetchContractKey, openapi.PrefetchContractKey](
          openapi.PrefetchContractKey.fromJson
        ),
        Mapping[json.JsUpdate.Reassignment, openapi.Reassignment1](
          openapi.Reassignment1.fromJson
        ),
        Mapping[v2.reassignment_commands.ReassignmentCommand, openapi.ReassignmentCommand](
          openapi.ReassignmentCommand.fromJson
        ),
        Mapping[v2.reassignment_commands.ReassignmentCommands, openapi.ReassignmentCommands](
          openapi.ReassignmentCommands.fromJson
        ),
        Mapping[json.JsUpdateTree.Reassignment, openapi.Reassignment](
          openapi.Reassignment.fromJson
        ),
        Mapping[
          v2.admin.user_management_service.RevokeUserRightsRequest,
          openapi.RevokeUserRightsRequest,
        ](
          openapi.RevokeUserRightsRequest.fromJson
        ),
        Mapping[
          v2.admin.user_management_service.RevokeUserRightsResponse,
          openapi.RevokeUserRightsResponse,
        ](
          openapi.RevokeUserRightsResponse.fromJson
        ),
        Mapping[v2.admin.user_management_service.Right, openapi.Right](
          openapi.Right.fromJson
        ),
        Mapping[v2.crypto.Signature, openapi.Signature](
          openapi.Signature.fromJson
        ),
        Mapping[
          v2.interactive.interactive_submission_service.SinglePartySignatures,
          openapi.SinglePartySignatures,
        ](
          openapi.SinglePartySignatures.fromJson
        ),
        Mapping[com.google.rpc.status.Status, openapi.JsStatus](
          openapi.JsStatus.fromJson
        ),
        Mapping[
          v2.command_service.SubmitAndWaitForReassignmentRequest,
          openapi.SubmitAndWaitForReassignmentRequest,
        ](
          openapi.SubmitAndWaitForReassignmentRequest.fromJson
        ),
        Mapping[v2.command_service.SubmitAndWaitResponse, openapi.SubmitAndWaitResponse](
          openapi.SubmitAndWaitResponse.fromJson
        ),
        Mapping[
          v2.command_submission_service.SubmitReassignmentRequest,
          openapi.SubmitReassignmentRequest,
        ](
          openapi.SubmitReassignmentRequest.fromJson
        ),
        Mapping[v2.offset_checkpoint.SynchronizerTime, openapi.SynchronizerTime](
          openapi.SynchronizerTime.fromJson
        ),
        Mapping[v2.transaction_filter.TemplateFilter, openapi.TemplateFilter1](
          openapi.TemplateFilter1.fromJson
        ),
        Mapping[
          v2.transaction_filter.CumulativeFilter.IdentifierFilter.TemplateFilter,
          openapi.TemplateFilter,
        ](
          openapi.TemplateFilter.fromJson
        ),
        Mapping[v2.topology_transaction.TopologyEvent, openapi.TopologyEvent](
          openapi.TopologyEvent.fromJson
        ),
        Mapping[v2.transaction_filter.TopologyFormat, openapi.TopologyFormat](
          openapi.TopologyFormat.fromJson
        ),
      )
    }
    object GrpcMappings3 {
      val value: Seq[Mapping[_, _]] = Seq(
        Mapping[json.JsUpdate.TopologyTransaction, openapi.TopologyTransaction](
          openapi.TopologyTransaction.fromJson
        ),
        Mapping[v2.topology_transaction.TopologyTransaction, openapi.JsTopologyTransaction](
          openapi.JsTopologyTransaction.fromJson
        ),
        Mapping[v2.trace_context.TraceContext, openapi.TraceContext](
          openapi.TraceContext.fromJson
        ),
        Mapping[LegacyDTOs.TransactionFilter, openapi.TransactionFilter](
          openapi.TransactionFilter.fromJson
        ),
        Mapping[v2.transaction_filter.TransactionFormat, openapi.TransactionFormat](
          openapi.TransactionFormat.fromJson
        ),
        Mapping[json.JsUpdate.Transaction, openapi.Transaction](
          openapi.Transaction.fromJson
        ),
        Mapping[json.JsUpdateTree.TransactionTree, openapi.TransactionTree](
          openapi.TransactionTree.fromJson
        ),
        Mapping[json.JsSchema.JsTreeEvent.TreeEvent, openapi.TreeEvent](
          openapi.TreeEvent.fromJson
        ),
        Mapping[v2.reassignment_commands.UnassignCommand, openapi.UnassignCommand1](
          openapi.UnassignCommand1.fromJson
        ),
        Mapping[
          v2.reassignment_commands.ReassignmentCommand.Command.UnassignCommand,
          openapi.UnassignCommand,
        ](
          openapi.UnassignCommand.fromJson
        ),
        Mapping[v2.reassignment.UnassignedEvent, openapi.UnassignedEvent](
          openapi.UnassignedEvent.fromJson
        ),
        Mapping[scalapb.UnknownFieldSet, openapi.UnknownFieldSet](
          openapi.UnknownFieldSet.fromJson
        ),
        Mapping[v2.transaction_filter.UpdateFormat, openapi.UpdateFormat](
          openapi.UpdateFormat.fromJson
        ),
        Mapping[
          v2.admin.identity_provider_config_service.UpdateIdentityProviderConfigRequest,
          openapi.UpdateIdentityProviderConfigRequest,
        ](
          openapi.UpdateIdentityProviderConfigRequest.fromJson
        ),
        Mapping[
          v2.admin.identity_provider_config_service.UpdateIdentityProviderConfigResponse,
          openapi.UpdateIdentityProviderConfigResponse,
        ](
          openapi.UpdateIdentityProviderConfigResponse.fromJson
        ),
        Mapping[
          v2.admin.party_management_service.UpdatePartyDetailsRequest,
          openapi.UpdatePartyDetailsRequest,
        ](
          openapi.UpdatePartyDetailsRequest.fromJson
        ),
        Mapping[
          v2.admin.party_management_service.UpdatePartyDetailsResponse,
          openapi.UpdatePartyDetailsResponse,
        ](
          openapi.UpdatePartyDetailsResponse.fromJson
        ),
        Mapping[
          v2.admin.user_management_service.UpdateUserIdentityProviderIdRequest,
          openapi.UpdateUserIdentityProviderIdRequest,
        ](
          openapi.UpdateUserIdentityProviderIdRequest.fromJson
        ),
        Mapping[v2.admin.user_management_service.UpdateUserRequest, openapi.UpdateUserRequest](
          openapi.UpdateUserRequest.fromJson
        ),
        Mapping[v2.admin.user_management_service.UpdateUserResponse, openapi.UpdateUserResponse](
          openapi.UpdateUserResponse.fromJson
        ),
        Mapping[v2.admin.user_management_service.User, openapi.User](
          openapi.User.fromJson
        ),
        Mapping[v2.version_service.UserManagementFeature, openapi.UserManagementFeature](
          openapi.UserManagementFeature.fromJson
        ),
        Mapping[v2.transaction_filter.WildcardFilter, openapi.WildcardFilter1](
          openapi.WildcardFilter1.fromJson
        ),
        Mapping[
          v2.transaction_filter.CumulativeFilter.IdentifierFilter.WildcardFilter,
          openapi.WildcardFilter,
        ](
          openapi.WildcardFilter.fromJson
        ),
        Mapping[
          v2.interactive.interactive_submission_service.ExecuteSubmissionAndWaitResponse,
          openapi.ExecuteSubmissionAndWaitResponse,
        ](
          openapi.ExecuteSubmissionAndWaitResponse.fromJson
        ),
        Mapping[v2.package_reference.VettedPackages, openapi.VettedPackages](
          openapi.VettedPackages.fromJson
        ),
        Mapping[v2.package_reference.VettedPackage, openapi.VettedPackage](
          openapi.VettedPackage.fromJson
        ),
        Mapping[
          v2.package_service.PackageMetadataFilter,
          openapi.PackageMetadataFilter,
        ](
          openapi.PackageMetadataFilter.fromJson
        ),
        Mapping[
          v2.package_service.TopologyStateFilter,
          openapi.TopologyStateFilter,
        ](
          openapi.TopologyStateFilter.fromJson
        ),
        Mapping[
          v2.package_service.ListVettedPackagesResponse,
          openapi.ListVettedPackagesResponse,
        ](
          openapi.ListVettedPackagesResponse.fromJson
        ),
        Mapping[
          v2.package_service.ListVettedPackagesRequest,
          openapi.ListVettedPackagesRequest,
        ](
          openapi.ListVettedPackagesRequest.fromJson
        ),
        Mapping[
          v2.admin.package_management_service.UpdateVettedPackagesResponse,
          openapi.UpdateVettedPackagesResponse,
        ](
          openapi.UpdateVettedPackagesResponse.fromJson
        ),
        Mapping[
          v2.admin.package_management_service.UpdateVettedPackagesRequest,
          openapi.UpdateVettedPackagesRequest,
        ](
          openapi.UpdateVettedPackagesRequest.fromJson
        ),
        Mapping[
          v2.admin.package_management_service.UpdateVettedPackagesRequest,
          openapi.UpdateVettedPackagesRequest,
        ](
          openapi.UpdateVettedPackagesRequest.fromJson
        ),
        Mapping[
          v2.admin.package_management_service.VettedPackagesRef,
          openapi.VettedPackagesRef,
        ](
          openapi.VettedPackagesRef.fromJson
        ),
        Mapping[v2.version_service.PackageFeature, openapi.PackageFeature](
          openapi.PackageFeature.fromJson
        ),
        Mapping[
          v2.admin.package_management_service.VettedPackagesChange,
          openapi.VettedPackagesChange,
        ](
          openapi.VettedPackagesChange.fromJson
        ),
        Mapping[v2.admin.package_management_service.VettedPackagesChange.Vet, openapi.Vet1](
          openapi.Vet1.fromJson
        ),
        Mapping[
          v2.admin.package_management_service.VettedPackagesChange.Operation.Vet,
          openapi.Vet,
        ](
          openapi.Vet.fromJson
        ),
        Mapping[v2.admin.package_management_service.VettedPackagesChange.Unvet, openapi.Unvet1](
          openapi.Unvet1.fromJson
        ),
        Mapping[
          v2.admin.package_management_service.VettedPackagesChange.Operation.Unvet,
          openapi.Unvet,
        ](
          openapi.Unvet.fromJson
        ),
        Mapping[
          v2.package_reference.PriorTopologySerial,
          openapi.PriorTopologySerial,
        ](
          openapi.PriorTopologySerial.fromJson
        ),
        Mapping[
          v2.package_reference.PriorTopologySerial.Serial.Prior,
          openapi.Prior,
        ](
          openapi.Prior.fromJson
        ),
        Mapping[
          v2.admin.party_management_service.AllocateExternalPartyRequest.SignedTransaction,
          openapi.SignedTransaction,
        ](
          openapi.SignedTransaction.fromJson
        ),
        Mapping[
          v2.admin.party_management_service.AllocateExternalPartyRequest,
          openapi.AllocateExternalPartyRequest,
        ](
          openapi.AllocateExternalPartyRequest.fromJson
        ),
        Mapping[
          v2.admin.party_management_service.AllocateExternalPartyResponse,
          openapi.AllocateExternalPartyResponse,
        ](
          openapi.AllocateExternalPartyResponse.fromJson
        ),
      )
    }

    object GrpcMappings4 {
      val value: Seq[Mapping[_, _]] = Seq(
        Mapping[
          v2.crypto.SigningPublicKey,
          openapi.SigningPublicKey,
        ](
          openapi.SigningPublicKey.fromJson
        ),
        Mapping[
          v2.admin.party_management_service.GenerateExternalPartyTopologyRequest,
          openapi.GenerateExternalPartyTopologyRequest,
        ](
          openapi.GenerateExternalPartyTopologyRequest.fromJson
        ),
        Mapping[
          v2.admin.party_management_service.GenerateExternalPartyTopologyResponse,
          openapi.GenerateExternalPartyTopologyResponse,
        ](
          openapi.GenerateExternalPartyTopologyResponse.fromJson
        ),
      )
    }

    object JsMappings1 {
      val value: Seq[Mapping[_, _]] =
        Seq(
          Mapping[json.JsCommands, openapi.JsCommands](
            openapi.JsCommands.fromJson
          ),
          Mapping[json.JsGetActiveContractsResponse, openapi.JsGetActiveContractsResponse](
            openapi.JsGetActiveContractsResponse.fromJson
          ),
          Mapping[
            json.js.AllocatePartyRequest,
            openapi.AllocatePartyRequest,
          ](
            openapi.AllocatePartyRequest.fromJson
          ),
          Mapping[json.JsContractEntry.JsActiveContract, openapi.JsActiveContract](
            openapi.JsActiveContract.fromJson
          ),
          Mapping[json.JsArchived, openapi.JsArchived](openapi.JsArchived.fromJson),
          Mapping[json.JsAssignedEvent, openapi.JsAssignedEvent](openapi.JsAssignedEvent.fromJson),
          Mapping[json.JsSchema.JsReassignmentEvent.JsAssignmentEvent, openapi.JsAssignmentEvent](
            openapi.JsAssignmentEvent.fromJson
          ),
          Mapping[json.JsSchema.JsCantonError, openapi.JsCantonError](
            openapi.JsCantonError.fromJson
          ),
          Mapping[json.JsContractEntry.JsContractEntry, openapi.JsContractEntry](
            openapi.JsContractEntry.fromJson
          ),
          // Wrappers for subclasses
          //      Mapping[json.JsContractEntry.JsEmpty.type , openapi.JsContractEntryOneOf1](
          //        openapi.JsContractEntryOneOf1.fromJson
          //      ),

          //      Mapping[json.JsContractEntryOneOf2, openapi.JsContractEntryOneOf2](
          //        openapi.JsContractEntryOneOf2.fromJson
          //      ),
          //      Mapping[json.JsContractEntryOneOf3, openapi.JsContractEntryOneOf3](
          //        openapi.JsContractEntryOneOf3.fromJson
          //      ),
          //      Mapping[json.JsContractEntry.JsActiveContract, openapi.JsContractEntryOneOf](
          //        openapi.JsContractEntryOneOf.fromJson
          //      ),
          Mapping[json.JsCreated, openapi.JsCreated](openapi.JsCreated.fromJson),
          Mapping[json.JsExecuteSubmissionRequest, openapi.JsExecuteSubmissionRequest](
            openapi.JsExecuteSubmissionRequest.fromJson
          ),
          Mapping[json.JsGetEventsByContractIdResponse, openapi.JsGetEventsByContractIdResponse](
            openapi.JsGetEventsByContractIdResponse.fromJson
          ),
          Mapping[json.JsGetTransactionResponse, openapi.JsGetTransactionResponse](
            openapi.JsGetTransactionResponse.fromJson
          ),
          Mapping[json.JsGetTransactionTreeResponse, openapi.JsGetTransactionTreeResponse](
            openapi.JsGetTransactionTreeResponse.fromJson
          ),
          Mapping[json.JsGetUpdateResponse, openapi.JsGetUpdateResponse](
            openapi.JsGetUpdateResponse.fromJson
          ),
          Mapping[json.JsGetUpdatesResponse, openapi.JsGetUpdatesResponse](
            openapi.JsGetUpdatesResponse.fromJson
          ),
        )
    }

    object JsMappings2 {
      val value: Seq[Mapping[_, _]] = Seq(
        Mapping[json.JsGetUpdateTreesResponse, openapi.JsGetUpdateTreesResponse](
          openapi.JsGetUpdateTreesResponse.fromJson
        ),
        Mapping[json.JsContractEntry.JsIncompleteAssigned, openapi.JsIncompleteAssigned](
          openapi.JsIncompleteAssigned.fromJson
        ),
        Mapping[json.JsContractEntry.JsIncompleteUnassigned, openapi.JsIncompleteUnassigned](
          openapi.JsIncompleteUnassigned.fromJson
        ),
        Mapping[json.JsSchema.JsInterfaceView, openapi.JsInterfaceView](
          openapi.JsInterfaceView.fromJson
        ),
        Mapping[json.JsPrepareSubmissionRequest, openapi.JsPrepareSubmissionRequest](
          openapi.JsPrepareSubmissionRequest.fromJson
        ),
        Mapping[json.JsPrepareSubmissionResponse, openapi.JsPrepareSubmissionResponse](
          openapi.JsPrepareSubmissionResponse.fromJson
        ),
        Mapping[
          json.JsSchema.JsReassignmentEvent.JsReassignmentEvent,
          openapi.JsReassignmentEvent,
        ](
          openapi.JsReassignmentEvent.fromJson
        ),
        Mapping[json.JsSchema.JsReassignment, openapi.JsReassignment](
          openapi.JsReassignment.fromJson
        ),
        Mapping[
          json.JsSubmitAndWaitForReassignmentResponse,
          openapi.JsSubmitAndWaitForReassignmentResponse,
        ](openapi.JsSubmitAndWaitForReassignmentResponse.fromJson),
        Mapping[
          json.JsSubmitAndWaitForTransactionRequest,
          openapi.JsSubmitAndWaitForTransactionRequest,
        ](openapi.JsSubmitAndWaitForTransactionRequest.fromJson),
        Mapping[
          json.JsSubmitAndWaitForTransactionResponse,
          openapi.JsSubmitAndWaitForTransactionResponse,
        ](openapi.JsSubmitAndWaitForTransactionResponse.fromJson),
        Mapping[
          json.JsSubmitAndWaitForTransactionTreeResponse,
          openapi.JsSubmitAndWaitForTransactionTreeResponse,
        ](openapi.JsSubmitAndWaitForTransactionTreeResponse.fromJson),
        Mapping[json.JsSchema.JsTransaction, openapi.JsTransaction](
          openapi.JsTransaction.fromJson
        ),
        Mapping[json.JsSchema.JsTransactionTree, openapi.JsTransactionTree](
          openapi.JsTransactionTree.fromJson
        ),
        Mapping[json.JsSchema.JsReassignmentEvent.JsUnassignedEvent, openapi.JsUnassignedEvent](
          openapi.JsUnassignedEvent.fromJson
        ),
        Mapping[
          json.JsExecuteSubmissionAndWaitRequest,
          openapi.JsExecuteSubmissionAndWaitRequest,
        ](
          openapi.JsExecuteSubmissionAndWaitRequest.fromJson
        ),
        Mapping[
          json.JsExecuteSubmissionAndWaitForTransactionRequest,
          openapi.JsExecuteSubmissionAndWaitForTransactionRequest,
        ](
          openapi.JsExecuteSubmissionAndWaitForTransactionRequest.fromJson
        ),
        Mapping[
          json.JsExecuteSubmissionAndWaitForTransactionResponse,
          openapi.JsExecuteSubmissionAndWaitForTransactionResponse,
        ](
          openapi.JsExecuteSubmissionAndWaitForTransactionResponse.fromJson
        ),
        Mapping[
          LegacyDTOs.GetUpdatesRequest,
          openapi.GetUpdatesRequest,
        ](
          openapi.GetUpdatesRequest.fromJson
        ),
        Mapping[
          LegacyDTOs.TransactionFilter,
          openapi.TransactionFilter,
        ](
          openapi.TransactionFilter.fromJson
        ),
      )
    }
  }
}
