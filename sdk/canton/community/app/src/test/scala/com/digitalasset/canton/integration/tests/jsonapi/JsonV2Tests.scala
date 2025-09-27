// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.jsonapi

import com.daml.ledger.api.v2.admin.object_meta.ObjectMeta
import com.daml.ledger.api.v2.admin.party_management_service.{
  AllocatePartyResponse,
  GetPartiesResponse,
  ListKnownPartiesResponse,
  PartyDetails,
}
import com.daml.ledger.api.v2.admin.user_management_service.{GetUserResponse, ListUsersResponse}
import com.daml.ledger.api.v2.admin.{
  identity_provider_config_service,
  party_management_service,
  user_management_service,
}
import com.daml.ledger.api.v2.command_service.SubmitAndWaitResponse
import com.daml.ledger.api.v2.commands.Commands.DeduplicationPeriod
import com.daml.ledger.api.v2.package_service.ListPackagesResponse
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.{
  TRANSACTION_SHAPE_ACS_DELTA,
  TRANSACTION_SHAPE_LEDGER_EFFECTS,
}
import com.daml.ledger.api.v2.transaction_filter.{
  EventFormat,
  Filters,
  TransactionFormat,
  TransactionShape,
  UpdateFormat,
}
import com.daml.ledger.api.v2.value.Identifier
import com.daml.ledger.api.v2.version_service.GetLedgerApiVersionResponse
import com.daml.ledger.api.v2.{
  command_completion_service,
  command_submission_service,
  reassignment_commands,
  state_service,
  transaction_filter,
  update_service,
}
import com.digitalasset.base.error.ErrorCategory
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.http.json.SprayJson
import com.digitalasset.canton.http.json.v2.JsCommandServiceCodecs.*
import com.digitalasset.canton.http.json.v2.JsContractEntry.JsActiveContract
import com.digitalasset.canton.http.json.v2.JsEventServiceCodecs.*
import com.digitalasset.canton.http.json.v2.JsIdentityProviderCodecs.*
import com.digitalasset.canton.http.json.v2.JsPackageCodecs.*
import com.digitalasset.canton.http.json.v2.JsPartyManagementCodecs.*
import com.digitalasset.canton.http.json.v2.JsSchema.DirectScalaPbRwImplicits.*
import com.digitalasset.canton.http.json.v2.JsSchema.JsServicesCommonCodecs.*
import com.digitalasset.canton.http.json.v2.JsSchema.{JsCantonError, JsEvent}
import com.digitalasset.canton.http.json.v2.JsStateServiceCodecs.*
import com.digitalasset.canton.http.json.v2.JsUpdateServiceCodecs.*
import com.digitalasset.canton.http.json.v2.JsUserManagementCodecs.*
import com.digitalasset.canton.http.json.v2.JsVersionServiceCodecs.*
import com.digitalasset.canton.http.json.v2.js.AllocatePartyRequest
import com.digitalasset.canton.http.json.v2.{
  IdentifierConverter,
  JsCommand,
  JsCommands,
  JsGetActiveContractsResponse,
  JsGetEventsByContractIdResponse,
  JsGetTransactionTreeResponse,
  JsGetUpdateTreesResponse,
  JsGetUpdatesResponse,
  JsSubmitAndWaitForTransactionRequest,
  JsSubmitAndWaitForTransactionResponse,
  JsSubmitAndWaitForTransactionTreeResponse,
  JsUpdate,
  LegacyDTOs,
}
import com.digitalasset.canton.http.util.ClientUtil.uniqueId
import com.digitalasset.canton.http.{Party, WebsocketConfig}
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer
import com.digitalasset.canton.integration.tests.jsonapi.AbstractHttpServiceIntegrationTestFuns.{
  HttpServiceTestFixtureData,
  dar1,
}
import com.digitalasset.canton.integration.tests.jsonapi.HttpServiceTestFixture.UseTls
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.OffsetAfterLedgerEnd
import com.digitalasset.canton.ledger.service.MetadataReader
import com.digitalasset.canton.logging.NamedLogging.loggerWithoutTracing
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.testing.utils.TestModels
import com.digitalasset.canton.tracing.{SerializableTraceContextConverter, W3CTraceContext}
import com.digitalasset.canton.util.JarResourceUtils
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.language.LanguageVersion
import com.google.protobuf
import com.google.protobuf.ByteString
import com.google.protobuf.field_mask.FieldMask
import io.circe.Json
import io.circe.parser.decode
import io.circe.syntax.*
import org.apache.commons.lang3.RandomStringUtils
import org.apache.pekko.http.scaladsl.model.Uri.Query
import org.apache.pekko.http.scaladsl.model.ws.{Message, TextMessage}
import org.apache.pekko.http.scaladsl.model.{HttpHeader, HttpMethods, StatusCodes, Uri}
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import scalaz.syntax.tag.*
import spray.json.{JsArray, JsNumber, JsObject, JsString, JsonParser}

import java.nio.file.Files
import java.util.UUID
import scala.concurrent.Future
import scala.concurrent.duration.*

/** Simple tests of all V2 endpoints.
  *
  * Uses TLS/https
  */
class JsonV2Tests
    extends AbstractHttpServiceIntegrationTestFuns
    with HttpServiceUserFixture.UserToken {
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  // Configure extremely small wait time to avoid long test times and test edge cases
  override def wsConfig: Option[WebsocketConfig] = Some(
    WebsocketConfig(httpListWaitTime = 1.milliseconds)
  )

  override def useTls: UseTls = UseTls.Tls

  "package management" should {
    "upload and download dar" in httpTestFixture { fixture =>
      val darPath = JarResourceUtils
        .resourceFile(
          TestModels.daml_lf_encoder_test_dar(
            LanguageVersion.StableVersions(LanguageVersion.Major.V2).max.pretty
          )
        )
        .toPath
      val darContent: ByteString = protobuf.ByteString.copyFrom(Files.readAllBytes(darPath))
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (_, headers) =>
        for {
          initialPackages <- fixture.getRequestString(Uri.Path("/v2/packages"), headers).map {
            case (_, result) =>
              decode[ListPackagesResponse](result).getOrElse(ListPackagesResponse(Nil))
          }
          _ <- fixture
            .postBinaryContent(Uri.Path("/v2/packages"), darContent, headers)
            .map { case (status, _) =>
              status should be(StatusCodes.OK)
            }
          newPackage <- fixture.getRequestString(Uri.Path("/v2/packages"), headers).map {
            case (_, result) =>
              val finalPackages =
                decode[ListPackagesResponse](result).getOrElse(ListPackagesResponse(Nil))

              val diff = finalPackages.packageIds.diff(initialPackages.packageIds)
              diff.size should be(1)
              diff.head
          }
          _ <- fixture.getRequestBinary(Uri.Path(s"/v2/packages/$newPackage"), headers).map {
            case (_, result) =>
              // TODO (i19593) Dar upload and download work on different object
              result should not be empty
          }
          _ <- fixture
            .getRequestString(Uri.Path(s"/v2/packages/$newPackage/status"), headers)
            .map { case (status, _) =>
              status should be(StatusCodes.OK)
            }
        } yield ()
      }
    }
  }

  import org.scalatest.matchers.{Matcher, MatchResult}
  def anyMatch[T](matcher: Matcher[T]) = Matcher { (items: Iterable[T]) =>
    MatchResult(
      items.exists(matcher(_).matches),
      s"The items $items did not contain any element matching $matcher",
      s"The items $items did contain an element matching $matcher",
    )
  }

  "party management service" should {
    "allocate, update and get party" in httpTestFixture { fixture =>
      val partyId = uniqueId()
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (_, headers) =>
        val jsAllocate = JsonParser(
          AllocatePartyRequest(partyIdHint = s"Carol:-_ $partyId").asJson
            .toString()
        )

        for {
          allocatedParty <- fixture
            .postJsonRequest(Uri.Path("/v2/parties"), jsAllocate, headers)
            .map { case (status, result) =>
              status should be(StatusCodes.OK)
              decode[AllocatePartyResponse](result.toString()).left
                .map(_.toString)
                .flatMap(_.partyDetails.toRight("").map(_.party))
                .getOrElse("")
            }
          // testing basic paging
          nextPageToken <- getRequestEncoded(
            fixture.uri
              withPath Uri.Path(s"/v2/parties")
              withQuery Query(("pageSize", "1")),
            headers,
          )
            .map { case (_, result) =>
              val parties = decode[ListKnownPartiesResponse](result).value
              parties.nextPageToken should not be empty
              parties.partyDetails.length should be(1)
              parties.nextPageToken
            }
          _ <- getRequestEncoded(
            fixture.uri
              withPath Uri.Path(s"/v2/parties")
              withQuery Query(("pageSize", "1"), ("pageToken", nextPageToken)),
            headers,
          )
            .map { case (_, result) =>
              val parties = decode[ListKnownPartiesResponse](result).value
              parties.nextPageToken should not be (nextPageToken)
              parties.partyDetails.length should be(1)
              parties.nextPageToken
            }
          _ <- getRequestEncoded(
            fixture.uri
              withPath Uri.Path(s"/v2/parties/$allocatedParty")
              withQuery Query(("identity-provider-id", "some-idp")),
            headers,
          )
            .map { case (status, result) =>
              status should be(StatusCodes.OK)
              inside(decode[GetPartiesResponse](result)) { case Right(parties) =>
                parties.partyDetails.map(_.party) should anyMatch(include(partyId))
              }
            }

          _ <- fixture
            .jsonRequest(
              HttpMethods.PATCH,
              Uri.Path(s"/v2/parties/$allocatedParty"),
              Some(
                JsonParser(
                  party_management_service
                    .UpdatePartyDetailsRequest(
                      partyDetails = Some(
                        PartyDetails(
                          party = allocatedParty,
                          isLocal = false,
                          localMetadata = Some(
                            ObjectMeta(resourceVersion = "", annotations = Map("test" -> "test"))
                          ),
                          identityProviderId = "",
                        )
                      ),
                      updateMask = Some(new FieldMask(Seq("local_metadata"))),
                    )
                    .asJson
                    .toString()
                )
              ),
              headers,
            )
            .map { case (status, _) =>
              status should be(StatusCodes.OK)
            }
        } yield ()

      }
    }
    "allocate a party on a specific synchronizers" in httpTestFixture { fixture =>
      val partyId = uniqueId()
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (_, headers) =>
        val jsAllocate = JsonParser(
          AllocatePartyRequest(
            partyIdHint = s"Carol:-_ $partyId",
            synchronizerId = validSynchronizerId.toProtoPrimitive,
          ).asJson
            .toString()
        )

        for {
          allocatedParty <- fixture
            .postJsonRequest(Uri.Path("/v2/parties"), jsAllocate, headers)
            .map { case (status, result) =>
              status should be(StatusCodes.OK)
              decode[AllocatePartyResponse](result.toString()).left
                .map(_.toString)
                .flatMap(_.partyDetails.toRight("").map(_.party))
                .getOrElse("")
            }
          _ <- getRequestEncoded(
            fixture.uri
              withPath Uri.Path(s"/v2/parties/$allocatedParty")
              withQuery Query(("identity-provider-id", "some-idp")),
            headers,
          )
            .map { case (status, result) =>
              status should be(StatusCodes.OK)
              inside(decode[GetPartiesResponse](result)) { case Right(parties) =>
                parties.partyDetails.map(_.party) should anyMatch(include(partyId))
              }
            }
        } yield ()
      }
    }
    "get participant id" in httpTestFixture { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (_, headers) =>
        for {
          _ <- fixture
            .getRequestString(Uri.Path("/v2/parties/participant-id"), headers)
            .map { case (status, result) =>
              status should be(StatusCodes.OK)
              inside(decode[party_management_service.GetParticipantIdResponse](result)) {
                case Right(participantIdResponse) =>
                  participantIdResponse.participantId should startWith("participant1")
              }

            }
        } yield ()
      }
    }
  }

  "version service" should {
    "get version" in httpTestFixture { fixture =>
      val semVerRegex =
        """^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$"""
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (_, headers) =>
        for {
          version <- fixture
            .getRequestString(Uri.Path("/v2/version"), headers)
            .map { case (_, result) =>
              decode[GetLedgerApiVersionResponse](result)
            }
        } yield inside(version) { case Right(v) =>
          v.version should fullyMatch regex semVerRegex
        }
      }
    }
  }

  "user management service" should {
    "create, update and delete user" in httpTestFixture { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (_, headers) =>
        val userId = "CreatedUser@^$.!`-#+'~_|:"
        val user = user_management_service.User.defaultInstance.copy(id = userId)
        val updated_user =
          user_management_service.User.defaultInstance.copy(id = userId, isDeactivated = true)

        for {
          _ <- fixture
            .postJsonRequest(
              Uri.Path("/v2/users"),
              toSprayJson(
                user_management_service.CreateUserRequest(user = Some(user), rights = Nil)
              ),
              headers,
            )
            .map { case (status, user) =>
              status should be(StatusCodes.OK)
              user
            }
          usersList <- fixture.getRequestString(Uri.Path("/v2/users"), headers).map {
            case (_, result) =>
              val users = decode[ListUsersResponse](result).getOrElse(ListUsersResponse(Nil, ""))
              users.users.map(_.id) should contain(userId)
              users
          }

          _ <- fixture.getRequestString(Uri.Path(s"/v2/users/$userId"), headers).map {
            case (_, result) =>
              val user = decode[GetUserResponse](result).getOrElse(GetUserResponse(None))
              user.user.map(_.id) should be(Some(userId))
              user
          }

          _ <- fixture
            .jsonRequest(
              HttpMethods.PATCH,
              Uri.Path(s"/v2/users/$userId"),
              Some(
                toSprayJson(
                  user_management_service.UpdateUserRequest(
                    user = Some(updated_user),
                    updateMask = Some(new FieldMask(Seq("is_deactivated"))),
                  )
                )
              ),
              headers,
            )
            .map { case (_, result) =>
              val user = decode[GetUserResponse](result.toString()).getOrElse(GetUserResponse(None))
              user.user.map(_.id) should be(Some(userId))
              user.user.map(_.isDeactivated) should be(Some(true))
            }
          _ <- fixture
            .jsonRequest(
              HttpMethods.DELETE,
              Uri.Path(s"/v2/users/$userId"),
              None,
              headers,
            )
            .map { case (status, result) =>
              val user = decode[GetUserResponse](result.toString())
              status should be(StatusCodes.OK)
              user.isRight should be(true)
            }
        } yield usersList.users.size should be >= 1
      }
    }
    "manipulate user rights" in httpTestFixture { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (_, headers) =>
        val testUserName = "TestUser"
        val user = user_management_service.User.defaultInstance.copy(id = testUserName)
        val adminPermission =
          user_management_service.Right(
            user_management_service.Right.Kind
              .ParticipantAdmin(user_management_service.Right.ParticipantAdmin())
          )
        for {
          _ <- fixture
            .postJsonRequest(
              Uri.Path("/v2/users"),
              toSprayJson(
                user_management_service.CreateUserRequest(user = Some(user), rights = Nil)
              ),
              headers,
            )
            .map { case (status, user) =>
              status should be(StatusCodes.OK)
              user
            }
          _ <- fixture
            .postJsonRequest(
              Uri.Path(s"/v2/users/$testUserName/rights"),
              toSprayJson(
                user_management_service
                  .GrantUserRightsRequest(testUserName, Seq(adminPermission), "")
              ),
              headers,
            )
            .map { case (status, result) =>
              val granted =
                decode[user_management_service.GrantUserRightsResponse](result.toString())
              granted.map(_.newlyGrantedRights.head.kind.isParticipantAdmin) should be(Right(true))
              status should be(StatusCodes.OK)
              granted
            }
          _ <- fixture
            .jsonRequest(
              HttpMethods.PATCH,
              Uri.Path(s"/v2/users/$testUserName/rights"),
              Some(
                toSprayJson(
                  user_management_service
                    .RevokeUserRightsRequest(testUserName, Seq(adminPermission), "")
                )
              ),
              headers,
            )
            .map { case (status, result) =>
              val revoked =
                decode[user_management_service.RevokeUserRightsResponse](result.toString())
              revoked.map(_.newlyRevokedRights.head.kind.isParticipantAdmin) should be(Right(true))
              status should be(StatusCodes.OK)
              revoked
            }

          _ <- fixture
            .getRequestString(
              Uri.Path(s"/v2/users/$testUserName/rights"),
              headers,
            )
            .map { case (_, result) =>
              val rights =
                decode[user_management_service.ListUserRightsResponse](result)
              rights.map(_.rights) should be(Right(Seq()))
              rights
            }

        } yield ()
      }
    }

    "update identity provider" in httpTestFixture { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (_, headers) =>
        val idpId = "idp-id-" + UUID.randomUUID().toString
        val testUserName = "TestUserWithIdps"
        val user = user_management_service.User.defaultInstance.copy(id = testUserName)
        for {
          _ <- fixture
            .postJsonRequest(
              Uri.Path("/v2/idps"),
              toSprayJson(
                identity_provider_config_service.CreateIdentityProviderConfigRequest(
                  Some(
                    identity_provider_config_service.IdentityProviderConfig(
                      identityProviderId = idpId,
                      isDeactivated = false,
                      issuer = "user-idp-test",
                      jwksUrl = "https://localhost",
                      audience = "",
                    )
                  )
                )
              ),
              headers,
            )
            .map { case (status, result) =>
              status should be(StatusCodes.OK)
            }

          _ <- fixture
            .postJsonRequest(
              Uri.Path("/v2/users"),
              toSprayJson(
                user_management_service.CreateUserRequest(user = Some(user), rights = Nil)
              ),
              headers,
            )
            .map { case (status, user) =>
              status should be(StatusCodes.OK)
              user
            }
          _ <- fixture
            .jsonRequest(
              HttpMethods.PATCH,
              Uri.Path(s"/v2/users/$testUserName/identity-provider-id"),
              Some(
                toSprayJson(
                  user_management_service
                    .UpdateUserIdentityProviderIdRequest(testUserName, "", idpId)
                )
              ),
              headers,
            )
            .map { case (status, _) =>
              status should be(StatusCodes.OK)
            }
        } yield ()
      }
    }
  }

  "identity provider service" should {
    "add,patch and delete identity provider config" in httpTestFixture { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (_, headers) =>
        val idpId =
          "idp-id-#:-_ " + UUID
            .randomUUID()
            .toString // we use extra LedgerString allowed characters

        def listIdps() =
          fixture.getRequestString(Uri.Path("/v2/idps"), headers).map { case (status, result) =>
            status should be(StatusCodes.OK)
            decode[identity_provider_config_service.ListIdentityProviderConfigsResponse](result)
          }

        for {
          initialIdps <- listIdps()
          _ <- fixture
            .postJsonRequest(
              Uri.Path("/v2/idps"),
              toSprayJson(
                identity_provider_config_service.CreateIdentityProviderConfigRequest(
                  Some(
                    identity_provider_config_service.IdentityProviderConfig(
                      identityProviderId = idpId,
                      isDeactivated = false,
                      issuer = "createIdpTest",
                      jwksUrl = "https://localhost",
                      audience = "",
                    )
                  )
                )
              ),
              headers,
            )
            .map { case (status, result) =>
              status should be(StatusCodes.OK)
            }

          _ <- fixture
            .jsonRequest(
              HttpMethods.PATCH,
              Uri.Path(s"/v2/idps/$idpId"),
              Some(
                toSprayJson(
                  identity_provider_config_service.UpdateIdentityProviderConfigRequest(
                    Some(
                      identity_provider_config_service.IdentityProviderConfig(
                        identityProviderId = idpId,
                        isDeactivated = true,
                        issuer = "",
                        jwksUrl = "",
                        audience = "",
                      )
                    ),
                    updateMask = Some(new FieldMask(Seq("is_deactivated"))),
                  )
                )
              ),
              headers,
            )
            .map { case (status, _) =>
              status should be(StatusCodes.OK)
            }

          _ <- fixture.getRequestString(Uri.Path(s"/v2/idps/$idpId"), headers).map {
            case (status, result) =>
              status should be(StatusCodes.OK)
              val idps =
                decode[identity_provider_config_service.GetIdentityProviderConfigResponse](result)
              inside(idps) { case Right(value) =>
                value.identityProviderConfig.map(_.isDeactivated) should be(Some(true))
              }
          }

          modifiedIdps <- listIdps()

          _ <- fixture
            .jsonRequest(
              HttpMethods.DELETE,
              Uri.Path(s"/v2/idps/$idpId"),
              None,
              headers,
            )
            .map { case (status, _) =>
              status should be(StatusCodes.OK)
            }
          _ <- Future {
            Threading.sleep(2000)
          }
          finalIdps <- listIdps()

        } yield inside(modifiedIdps) { case Right(idpsAfterCreate) =>
          inside(initialIdps) { case Right(idpsBeforeCreate) =>
            idpsAfterCreate.identityProviderConfigs.size should be(
              idpsBeforeCreate.identityProviderConfigs.size + 1
            )
            inside(finalIdps) { case Right(idpsAfterDelete) =>
              idpsAfterDelete should be(
                idpsBeforeCreate
              )
            }
          }
        }
      }
    }
  }

  "command service" should {
    "create commands and return proper traceContext" in httpTestFixture { fixture =>
      val metadataDar1: MetadataReader.LfMetadata =
        MetadataReader.readFromDar(dar1).valueOr(e => fail(s"Cannot read dar1 metadata: $e"))

      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        val iouPkgId: PackageId = MetadataReader
          .templateByName(metadataDar1)(Ref.QualifiedName.assertFromString("Iou:Iou"))
          .head
          ._1
        var sId = 0

        def jsCommands(jsCommand: JsCommand.Command) = {
          sId += 1
          JsCommands(
            commands = Seq(jsCommand),
            workflowId = None,
            userId = Some(s"defaultuser$sId"),
            commandId = "somecommandid",
            deduplicationPeriod = Some(DeduplicationPeriod.Empty),
            actAs = Seq(alice.unwrap),
            readAs = Seq(alice.unwrap),
            submissionId = Some(s"somesubmissionid$sId"),
            synchronizerId = None,
            minLedgerTimeAbs = None,
            minLedgerTimeRel = None,
            disclosedContracts = Seq.empty,
            packageIdSelectionPreference = Seq.empty,
          )
        }

        val createJsCommand = JsCommand.CreateCommand(
          templateId = Identifier(iouPkgId, "Iou", "Iou"),
          createArguments = io.circe.parser
            .parse(
              s"""{"observers":[],"issuer":"$alice","amount":999.99,"currency":"USD","owner":"$alice"}"""
            )
            .value,
        )

        def generateHex(length: Int): String =
          RandomStringUtils.random(length, "0123456789abcdef").toLowerCase

        val randomTraceId = generateHex(32)

        val testContextHeaders =
          extractHeaders(W3CTraceContext(s"00-$randomTraceId-93bb0fa23a8fb53a-01"))

        for {

          _ <- loggerFactory.assertLogsSeq(SuppressionRule.Level(org.slf4j.event.Level.DEBUG))(
            postJsonRequest(
              uri =
                fixture.uri.withPath(Uri.Path("/v2/commands/submit-and-wait-for-transaction-tree")),
              json = SprayJson
                .parse(jsCommands(createJsCommand).asJson.noSpaces)
                .valueOr(err => fail(s"$err")),
              headers = headers ++ testContextHeaders,
            ).map { case (statusCode, result) =>
              statusCode should be(StatusCodes.OK)
              val transactionTreeResponse =
                decode[JsSubmitAndWaitForTransactionTreeResponse](result.toString())
              inside(transactionTreeResponse) { case Right(response) =>
                response.transactionTree.eventsById should not be empty
                val resultTraceId = SerializableTraceContextConverter
                  .fromDamlProtoSafeOpt(loggerWithoutTracing(logger))(
                    response.transactionTree.traceContext
                  )
                  .traceContext
                  .traceId
                resultTraceId.value should be(randomTraceId)
              }
            },
            evs =>
              atLeast(1, evs.map(_.message)) should include(
                "POST /v2/commands/submit-and-wait-for-transaction-tree"
              ),
          )
          // check wrong template error
          _ <- postJsonRequest(
            uri = fixture.uri.withPath(Uri.Path("/v2/commands/submit-and-wait")),
            json = SprayJson
              .parse(
                jsCommands(
                  createJsCommand.copy(templateId = Identifier("Bad", "Template", "Id"))
                ).asJson.noSpaces
              )
              .valueOr(err => fail(s"$err")),
            headers = headers,
          ).map { case (statusCode, result) =>
            statusCode should be(StatusCodes.NotFound)
            val cantonError =
              decode[JsCantonError](result.toString())
            cantonError.value.errorCategory should be(
              ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing.asInt
            )
          }
          // malformed  template error
          _ <- postJsonRequest(
            uri = fixture.uri.withPath(Uri.Path("/v2/commands/submit-and-wait")),
            json = SprayJson
              .parse(
                jsCommands(
                  createJsCommand.copy(templateId = Identifier("###TemplateMalformed", "x", "y"))
                ).asJson.noSpaces
              )
              .valueOr(err => fail(s"$err")),
            headers = headers,
          ).map { case (statusCode, result) =>
            statusCode should be(StatusCodes.BadRequest)
            val cantonError =
              decode[JsCantonError](result.toString())
            cantonError.value.errorCategory should be(
              ErrorCategory.InvalidIndependentOfSystemState.asInt
            )
          }
          _ <- postJsonRequest(
            uri = fixture.uri.withPath(Uri.Path("/v2/commands/submit-and-wait")),
            json = SprayJson
              .parse(jsCommands(createJsCommand).asJson.noSpaces)
              .valueOr(err => fail(s"$err")),
            headers = headers,
          ).map { case (statusCode, result) =>
            statusCode should be(StatusCodes.OK)
            val updateIdResponse =
              decode[SubmitAndWaitResponse](result.toString())
            inside(updateIdResponse) { case Right(response) =>
              response.updateId should not be empty
              response.completionOffset should be > 0L
            }
          }
          (contractId, assignment) <- postJsonRequest(
            uri = fixture.uri.withPath(Uri.Path("/v2/commands/submit-and-wait-for-transaction")),
            json = SprayJson
              .parse(
                JsSubmitAndWaitForTransactionRequest(
                  jsCommands(createJsCommand),
                  Some(
                    TransactionFormat(
                      eventFormat = Some(
                        EventFormat(
                          filtersByParty = Map(alice.unwrap -> Filters(Nil)),
                          filtersForAnyParty = None,
                          verbose = true,
                        )
                      ),
                      transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
                    )
                  ),
                ).asJson.noSpaces
              )
              .valueOr(err => fail(s"$err")),
            headers = headers,
          ).map { case (statusCode, result) =>
            statusCode should be(StatusCodes.OK)
            val transactionResponse =
              decode[JsSubmitAndWaitForTransactionResponse](result.toString()).getOrElse(fail())
            val contractId =
              transactionResponse.transaction.events.collect { case ce: JsEvent.CreatedEvent =>
                ce.contractId
              }.head
            (
              contractId,
              reassignment_commands.ReassignmentCommands(
                workflowId = "",
                userId = s"defaultuser$sId",
                commandId = transactionResponse.transaction.commandId,
                commands = Seq(
                  reassignment_commands.ReassignmentCommand(
                    reassignment_commands.ReassignmentCommand.Command.UnassignCommand(
                      reassignment_commands.UnassignCommand(
                        contractId = contractId,
                        source = validSynchronizerId.toProtoPrimitive,
                        target =
                          validSynchronizerId.toProtoPrimitive, // Delibaretely wrong synchronizer here - we expect bad request
                      )
                    )
                  )
                ),
                submitter = "Alice",
                submissionId = "1",
              ),
            )
          }
          _ <- postJsonRequest(
            uri = fixture.uri.withPath(Uri.Path("/v2/commands/async/submit")),
            json = SprayJson
              .parse(jsCommands(createJsCommand).asJson.noSpaces)
              .valueOr(err => fail(s"$err")),
            headers = headers,
          ).map { case (statusCode, result) =>
            statusCode should be(StatusCodes.OK)
            val response =
              decode[command_submission_service.SubmitResponse](result.toString())
            response.isRight should be(true)
          }
          assignmentReq = command_submission_service
            .SubmitReassignmentRequest(reassignmentCommands = Some(assignment))

          _ <- postJsonRequest(
            uri = fixture.uri.withPath(Uri.Path("/v2/commands/async/submit-reassignment")),
            json = SprayJson
              .parse(assignmentReq.asJson.noSpaces)
              .valueOr(err => fail(s"$err")),
            headers = headers,
          ).map { case (statusCode, resp) =>
            // We would need 2 synchronizers to properly test submit-reassignment
            // Temporary solution is just to make a call and expect it is rejected
            val errorE = decode[JsCantonError](resp.toString)
            inside(errorE) { case Right(error) =>
              error.cause should include("Cannot unassign contract")
            }
            statusCode should be(StatusCodes.BadRequest)
          }
        } yield ()

      }
    }

    "return completions as stream" in httpTestFixture { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        for {
          jwt <- jwtForParties(fixture.uri)(List(alice), List())
          _ <- createCommand(fixture, alice, headers)
          completions <- {
            val webSocketFlow =
              websocket(fixture.uri.withPath(Uri.Path("/v2/commands/completions")), jwt)
            val req = command_completion_service.CompletionStreamRequest(
              userId = "defaultuser1",
              parties = Seq(alice.unwrap),
              beginExclusive = 0,
            )
            Source
              .single(
                TextMessage(
                  req.asJson.noSpaces
                )
              )
              .concatMat(Source.maybe[Message])(Keep.left)
              .via(webSocketFlow)
              .collect { case m: TextMessage =>
                m.getStrictText
              }
              .map(
                decode[command_completion_service.CompletionStreamResponse](
                  _
                ).value.completionResponse
              )
              .collect {
                case command_completion_service.CompletionStreamResponse.CompletionResponse
                      .Completion(value) =>
                  value
              }
              .take(1)
              .toMat(Sink.seq)(Keep.right)
              .run()
          }
        } yield {
          completions.head.commandId should be("somecommandid")
        }
      }
    }

    "return completions as list" in httpTestFixture { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        for {
          jwt <- jwtForParties(fixture.uri)(List(alice), List())
          _ <- createCommand(fixture, alice, headers, "cmd1", "completions1")
          _ <- createCommand(fixture, alice, headers, "cmd2", "completions1")
          (status, result) <- fixture.postJsonStringRequest(
            fixture.uri withPath Uri.Path("/v2/commands/completions") withQuery Query(
              ("limit", "2"),
              ("stream_idle_timeout_ms", "1000"),
            ),
            command_completion_service
              .CompletionStreamRequest(
                userId = "completions1",
                parties = Seq(alice.unwrap),
                beginExclusive = 0,
              )
              .asJson
              .toString(),
            headers,
          )
        } yield {
          status should be(StatusCodes.OK)
          val completions = decode[Seq[command_completion_service.CompletionStreamResponse]](
            result.toString()
          ).value
          completions.size should be(2)
        }
      }
    }
  }

  "event service " should {
    "return events by contract id" in httpTestFixture { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        for {
          (contractId, _) <- createCommand(fixture, alice, headers)
          _ <- fixture
            .postJsonRequest(
              Uri.Path("/v2/events/events-by-contract-id"),
              SprayJson
                .parse(s"""{
                "contractId": "$contractId",
                "eventFormat" : {
                     "filtersForAnyParty" : {
                          "cumulative" : [{
                            "identifierFilter" :  {
                                 "WildcardFilter" : {
                                      "value" : {
                                           "includeCreatedEventBlob" : false
                                      }
                                 }
                              }
                          }]
                     },
                     "verbose": false
                }
              }""")
                .toEither
                .value,
              headers,
            )
            .map { case (status, result) =>
              status should be(StatusCodes.OK)
              val response = decode[JsGetEventsByContractIdResponse](result.toString())
              inside(response) { case Right(events) =>
                events.created.map(_.createdEvent).map(_.contractId) should be(
                  Some(contractId)
                )
              }
            }
        } yield ()
      }
    }
  }

  "state service" should {
    "return active contracts" in httpTestFixture { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        for {
          jwt <- jwtForParties(fixture.uri)(List(alice), List())
          _ <- createCommand(fixture, alice, headers)
          endOffset <- fixture.client.stateService.getLedgerEndOffset()
          result <- {
            val webSocketFlow =
              websocket(fixture.uri.withPath(Uri.Path("/v2/state/active-contracts")), jwt)
            val req = state_service
              .GetActiveContractsRequest(
                activeAtOffset = endOffset,
                eventFormat = Some(allTransactionsFormat),
              )

            val message = TextMessage(
              req.asJson.noSpaces
            )
            Source
              .single(
                message
              )
              .concatMat(Source.maybe[Message])(Keep.left)
              .via(webSocketFlow)
              .take(1)
              .collect { case m: TextMessage =>
                m.getStrictText
              }
              .toMat(Sink.seq)(Keep.right)
              .run()
              .map(s => decode[JsGetActiveContractsResponse](s.head))
          }
        } yield {
          inside(result.value.contractEntry) { case ac: JsActiveContract =>
            IdentifierConverter.toJson(ac.createdEvent.templateId) should endWith("Iou:Iou")
          }
        }
      }
    }
    "return active contracts (legacy fields)" in httpTestFixture { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        for {
          jwt <- jwtForParties(fixture.uri)(List(alice), List())
          _ <- createCommand(fixture, alice, headers)
          endOffset <- fixture.client.stateService.getLedgerEndOffset()
          result <- {
            val webSocketFlow =
              websocket(fixture.uri.withPath(Uri.Path("/v2/state/active-contracts")), jwt)
            val req = LegacyDTOs
              .GetActiveContractsRequest(
                filter = Some(allTransactionsFilter),
                activeAtOffset = endOffset,
                verbose = true,
                eventFormat = None,
              )

            val message = TextMessage(
              req.asJson.noSpaces
            )
            Source
              .single(
                message
              )
              .concatMat(Source.maybe[Message])(Keep.left)
              .via(webSocketFlow)
              .take(1)
              .collect { case m: TextMessage =>
                m.getStrictText
              }
              .toMat(Sink.seq)(Keep.right)
              .run()
              .map(s => decode[JsGetActiveContractsResponse](s.head))
          }
        } yield {
          inside(result.value.contractEntry) { case ac: JsActiveContract =>
            IdentifierConverter.toJson(ac.createdEvent.templateId) should endWith("Iou:Iou")
          }
        }
      }
    }
    "return active contracts (empty fields)" in httpTestFixture { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        for {
          jwt <- jwtForParties(fixture.uri)(List(alice), List())
          _ <- createCommand(fixture, alice, headers)
          endOffset <- fixture.client.stateService.getLedgerEndOffset()
          _ <- {
            val webSocketFlow =
              websocket(fixture.uri.withPath(Uri.Path("/v2/state/active-contracts")), jwt)
            val req = LegacyDTOs
              .GetActiveContractsRequest(
                filter = None,
                activeAtOffset = endOffset,
                eventFormat = None,
              )

            val message = TextMessage(
              req.asJson.noSpaces
            )
            Source
              .single(
                message
              )
              .concatMat(Source.maybe[Message])(Keep.left)
              .via(webSocketFlow)
              .take(1)
              .collect { case m: TextMessage =>
                m.getStrictText
              }
              .toMat(Sink.seq)(Keep.right)
              .run()
              .map { value =>
                value
                  .map(decode[JsCantonError])
                  .collect { case Right(error) =>
                    error.errorCategory shouldBe ErrorCategory.InvalidIndependentOfSystemState.asInt
                    error.code should include("INVALID_ARGUMENT")
                    error.cause should include(
                      "Either filter/verbose or event_format is required. Please use either backwards compatible arguments (filter and verbose) or event_format."
                    )
                  }
                  .head
              }
          }
        } yield ()
      }
    }
    "return active contracts list" in httpTestFixture { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        for {
          jwt <- jwtForParties(fixture.uri)(List(alice), List())
          _ <- createCommand(fixture, alice, headers)
          endOffset <- fixture.client.stateService.getLedgerEndOffset()
          result <- fixture
            .postJsonRequest(
              Uri.Path("/v2/state/active-contracts"),
              SprayJson
                .parse(
                  state_service
                    .GetActiveContractsRequest(
                      activeAtOffset = endOffset,
                      eventFormat = Some(allTransactionsFormat),
                    )
                    .asJson
                    .toString()
                )
                .valueOr(err => fail(s"$err")),
              headers,
            )
            .map { case (_, list) =>
              decode[Seq[JsGetActiveContractsResponse]](list.toString())
            }

        } yield {
          result.value.size should be >= 1
          inside(result.value.last.contractEntry) { case ac: JsActiveContract =>
            IdentifierConverter.toJson(ac.createdEvent.templateId) should endWith("Iou:Iou")
          }
        }
      }
    }
    "return active contracts list (event_format and verbose fields set)" in httpTestFixture {
      fixture =>
        fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
          for {
            _ <- createCommand(fixture, alice, headers)
            endOffset <- fixture.client.stateService.getLedgerEndOffset()
            _ <- fixture
              .postJsonRequest(
                Uri.Path("/v2/state/active-contracts"),
                SprayJson
                  .parse(
                    LegacyDTOs
                      .GetActiveContractsRequest(
                        filter = None,
                        activeAtOffset = endOffset,
                        verbose = true,
                        eventFormat = Some(allTransactionsFormat),
                      )
                      .asJson
                      .toString()
                  )
                  .valueOr(err => fail(s"$err")),
                headers,
              )
              .map { case (status, result) =>
                status should be(StatusCodes.BadRequest)
                val cantonError =
                  decode[JsCantonError](result.toString())
                cantonError.value.errorCategory should be(
                  ErrorCategory.InvalidIndependentOfSystemState.asInt
                )
                cantonError.value.cause should include(
                  "Both event_format and verbose are set. Please use either backwards compatible arguments (filter and verbose) or event_format, but not both."
                )
              }
          } yield ()
        }
    }
    "return active contracts list (event_format and filter fields set)" in httpTestFixture {
      fixture =>
        fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
          for {
            _ <- createCommand(fixture, alice, headers)
            endOffset <- fixture.client.stateService.getLedgerEndOffset()
            _ <- fixture
              .postJsonRequest(
                Uri.Path("/v2/state/active-contracts"),
                SprayJson
                  .parse(
                    LegacyDTOs
                      .GetActiveContractsRequest(
                        filter = Some(allTransactionsFilter),
                        activeAtOffset = endOffset,
                        eventFormat = Some(allTransactionsFormat),
                      )
                      .asJson
                      .toString()
                  )
                  .valueOr(err => fail(s"$err")),
                headers,
              )
              .map { case (status, result) =>
                status should be(StatusCodes.BadRequest)
                val cantonError =
                  decode[JsCantonError](result.toString())
                cantonError.value.errorCategory should be(
                  ErrorCategory.InvalidIndependentOfSystemState.asInt
                )
                cantonError.value.cause should include(
                  "Both event_format and filter are set. Please use either backwards compatible arguments (filter and verbose) or event_format, but not both."
                )
              }
          } yield ()
        }
    }
    "handle offset after ledger end for active contracts list" in httpTestFixture { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        for {
          _ <- jwtForParties(fixture.uri)(List(alice), List())
          _ <- createCommand(fixture, alice, headers)
          endOffset <- fixture.client.stateService.getLedgerEndOffset()
          error <- fixture
            .postJsonRequest(
              Uri.Path("/v2/state/active-contracts"),
              SprayJson
                .parse(
                  state_service
                    .GetActiveContractsRequest(
                      activeAtOffset = endOffset + 100,
                      eventFormat = Some(allTransactionsFormat),
                    )
                    .asJson
                    .toString()
                )
                .valueOr(err => fail(s"$err")),
              headers,
            )
            .map { case (status, response) =>
              status should be(StatusCodes.BadRequest)
              decode[JsCantonError](response.toString())
            }
        } yield {
          error.value.cause should include("active_at_offset")
          error.value.code should be(OffsetAfterLedgerEnd.id)
        }
      }
    }
    "return connected synchronizers " in httpTestFixture { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        for {
          _ <- getRequestEncoded(
            fixture.uri withPath Uri.Path(
              "/v2/state/connected-synchronizers"
            ) withRawQueryString (s"party=$alice"),
            headers,
          )
            .map { case (status, result) =>
              status should be(StatusCodes.OK)
              val resp = decode[state_service.GetConnectedSynchronizersResponse](result)
              inside(resp) { case Right(getSynchronizersResponse) =>
                getSynchronizersResponse.connectedSynchronizers should not be empty
              }
            }
        } yield ()
      }
    }
    "get ledger end" in httpTestFixture { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        for {
          endOffset <- fixture.client.stateService.getLedgerEndOffset()
          _ <- getRequestEncoded(
            fixture.uri withPath Uri.Path(
              "/v2/state/ledger-end"
            ),
            headers,
          )
            .map { case (status, result) =>
              status should be(StatusCodes.OK)
              inside(decode[state_service.GetLedgerEndResponse](result)) { case Right(resp) =>
                resp.offset should be > 0L
                resp.offset shouldBe endOffset
              }
            }
        } yield ()
      }
    }
    "get latest pruned offsets" in httpTestFixture { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        for {
          _ <- getRequestEncoded(
            fixture.uri withPath Uri.Path(
              "/v2/state/latest-pruned-offsets"
            ),
            headers,
          )
            .map { case (status, result) =>
              status should be(StatusCodes.OK)
              assert(decode[state_service.GetLatestPrunedOffsetsResponse](result).isRight)
            }
        } yield ()
      }
    }
  }

  "update service" should {
    "return updates" in httpTestFixture { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        for {
          jwt <- jwtForParties(fixture.uri)(List(alice), List())
          (_, offset) <- createCommand(fixture, alice, headers)
          (updateId, _, alternateParty) <- {
            val webSocketFlow =
              websocket(fixture.uri.withPath(Uri.Path("/v2/updates")), jwt)
            Source
              .single(
                TextMessage(
                  updatesRequest.asJson.noSpaces
                )
              )
              .concatMat(Source.maybe[Message])(Keep.left)
              .via(webSocketFlow)
              .take(2)
              .collect { case m: TextMessage =>
                m.getStrictText
              }
              .toMat(Sink.seq)(Keep.right)
              .run()
              .map { updates =>
                updates
                  .map(decode[JsGetUpdatesResponse])
                  .collect { case Right(JsGetUpdatesResponse(JsUpdate.Transaction(tx))) =>
                    inside(tx.events.head) { case event: JsEvent.CreatedEvent =>
                      (tx.updateId, event.nodeId, event.signatories.head)
                    }
                  }
                  .head
              }
          }
          // test finite stream
          _ <- {
            fixture
              .postJsonStringRequest(
                fixture.uri withPath Uri.Path("/v2/updates"),
                updatesRequest.copy(endInclusive = Some(offset)).asJson.toString(),
                headers,
              )
              .map { case (status, result) =>
                status should be(StatusCodes.OK)
                val responses = decode[Seq[JsGetUpdatesResponse]](result.toString()).value
                responses.size should be >= 1 // if zero it means timeouted (endInclusive is set -> so timeout should not be active)
              }
          }
          _ <- {
            fixture
              .postJsonStringRequest(
                fixture.uri withPath Uri.Path("/v2/updates") withQuery Query(
                  ("stream_idle_timeout_ms", "500")
                ),
                updatesRequest.asJson.toString(),
                headers,
              )
              .map { case (status, result) =>
                status should be(StatusCodes.OK)

                val responses = decode[Seq[JsGetUpdatesResponse]](result.toString()).value
                responses.size should be >= 1
              }
          }
          _ <- {
            val webSocketFlow =
              websocket(fixture.uri.withPath(Uri.Path("/v2/updates/flats")), jwt)
            Source
              .single(
                TextMessage(
                  updatesRequestLegacy.asJson.noSpaces
                )
              )
              .concatMat(Source.maybe[Message])(Keep.left)
              .via(webSocketFlow)
              .take(2)
              .collect { case m: TextMessage =>
                m.getStrictText
              }
              .toMat(Sink.seq)(Keep.right)
              .run()
              .map { updates =>
                updates
                  .map(decode[JsGetUpdatesResponse])
                  .collect { case Right(JsGetUpdatesResponse(JsUpdate.Transaction(tx))) =>
                    inside(tx.events.head) { case event: JsEvent.CreatedEvent =>
                      (tx.updateId, event.nodeId, event.signatories.head)
                    }
                  }
                  .head
              }
          }
          _ <- {
            val webSocketFlow =
              websocket(fixture.uri.withPath(Uri.Path("/v2/updates/flats")), jwt)
            Source
              .single(
                TextMessage(
                  updatesRequestLegacy
                    .copy(filter = None)
                    .asJson
                    .noSpaces
                )
              )
              .concatMat(Source.maybe[Message])(Keep.left)
              .via(webSocketFlow)
              .take(1)
              .collect { case m: TextMessage =>
                m.getStrictText
              }
              .toMat(Sink.seq)(Keep.right)
              .run()
              .map { updates =>
                updates
                  .map(decode[JsCantonError])
                  .collect { case Right(error) =>
                    error.errorCategory shouldBe ErrorCategory.InvalidIndependentOfSystemState.asInt
                    error.code should include("INVALID_ARGUMENT")
                    error.cause should include(
                      "Either filter/verbose or update_format is required. Please use either backwards compatible arguments (filter and verbose) or update_format."
                    )
                  }
                  .head
              }
          }

          _ <- {
            fixture
              .postJsonStringRequest(
                fixture.uri withPath Uri.Path("/v2/updates/flats") withQuery Query(
                  ("stream_idle_timeout_ms", "1500")
                ),
                updatesRequestLegacy.asJson.toString(),
                headers,
              )
              .map { case (status, result) =>
                status should be(StatusCodes.OK)

                val responses = decode[Seq[JsGetUpdatesResponse]](result.toString()).value
                responses.size should be >= 1
              }
          }
          _ <- {
            val webSocketFlow =
              websocket(fixture.uri.withPath(Uri.Path("/v2/updates/trees")), jwt)
            Source
              .single(
                TextMessage(
                  updatesRequestLegacy.asJson.noSpaces
                )
              )
              .concatMat(Source.maybe[Message])(Keep.left)
              .via(webSocketFlow)
              .take(1)
              .collect { case m: TextMessage =>
                m.getStrictText
              }
              .toMat(Sink.seq)(Keep.right)
              .run()
              .map { updates =>
                inside(decode[JsGetUpdateTreesResponse](updates.head)) { case Right(value) =>
                  value.update should not be null
                }
              }
          }
          _ <- {
            val webSocketFlow =
              websocket(fixture.uri.withPath(Uri.Path("/v2/updates/trees")), jwt)
            Source
              .single(
                TextMessage(
                  updatesRequestLegacy
                    .copy(filter = None)
                    .asJson
                    .noSpaces
                )
              )
              .concatMat(Source.maybe[Message])(Keep.left)
              .via(webSocketFlow)
              .take(1)
              .collect { case m: TextMessage =>
                m.getStrictText
              }
              .toMat(Sink.seq)(Keep.right)
              .run()
              .map { updates =>
                updates
                  .map(decode[JsCantonError])
                  .collect { case Right(error) =>
                    error.errorCategory shouldBe ErrorCategory.InvalidIndependentOfSystemState.asInt
                    error.code should include("INVALID_ARGUMENT")
                    error.cause should include(
                      "Either filter/verbose or update_format is required. Please use either backwards compatible arguments (filter and verbose) or update_format."
                    )
                  }
                  .head
              }
          }
          _ <- {
            fixture
              .postJsonStringRequest(
                fixture.uri withPath Uri.Path("/v2/updates/trees") withQuery Query(
                  ("stream_idle_timeout_ms", "1500")
                ),
                updatesRequestLegacy.asJson.toString(),
                headers,
              )
              .map { case (status, result) =>
                status should be(StatusCodes.OK)

                val responses = decode[Seq[JsGetUpdateTreesResponse]](result.toString()).value
                responses.size should be >= 1
              }
          }
          _ <- {
            fixture
              .postJsonStringRequest(
                fixture.uri withPath Uri.Path("/v2/updates/trees") withQuery Query(
                  ("stream_idle_timeout_ms", "1500")
                ),
                updatesRequestLegacy
                  .copy(updateFormat = Some(updateFormat(alice.unwrap)))
                  .asJson
                  .toString(),
                headers,
              )
              .map { case (status, result) =>
                status should be(StatusCodes.BadRequest)
                val cantonError =
                  decode[JsCantonError](result.toString())
                cantonError.value.errorCategory should be(
                  ErrorCategory.InvalidIndependentOfSystemState.asInt
                )
                cantonError.value.cause should include(
                  "Both update_format and filter are set. Please use either backwards compatible arguments (filter and verbose) or update_format, but not both."
                )
              }
          }
          _ <- getRequestEncoded(
            fixture.uri withPath Uri.Path(
              s"/v2/updates/transaction-tree-by-offset/$offset"
            ) withRawQueryString (s"parties=$alice"),
            headers,
          )
            .map { case (status, _) =>
              status should be(StatusCodes.OK)
            }
          _ <- postJsonRequest(
            uri = fixture.uri.withPath(Uri.Path("/v2/updates/transaction-by-offset")),
            json = toSprayJson(
              LegacyDTOs.GetTransactionByOffsetRequest(
                offset = offset,
                transactionFormat = transactionFormat(alice.unwrap),
                requestingParties = Nil,
              )
            ),
            headers = headers,
          )
            .map { case (status, _) =>
              status should be(StatusCodes.OK)
            }
          _ <- postJsonRequest(
            uri = fixture.uri.withPath(Uri.Path("/v2/updates/transaction-by-offset")),
            json = toSprayJson(
              LegacyDTOs.GetTransactionByOffsetRequest(
                offset = offset,
                transactionFormat = None,
                requestingParties = Seq(alice.unwrap),
              )
            ),
            headers = headers,
          )
            .map { case (status, _) =>
              status should be(StatusCodes.OK)
            }
          _ <- postJsonRequest(
            uri = fixture.uri.withPath(Uri.Path("/v2/updates/transaction-by-offset")),
            json = toSprayJson(
              LegacyDTOs.GetTransactionByOffsetRequest(
                offset = offset,
                transactionFormat = None,
                requestingParties = Nil,
              )
            ),
            headers = headers,
          )
            .map { case (status, result) =>
              status should be(StatusCodes.BadRequest)
              val cantonError =
                decode[JsCantonError](result.toString())
              cantonError.value.errorCategory should be(
                ErrorCategory.InvalidIndependentOfSystemState.asInt
              )
              cantonError.value.cause should include(
                "Either transaction_format or requesting_parties is required. Please use either backwards compatible arguments (requesting_parties) or transaction_format."
              )
            }
          _ <- postJsonRequest(
            uri = fixture.uri.withPath(Uri.Path("/v2/updates/transaction-by-id")),
            json = toSprayJson(
              LegacyDTOs.GetTransactionByIdRequest(
                updateId = updateId,
                transactionFormat = transactionFormat(alternateParty), // the party is different
                requestingParties = Nil,
              )
            ),
            headers = headers,
          )
            .map { case (status, _) =>
              status should be(StatusCodes.OK)
            }
          _ <- postJsonRequest(
            uri = fixture.uri.withPath(Uri.Path("/v2/updates/transaction-by-id")),
            json = toSprayJson(
              LegacyDTOs.GetTransactionByIdRequest(
                updateId = updateId,
                transactionFormat = None,
                requestingParties = Seq(alternateParty),
              )
            ),
            headers = headers,
          )
            .map { case (status, _) =>
              status should be(StatusCodes.OK)
            }
          _ <- postJsonRequest(
            uri = fixture.uri.withPath(Uri.Path("/v2/updates/transaction-by-id")),
            json = toSprayJson(
              LegacyDTOs.GetTransactionByIdRequest(
                updateId = updateId,
                transactionFormat = transactionFormat(alternateParty),
                requestingParties = Seq(alternateParty),
              )
            ),
            headers = headers,
          )
            .map { case (status, result) =>
              status should be(StatusCodes.BadRequest)
              val cantonError =
                decode[JsCantonError](result.toString())
              cantonError.value.errorCategory should be(
                ErrorCategory.InvalidIndependentOfSystemState.asInt
              )
              cantonError.value.cause should include(
                "Both transaction_format and requesting_parties are set. Please use either backwards compatible arguments (requesting_parties) or transaction_format but not both."
              )
            }

          _ <- postJsonRequest(
            uri = fixture.uri.withPath(Uri.Path("/v2/updates/update-by-offset")),
            json = toSprayJson(
              update_service.GetUpdateByOffsetRequest(
                offset = offset,
                updateFormat = Some(updateFormat(alice.unwrap)),
              )
            ),
            headers = headers,
          )
            .map { case (status, _) =>
              status should be(StatusCodes.OK)
            }

          _ <- postJsonRequest(
            uri = fixture.uri.withPath(Uri.Path("/v2/updates/update-by-id")),
            json = toSprayJson(
              update_service.GetUpdateByIdRequest(
                updateId = updateId,
                updateFormat = Some(updateFormat(alternateParty)), // the party is different
              )
            ),
            headers = headers,
          )
            .map { case (status, _) =>
              status should be(StatusCodes.OK)
            }

          _ <- getRequestEncoded(
            fixture.uri withPath Uri.Path(
              s"/v2/updates/transaction-tree-by-id/$updateId"
            ) withRawQueryString (s"parties=$alternateParty"),
            headers,
          )
            .map { case (status, result) =>
              status should be(StatusCodes.OK)
              inside(decode[JsGetTransactionTreeResponse](result)) { case Right(tree) =>
                tree.transaction.updateId should be(updateId)
              }
            }
        } yield ()
      }
    }
  }
  "json api" should {
    "handle date" in httpTestFixture { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        for {
          (contractId, _) <- createCommand(fixture, alice, headers)
          (regularDate, _) <- exerciseCommand(
            fixture,
            alice,
            headers,
            contractId,
            "DateCheckAddDays",
            io.circe.parser
              .parse(
                s"""{"payload":"2000-01-01"}"""
              )
              .value,
            "regular_date_check",
          )

          (oldDate, _) <- exerciseCommand(
            fixture,
            alice,
            headers,
            contractId,
            "DateCheckAddDays",
            io.circe.parser
              .parse(
                s"""{"payload":"1930-04-12"}"""
              )
              .value,
            "old_date_check",
          )
        } yield {
          regularDate.toString() should be(""""2000-02-01"""")
          oldDate.toString() should be(""""1930-05-13"""")
        }
      }
    }
    "handle timestamp" in httpTestFixture { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        for {
          (contractId, _) <- createCommand(fixture, alice, headers)
          (regularTime, _) <- exerciseCommand(
            fixture,
            alice,
            headers,
            contractId,
            "TimestampCheckAddHours",
            io.circe.parser
              .parse(
                s"""{"payload":"2000-01-01T10:12:32.777111Z"}"""
              )
              .value,
            "regular_timestamp_check",
          )
          (oldTime, _) <- exerciseCommand(
            fixture,
            alice,
            headers,
            contractId,
            "TimestampCheckAddHours",
            io.circe.parser
              .parse(
                s"""{"payload":"1930-04-14T10:12:32.777111Z"}"""
              )
              .value,
            "old_timestamp_check",
          )
        } yield {
          regularTime.toString() should be(""""2000-01-01T12:12:32.777111Z"""")
          oldTime.toString() should be(""""1930-04-14T12:12:32.777111Z"""")
        }
      }
    }
    "handle textmap" in httpTestFixture { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        for {
          (contractId, _) <- createCommand(fixture, alice, headers)
          (result, _) <- exerciseCommand(
            fixture,
            alice,
            headers,
            contractId,
            "TextMapCheck",
            io.circe.parser
              .parse(
                s"""{"payload":{ "a" : "1", "b" : "2" }}"""
              )
              .value,
          )
        } yield {
          result.noSpaces should be(
            s"""{"a":"1","b":"42"}"""
          )
        }
      }
    }
  }

  private def submitCommand[T](
      fixture: HttpServiceTestFixtureData,
      party: Party,
      headers: List[HttpHeader],
      jsCommand: JsCommand.Command,
      transactionShape: TransactionShape,
      parseResponse: JsSubmitAndWaitForTransactionResponse => T,
      commandId: String,
      userId: String,
  ): Future[T] = {
    val jsCommandJson = JsObject(
      ("commands", JsArray(toSprayJson(jsCommand))),
      ("commandId", JsString(commandId)),
      ("actAs", JsArray(JsString(party.unwrap))),
      ("userId", JsString(userId)),
      ("minLedgerTimeRel", JsObject("nanos" -> JsNumber(100), "seconds" -> JsNumber(1))),
    )

    val jsSubmitAndWaitForTransactionRequest = JsObject(
      "commands" -> jsCommandJson,
      "transactionFormat" -> toSprayJson(
        TransactionFormat(
          eventFormat = Some(
            EventFormat(
              filtersByParty = Map(party.unwrap -> Filters(Nil)),
              filtersForAnyParty = None,
              verbose = true,
            )
          ),
          transactionShape = transactionShape,
        )
      ),
    )

    postJsonRequest(
      uri = fixture.uri.withPath(Uri.Path("/v2/commands/submit-and-wait-for-transaction")),
      json = jsSubmitAndWaitForTransactionRequest,
      headers = headers,
    ).map { case (statusCode, result) =>
      assert(statusCode == StatusCodes.OK, s"Expected OK, got $statusCode with response: $result")
      val transactionResponse =
        decode[JsSubmitAndWaitForTransactionResponse](result.toString()).getOrElse(fail())
      parseResponse(transactionResponse)
    }
  }

  private def exerciseCommand(
      fixture: HttpServiceTestFixtureData,
      party: Party,
      headers: List[HttpHeader],
      contractId: String,
      choice: String,
      argument: Json,
      commandId: String = "someexecommandid",
      userId: String = "defaultuser1",
  ): Future[(Json, Long)] = {
    val metadataDar1 =
      MetadataReader.readFromDar(dar1).valueOr(e => fail(s"Cannot read dar1 metadata: $e"))
    val iouPkgId = MetadataReader
      .templateByName(metadataDar1)(Ref.QualifiedName.assertFromString("Iou:Iou"))
      .head
      ._1

    val exerciseCommand = JsCommand.ExerciseCommand(
      templateId = Identifier(iouPkgId, "Iou", "Iou"),
      contractId = contractId,
      choice = choice,
      choiceArgument = argument,
    )

    submitCommand(
      fixture,
      party,
      headers,
      exerciseCommand,
      TRANSACTION_SHAPE_LEDGER_EFFECTS,
      parseResponse = _.transaction.events
        .collect { case ee: JsEvent.ExercisedEvent => (ee.exerciseResult, ee.offset) }
        .head,
      commandId,
      userId,
    )
  }

  private def createCommand(
      fixture: HttpServiceTestFixtureData,
      party: Party,
      headers: List[HttpHeader],
      commandId: String = "somecommandid",
      userId: String = "defaultuser1",
  ): Future[(String, Long)] = {
    val metadataDar1 =
      MetadataReader.readFromDar(dar1).valueOr(e => fail(s"Cannot read dar1 metadata: $e"))
    val iouPkgId = MetadataReader
      .templateByName(metadataDar1)(Ref.QualifiedName.assertFromString("Iou:Iou"))
      .head
      ._1

    val createCommand = JsCommand.CreateCommand(
      templateId = Identifier(iouPkgId, "Iou", "Iou"),
      createArguments = io.circe.parser
        .parse(
          s"""{"observers":[],"issuer":"$party","amount":"999.99","currency":"USD","owner":"$party"}"""
        )
        .value,
    )

    submitCommand(
      fixture,
      party,
      headers,
      createCommand,
      TRANSACTION_SHAPE_ACS_DELTA,
      parseResponse = _.transaction.events
        .collect { case ce: JsEvent.CreatedEvent => (ce.contractId, ce.offset) }
        .head,
      commandId,
      userId,
    )
  }

  private val allTransactionsFilter = LegacyDTOs.TransactionFilter(
    filtersByParty = Map.empty,
    filtersForAnyParty = Some(
      transaction_filter.Filters(
        cumulative = Seq(
          transaction_filter.CumulativeFilter(
            identifierFilter = transaction_filter.CumulativeFilter.IdentifierFilter
              .WildcardFilter(
                transaction_filter.WildcardFilter(includeCreatedEventBlob = true)
              )
          )
        )
      )
    ),
  )

  private val allTransactionsFormat = transaction_filter.EventFormat(
    filtersByParty = Map.empty,
    filtersForAnyParty = Some(
      transaction_filter.Filters(
        cumulative = Seq(
          transaction_filter.CumulativeFilter(
            identifierFilter = transaction_filter.CumulativeFilter.IdentifierFilter
              .WildcardFilter(
                transaction_filter.WildcardFilter(includeCreatedEventBlob = true)
              )
          )
        )
      )
    ),
    verbose = false,
  )

  private val updatesRequestLegacy = LegacyDTOs.GetUpdatesRequest(
    beginExclusive = 0,
    endInclusive = None,
    filter = Some(allTransactionsFilter),
    verbose = true,
    updateFormat = None,
  )

  private val updatesRequest = update_service.GetUpdatesRequest(
    beginExclusive = 0,
    endInclusive = None,
    updateFormat = Some(
      UpdateFormat(
        includeTransactions = Some(
          TransactionFormat(
            eventFormat = Some(allTransactionsFormat),
            transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
          )
        ),
        includeReassignments = None,
        includeTopologyEvents = None,
      )
    ),
  )

  private def toSprayJson[T](t: T)(implicit encoder: io.circe.Encoder[T]) = JsonParser(
    t.asJson.toString()
  )

  private def transactionFormat(party: String): Option[TransactionFormat] = Some(
    TransactionFormat(
      eventFormat = Some(
        EventFormat(
          filtersByParty = Map(party -> Filters(Nil)),
          filtersForAnyParty = None,
          verbose = false,
        )
      ),
      transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
    )
  )

  private def updateFormat(party: String): UpdateFormat =
    UpdateFormat(
      includeTransactions = transactionFormat(party),
      includeReassignments = None,
      includeTopologyEvents = None,
    )
}
