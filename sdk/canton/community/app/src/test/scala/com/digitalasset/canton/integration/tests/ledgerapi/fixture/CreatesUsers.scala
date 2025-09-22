// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.fixture

import com.daml.grpc.AuthCallCredentials
import com.daml.ledger.api.v2.admin.object_meta.ObjectMeta
import com.daml.ledger.api.v2.admin.party_management_service.PartyManagementServiceGrpc.PartyManagementServiceStub as PartyStub
import com.daml.ledger.api.v2.admin.party_management_service.{
  ListKnownPartiesRequest,
  PartyManagementServiceGrpc,
}
import com.daml.ledger.api.v2.admin.user_management_service as proto
import com.daml.ledger.api.v2.admin.user_management_service.User

import scala.concurrent.{ExecutionContext, Future}

trait CreatesUsers {
  self: CantonFixture =>

  case class PrerequisiteUser(
      userId: String,
      readAsParties: List[String] = List.empty,
      actAsParties: List[String] = List.empty,
      executeAsParties: List[String] = List.empty,
      readAsAnyParty: Boolean = false,
      executeAsAnyParty: Boolean = false,
  )

  private def wrapStub[Stub <: io.grpc.stub.AbstractStub[Stub]](
      token: Option[String],
      stub: Stub,
  ): Stub =
    token.fold(stub)(AuthCallCredentials.authorizingStub(stub, _))
  private def resolveParties(stub: PartyStub, partyHints: List[String])(implicit
      ec: ExecutionContext
  ): Future[Seq[String]] = partyHints match {
    case Nil => Future.successful(List.empty)
    case _ =>
      stub
        .listKnownParties(
          ListKnownPartiesRequest(
            pageToken = "",
            pageSize = 0,
            identityProviderId = "",
          )
        )
        .map(
          _.partyDetails
            .map(_.party)
            .filter(resolved => partyHints.exists(unresolved => resolved.startsWith(unresolved)))
        )
  }

  def createUser(
      token: Option[String],
      prerequisiteUser: PrerequisiteUser,
  )(implicit ec: ExecutionContext): Future[User] = {
    val userStub = wrapStub(token, proto.UserManagementServiceGrpc.stub(channel))
    val partyStub = wrapStub(token, PartyManagementServiceGrpc.stub(channel))
    for {
      readAsParties <- resolveParties(partyStub, prerequisiteUser.readAsParties)
      actAsParties <- resolveParties(partyStub, prerequisiteUser.actAsParties)
      executeAsParties <- resolveParties(partyStub, prerequisiteUser.executeAsParties)
      primaryParty = (readAsParties ++ actAsParties).headOption.getOrElse("")
      user = User(
        id = prerequisiteUser.userId,
        primaryParty = primaryParty,
        isDeactivated = false,
        metadata = Some(ObjectMeta.defaultInstance),
        identityProviderId = "",
      )
      rights = readAsParties.map(p =>
        proto.Right(proto.Right.Kind.CanReadAs(proto.Right.CanReadAs(p)))
      ) ++
        actAsParties.map(p => proto.Right(proto.Right.Kind.CanActAs(proto.Right.CanActAs(p)))) ++
        executeAsParties.map(p =>
          proto.Right(proto.Right.Kind.CanExecuteAs(proto.Right.CanExecuteAs(p)))
        ) ++ (if (prerequisiteUser.readAsAnyParty)
                Seq(
                  proto.Right(proto.Right.Kind.CanReadAsAnyParty(proto.Right.CanReadAsAnyParty()))
                )
              else Seq.empty) ++ (if (prerequisiteUser.executeAsAnyParty)
                                    Seq(
                                      proto.Right(
                                        proto.Right.Kind
                                          .CanExecuteAsAnyParty(proto.Right.CanExecuteAsAnyParty())
                                      )
                                    )
                                  else Seq.empty)
      req = proto.CreateUserRequest(Some(user), rights)
      user <- userStub.createUser(req).map(_.getUser)
    } yield user
  }

  protected def createPrerequisiteUsers(
      token: Option[String],
      prerequisiteUsers: List[PrerequisiteUser],
  )(implicit
      ec: ExecutionContext
  ): Unit =
    timeouts.default.await_("creating prerequisite users")(
      Future.sequence(prerequisiteUsers.map(createUser(token, _)))
    )

}
