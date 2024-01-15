// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc

import com.daml.ledger.api.v1.admin.{user_management_service => proto}
import com.daml.ledger.javaapi.data._
import com.daml.ledger.rxjava._
import com.daml.ledger.rxjava.grpc.helpers.LedgerServices
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.Optional

class UserManagementClientImplTest extends AnyFlatSpec with Matchers with AuthMatchers {

  private val ledgerServices = new LedgerServices("user-management-service-ledger")

  behavior of "UserManagementClientImpl"

  it should "send the expected requests" in {
    ledgerServices.withUserManagementClient() { (client, service) =>
      client.createUser(new CreateUserRequest("createId", "createParty")).blockingGet()
      client.getUser(new GetUserRequest("get")).blockingGet()
      client.deleteUser(new DeleteUserRequest("delete")).blockingGet()
      client.listUsers().blockingGet()
      client.listUsers(new ListUsersRequest(Optional.empty(), 10)).blockingGet()
      client.listUsers(new ListUsersRequest(Optional.of("page-token"), 20)).blockingGet()
      client
        .grantUserRights(
          new GrantUserRightsRequest(
            "grant",
            new User.Right.CanReadAs("grantReadAsParty"),
            new User.Right.CanActAs("grantActAsParty"),
          )
        )
        .blockingGet()
      client
        .revokeUserRights(
          new RevokeUserRightsRequest("revoke", User.Right.ParticipantAdmin.INSTANCE)
        )
        .blockingGet()
      client.listUserRights(new ListUserRightsRequest("listRights")).blockingGet()

      val requests = service.requests()
      requests should have length 9
      requests should contain theSameElementsAs Array(
        proto.CreateUserRequest(
          Some(proto.User("createId", "createParty")),
          Vector(proto.Right(proto.Right.Kind.CanActAs(proto.Right.CanActAs("createParty")))),
        ),
        proto.GetUserRequest("get"),
        proto.DeleteUserRequest("delete"),
        proto.ListUsersRequest(),
        proto.ListUsersRequest(pageSize = 10),
        proto.ListUsersRequest("page-token", 20),
        proto.GrantUserRightsRequest(
          "grant",
          Vector(
            proto.Right(proto.Right.Kind.CanReadAs(proto.Right.CanReadAs("grantReadAsParty"))),
            proto.Right(proto.Right.Kind.CanActAs(proto.Right.CanActAs("grantActAsParty"))),
          ),
        ),
        proto.RevokeUserRightsRequest(
          "revoke",
          Vector(proto.Right(proto.Right.Kind.ParticipantAdmin(proto.Right.ParticipantAdmin()))),
        ),
        proto.ListUserRightsRequest("listRights"),
      )
    }
  }

  behavior of "UserManagementClientImpl (Authorization)"

  private def toAuthenticatedServer(fn: UserManagementClient => Any): Any =
    ledgerServices.withUserManagementClient(mockedAuthService) { (client, _) => fn(client) }

  it should "deny access without token" in {
    withClue("createUser") {
      expectUnauthenticated {
        toAuthenticatedServer { client =>
          client.createUser(new CreateUserRequest("id", "party")).blockingGet()
        }
      }
    }
    withClue("getUser") {
      expectUnauthenticated {
        toAuthenticatedServer { client =>
          client.getUser(new GetUserRequest("userId")).blockingGet()
        }
      }
    }
    withClue("deleteUser") {
      expectUnauthenticated {
        toAuthenticatedServer { client =>
          client.deleteUser(new DeleteUserRequest("userId")).blockingGet()
        }
      }
    }
    withClue("listUsers") {
      expectUnauthenticated {
        toAuthenticatedServer { client =>
          client.listUsers().blockingGet()
        }
      }
      expectUnauthenticated {
        toAuthenticatedServer { client =>
          client.listUsers(new ListUsersRequest(Optional.empty(), 100)).blockingGet()
        }
      }
    }
    withClue("grantUserRights") {
      expectUnauthenticated {
        toAuthenticatedServer { client =>
          client
            .grantUserRights(
              new GrantUserRightsRequest("userId", User.Right.ParticipantAdmin.INSTANCE)
            )
            .blockingGet()
        }
      }
    }
    withClue("revokeUserRights") {
      expectUnauthenticated {
        toAuthenticatedServer { client =>
          client
            .revokeUserRights(
              new RevokeUserRightsRequest("userId", User.Right.ParticipantAdmin.INSTANCE)
            )
            .blockingGet()
        }
      }
    }
    withClue("listUserRights") {
      expectUnauthenticated {
        toAuthenticatedServer { client =>
          client.listUserRights(new ListUserRightsRequest("userId")).blockingGet()
        }
      }
    }
  }

  it should "deny access without sufficient authorization" in {
    withClue("createUser") {
      expectUnauthenticated {
        toAuthenticatedServer { client =>
          client.createUser(new CreateUserRequest("id", "party"), emptyToken).blockingGet()
        }
      }
    }
    withClue("getUser") {
      expectUnauthenticated {
        toAuthenticatedServer { client =>
          client.getUser(new GetUserRequest("userId"), emptyToken).blockingGet()
        }
      }
    }
    withClue("deleteUser") {
      expectUnauthenticated {
        toAuthenticatedServer { client =>
          client.deleteUser(new DeleteUserRequest("userId"), emptyToken).blockingGet()
        }
      }
    }
    withClue("listUsers") {
      expectUnauthenticated {
        toAuthenticatedServer { client =>
          client.listUsers(emptyToken).blockingGet()
        }
      }
      expectUnauthenticated {
        toAuthenticatedServer { client =>
          client.listUsers(new ListUsersRequest(Optional.empty(), 100), emptyToken).blockingGet()
        }
      }
    }
    withClue("grantUserRights") {
      expectUnauthenticated {
        toAuthenticatedServer { client =>
          client
            .grantUserRights(
              new GrantUserRightsRequest("userId", User.Right.ParticipantAdmin.INSTANCE),
              emptyToken,
            )
            .blockingGet()
        }
      }
    }
    withClue("revokeUserRights") {
      expectUnauthenticated {
        toAuthenticatedServer { client =>
          client
            .revokeUserRights(
              new RevokeUserRightsRequest("userId", User.Right.ParticipantAdmin.INSTANCE),
              emptyToken,
            )
            .blockingGet()
        }
      }
    }
    withClue("listUserRights") {
      expectUnauthenticated {
        toAuthenticatedServer { client =>
          client.listUserRights(new ListUserRightsRequest("userId"), emptyToken).blockingGet()
        }
      }
    }
  }

  it should "allow access with sufficient authorization" in {

    withClue("createUser") {
      toAuthenticatedServer { client =>
        client.createUser(new CreateUserRequest("id", "party"), adminToken).blockingGet()
      }
    }
    withClue("getUser") {
      toAuthenticatedServer { client =>
        client.getUser(new GetUserRequest("userId"), adminToken).blockingGet()
      }
    }
    withClue("deleteUser") {
      toAuthenticatedServer { client =>
        client.deleteUser(new DeleteUserRequest("userId"), adminToken).blockingGet()
      }
    }
    withClue("listUsers") {
      toAuthenticatedServer { client =>
        client.listUsers(adminToken).blockingGet()
      }
      toAuthenticatedServer { client =>
        client.listUsers(new ListUsersRequest(Optional.empty(), 100), adminToken).blockingGet()
      }
    }
    withClue("grantUserRights") {
      toAuthenticatedServer { client =>
        client
          .grantUserRights(
            new GrantUserRightsRequest("userId", User.Right.ParticipantAdmin.INSTANCE),
            adminToken,
          )
          .blockingGet()
      }
    }
    withClue("revokeUserRights") {
      toAuthenticatedServer { client =>
        client
          .revokeUserRights(
            new RevokeUserRightsRequest("userId", User.Right.ParticipantAdmin.INSTANCE),
            adminToken,
          )
          .blockingGet()
      }
    }
    withClue("listUserRights") {
      toAuthenticatedServer { client =>
        client.listUserRights(new ListUserRightsRequest("userId"), adminToken).blockingGet()
      }
    }
  }

}
