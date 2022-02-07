// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.testtool.infrastructure.Allocation.{ParticipantAllocation, Participants}
import com.daml.ledger.api.testtool.infrastructure.participant.{Features, ParticipantTestContext}
import com.daml.ledger.api.v1.admin.user_management_service.{
  DeleteUserRequest,
  ListUsersRequest,
  ListUsersResponse,
  User,
}
import com.daml.lf.data.Ref

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

private[testtool] abstract class LedgerTestSuite {
  private val testCaseBuffer: ListBuffer[LedgerTestCase] = ListBuffer()

  final lazy val tests: Vector[LedgerTestCase] = testCaseBuffer.toVector

  protected final def test(
      shortIdentifier: String,
      description: String,
      participants: ParticipantAllocation,
      timeoutScale: Double = 1.0,
      runConcurrently: Boolean = true,
      repeated: Int = 1,
      enabled: Features => Boolean = _ => true,
      disabledReason: String = "No reason",
      cleanUpNewUsers: Boolean = false,
  )(testCase: ExecutionContext => PartialFunction[Participants, Future[Unit]]): Unit = {
    testGivenAllParticipants(
      shortIdentifier,
      description,
      participants,
      timeoutScale,
      runConcurrently,
      repeated,
      enabled,
      disabledReason,
      cleanupNewUsers = cleanUpNewUsers,
    )((ec: ExecutionContext) => (_: Seq[ParticipantTestContext]) => testCase(ec))
  }

  /** @param cleanupNewUsers Keeps track of users present before the test case started and after it has finished and deletes difference.
    *                            NOTE: If set then the test case will not run concurrently.
    */
  protected final def testGivenAllParticipants(
      shortIdentifier: String,
      description: String,
      participants: ParticipantAllocation,
      timeoutScale: Double = 1.0,
      runConcurrently: Boolean = true,
      repeated: Int = 1,
      enabled: Features => Boolean = _ => true,
      disabledReason: String = "No reason",
      cleanupNewUsers: Boolean = false,
  )(
      testCase: ExecutionContext => Seq[ParticipantTestContext] => PartialFunction[
        Participants,
        Future[Unit],
      ]
  ): Unit = {
    if (cleanupNewUsers) {
      assert(!runConcurrently, "Cleaning-up of created user cannot be run concurrently!")
    }
    val shortIdentifierRef = Ref.LedgerString.assertFromString(shortIdentifier)
    testCaseBuffer.append(
      new LedgerTestCase(
        this,
        shortIdentifierRef,
        description,
        timeoutScale,
        runConcurrently,
        repeated,
        enabled,
        disabledReason,
        participants,
        withUsersCleanup(testCase),
      )
    )
  }

  /** Deletes users created during test case execution in order to keep the user management state
    * the same for each test case.
    *
    * @return decorated test case function that handles users cleanup
    */
  private def withUsersCleanup(
      testCase: ExecutionContext => Seq[ParticipantTestContext] => PartialFunction[
        Participants,
        Future[Unit],
      ]
  ): ExecutionContext => Seq[ParticipantTestContext] => PartialFunction[Participants, Future[
    Unit
  ]] = {

    def getAllUsers(
        ledger: ParticipantTestContext
    )(implicit ec: ExecutionContext): Future[Seq[User]] = {
      def iter(pageToken: String): Future[Seq[User]] = {
        ledger.userManagement
          .listUsers(ListUsersRequest(pageToken = pageToken, pageSize = 10000))
          .flatMap { res: ListUsersResponse =>
            if (res.nextPageToken.nonEmpty) {
              iter(res.nextPageToken).map(otherUsers => res.users ++ otherUsers)
            } else {
              Future.successful(res.users)
            }
          }
      }

      iter(pageToken = "")
    }

    def deleteUsers(
        ledger: ParticipantTestContext
    )(users: Seq[User])(implicit ec: ExecutionContext): Future[Unit] = {
      Future
        .sequence(
          users.map(u => ledger.userManagement.deleteUser(DeleteUserRequest(u.id)))
        )
        .map(_ => ())
    }

    (ec: ExecutionContext) => { (participantTestContexts: Seq[ParticipantTestContext]) =>
      {
        case (ps: Participants) => {

          implicit val ec2: ExecutionContext = ec

          def deleteNewUsers(
              ledger: ParticipantTestContext,
              usersBefore: Seq[User],
          ): Future[Unit] = {
            for {
              usersAfter <- getAllUsers(ledger)
              newUsers = usersAfter.toSet -- usersBefore.toSet
              _ <- deleteUsers(ledger)(newUsers.toList)
            } yield ()
          }

          val ledgers =
            ps.allocatedParticipants.map(_.ledger).filter(_.features.userManagement.supported)
          for {
            ledgerUsersBefore <- Future.sequence(
              ledgers.map(l => getAllUsers(l)(ec).map(users => (l, users)))
            )
            result <- testCase(ec)(participantTestContexts)(ps)
              // Deleting new users regardless of success or failure of the testCase:
              .transformWith((tr: Try[Unit]) => {
                val cleanUpF = Future.sequence(ledgerUsersBefore.map { case (ledger, usersBefore) =>
                  deleteNewUsers(ledger, usersBefore)
                })
                tr match {
                  case Success(v) => cleanUpF.map(_ => v)
                  case Failure(t) => cleanUpF.transformWith(_ => Future.failed(t))
                }
              })
          } yield {
            result
          }
        }
      }
    }
  }

  private[testtool] def name: String = getClass.getSimpleName
}
