package com.daml.lf.engine.trigger

import akka.actor.ActorSystem
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import org.scalatest.{AsyncFlatSpec, Matchers}
import org.scalatest.concurrent.Eventually

import scala.concurrent.ExecutionContext

class AuthServiceClientTest extends AsyncFlatSpec with Eventually with Matchers {

  def testId: String = this.getClass.getSimpleName
  implicit val system: ActorSystem = ActorSystem(testId)
  implicit val esf: ExecutionSequencerFactory = new AkkaExecutionSequencerPool(testId)(system)
  implicit val ec: ExecutionContext = system.dispatcher

  it should "authorize a user to get an auth service bearer token" in AuthServiceFixture
    .withAuthServiceClient(testId) { authServiceClient =>
      for {
        Right(AuthServiceToken(token)) <- authServiceClient.authorize("username", "password")
        _ <- token should not be empty
      } yield succeed
    }
}
