package com.daml.platform.sandbox.auth
import com.daml.platform.testing.StreamConsumer
import io.grpc.reflection.v1alpha.{ServerReflectionGrpc, ServerReflectionResponse}

import scala.concurrent.Future

class ListServicesAuthIT extends UnsecuredServiceCallAuthTests {
  override def serviceCallName: String = "ServerReflection#List"

  override def serviceCallWithoutToken(): Future[Any] =
    new StreamConsumer[ServerReflectionResponse](observer =>
      stub(ServerReflectionGrpc.newStub(channel), None).serverReflectionInfo(observer).onCompleted()
    ).first()
}
