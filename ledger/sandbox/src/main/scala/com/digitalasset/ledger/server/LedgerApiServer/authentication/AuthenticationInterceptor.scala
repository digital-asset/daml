package com.digitalasset.ledger.server.LedgerApiServer.authentication

import java.util.{ServiceConfigurationError, ServiceLoader}

import com.digitalasset.ledger.server.authentication.api.LedgerApiAuthenticator
import com.typesafe.scalalogging.StrictLogging
import io.grpc.Metadata.Key
import io.grpc._

import scala.collection.JavaConverters._

class AuthenticationInterceptor private(authenticators: List[LedgerApiAuthenticator]) extends ServerInterceptor {

  private val authenticationToken = Key.of("authentication_token", Metadata.ASCII_STRING_MARSHALLER)

  override def interceptCall[ReqT, RespT](serverCall: ServerCall[ReqT, RespT], metadata: Metadata, serverCallHandler: ServerCallHandler[ReqT, RespT]): ServerCall.Listener[ReqT] = {
    val token = metadata.get(authenticationToken)
    authenticators.foreach(auth => if (auth.authenticate(token)) throw new StatusRuntimeException(Status.PERMISSION_DENIED))
    serverCallHandler.startCall(serverCall, metadata)
  }
}

object AuthenticationInterceptor extends StrictLogging{
  def initialize(): AuthenticationInterceptor = {
    try {
      val classpathAuthenticators = ServiceLoader.load(classOf[LedgerApiAuthenticator]).iterator().asScala.toList
      new AuthenticationInterceptor(classpathAuthenticators)
    } catch {
      case ex: ServiceConfigurationError =>
        logger.error("Error initializing AuthenticationInterceptor!", ex)
        throw ex
    }
  }
}

class OnlyBobAuthenticator extends LedgerApiAuthenticator {
  override def authenticate(authenticationData: String): Boolean = {
    authenticationData == "Bob"
  }
}
