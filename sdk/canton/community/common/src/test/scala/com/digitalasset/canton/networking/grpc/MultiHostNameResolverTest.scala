// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.{BaseTestWordSpec, HasExecutorService}
import io.grpc.NameResolver.{Listener2, ServiceConfigParser}
import io.grpc.internal.GrpcUtil
import io.grpc.{NameResolver, Status, SynchronizationContext}

import java.net.InetSocketAddress
import scala.concurrent.{Future, Promise}

@SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
class MultiHostNameResolverTest extends BaseTestWordSpec with HasExecutorService {
  private def mkEndpoints(endpoints: String*): NonEmpty[Seq[Endpoint]] =
    NonEmpty
      .from(
        endpoints
          .map(_.split(":").take(2))
          .map(e => Endpoint(e(0), Port.tryCreate(e(1).toInt)))
      )
      .value

  // pretty dodgy attempt to invoke the name resolver for asserting the behavior it exhibits
  // we're testing our usage of GRPC internals so may prove flaky
  // will likely attempt to resolve addresses using DNS so may fail if a network is not accessible
  private def resolve(endpoints: String*): Future[Either[Status, NameResolver.ResolutionResult]] = {
    val synchronizationContext = new SynchronizationContext(
      // this is pretty crude but if this is hit it will log an error that will cause our error check to fail
      (_, unhandledException) =>
        logger.error(s"Unhandled exception on grpc sync context", unhandledException)
    )

    val resolver = new MultiHostNameResolver(
      mkEndpoints(endpoints*),
      NameResolver.Args
        .newBuilder()
        .setDefaultPort(80)
        .setProxyDetector(GrpcUtil.DEFAULT_PROXY_DETECTOR)
        .setSynchronizationContext(synchronizationContext)
        .setServiceConfigParser(mock[ServiceConfigParser])
        .setScheduledExecutorService(scheduledExecutor())
        .build(),
    )

    // we're going to assume in our usage that the listener is only called once with an error or result
    // technically the listener supports updating results overtime but that's a feature we don't currently use
    val resultP = Promise[Either[Status, NameResolver.ResolutionResult]]()

    resolver.start(new Listener2 {
      override def onResult(resolutionResult: NameResolver.ResolutionResult): Unit =
        resultP.trySuccess(Right(resolutionResult))
      override def onError(error: Status): Unit = resultP.trySuccess(Left(error))
    })

    resultP.future
  }

  "resolving addresses" should {
    "handle localhost" in {
      val result = valueOrFail(resolve("localhost:1234").futureValue)("resolve localhost address")
      val address = result.getAddressesOrError.getValue
        .get(0)
        .getAddresses
        .get(0)
        .asInstanceOf[InetSocketAddress]

      // we wont assert the actual address as it could be different if resolved to an ipv4 or ipv6 address
      // we'll just be happy an address was clearly found and check that the port is correct
      address.getPort shouldBe 1234
    }

    "handle ip address" in {
      val result = valueOrFail(resolve("192.0.2.1:1234").futureValue)("resolve ip address")
      val address = result.getAddressesOrError.getValue
        .get(0)
        .getAddresses
        .get(0)
        .asInstanceOf[InetSocketAddress]

      // we wont assert the actual address as it could be different if resolved to an ipv4 or ipv6 address
      // we'll just be happy an address was clearly found and check that the port is correct
      address.getPort shouldBe 1234
    }

    "produce error if provided bad address" in {
      val result = leftOrFail(resolve("nope.this.does.not.exist:1234").futureValue)(
        "produce error for bad address"
      )

      result.getCode shouldBe Status.Code.UNAVAILABLE
    }

    "aggregate multiple successful addresses" in {
      val result =
        valueOrFail(resolve("localhost:1234", "localhost:1235").futureValue)(
          "aggregate multiple successful addresses"
        )

      val countOfAddresses = result.getAddressesOrError.getValue.size()

      // Count is 2 or 4, depending on whether ipv6 addresses are included in the result
      countOfAddresses should (be(2) or be(4))
    }

    "fail if one address fails" in {
      val error = leftOrFail(
        resolve("localhost:1", "localhost:2", "nope.this.does.not.exist:3").futureValue
      )("resolve with error")

      error.getCode shouldBe Status.Code.UNAVAILABLE
    }

    "add localhost endpoint to attributes" in {
      val result = valueOrFail(resolve("localhost:1234").futureValue)("resolve localhost address")
      val endpointFromAttributes =
        result.getAddressesOrError.getValue.get(0).getAttributes.get(Endpoint.ATTR_ENDPOINT)

      // the actual endpoint object is passed as an attribute, which is useful during authentication
      // in order to pick the right connection to fetch tokens from for the current call
      endpointFromAttributes shouldBe Endpoint("localhost", Port.tryCreate(1234))
    }

    "add ip address endpoint to attributes" in {
      val result = valueOrFail(resolve("192.0.2.1:1234").futureValue)("resolve ip address")
      val endpointFromAttributes =
        result.getAddressesOrError.getValue.get(0).getAttributes.get(Endpoint.ATTR_ENDPOINT)

      // the actual endpoint object is passed as an attribute, which is useful during authentication
      // in order to pick the right connection to fetch tokens from for the current call
      endpointFromAttributes shouldBe Endpoint("192.0.2.1", Port.tryCreate(1234))
    }
  }
}
