// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc

import java.util.concurrent.TimeUnit

import com.daml.ledger.rxjava._
import com.daml.ledger.rxjava.grpc.helpers.{LedgerServices, TestConfiguration}
import com.daml.ledger.api.v1.package_service._
import com.google.protobuf.ByteString
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

class PackageClientImplTest extends AnyFlatSpec with Matchers with AuthMatchers with OptionValues {

  val ledgerServices = new LedgerServices("package-service-ledger")

  behavior of "[7.1] PackageClientImpl.listPackages"

  it should "return the packages from the Ledger" in {
    ledgerServices.withPackageClient(
      listPackageResponseFuture("first"),
      defaultGetPackageResponseFuture,
      defaultGetPackageStatusResponseFuture,
    ) { (client, _) =>
      client
        .listPackages()
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingFirst() shouldBe "first"
    }
  }

  behavior of "[7.2] PackageClientImpl.listPackages"

  it should "request the list of packages with the correct ledger ID" in {
    ledgerServices.withPackageClient(
      listPackageResponseFuture("first"),
      defaultGetPackageResponseFuture,
      defaultGetPackageStatusResponseFuture,
    ) { (client, service) =>
      client
        .listPackages()
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingFirst()
      service.getLastListPackageRequest.value.ledgerId shouldBe ledgerServices.ledgerId
    }
  }

  behavior of "[7.3] PackageClientImpl.getPackage"

  it should "return the package from the Ledger" in {
    ledgerServices.withPackageClient(
      listPackageResponseFuture(),
      defaultGetPackageResponseFuture,
      defaultGetPackageStatusResponseFuture,
    ) { (client, _) =>
      val getPackage = client
        .getPackage("")
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingGet()
      getPackage.getArchivePayload shouldBe ByteString.EMPTY.toByteArray
      getPackage.getArchivePayload shouldBe defaultGetPackageResponse.archivePayload.toByteArray
    }
  }

  behavior of "[7.4] PackageClientImpl.getPackage"

  it should "request the package with the correct ledger ID and package ID" in {
    ledgerServices.withPackageClient(
      listPackageResponseFuture(),
      defaultGetPackageResponseFuture,
      defaultGetPackageStatusResponseFuture,
    ) { (client, service) =>
      client
        .getPackage("packageId")
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingGet()
      service.getLastGetPackagesRequest.value.ledgerId shouldEqual ledgerServices.ledgerId
      service.getLastGetPackagesRequest.value.packageId shouldEqual "packageId"
    }
  }

  behavior of "[7.5] PackageClientImpl.getPackageStatus"

  it should "return the packageStatus from the Ledger" in {
    ledgerServices.withPackageClient(
      listPackageResponseFuture(),
      defaultGetPackageResponseFuture,
      defaultGetPackageStatusResponseFuture,
    ) { (client, _) =>
      val getPackageStatus = client
        .getPackageStatus("packageId")
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingGet()
      getPackageStatus.getPackageStatusValue.name shouldBe defaultGetPackageStatusResponse.packageStatus.name
    }
  }

  behavior of "[7.6] PackageClientImpl.getPackageStatus"

  it should "send a request with the correct ledger ID and packageID" in {
    ledgerServices.withPackageClient(
      listPackageResponseFuture(),
      defaultGetPackageResponseFuture,
      defaultGetPackageStatusResponseFuture,
    ) { (client, service) =>
      client
        .getPackageStatus("packageId")
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingGet()
      service.getLastGetPackageStatusRequest.value.ledgerId shouldBe ledgerServices.ledgerId
      service.getLastGetPackageStatusRequest.value.packageId shouldBe "packageId"
    }
  }

  behavior of "Authorization"

  private def toAuthenticatedServer(fn: PackageClient => Any): Any =
    ledgerServices.withPackageClient(
      listPackageResponseFuture(),
      defaultGetPackageResponseFuture,
      defaultGetPackageStatusResponseFuture,
      mockedAuthService,
    ) { (client, _) =>
      fn(client)
    }

  it should "deny access without token" in {
    withClue("getPackage") {
      expectUnauthenticated {
        toAuthenticatedServer { client =>
          client.getPackage("...").blockingGet()
        }
      }
    }
    withClue("getPackageStatus") {
      expectUnauthenticated {
        toAuthenticatedServer { client =>
          client.getPackageStatus("...").blockingGet()
        }
      }
    }
    withClue("listPackages") {
      expectUnauthenticated {
        toAuthenticatedServer { client =>
          client.listPackages().blockingFirst()
        }
      }
    }
  }

  it should "deny access without sufficient authorization" in {
    withClue("getPackage") {
      expectUnauthenticated {
        toAuthenticatedServer { client =>
          client.getPackage("...", emptyToken).blockingGet()
        }
      }
    }
    withClue("getPackageStatus") {
      expectUnauthenticated {
        toAuthenticatedServer { client =>
          client.getPackageStatus("...", emptyToken).blockingGet()
        }
      }
    }
    withClue("listPackages") {
      expectUnauthenticated {
        toAuthenticatedServer { client =>
          client.listPackages(emptyToken).blockingFirst()
        }
      }
    }
  }

  it should "allow access with sufficient authorization" in {
    toAuthenticatedServer { client =>
      withClue("getPackage") {
        client.getPackage("...", publicToken).blockingGet()
      }
    }
    toAuthenticatedServer { client =>
      withClue("getPackageStatus") {
        client.getPackageStatus("...", publicToken).blockingGet()
      }
    }
    toAuthenticatedServer { client =>
      withClue("listPackages") {
        client.listPackages(publicToken).blockingIterable()
      }
    }
  }

  private def listPackageResponse(pids: String*) = new ListPackagesResponse(pids)

  private def listPackageResponseFuture(pids: String*) =
    Future.successful(listPackageResponse(pids: _*))

  private val defaultGetPackageResponsePayload = ByteString.EMPTY
  private val defaultGetPackageResponse =
    new GetPackageResponse(HashFunction.values.head, defaultGetPackageResponsePayload)
  private val defaultGetPackageResponseFuture = Future.successful(defaultGetPackageResponse)
  private val defaultGetPackageStatusResponse = new GetPackageStatusResponse(
    PackageStatus.values.head
  )
  private val defaultGetPackageStatusResponseFuture =
    Future.successful(defaultGetPackageStatusResponse)

}
