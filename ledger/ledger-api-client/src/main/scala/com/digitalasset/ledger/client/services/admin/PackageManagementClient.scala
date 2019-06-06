package com.digitalasset.ledger.client.services.admin

import java.time.Instant

import com.daml.ledger.participant.state.v1.PackageDetails
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.ledger.api.v1.admin.package_management_service.ListKnownPackagesRequest
import com.digitalasset.ledger.api.v1.admin.package_management_service.PackageManagementServiceGrpc.PackageManagementService

import scala.concurrent.{ExecutionContext, Future}

class PackageManagementClient(service: PackageManagementService)(implicit ec: ExecutionContext) {
  def listKnownPackages(): Future[Seq[(PackageId, PackageDetails)]] = {
    service.listKnownPackages(ListKnownPackagesRequest()).map{ resp =>
      resp.packageDetails.map { details =>
        val knownSince = details.knownSince.get
        (PackageId.assertFromString(details.packageId), PackageDetails(details.packageSize, Instant.ofEpochSecond(knownSince.seconds, knownSince.nanos.toLong), details.sourceDescription))
      }
    }
  }
}
