package com.daml.platform.index

import com.daml.ledger.api.domain.{Filters, InclusiveFilters, InterfaceFilter, TransactionFilter}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{Identifier, Party}
import com.daml.platform.store.dao.EventProjectionProperties
import com.daml.platform.store.packagemeta.PackageMetadataView.PackageMetadata
import org.openjdk.jmh.annotations._

@State(Scope.Benchmark)
class IfaceSubscriptionsBench {

  @Param(Array("10", "1000", "1000000"))
  var elementsCount: Int = _

  var metadata: PackageMetadata = _
  var transactionFilter: TransactionFilter = _

  var result: Map[Party, Set[Identifier]] = _
  var eventProjectionProperties: EventProjectionProperties = _

  @Setup(Level.Invocation)
  def setupIteration(): Unit = {
    def party(i: Int) = Ref.Party.assertFromString("party" + i)
    def template(i: Int) = Ref.Identifier.assertFromString("PackageName:ModuleName:template" + i)
    def iface(i: Int) = Ref.Identifier.assertFromString("PackageName:ModuleName:iface" + i)

    metadata = PackageMetadata(
      interfacesImplementedBy = (1 to math.sqrt(elementsCount.toDouble).toInt).map { el =>
        iface(el) -> (1 to math.sqrt(elementsCount.toDouble).toInt).map(template).toSet
      }.toMap
    )
    transactionFilter = new TransactionFilter(
      Map(
        party(1) -> Filters(
          Some(
            InclusiveFilters(
              (1 to elementsCount).map(template).toSet,
              (1 to elementsCount).map(template).toSet.map(InterfaceFilter(_, true)),
            )
          )
        )
      )
    )

  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @Fork(value = 5)
  @Warmup(iterations = 5)
  @Measurement(iterations = 5)
  def templateFilterBench(): Unit = {
    result = IndexServiceImpl.templateFilter(metadata, transactionFilter)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @Fork(value = 5)
  @Warmup(iterations = 5)
  @Measurement(iterations = 5)
  def eventProjectionPropertiesBench(): Unit = {
    eventProjectionProperties = EventProjectionProperties(
      transactionFilter,
      true,
      interfaceId => metadata.interfacesImplementedBy.getOrElse(interfaceId, Set.empty),
    )
  }
}
