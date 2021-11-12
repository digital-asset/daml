// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
//
//package com.daml.ledger.api.benchtool.submission
//
//import com.daml.ledger.api.benchtool.{
//  StreamDescriptor,
//  SubmissionDescriptor,
//  WorkflowDescriptor,
//  WorkflowParser,
//}
//import org.scalatest.matchers.should.Matchers
//import org.scalatest.prop.TableDrivenPropertyChecks
//import org.scalatest.wordspec.AnyWordSpec
//
//import java.io.StringReader
//
//class WorkflowParserSpec extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {
//  import com.daml.ledger.api.benchtool.SubmissionDescriptor._
//
//  "DescriptorParser" should {
//    "parse submission description" in {
//      val yaml =
//        """submission:
//          |  num_instances: 123
//          |  num_observers: 5
//          |  unique_parties: false
//          |  instance_distribution:
//          |    - template: Foo1
//          |      weight: 50
//          |      payload_size_bytes: 100
//          |      archive_probability: 0.9
//          |    - template: Foo2
//          |      weight: 25
//          |      payload_size_bytes: 150
//          |      archive_probability: 0.8
//          |    - template: Foo3
//          |      weight: 25
//          |      payload_size_bytes: 30
//          |      archive_probability: 0.7
//          |streams: []""".stripMargin
//      parseYaml(yaml) shouldBe Right(
//        WorkflowDescriptor(
//          submission = Some(
//            SubmissionDescriptor(
//              numberOfInstances = 123,
//              numberOfObservers = 5,
//              uniqueParties = false,
//              instanceDistribution = List(
//                ContractDescription(
//                  template = "Foo1",
//                  weight = 50,
//                  payloadSizeBytes = 100,
//                  archiveChance = 0.9,
//                ),
//                ContractDescription(
//                  template = "Foo2",
//                  weight = 25,
//                  payloadSizeBytes = 150,
//                  archiveChance = 0.8,
//                ),
//                ContractDescription(
//                  template = "Foo3",
//                  weight = 25,
//                  payloadSizeBytes = 30,
//                  archiveChance = 0.7,
//                ),
//              ),
//            )
//          )
//        )
//      )
//    }
//
//    "parse streams description" in {
//      val yaml =
//        """streams:
//          |  - type: "active-contracts"
//          |    name: "stream-1"
//          |    filters:
//          |      - party: Obs_0
//          |        templates:
//          |          - Foo1
//          |          - Foo2
//          |      - party: Obs_1
//          |        templates: []
//          |      - party: Obs_2
//          |        templates:
//          |          - Foo3
//          |  - type: "transactions"
//          |    name: "stream-2"
//          |    filters:
//          |      - party: Obs_1
//          |        templates:
//          |          - Foo1
//          |          - Foo2
//          |      - party: Obs_2
//          |        templates: []""".stripMargin
//      parseYaml(yaml) shouldBe Right(
//        WorkflowDescriptor(
//          submission = None,
//          streams = List(
//            StreamDescriptor(
//              streamType = StreamDescriptor.StreamType.ActiveContracts,
//              name = "stream-1",
//              filters = List(
//                StreamDescriptor.PartyFilter("Obs_0", List("Foo1", "Foo2")),
//                StreamDescriptor.PartyFilter("Obs_1", List()),
//                StreamDescriptor.PartyFilter("Obs_2", List("Foo3")),
//              ),
//            ),
//            StreamDescriptor(
//              streamType = StreamDescriptor.StreamType.Transactions,
//              name = "stream-2",
//              filters = List(
//                StreamDescriptor.PartyFilter("Obs_1", List("Foo1", "Foo2")),
//                StreamDescriptor.PartyFilter("Obs_2", List()),
//              ),
//            ),
//          ),
//        )
//      )
//    }
//  }
//
//  def parseYaml(
//      yaml: String
//  ): Either[WorkflowParser.ParserError, WorkflowDescriptor] =
//    WorkflowParser.parse(new StringReader(yaml))
//}
