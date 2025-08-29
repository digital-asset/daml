// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.quickstart.iou;

import static java.util.UUID.randomUUID;

import com.daml.ledger.javaapi.data.*;
import com.daml.ledger.javaapi.data.codegen.Update;
import com.daml.ledger.rxjava.DamlLedgerClient;
import com.daml.ledger.rxjava.LedgerClient;
import com.daml.quickstart.model.iou.Iou;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import io.reactivex.disposables.Disposable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Spark;

public class IouMain {

  private static final Logger logger = LoggerFactory.getLogger(IouMain.class);

  // application id used for sending commands
  public static final String APP_ID = "IouApp";

  public static void main(String[] args) {
    // Extract host and port from arguments
    if (args.length < 4) {
      System.err.println("Usage: LEDGER_HOST LEDGER_PORT PARTY REST_PORT");
      System.exit(-1);
    }
    String ledgerhost = args[0];
    int ledgerport = Integer.valueOf(args[1]);
    String party = args[2];
    int restport = Integer.valueOf(args[3]);
    Optional<Set<String>> partyFilter = Optional.of(Collections.singleton(party));

    AtomicLong idCounter = new AtomicLong(0);
    ConcurrentHashMap<Long, Iou> contracts = new ConcurrentHashMap<>();
    BiMap<Long, Iou.ContractId> idMap = Maps.synchronizedBiMap(HashBiMap.create());

    // Connect to gRPC services
    Channel channel = NettyChannelBuilder
            .forAddress(ledgerhost, ledgerport)
            .maxInboundMessageSize(10485760)
            .usePlaintext()
            .build();

    StateServiceGrpc.StateServiceBlockingStub stateServiceBlockingStub = StateServiceGrpc.newBlockingStub(channel);
    long ledgerEnd =
            stateServiceBlockingStub.getLedgerEnd(StateServiceOuterClass.GetLedgerEndRequest.newBuilder().build()).getOffset();

    var contractFilter = Iou.contractFilter();
    var eventFormat = contractFilter.eventFormat(partyFilter);
    GetActiveContractsRequest request = new GetActiveContractsRequest(eventFormat, ledgerEnd);
    Iterator<StateServiceOuterClass.GetActiveContractsResponse> activeContracts =
            stateServiceBlockingStub.getActiveContracts(request.toProto());

    activeContracts.forEachRemaining(r -> {
      GetActiveContractsResponse response = GetActiveContractsResponse.fromProto(r);
      long id = idCounter.getAndIncrement();
      var contract = contractFilter.toContract(response.getContractEntry().get().getCreatedEvent());
      contracts.put(id, contract.data);
      idMap.put(id, contract.id);
    });

    Gson g = new Gson();
    Spark.port(restport);
    Spark.get("/iou", "application/json", (req, res) -> g.toJson(contracts));
    Spark.get(
        "/iou/:id",
        "application/json",
        (req, res) -> g.toJson(contracts.getOrDefault(Long.parseLong(req.params("id")), null)));
    Spark.put(
        "/iou",
        (req, res) -> {
          Iou iou = g.fromJson(req.body(), Iou.class);
          var iouCreate = iou.create();
          var createdContractId = submitCreate(commandService, party, iouCreate, Iou.ContractId::new);
          return "Iou creation submitted: " + createdContractId;
        },
        g::toJson);
    Spark.post(
        "/iou/:id/transfer",
        "application/json",
        (req, res) -> {
          Map m = g.fromJson(req.body(), Map.class);
          Iou.ContractId contractId = idMap.get(Long.parseLong(req.params("id")));
          var update = contractId.exerciseIou_Transfer(m.get("newOwner").toString());
          var result = submitExercise(commandService, party, update, Iou.CHOICE_Iou_Transfer);
          return "Iou transfer submitted with exercise result: " + result;
        },
        g::toJson);

    // Run until user terminates.
    while (true)
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
  }

  private static <T> Created<T> submitCreate(CommandServiceGrpc.CommandServiceBlockingStub commandService,
                                             String party,
                                             Update<Created<T>> update, Function<String, T> convertContractId) {
    var updateSubmission = UpdateSubmission.create(APP_ID, randomUUID().toString(), update).withActAs(party);
    var request = new SubmitAndWaitForTransactionRequest(updateSubmission.toCommandsSubmission());
    SubmitAndWaitForTransactionResponse response =
            SubmitAndWaitForTransactionResponse.fromProto(commandService.submitAndWaitForTransaction(request.toProto()));
    Event event = EventUtils.singleCreatedEvent(response.getTransaction().getEvents());
    return Created.fromEvent(convertContractId, (CreatedEvent) event);
  }

  private static <T> Exercised<T> submitExercise(CommandServiceGrpc.CommandServiceBlockingStub commandService,
                                                 String party, Update<Exercised<T>> update,
                                                 Choice<?, ?, T> choice) {
    var updateSubmission = UpdateSubmission.create(APP_ID, randomUUID().toString(), update).withActAs(party);

    var contractFilter = Iou.contractFilter();
    var eventFormar = contractFilter.eventFormat(Optional.of(Collections.singleton(party)));
    var format = new TransactionFormat(eventFormar, TransactionShape.LEDGER_EFFECTS);
    var request = new SubmitAndWaitForTransactionRequest(updateSubmission.toCommandsSubmission(), format);
    SubmitAndWaitForTransactionResponse response =
            SubmitAndWaitForTransactionResponse.fromProto(commandService.submitAndWaitForTransaction(request.toProto()));
    Transaction transaction = response.getTransaction();
    Optional<Event> exercisedOpt = transaction.getEvents().stream().filter(e -> e instanceof ExercisedEvent).findFirst();
    if (exercisedOpt.isEmpty()) {
      throw new RuntimeException("No exercised event found in transaction");
    }
    ExercisedEvent event = (ExercisedEvent)exercisedOpt.get();
    return Exercised.fromEvent(choice.returnTypeDecoder, event);
  }
}
