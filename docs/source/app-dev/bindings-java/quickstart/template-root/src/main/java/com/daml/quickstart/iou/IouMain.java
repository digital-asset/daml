// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.quickstart.iou;

import static java.util.UUID.randomUUID;

import com.daml.ledger.javaapi.data.*;
import com.daml.ledger.javaapi.data.codegen.Update;
import com.daml.ledger.rxjava.DamlLedgerClient;
import com.daml.ledger.rxjava.LedgerClient;
import com.daml.ledger.rxjava.grpc.CommandsBuilder;
import com.daml.quickstart.model.iou.Iou;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import io.reactivex.disposables.Disposable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
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

    // Create a client object to access services on the ledger.
    DamlLedgerClient client = DamlLedgerClient.newBuilder(ledgerhost, ledgerport).build();

    // Connects to the ledger and runs initial validation.
    client.connect();

    String ledgerId = client.getLedgerId();

    logger.info("ledger-id: {}", ledgerId);

    AtomicLong idCounter = new AtomicLong(0);
    ConcurrentHashMap<Long, Iou> contracts = new ConcurrentHashMap<>();
    BiMap<Long, Iou.ContractId> idMap = Maps.synchronizedBiMap(HashBiMap.create());
    AtomicReference<LedgerOffset> acsOffset =
        new AtomicReference<>(LedgerOffset.LedgerBegin.getInstance());

    client
        .getActiveContractSetClient()
        .getActiveContracts(Iou.contractFilter(), Collections.singleton(party), true)
        .blockingForEach(
            response -> {
              response.offset.ifPresent(offset -> acsOffset.set(new LedgerOffset.Absolute(offset)));
              response.activeContracts.forEach(
                  contract -> {
                    long id = idCounter.getAndIncrement();
                    contracts.put(id, contract.data);
                    idMap.put(id, contract.id);
                  });
            });

    Disposable ignore =
        client
            .getTransactionsClient()
            .getTransactions(
                Iou.contractFilter(), acsOffset.get(), Collections.singleton(party), true)
            .forEach(
                t -> {
                  for (Event event : t.getEvents()) {
                    if (event instanceof CreatedEvent) {
                      CreatedEvent createdEvent = (CreatedEvent) event;
                      long id = idCounter.getAndIncrement();
                      Iou.Contract contract = Iou.Contract.fromCreatedEvent(createdEvent);
                      contracts.put(id, contract.data);
                      idMap.put(id, contract.id);
                    } else if (event instanceof ArchivedEvent) {
                      ArchivedEvent archivedEvent = (ArchivedEvent) event;
                      long id =
                          idMap.inverse().get(new Iou.ContractId(archivedEvent.getContractId()));
                      contracts.remove(id);
                      idMap.remove(id);
                    }
                  }
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
          var createdContractId = submit(client, party, iouCreate);
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
          var result = submit(client, party, update);
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

  private static <U> U submit(LedgerClient client, String party, Update<U> update) {
    var params =
        CommandsBuilder.create(APP_ID, randomUUID().toString(), update.commands())
            .withParty(party);

    return client.getCommandClient().submitAndWaitForResult(params, update).blockingGet();
  }
}
