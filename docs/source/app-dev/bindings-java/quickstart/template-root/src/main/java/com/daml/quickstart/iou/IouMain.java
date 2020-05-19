// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.quickstart.iou;

import com.daml.ledger.rxjava.DamlLedgerClient;
import com.daml.ledger.rxjava.LedgerClient;
import com.daml.ledger.javaapi.data.*;
import com.daml.quickstart.model.iou.Iou;
import com.google.common.collect.BiMap;

import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.protobuf.Empty;
import io.reactivex.disposables.Disposable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Spark;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;


public class IouMain {

    private final static Logger logger = LoggerFactory.getLogger(IouMain.class);

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
        DamlLedgerClient client = DamlLedgerClient.forHostWithLedgerIdDiscovery(ledgerhost, ledgerport, Optional.empty());

        // Connects to the ledger and runs initial validation.
        client.connect();

        String ledgerId = client.getLedgerId();

        logger.info("ledger-id: {}", ledgerId);

        TransactionFilter iouFilter = filterFor(Iou.TEMPLATE_ID, party);

        AtomicLong idCounter = new AtomicLong(0);
        ConcurrentHashMap<Long, Iou> contracts = new ConcurrentHashMap<>();
        BiMap<Long, Iou.ContractId> idMap = Maps.synchronizedBiMap(HashBiMap.create());
        AtomicReference<LedgerOffset> acsOffset = new AtomicReference<>(LedgerOffset.LedgerBegin.getInstance());

        client.getActiveContractSetClient().getActiveContracts(iouFilter, true)
                .blockingForEach(response -> {
                    response.getOffset().ifPresent(offset -> acsOffset.set(new LedgerOffset.Absolute(offset)));
                    response.getCreatedEvents().stream()
                            .map(Iou.Contract::fromCreatedEvent)
                            .forEach(contract -> {
                                long id = idCounter.getAndIncrement();
                                contracts.put(id, contract.data);
                                idMap.put(id, contract.id);
                            });
                });

        Disposable ignore = client.getTransactionsClient().getTransactions(acsOffset.get(), iouFilter, true)
                .forEach(t -> {
                    for (Event event : t.getEvents()) {
                        if (event instanceof CreatedEvent) {
                            CreatedEvent createdEvent = (CreatedEvent) event;
                            long id = idCounter.getAndIncrement();
                            Iou.Contract contract = Iou.Contract.fromCreatedEvent(createdEvent);
                            contracts.put(id, contract.data);
                            idMap.put(id, contract.id);
                        } else if (event instanceof ArchivedEvent) {
                            ArchivedEvent archivedEvent = (ArchivedEvent) event;
                            long id = idMap.inverse().get(new Iou.ContractId(archivedEvent.getContractId()));
                            contracts.remove(id);
                            idMap.remove(id);
                        }
                    }
                });

        Gson g = new Gson();
        Spark.port(restport);
        Spark.get("/iou", "application/json", (req, res) -> g.toJson(contracts));
        Spark.get("/iou/:id", "application/json", (req, res) ->
                g.toJson(contracts.getOrDefault(Long.parseLong(req.params("id")), null))
        );
        Spark.put("/iou", (req, res) -> {
            Iou iou = g.fromJson(req.body(), Iou.class);
            CreateCommand iouCreate = iou.create();
            submit(client, party, iouCreate);
            return "Iou creation submitted.";
        }, g::toJson);
        Spark.post("/iou/:id/transfer", "application/json", (req, res) -> {
            Map m = g.fromJson(req.body(), Map.class);
            Iou.ContractId contractId = idMap.get(Long.parseLong(req.params("id")));
            ExerciseCommand exerciseCommand = contractId.exerciseIou_Transfer(m.get("newOwner").toString());
            submit(
                    client,
                    party,
                    exerciseCommand
            );
            return "Iou transfer submitted.";
        }, g::toJson);

        // Run until user terminates.
        while (true)
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
    }

    private static Empty submit(LedgerClient client, String party, Command c) {
        return client.getCommandSubmissionClient().submit(
                UUID.randomUUID().toString(),
                "IouApp",
                UUID.randomUUID().toString(),
                party,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Collections.singletonList(c))
                .blockingGet();
    }

    private static TransactionFilter filterFor(Identifier templateId, String party) {
        InclusiveFilter inclusiveFilter = new InclusiveFilter(Collections.singleton(templateId));
        Map<String, Filter> filter = Collections.singletonMap(party, inclusiveFilter);
        return new FiltersByParty(filter);
    }
}
