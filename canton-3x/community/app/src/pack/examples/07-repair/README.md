# Repair Examples

The repair example features multiple topologies for repair-based tutorials, each with different sets of participants
and domains depending on the demonstrated capability.

## 1. [Recovering from a broken Domain](https://docs.daml.com/canton/usermanual/repairing.html#recovering-from-a-lost-domain)

depends on files:
- Participant configurations: participant1.conf and participant2.conf
- Domain configurations: domain-repair-lost.conf and domain-repair-new.conf
- enable-preview-commands.conf to enable "preview" and "repair" commands
- Initialization script: domain-repair-init.canton that populates the participants and "lostDomain" with Iou contracts

To set up this scenario, run

```
    ../../bin/canton -Dcanton-examples.dar-path=../../dars/CantonExamples.dar \
      -c participant1.conf,participant2.conf,domain-repair-lost.conf,domain-repair-new.conf \
      -c ../../config/storage/h2.conf,enable-preview-commands.conf \
      --bootstrap domain-repair-init.canton
```

## 2. [Importing contracts to Canton](https://docs.daml.com/canton/usermanual/repairing.html#importing-existing-contracts)

depends on files:
- Participant configurations: participant1.conf, participant2.conf, participant3.conf for the "import ledger", and participant4.conf for the "export ledger"
- Domain configurations: domain-export-ledger.conf and domain-import-ledger.conf for the export and import ledgers respectively
- enable-preview-commands.conf to enable "preview" and "repair" commands
- Initialization script: import-ledger-init.canton that populates the export ledger with Paint agreement and Iou contracts

To set up this scenario, run

```
    ../../bin/canton -Dcanton-examples.dar-path=../../dars/CantonExamples.dar \
      -c participant1.conf,participant2.conf,participant3.conf,participant4.conf,domain-export-ledger.conf,domain-import-ledger.conf \
      -c ../../config/storage/h2.conf,enable-preview-commands.conf \
      --bootstrap import-ledger-init.canton
```
