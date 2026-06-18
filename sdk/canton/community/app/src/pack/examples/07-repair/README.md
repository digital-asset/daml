# Repair Examples

The repair example features multiple topologies for repair-based tutorials, each with different sets of participants
and synchronizers depending on the demonstrated capability.

## 1. Recovering from a broken Synchronizer

depends on files:
- Participant configurations: participant1.conf and participant2.conf
- Synchronizer configurations: synchronizer-repair-lost.conf and synchronizer-repair-new.conf
- enable-preview-commands.conf to enable "preview" and "repair" commands
- Initialization script: synchronizer-repair-init.canton that populates the participants and "lostSynchronizer" with Iou contracts

To set up this scenario, run

```
    ../../bin/canton -Dcanton-examples.dar-path=../../dars/CantonExamples.dar \
      -c participant1.conf,participant2.conf,synchronizer-repair-lost.conf,synchronizer-repair-new.conf \
      -c ../../config/storage/h2.conf,enable-preview-commands.conf \
      --bootstrap synchronizer-repair-init.canton
```
