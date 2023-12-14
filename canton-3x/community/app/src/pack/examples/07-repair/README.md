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
      -c ../03-advanced-configuration/storage/h2.conf,enable-preview-commands.conf \
      --bootstrap domain-repair-init.canton
```
