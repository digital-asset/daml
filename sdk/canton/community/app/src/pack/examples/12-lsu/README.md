# Logical Synchronizer Upgrade
This example shows how to run a logical synchronizer upgrade.

## Running the demo
The scenario is a simple one:

- One sequencer, one mediator
- Two participants, each hosting a party (Alice -> P1, Bank -> P2)
- An Iou contract

Synchronizer running pv=latest is upgraded to a new synchronizer running pv=dev.

To run the demo, follow the script in `demo-script.txt`.

## Status of the project
Status as of 2025-07-28 (see also [GitHub milestone](https://github.com/DACH-NY/canton/milestone/100)).

### DONE

Topology:

- Upgrade announcement, which triggers topology freeze.
- Each sequencer can announce its successor.
- When a participant node knows the successors of all its sequencers, it performs handshake with new synchronizer
  and download topology state.

Sequencer nodes:

- Only time proofs are delivered after upgrade time on the old synchronizer.
- The new synchronizer rejects submissions before upgrade time.

Participant:

- Register callback to perform the upgrade when upgrade time is reached.
- Upgrade is performed.

### WIP

- Upgrade happens in the participant even if node is restarted ([#26997](https://github.com/DACH-NY/canton/issues/26997))

### To do

- In-flight requests are handled
- Manual upgrade on the participant
- Upgrade is resilient to crashes
- Disaster recovery
- Purge data related to old synchronizer in the stores
- Reassignments work across upgrades
- UX improvements
- More test coverage
