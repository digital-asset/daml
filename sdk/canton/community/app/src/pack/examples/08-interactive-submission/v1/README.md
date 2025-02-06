# Interactive Submission Example

This folder contains python scripts demonstrating how to interact with the interactive submission service of the Ledger API.

## Requirements

- Python 3.x
- Java 17

## Usage

### Setup

```shell
pip install -r requirements.txt
./setup.sh
```

### Start canton

In a separate window, start a canton instance against which we will demonstrate interactive submissions.
Interactive submissions are only available from canton 3.2.
If you cloned the Canton repo, you can build Canton from it.
If you're running this from the release artifact, it should already contain a canton binary.
Alternatively, you can download a snapshot at https://www.canton.io/releases/canton-open-source-3.2.0-snapshot.20241204.14518.0.vfd036cca.zip

Once a 3.2 canton binary is available, run

```shell
bin/canton -c interactive-submission.conf --bootstrap bootstrap.canton
```

Wait for the "Welcome to Canton!" message to be displayed and the Canton console to appear.

### Demo

Run the demo with

```shell
python interactive_submission.py run-demo
```

This demo is a simplified setup that demonstrates the capabilities of external signing via interactive submissions.
The demo scenario is as follows:
- Create 2 parties "alice" and "bob" (see [external_party_onboarding.py](external_party_onboarding.py))
    - Both parties are setup with their own set of signing keys and onboarded onto participant1 on the running canton instance
- `alice` creates a ping contract with bob as a responder
- `bob` exercises the `Respond` choice, which archives the contract

Note that in all interactions between the python application and Canton, transactions are signed with a private key that is kept private for the party and never provided to any Canton node. Only the public key part is shared with the Canton nodes.
