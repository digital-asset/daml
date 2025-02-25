# Interactive Submission Example

This folder contains python scripts demonstrating how to interact with the interactive submission service of the Ledger API.

## Requirements

- Python 3.x
- Java 17

## Usage

### Setup

> [!NOTE]
> It is recommended to use a dedicated python environment to avoid conflicting dependencies.
> If you're not already using one, considering using [venv](https://docs.python.org/3/library/venv.html)
> To create a new venv virtual environment simply run the following commands:
> ```shell
> python -m venv .venv
> source .venv/bin/activate
> ```

Once your python environment is ready, run the following script to setup dependencies and generate the necessary python files
to interact with the interactive submission service.

```shell
pip install -r requirements.txt
./setup.sh
```

### Start canton

In a separate window, start a canton instance against which we will demonstrate interactive submissions.
If you cloned the Canton repo, you can build Canton from it.
If you're running this from the release artifact, it should already contain a canton binary.

Once a canton binary is available, run

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

## Onboard an external party

```shell
python interactive_submission.py create-party --name alice
```

Will create an external party on the ledger and write their private and public keys to local `der` files.

Output:

```text
Onboarding alice
Waiting for alice to appear in topology
Party ID: alice::122076f2a757c1ea944f52fc1fa854aa78077672efa32d7903e97cbf92646331876d
Written private key to: alice::122076f2a757c1ea944f52fc1fa854aa78077672efa32d7903e97cbf92646331876d-private-key.der
Written public key to: alice::122076f2a757c1ea944f52fc1fa854aa78077672efa32d7903e97cbf92646331876d-public-key.der
```

By default the `synchronizer ID` and `participant ID` will be picked up from the files written by the canton bootstrap script in this directory.
They can be overridden with ` --synchronizer-id synchronizer_id` and `--participant-id participant_id`.