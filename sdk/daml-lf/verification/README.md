# Proof of the advance method of the Contract State Machine in UCK mode

Formal verification in [Stainless](https://stainless.epfl.ch/) of the advance method of the [ContractStateMachine](../transaction/src/main/scala/com/digitalasset/daml/lf/transaction/ContractStateMachine.scala) in strict mode.

More precisely, under reasonable assumptions, if $tr$ is a transaction, $init$ a State, and $\textup{traverse}(tr, \varepsilon)$ is well-defined then:

 $$\textup{traverse}(tr, init) = init.\textup{advance}(\textup{traverse}(tr, \varepsilon))$$ 

 where $\varepsilon$ is the empty State.

A pen and paper proof of the main component of the verification can be found [here](latex/proof.pdf).

## Components

- `latex` : the pen and paper proof.
- `scripts` : the verification scripts and its helpers.
- `transaction` :  all the proofs related to how the CSM handles a node.
- `translation` : original file and proofs of the translation to a simplified version of it
- `tree` : all the proofs related to how the CSM handles a transaction.
- `utils` : helpers and theorems about collections.

## Developer Environment Setup

``` nix-shell ./stainless.nix -A stainlessEnv ```

## Build

To build the Stainless version used in the proof:

 1. Clone [Stainless repo](https://github.com/epfl-lara/stainless)
 2. Run sbt universal:stage
 3. The generated binary can be found at the following location (in the following, this path is referred to as `<stainless_path>`):
    `$STAINLESS_REPO_ROOT/frontends/dotty/target/universal/stage/bin/stainless-dotty`
 4. The verification currently works with JDK 17

### Building documentation

To build the PDF version of the proof documentation:
``` pdflatex ./latex/proof.tex ```

## Verification

The verification happens in 2 major steps:
 - The translation from the original file to a [simplified version](transaction/ContractStateMachineAlt.scala) that is easy to work with in stainless and a proof of the soudness of this translation.
 - The verification of the property

To verify the former you can execute the verification script with the following argument:

``` scripts/verification_script.sh <stainless_path> translate```

The scripts takes the original files and uses a regex to create a temporary copy of the file that modifies the imports,
removes the exceptions and in general features that are not yet supported in Stainless.



 To verify the latter, you can either execute the following command:

```stainless utils/* transaction/* tree/* --watch=false --timeout=30 --vc-cache=false --compact=true --solvers=nativez3```

or execute the script with the following argument:

``` scripts/verification_script.sh <stainless_path> verify```

If in the first command you find that the timeout is too big you can reduce it as you wish (it is recommended to keep it above 10 if you don't want to have any suprises).


