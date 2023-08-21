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


## Verification

The verification happens in 2 major steps:
 - The translation from the original file to a simplified version that works well with stainless and a proof of the soudness of this translation.
 - The verification of the property

To verify the former you can execute the verification script with the following argument:

``` scripts/verification_script.sh <stainless_path> translate```



 To verify the latter, you can either execute the following command:

```stainless utils/* transaction/* tree/* --watch=false --timeout=30 --vc-cache=false --compact=true --solvers=nativez3```

or execute the script with the following argument:

``` scripts/verification_script.sh <stainless_path> verify```

If in the first command you find that the timeout is too big you can reduce it as you will (it is recommended to keep it above 10 if you don't want to have any suprise).


