
# `nim`

Example ledger App, written in Haskell, which runs a multi-player game-server for Nim.
The ledger maintains the game state, and encodes the rules of Nim. See: `daml/Nim.daml`.
Players connect to the ledger using `nim`.

The state of a game of Nim consists of 3 piles of matchsticks, initially containing 7, 5 and 3 sticks respectively. On your turn, you must take 1,2, or 3 matchsticks from a single pile. Turns alternate. The player who takes the final match is the loser.

## Build

    $ bazel build language-support/hs/bindings/examples/nim/...

## Start a sandbox ledger running the Nim game server

    $ daml sandbox bazel-out/k8-fastbuild/bin/language-support/hs/bindings/examples/nim/Nim.dar

## Start nim (as default player Alice)

    $ bazel run language-support/hs/bindings/examples/nim

## (Optional) Start more consoles for other players in different terminals

    $ bazel run language-support/hs/bindings/examples/nim -- Bob
    $ bazel run language-support/hs/bindings/examples/nim -- Charles

## Play as Alice

    Alice> offer Bob
    Alice> offer Charles Dave
    Alice> show

## Start a robot to play as Bob

    $ bazel run language-support/hs/bindings/examples/nim -- --robot Bob

## Quit and restart Alice's console. State is recovered from the ledger.

    Alice> move 1 2 3
    Alice> show
    Alice> <Ctr-C>
    $ bazel run language-support/hs/bindings/examples/nim
    Alice> show

## Play as Dave against Alice

    $ bazel run language-support/hs/bindings/examples/nim -- Dave
    Dave> accept 1
