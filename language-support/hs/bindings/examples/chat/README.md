
# `chat`

Another example ledger App, written in Haskell.

A chat room, where the ledger plays the role of distibuting messages, and maintaining history

## Build

    $ bazel build language-support/hs/bindings/examples/chat

## Start a sandbox ledger running the Nim game server

    $ daml sandbox bazel-out/k8-fastbuild/bin/language-support/hs/bindings/example/chat/Chat.dar

## Start chat-consoles for multiple participants in different terminals

    $ bazel run language-support/hs/bindings/examples:chat -- Alice
    $ bazel run language-support/hs/bindings/examples:chat -- Bob
    $ ...

## Chat as Alice

    Alice> :link Bob
    linked: Bob
    Alice> :link Nick
    linked: Nick
    Alice> ?
    [Bob,Nick]
    Alice> Hey to both of you

## Chat as Dave

    Dave> :link Edwina
    linked: Edwina
    Dave> ?
    Edwina

## Chat as Bob

    linked: Alice
    linked: Nick
    Bob> ?
    [Alice,Nick]
    Bob> :link Dave
    linked: Dave
    linked: Edwina
    Bob> ?
    [Alice,Nick,Dave,Edwina]
    Bob> Hey everyone

## Chat as Nick

    linked: Alice
    linked: Bob
    linked: Dave
    linked: Edwina
    Nick> ?
    [Alice,Bob,Dave,Edwina]
    Nick> Morning everybody
    Nick> !Alice
    Nick> (Alice) Who are Dave and Edwina?
