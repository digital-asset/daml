# Steps for running a demo of nim

Follow these steps to run a demo of Nim!

Notes on the format of this file:

- For lines that start `$`, type them at the terminal.
- For lines that start `Alice>` or `Bob>` or `Charlie>`, type them at the relevant `nim` console. You'll end up with three terminals running simultaneously.

## 0. Setup: Build application and update the SDK

    $ cd daml
    $ bazel build language-support/hs/bindings/examples/nim/...
    $ daml install latest --install-assistant=yes

## 1. Look at [Nim.daml](https://github.com/digital-asset/daml/blob/main/language-support/hs/bindings/examples/nim/daml/Nim.daml)

`Nim.daml` models the operation of the game room and the rules of Nim:

    template GameOffer...

    data Game = Game with
        player1 : Party
        player2 : Party
        piles : [Int]

    data Move = Move with
        pileNum : Int -- numbered 1,2,3...
        howMany : Int

    template GameInProgress...

    playMove : Move -> Game -> Either RejectionMessage Game
    ...

## 2. Start sandbox running the Nim game server

    $ daml sandbox bazel-out/k8-fastbuild/bin/language-support/hs/bindings/examples/nim/Nim.dar

## 3. Open a new terminal and start nim for Alice

In a new terminal:

    $ clear; bazel-bin/language-support/hs/bindings/examples/nim/nim Alice
    replaying 0 transactions
    Alice>

    Alice> show
    Offers:
    Finished:
    In play:

This is an empty state: no offers, no games finished, no matches in play.
(You can also see the status by typing` <return>`.)

## 4. As Alice, offer a game to Charlie

    Alice> offer Charlie
    - Match-1: Alice has offered to play a new game.

    Alice> show
    - Match-1: You offered a game against Charlie.

Alice's local state shows that she has sent Charlie an offer to play a game of Nim.

## 5. Open a new terminal and start nim for Bob

In a new terminal:

    $ clear; bazel-bin/language-support/hs/bindings/examples/nim/nim Bob
    replaying 0 transactions
    Bob> show

Bob's status is empty, because Bob has no right to see that Alice offered a match to Charlie.

## 6. Open a new terminal and start nim for Charlie

In a new terminal:

    $ clear; bazel-bin/language-support/hs/bindings/examples/nim/nim Charlie
    replaying 1 transactions
    Charlie> show
    - Match-1: Alice offered a game against you.

Charlie sees the match offered by Alice, even though he had never connected before. Charlie's initial state is recovered by replaying the relevant transactions sent from the ledger.

## 7. As Alice, offer a game to Bob

    Alice> offer Bob

The event is seen in both Alice's console:

    Match-2: Alice has offered to play a new game.

And Bob's console:

    Match-1: Alice has offered to play a new game.

Alice and Bob have different local numbers for the same game. Both players are informed by monitoring the transactions sent from the ledger. This asynchronous monitoring in indicated by the darker blue messages. Cyan messages (i.e. when we "show") are synchronous replies to player commands.

## 8. As Alice, quit and restart nim

    Alice> C-c
    $ clear; bazel-bin/language-support/hs/bindings/examples/nim/nim Alice
    replaying 2 transactions

    Alice> show
    - Match-1: You offered a game against Charlie.
    - Match-2: You offered a game against Bob.

The console keeps no persistent local state. Alice's state is recovered from the ledger.

## 9. As Charlie, accept Alice's game offer

    Charlie> accept 1

Alice and Charlie both see two transactions:

    Match-1: The offer has been accepted.
    Match-1: The game has started.

That game offer was accepted (the underlying daml contract was archived)
The game proper has started. Both players see the initial state of the Nim game.

    Charlie> show
    - Match-1: (--iiiiiii--iiiii--iii--), Alice to move.

    Alice> show
    - Match-1: (--iiiiiii--iiiii--iii--), you to move, against Charlie.

The difference in wording is performed by the ledger app, not the ledger.

## 10. As Alice, make a move

"In match 1, from pile 2, take 3 matchsticks":

    Alice> move 1 2 3

Both Alice and Charlie are informed and can see the updated game state.

    Match-1: Alice has played a move [2:3]

    Alice> show
    - Match-1: (--iiiiiii--ii--iii--), Charlie to move.

## 11. As Alice, try to make a move when it isn't her turn (rejected)

    Alice> move 1 2 1
    command rejected by ledger:... requires controllers: Charlie, but only Alice were given...

Important to note: The ledger app has blindly submitted the move from the player. The check and rejection is performed entirely by the ledger, not the ledger app: the game logic is contained solely in the Daml model.

We also see the rejection in the terminal where we are running the sandbox.

## 12. As Alice, try to accept the game she offered to Bob (rejected)

    Alice> accept 2
    command rejected by ledger:...User abort: acceptor not an offeree...

As before, the ledger ensures the legal workflow. There are no special checks in the app.

## 13. As Charlie, try to make an illegal move (rejected)

    Charlie> move 1 4 1
    command rejected by ledger... User abort: no such pile.

    Charlie> move 1 1 4
    command rejected by ledger... User abort: may only take 1,2 or 3

    Charlie> move 1 2 3
    command rejected by ledger... User abort: not that many in pile.

## 14. As Charlie, make a correct move

    Charlie> move 1 2 2

Seen in both Alice and Bob's console:

    Match-1: Charlie has played a move [2:2]

## 15. As Charlie, offer a game to Alice OR Bob

Charlie is having so much fun he wants to start a 2nd game. He doesn't care who it is against:

    Charlie> offer Alice Bob

Seen in everyone's console:

    ...Charlie has offered to play a new game.

## 16. As Bob, accept Charlie's game offer

    Bob> accept 2

This event is seen in everyone's console, but not everyone sees all information! In particular, Alice sees only that the offer has been accepted (by someone), but not that the game has been started. This is the sub-transaction privacy semantics of Daml in action.


## 17. Bob goes on holiday, so replace him with Robot Bob

Nearly at the end of the demo. Quit Bob's console, and start up a robot to play his turns for him!

    Bob> C-c
    $ clear; bazel-bin/language-support/hs/bindings/examples/nim/nim --robot Bob
    Running robot for: Bob
    replaying 4 transactions

The robot is an example of a Ledger Nanobot, continuously monitoring ledger events and responding with actions as appropriate. In this case, it will play a random move, or accept any new game offer.

    Match-1: The offer has been accepted.
    Match-1: The game has started.

And indeed we see the earlier game offered by Alice is accepted.

## 18. As Alice, play against Bob for a while

    Alice> move 2 1 1
    Match-2: Bob has played a move [???]

    Alice> move 2 1 1
    Match-2: Bob has played a move [???]

Robot Bob's moves are random, so indicated by `???` above.

## 19. Alice also goes on holiday, so replace her with Robot Alice

    Alice> C-c
    $ clear; bazel-bin/language-support/hs/bindings/examples/nim/nim --robot Alice

The robots complete the game!

## 20. As Alice, check in to see who won

    C-c
    $ clear; bazel-bin/language-support/hs/bindings/examples/nim/nim Alice
    Alice> show
