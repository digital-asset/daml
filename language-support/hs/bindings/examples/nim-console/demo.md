
# Steps for running a demo of nim-console...

Notes on the format of this file:

- Fixed width font lines starting `$` are typed at the terminal.
- Fixed width font lines starting `Alice>` or `Bob>` or `Charlie>` are entered at the relevant `nim-console`.
- Other lines are explanatory text.

## -1. Build applictaion; update the daml SDK

    $ cd daml
    $ bazel build language-support/hs/bindings/...
    $ daml install latest --activate

## 0. Study Nim.daml

Where the operation of the game room, and the rules of Nim, are modelled in DAML.

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

## 1. Start sandbox running the Nim game server

    $ daml sandbox bazel-out/k8-fastbuild/bin/language-support/hs/bindings/Nim.dar


## 2. (new terminal) Start nim-console for Alice

    $ clear; bazel-bin/language-support/hs/bindings/nim Alice
    replaying 0 transactions
    Alice>

    Alice> show
    Offers:
    Finished:
    In play:

We see an empty state: no offers, no games finished, no matches in play.
(The status can also be show by typing <return>)


## 3. Alice offers game to Charlie

    Alice> offer Charlie
    - Match-1: Alice has offered to play a new game.

    Alice> show
    - Match-1: You offered a game against Charlie.

Alice's local state shows that she has sent Charlie an offer to play a game of Nim.


## 4. (new terminal) Start nim-console for Bob

    $ clear; bazel-bin/language-support/hs/bindings/nim Bob
    replaying 0 transactions
    Bob> show

Bob's status is empty. Because Bob has no right to see that Alice offered a match to Charlie.


## 5. (new terminal) Start nim-console for Charlie

    $ clear; bazel-bin/language-support/hs/bindings/nim Charlie
    replaying 1 transactions
    Charlie> show
    - Match-1: Alice offered a game against you.

Charlie sees the match offered by Alice, even though he had never connected before. Charlie's initial state is recovered by replaying the relevant transactions sent from the ledger.


## 6. Alice also offers a game to bob

    Alice> offer Bob

The event is seen in both Alice's console:

    Match-2: Alice has offered to play a new game.

And Bob's console:

    Match-1: Alice has offered to play a new game.

Alice and Bob have different local numbers for the same game. Both players are informed by monitoring the transactions sent from the ledger. This asynchronous monitoring in indicated by the darker blue messages. Cyan messages (i.e. when we "show") are synchronous replies to player commands.


## 7. Alice quits and restarts nim-console

    Alice> C-c
    $ clear; bazel-bin/language-support/hs/bindings/nim Alice
    replaying 2 transactions

    Alice> show
    - Match-1: You offered a game against Charlie.
    - Match-2: You offered a game against Bob.

The nim-console keeps no persistent local state. Alice's state is recovered from the ledger.


## 8. Charlie accepts Alice's game offer

    Charlie> accept 1

Alice and Charlie both see two transactions:

    Match-1: The offer has been accepted.
    Match-1: The game has started.

That game offer was accepted (the underlying daml contract was archived)
The game proper has started.
Both players see the initial state of the Nim game.

    Charlie> show
    - Match-1: (--iiiiiii--iiiii--iii--), Alice to move.

    Alice> show
    - Match-1: (--iiiiiii--iiiii--iii--), you to move, against Charlie.

The difference in wording is performed by the ledger-app, not the ledger.


## 9. Alice makes a move

"In match 1, from pile 2, take 3 matchsticks":

    Alice> move 1 2 3

Both Alice and Charlie are informed and can see the updated game state.

    Match-1: Alice has played a move [2:3]

    Alice> show
    - Match-1: (--iiiiiii--ii--iii--), Charlie to move.


## 10. Alice tries to make a move when it is not her turn (rejected)

    Alice> move 1 2 1
    command rejected by ledger:... requires controllers: Charlie, but only Alice were given...

Important to note: The ledger-app has blindly submitted the move from the player. The check and rejection is performed entirely by the ledger, not the ledger-app. The game logic is contained solely in the Daml Nim model. We also see the rejection in the terminal where we are running the sandbox.


## 11. Alice tries to accept the game she offered to Bob (rejected)

    Alice> accept 2
    command rejected by ledger:...User abort: acceptor not an offeree...

As before, the ledger ensures the legal workflow. There are no special checks in the app.


## 12. Charlie tries to make an illegal move (rejected)

    Charlie> move 1 4 1
    command rejected by ledger... User abort: no such pile.

    Charlie> move 1 1 4
    command rejected by ledger... User abort: may only take 1,2 or 3

    Charlie> move 1 2 3
    command rejected by ledger... User abort: not that many in pile.


## 13. Charlie finally makes a correct move

    Charlie> move 1 2 2

Seen in both Alice and Bob's console

    Match-1: Charlie has played a move [2:2]


## 14. Charlie offers a game to Alice/Bob

Charlie is having so much fun he wants to start a 2nd game. He doesn't care who it is against,

    Charlie> offer Alice Bob

Seen in everyone's console:

    ...Charlie has offered to play a new game.


## 15. Bob accepts Charlie's game offer

    Bob> accept 2

This event is seen in everyone's console, but not everyone sees all information! In particular, Alice sees only that the offer has been accepted (by someone), but not that the game has been started. This is the sub-transaction privacy semantics of Daml in action.


## 16. Bob goes on holiday; Robot Bob takes over

Nearly at the end of the demo. Bob quits his console, and starts up a robot to play his turns for him!

    Bob> C-c
    $ clear; bazel-bin/language-support/hs/bindings/nim --robot Bob
    Running robot for: Bob
    replaying 4 transactions

The robot is an example of a Ledger Nanobot, continuously monitoring ledger events and responding with actions as appropriate. In this case, it will play a random move, or accept any new game offer.

    Match-1: The offer has been accepted.
    Match-1: The game has started.

And indeed we see the earlier game offered by Alice is accepted.


## 17. Alice plays against Bob for a while

    Alice> move 2 1 1
    Match-2: Bob has played a move [???]

    Alice> move 2 1 1
    Match-2: Bob has played a move [???]

Robot Bob's moves are random, so indicated by `???` above.


## 18. Alice also goes on holiday; Robot Alice

    Alice> C-c
    $ clear; bazel-bin/language-support/hs/bindings/nim --robot Alice

The robots complete the game!


## 19. Alice checks in to see who won

    C-c
    $ clear; bazel-bin/language-support/hs/bindings/nim Alice
    Alice> show
