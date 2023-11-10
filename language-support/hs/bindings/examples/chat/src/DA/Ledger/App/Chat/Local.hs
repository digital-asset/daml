-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- The local state maintained non-persistently by the chat-console
module DA.Ledger.App.Chat.Local (
    State, initState, known, history,
    UserCommand(..), externalizeCommand,
    applyTransQuiet, applyTrans, Announce,
    introduceEveryone,
    ) where

import DA.Ledger.App.Chat.Contracts (ChatContract)
import DA.Ledger.App.Chat.Contracts qualified as C
import DA.Ledger.App.Chat.Domain (Party,Introduce(..),Message(..),Broadcast(..))
import Data.List ((\\))
import Data.Text.Lazy (Text)
import Data.Text.Lazy as Text (unpack)

-- user commands, to be interpreted w.r.t the local state

data UserCommand
    = Speak Text
    | Link Party
    deriving Show

-- local state, accumulates external transitions

data State = State {
    known :: [Party],
    ans :: [Announce]
    }
    deriving (Show)

history :: State -> [Announce]
history = reverse . ans

data MKind = MSay | MShout deriving (Show)

initState :: State
initState = State {
    known = [],
    ans = []
    }

-- externalize a user-centric command into a Chat Contract (creation)

externalizeCommand :: Party -> Maybe Party -> State -> UserCommand -> Either String ChatContract
externalizeCommand whoami talking State{known} = \case
    Link party ->
        return $ C.Introduce $ Introduce { from = whoami, people = party:known }
    Speak text -> case talking of
        Just to ->
            return $ C.Message $ Message { from = whoami, to = to, body = text }
        Nothing
            | null known -> Left "no one is listening"
            | otherwise ->
              return $ C.Broadcast $ Broadcast { from = whoami, to = known, body = text }

-- accumulate an external Chat Contract (transaction) into the local state

applyTransQuiet :: Party -> State -> ChatContract -> State
applyTransQuiet whoami s cc = s' where (s',_,_) = applyTrans whoami s cc

applyTrans :: Party -> State -> ChatContract -> (State,[Announce],[ChatContract])
applyTrans whoami s@State{known,ans} = \case

    C.Introduce Introduce{from,people} ->
        (s', ans', replies)
        where s' = s {known = known', ans = reverse ans' ++ ans }
              ans' = map ANewLink new
              new = (from:people) \\ (whoami:known)
              known' = known ++ new
              others = known \\ (from:people)
              replies
                  | null others = []
                  | otherwise = [introduceEveryone whoami s']

    C.Message Message{from,to,body} ->
        (s { ans = an : ans }, [an | from /= whoami], [])
        where an =
                  if from == whoami
                  then AMessageSent{to,body}
                  else AMessageIn{from,body}

    C.Broadcast Broadcast{from,body} ->
        (s { ans = an : ans }, [an | from /= whoami], [])
        where an = ABroadcast{from,body}


introduceEveryone :: Party -> State -> ChatContract
introduceEveryone whoami State{known} =
    C.Introduce $ Introduce { from = whoami, people = known }


data Announce
    = AMessageIn { from :: Party, body :: Text }
    | AMessageSent { to :: Party, body :: Text }
    | ABroadcast { from :: Party, body :: Text }
    | ANewLink { who :: Party }

instance Show Announce where
    show = \case
        AMessageIn{from,body} -> "(" ++ show from ++ ") " ++ Text.unpack body
        AMessageSent{to,body} -> "-> (" ++ show to ++") " ++ Text.unpack body
        ABroadcast{from,body} -> "(" ++ show from ++"!) " ++ Text.unpack body
        ANewLink{who} -> "linked: " <> show who
