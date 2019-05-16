-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module Local(State(..), Onum(..), Gnum(..), initState, applyTransPureSimple, applyTrans,
             LCommand(..), externCommand,lookForAnAction,
             OpenState, getOpenState,
             LTrans,
             ) where

import Prelude hiding(id)
import Control.Monad(when)
import qualified Data.List as List(find,concatMap)
import Data.List ((\\))
import Data.Maybe(mapMaybe,listToMaybe)
import qualified Data.Map.Strict as Map(toList,lookup,empty,adjust,insert,elems,keys)
import Data.Map.Strict (Map)

import Domain
import External

----------------------------------------------------------------------
-- local command, map to X in context of state

data LCommand
    = OfferNewGameToAnyone
    | OfferGameL Player
    | AcceptOfferL Onum
    | MakeMoveL Gnum Move
    deriving Show

externCommand :: Player -> State -> LCommand -> Maybe XCommand
externCommand who state@State{knownPlayers} = \case
    OfferNewGameToAnyone -> do
        return $ OfferGame (Offer {from = who, to = knownPlayers \\ [who] })
    OfferGameL player -> do
        return $ OfferGame (Offer {from = who, to = [player] }) --can play self!
    AcceptOfferL lid -> do
        xid <- externOid state lid
        return $ AcceptOffer who xid
    MakeMoveL lid move -> do
        xid <- externGid state lid
        return $ MakeMove who xid move

externOid :: State -> Onum -> Maybe Xoid
externOid State{offers} low = (fmap fst . List.find (\(_,(l,_,status)) -> l==low && isOpen status) . Map.toList) offers

externGid :: State -> Gnum -> Maybe Xgid
externGid State{games} low = (fmap fst . List.find (\(_,(l,_,status)) -> l==low && isOpen status) . Map.toList) games

----------------------------------------------------------------------
-- local trans, for reporting what happing in terms of local oid/gid
data LTrans
    = NewOfferL Onum Offer
    | OfferNowUnavailable Onum Offer
    | NewGameL Gnum Game
    | GameMoveL Gnum Game
    deriving Show

-- TODO: return list, then caller can make random choice!
lookForAnAction :: State -> Maybe LCommand
lookForAnAction State{whoami,offers,games} =
    listToMaybe $
        -- prefer to play a game move..
        mapMaybe
        (\(onum,offer,status) ->
                if whoami `elem` to offer && isOpen status
                then Just $ AcceptOfferL onum
                else Nothing
        ) (Map.elems offers)
        ++
        -- otherwise accept any pending offer
        List.concatMap
        -- randomize move order here !
        (\(gnum,game,status) ->
                if whoami == p1 game && isOpen status
                then map (MakeMoveL gnum) (legalMovesOfGame game)
                else []
        ) (Map.elems games)

----------------------------------------------------------------------
-- local state, accumulates external transitions

data State = State {
    whoami :: Player,
    knownPlayers :: [Player],
    offers :: Map Xoid (Onum,Offer,Status),
    games :: Map Xgid (Gnum,Game,Status),

    -- TODO: share the next number thing for offers and games
    -- when an offer is converted to a game, keep the number
    -- this mean a X-NewGame wil need to refernce the offer is comes from
    nextOfferId :: Onum,
    nextGameId :: Gnum
    }
    deriving (Show)

data Status = Open | Closed
    deriving (Show)

isOpen :: Status -> Bool
isOpen = \case Open -> True; Closed -> False

initState :: Player -> [Player] -> State
initState whoami knownPlayers = State {
    whoami,
    knownPlayers, -- TODO: = [] when support Hello
    offers = Map.empty,
    games = Map.empty,
    nextOfferId = Onum 1,
    nextGameId = Gnum 1
    }

data Onum = Onum Int deriving (Show,Eq) -- local offer id
data Gnum = Gnum Int deriving (Show,Eq) -- local game id

incOfferId :: Onum -> Onum
incOfferId (Onum n) = Onum (n+1)

incGameId :: Gnum -> Gnum
incGameId (Gnum n) = Gnum (n+1)

applyTransPureSimple :: State -> XTrans -> State
applyTransPureSimple s xt = either error snd (applyTrans s xt)

applyTrans :: State -> XTrans -> Either String ([LTrans], State)
applyTrans state0 = \case
    NewOffer{xoid,offer} -> do
        let State{offers,nextOfferId=onum} = state0
        when (xoid `elem` Map.keys offers) $ fail "new offer, dup id"
        return (
            [NewOfferL onum offer],
            state0 { offers = Map.insert xoid (onum,offer,Open) offers,
                     nextOfferId = incOfferId onum })

    OfferWithdrawn{xoid,offer} -> do
        let State{offers} = state0
        case Map.lookup xoid offers of
            Nothing -> fail "offer withdrawm, unknown id"
            Just (onum,_,_) ->
                return (
                [OfferNowUnavailable onum offer],
                state0 { offers = Map.adjust archive xoid offers })

    NewGame {xgid,game} -> do
        let State{games,nextGameId=gnum} = state0
        when (xgid `elem` Map.keys games) $ fail "new game, dup id"
        return (
            [NewGameL gnum game],
            state0 { games = Map.insert xgid (gnum,game,Open) games,
                     nextGameId = incGameId gnum })

    GameMove {oldXgid,newXgid,game} -> do
        let State{games} = state0
        when (newXgid `elem` Map.keys games) $ fail "game move, dup new id"
        case Map.lookup oldXgid games of
            Nothing -> fail "game move, unknown old id"
            Just (gnum,_,_) -> return (
                [GameMoveL gnum game],
                state0 { games = Map.insert newXgid (gnum,game,Open) (Map.adjust archive oldXgid games) }
                )

  where archive (k,v,_) = (k,v,Closed)


----------------------------------------------------------------------
-- Visualize local open state

data OpenState = OpenState {
    me :: Player,
    oOffers :: [(Onum,Offer)],
    oGames :: [(Gnum,Game)]
    }

instance Show OpenState where
    show OpenState{me,oOffers,oGames} = unlines (
        [show me] ++
        ["- offers:"] ++
        map (\(id,offer) -> "- " <> show id <> " = " <> show offer) oOffers ++
        ["- games:"] ++
        map (\(id,game) -> "- " <> show id <> " = " <> show game) oGames
        )

getOpenState :: State -> OpenState
getOpenState State{whoami=me,offers,games} = OpenState{me,oOffers,oGames}
    -- TODO: sort the games and offers, by index number
    -- geerally just make this prettier
    -- also, maybe have a summary version, which can show when switch whoami
    where
        oOffers = (map (\(id,offer,_) -> (id,offer)) . filter (\(_,_,status) -> isOpen status) . Map.elems) offers
        oGames = (map (\(id,game,_) -> (id,game)) . filter (\(_,_,status) -> isOpen status) . Map.elems) games
