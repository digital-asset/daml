-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- The local state maintained non-persistently by the console
module DA.Ledger.App.Nim.Local(
    MatchNumber(..),
    State(..), initState, prettyState,
    applyManyTrans,
    applyTrans, Announce,
    UserCommand(..), externalizeCommand, possibleActions
    ) where

import Control.Monad(when)
import Data.List ((\\),sortBy,intercalate)
import Data.Map.Strict (Map)
import Data.Maybe(mapMaybe)
import qualified Data.List as List(find,concatMap)
import qualified Data.Map.Strict as Map(toList,lookup,empty,adjust,insert,elems,keys)

import DA.Ledger.Types(ContractId)
import DA.Ledger.App.Nim.Domain
import DA.Ledger.App.Nim.NimTrans
import DA.Ledger.App.Nim.NimCommand
import DA.Ledger.App.Nim.Logging

-- local state, accumulates external transitions

data State = State {
    whoami :: Player,
    knownPlayers :: [Player],
    offers :: Map Oid (MatchNumber,Offer,Status),
    games :: Map Gid (MatchNumber,Game,Status),
    nextMatchNum :: Int
    }
    deriving (Show)

data Status = Open | Closed deriving (Eq,Show)

initState :: Player -> [Player] -> State
initState whoami knownPlayers = State {
    whoami,
    knownPlayers, -- TODO: determine automatically
    offers = Map.empty,
    games = Map.empty,
    nextMatchNum = 1
    }

data MatchNumber = MatchNumber Int deriving (Eq,Ord)
instance Show MatchNumber where
    show (MatchNumber n) = "Match-" <> show n

-- user commands, w.r.t the local state

data UserCommand
    = OfferNewGameToAnyone
    | OfferGameL [Player]
    | AcceptOfferL MatchNumber
    | MakeMoveL MatchNumber Move
    deriving Show

possibleActions :: State -> [UserCommand]
possibleActions State{whoami,offers,games} =
        mapMaybe
        (\(onum,offer,status) ->
                if whoami `elem` to offer && status == Open
                then Just $ AcceptOfferL onum
                else Nothing
        ) (Map.elems offers)
        ++
        List.concatMap
        (\(onum,game,status) ->
                if whoami == p1 game && status == Open
                then map (MakeMoveL onum) (legalMovesOfGame game)
                else []
        ) (Map.elems games)

-- externalize a user-centric command into a Nim command

externalizeCommand :: Player -> State -> UserCommand -> Maybe NimCommand
externalizeCommand who state@State{knownPlayers} = \case
    OfferNewGameToAnyone -> do
        return $ OfferGame (Offer {from = who, to = knownPlayers \\ [who] })
    OfferGameL players -> do
        return $ OfferGame (Offer {from = who, to = players }) --can play self!
    AcceptOfferL lid -> do
        xid <- externOid state lid
        return $ AcceptOffer who xid
    MakeMoveL lid move -> do
        xid <- externGid state lid
        return $ MakeMove xid move

type Oid = ContractId
type Gid = ContractId

externOid :: State -> MatchNumber -> Maybe Oid
externOid State{offers} low = (fmap fst . List.find (\(_,(l,_,status)) -> l==low && status == Open) . Map.toList) offers

externGid :: State -> MatchNumber -> Maybe Gid
externGid State{games} low = (fmap fst . List.find (\(_,(l,_,status)) -> l==low && status == Open) . Map.toList) games


-- aaccumulate an external Nim transition into the local state

applyManyTrans :: Logger -> State -> [NimTrans] -> IO State
applyManyTrans log s = \case
    [] -> return s
    x:xs -> do
        case applyTrans s x of
            Left mes -> do log $ "applyTrans fail: " <> mes; applyManyTrans log s xs
            Right (_announcements,s) -> do
                --mapM_ (log . show) _announcements
                applyManyTrans log s xs

applyTrans :: State -> NimTrans -> Either String ([Announce], State)
applyTrans state0 = \case

    NewOffer{oid,offer} -> do
        let State{offers,nextMatchNum=n} = state0
        let onum = MatchNumber n
        when (oid `elem` Map.keys offers) $ Left $ "new offer, dup id: " <> show oid
        return (
            [AnnounceNewOffer onum offer],
            state0 { offers = Map.insert oid (onum,offer,Open) offers,
                     nextMatchNum = n + 1 })

    OfferWithdrawn{oid} -> do
        let State{offers} = state0
        case Map.lookup oid offers of
            Nothing -> Left "offer withdrawm, unknown id"
            Just (onum,offer,_) ->
                return (
                [AnnounceOfferLapsed onum offer],
                state0 { offers = Map.adjust archive oid offers })

    NewGame {oid,gid,game} -> do
        let State{games,offers} = state0
        when (gid `elem` Map.keys games) $ Left "new game, dup id"
        case Map.lookup oid offers of
            Nothing -> Left "new game, unknown offer id"
            Just (onum,_,_) ->
                return (
                [AnnounceNewGame onum game],
                state0 { games = Map.insert gid (onum,game,Open) games })

    GameMove {oldGid,newGid,game} -> do
        let State{games} = state0
        when (newGid `elem` Map.keys games) $ Left "game move, dup new id"
        case Map.lookup oldGid games of
            Nothing -> Left "game move, unknown old id"
            Just (onum,oldGame,_) -> return (
                [AnnounceGameMove onum game (deduceMoves oldGame game)],
                state0 { games = Map.insert newGid (onum,game,Open) (Map.adjust archive oldGid games)})

  where archive (k,v,_) = (k,v,Closed)

-- announce what happened from the perspective of the local player
-- TODO: distinguish offers/moves by me/others
data Announce
    = AnnounceNewOffer MatchNumber Offer
    | AnnounceOfferLapsed MatchNumber Offer
    | AnnounceNewGame MatchNumber Game
    | AnnounceGameMove MatchNumber Game [Move]

instance Show Announce where
    show = \case
     AnnounceNewOffer m Offer{from} ->
         show m <> ": " <> show from <> " has offered to play a new game."
     AnnounceOfferLapsed m _ ->
         show m <> ": The offer has been accepted."
     AnnounceNewGame m _ ->
         show m <> ": The game has started."
     AnnounceGameMove m Game{p2} reconstructedMoves ->
         show m <> ": " <> show p2 <> " has played a move " <> show reconstructedMoves

-- Visualize local open state

prettyState :: State -> String
prettyState = show . getOpenState

data OpenState = OpenState {
    me :: Player,
    notStarted :: [(MatchNumber,MatchStatus)],
    finised :: [(MatchNumber,MatchStatus)],
    inProgress :: [(MatchNumber,MatchStatus)]
    }

instance Show OpenState where
    show OpenState{notStarted,finised,inProgress} = unlines (
        ["Offers:"] ++
        map (\(num,m) -> "- " <> show num <> ": " <> show m) notStarted ++
        ["Finished:"] ++
        map (\(num,m) -> "- " <> show num <> ": " <> show m) finised ++
        ["In play:"] ++
        map (\(num,m) -> "- " <> show num <> ": " <> show m) inProgress
        )

getOpenState :: State -> OpenState
getOpenState State{whoami=me,offers,games} = OpenState{me,notStarted,finised,inProgress}
    where
        notStarted = filter (\(_,m) -> not (startedMatch m)) allMatches
        finised = filter (\(_,m) -> finishedMatch m) allMatches
        inProgress = filter (\(_,m) -> startedMatch m && not (finishedMatch m)) allMatches
        allMatches = (
            sortBy (\(a,_) (b,_) -> compare a b)
            . map (\(id,game,_) -> (id,game))
            . filter (\(_,_,status) -> status == Open)
            . map (\(num,og,status) -> (num, matchStatus me og, status))
            ) (
            map (\(n,o,s) -> (n,Left o,s)) (Map.elems offers)
            ++ map (\(n,g,s) -> (n,Right g,s)) (Map.elems games)
            )

-- MatchStatus

newtype Piles = Piles [Int]

instance Show Piles where
    show (Piles piles) =
        "(--" <> intercalate "--" (map showPile piles) <> "--)"
        where showPile = \case 0 -> "X"; n -> replicate n 'i'

data MatchStatus
    = YouOffer [Player]
    | TheyOffer Player [Player]
    | YouNext Piles Player
    | ThemNext Piles Player
    | YouWinner Player
    | ThemWinner Player

matchStatus :: Player -> Either Offer Game -> MatchStatus
matchStatus player = \case
    Left offer -> matchStatusOfOffer player offer
    Right game -> matchStatusOfGame player game

matchStatusOfGame :: Player -> Game -> MatchStatus
matchStatusOfGame whoami Game{p1,p2,piles} = do
    if sum piles == 0
        then if p1==whoami then YouWinner p2 else ThemWinner p1
        else if p1==whoami then YouNext (Piles piles) p2 else ThemNext (Piles piles) p1

matchStatusOfOffer :: Player -> Offer -> MatchStatus
matchStatusOfOffer whoami Offer{from,to} = do
    if from==whoami
    then YouOffer to
    else TheyOffer from (filter (/= whoami) to)

startedMatch :: MatchStatus -> Bool
startedMatch = \case YouOffer _ -> False; TheyOffer _ _ -> False;  _ -> True

finishedMatch :: MatchStatus -> Bool
finishedMatch = \case YouWinner _ -> True; ThemWinner _ -> True;  _ -> False

instance Show MatchStatus where
    show = \case
        YouOffer players ->
            "You offered a game against " <> oneOf (map show players) <> "."
        TheyOffer opp more ->
            show opp <> " offered a game against " <> oneOf ("you":map show more) <> "."
        YouNext piles opp ->
            show piles <> ", you to move, against " <> show opp <> "."
        ThemNext piles opp ->
            show piles <> ", " <> show opp <> " to move."
        YouWinner opp ->
            "You won (beating " <> show opp <> ")."
        ThemWinner opp ->
            "You lost to " <> show opp <> "."

oneOf :: [String] -> String
oneOf = \case
    [] -> "no one" -- never!
    [x] -> x
    xs -> intercalate ", " (init xs) <> " or " <> last xs
