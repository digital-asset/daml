{-# LANGUAGE LambdaCase      #-}
{-# LANGUAGE NamedFieldPuns  #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE RankNTypes      #-}

-- Domain types (will originate in Daml model)

module Domain(Player(..),
              Offer(..),
              Game(..),
              Move(..),
              playersOfGame,
              playersOfOffer,
              legalMovesOfGame,
              initGame,
              playMove
             ) where

import Data.List as List(splitAt)

data Player = Player String
    deriving (Eq,Ord)

instance Show Player where show (Player s) = s

data Offer = Offer { from :: Player, to :: [Player] }
    deriving (Show)

data Game = Game { p1 :: Player, p2 :: Player, piles :: [Int] }
    deriving (Show)

data Move = Move { pileNum :: Int, howMany :: Int }
    deriving Show

playersOfGame :: Game -> [Player]
playersOfGame Game{p1,p2} = [p1,p2]

playersOfOffer :: Offer -> [Player]
playersOfOffer Offer{from,to} = from : to

legalMovesOfGame :: Game -> [Move]
legalMovesOfGame Game{piles} = do
    (pileNum,remaining) <- zip [1..] piles
    howMany <- [1..min 3 remaining]
    return $ Move {pileNum,howMany}

initGame :: Player -> Player -> Game
initGame p1 p2 = Game {p1, p2, piles = standardInitPiles}

standardInitPiles :: [Int]
standardInitPiles = [7,5,3]

type Rejection = String

playMove :: Move -> Game -> Either Rejection Game
playMove Move{pileNum,howMany} Game{p1,p2,piles} =
    case List.splitAt (pileNum - 1) piles of
        (xs,selected:ys) ->
            if howMany > 3 then Left "may only take 1,2 or 3"
            else if selected < howMany then Left "not that many in pile"
            else Right $ Game { p1 = p2, p2 = p1, piles = xs ++ [selected - howMany] ++ ys }
        _ -> Left"no such pile"
