-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Nim domain types. These should be derived automatically from the Daml model.

module DA.Ledger.App.Nim.Domain(
    Player(..), partyOfPlayer,
    Offer(..),
    Game(..),
    Move(..),
    legalMovesOfGame, -- for Robot
    deduceMoves,
    ) where

import Data.List.Extra(zipWithFrom)
import DA.Ledger.Types
import DA.Ledger.IsLedgerValue(IsLedgerValue(..))
import qualified Data.Text.Lazy as Text

data Player = Player { unPlayer :: String } deriving (Eq,Ord)
instance Show Player where show (Player s) = s

partyOfPlayer :: Player -> Party
partyOfPlayer = Party . Text.pack . unPlayer

instance IsLedgerValue Player where
    toValue = toValue . Party . Text.pack . unPlayer
    fromValue = fmap (Player . Text.unpack . unParty) . fromValue

data Offer = Offer { from :: Player, to :: [Player] }
    deriving (Show)

instance IsLedgerValue Offer where
    toValue Offer{from,to} = VList [toValue from, toValue to]
    fromValue = \case
        VList [v1,v2] -> do
            from <- fromValue v1
            to <- fromValue v2
            return Offer{from,to}
        _ -> Nothing

data Game = Game { p1 :: Player, p2 :: Player, piles :: [Int] } deriving (Show)

instance IsLedgerValue Game where
    toValue Game{} = undefined -- we never send games to the ledger
    fromValue = \case
        VList[VRecord Record{fields=[
                                    RecordField{fieldValue=v1},
                                    RecordField{fieldValue=v2},
                                    RecordField{fieldValue=v3}]
                            }] -> do
            p1 <- fromValue v1
            p2 <- fromValue v2
            piles <- fromValue v3
            return Game{p1,p2,piles}
        _ ->
            Nothing

data Move = Move { pileNum :: Int, howMany :: Int }

instance Show Move where
    show Move{pileNum,howMany} = show pileNum <> ":" <> show howMany

instance IsLedgerValue Move where
    toValue Move{pileNum,howMany} =
        VRecord(Record{rid=Nothing,
                       fields=[
                              RecordField "" (toValue pileNum),
                              RecordField "" (toValue howMany)]})
    fromValue = undefined -- we never receive moves from the ledger

legalMovesOfGame :: Game -> [Move]
legalMovesOfGame Game{piles} = do
    (pileNum,remaining) <- zip [1..] piles
    howMany <- [1..min 3 remaining]
    return $ Move {pileNum,howMany}

deduceMoves :: Game -> Game -> [Move]
deduceMoves Game{piles=p1} Game{piles=p2} =
    filter (\Move{howMany} -> howMany > 0)
    $ zipWithFrom (\pileNum (x,y) -> Move {pileNum, howMany = x - y}) 1
    $ zip p1 p2
