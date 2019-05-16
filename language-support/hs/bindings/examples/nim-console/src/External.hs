-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module External(XCommand(..),
                XTrans(..),
                Xoid, genXoid,
                Xgid, genXgid,
               ) where

import Domain

import qualified Data.UUID as UUID(toString)
import System.Random (randomIO)

-- external commands

data XCommand
    = OfferGame Offer
    | AcceptOffer Player Xoid
    | MakeMove Player Xgid Move
    -- | ClaimWin Xgid -- TODO
    deriving Show


-- external transitions

data XTrans
    = NewOffer { xoid :: Xoid, offer :: Offer }
    | OfferWithdrawn { xoid :: Xoid, offer :: Offer } -- because someone accepted
    | NewGame { xgid :: Xgid, game :: Game } -- replaces an offer
    | GameMove { oldXgid :: Xgid, newXgid :: Xgid, game :: Game }
    deriving Show

-- order id & game id

data Xoid = Xoid String
    deriving (Eq,Ord)
instance Show Xoid where show (Xoid s) = s

data Xgid = Xgid String
    deriving (Eq,Ord)
instance Show Xgid where show (Xgid s) = s

genXgid :: IO Xgid
genXgid = do fmap (Xgid . UUID.toString) randomIO

genXoid :: IO Xoid
genXoid = do fmap (Xoid . UUID.toString) randomIO
