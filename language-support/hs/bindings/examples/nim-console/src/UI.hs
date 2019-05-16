-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module UI(main) where

import Control.Concurrent
import Control.Monad.Trans.Class (lift)
import Data.Foldable(forM_)
import Prelude hiding (id)
import System.Console.ANSI(Color(..))
import qualified System.Console.Haskeline as HL (InputT,runInputT,getInputLine,defaultSettings,getExternalPrint)
import Text.Read (readMaybe)
import System.Time.Extra(sleep)

import Domain
import Stream
import PastAndFuture
import External

import Local(State,LCommand,Onum(..),Gnum(..))
import qualified Local

import qualified SimLedger as Ledger
import SimLedger(Handle)

import Logging

----------------------------------------------------------------------
-- players, colours

alice,bob,charles :: Player
alice = Player "alice"
bob = Player "bob"
charles = Player "charles"


message :: String -> IO ()
message = colourMes Cyan plainMes

----------------------------------------------------------------------
-- PlayerState

data PlayerState = PlayerState {
    player :: Player,
    sv :: MVar State,
    stream :: Stream XTrans
    }

makePlayerState :: Handle -> Mes -> Player -> IO PlayerState
makePlayerState h xmes player = do
    -- TODO: handle knownPlayers by "Hello" contracts
    let knownPlayers = [alice,bob,charles] -- john *not* a known player
    let s = Local.initState player knownPlayers
    sv <- newMVar s
    stream <- manageUpdates h player (playerMes player xmes) sv
    return PlayerState{player,sv,stream}


playerMes :: Player -> Mes -> Mes
playerMes player mes =
    tagMes ("(" <> show player <> ") ") $
    --colourMes (colourForPlayer player) mes
    colourMes Blue  mes

----------------------------------------------------------------------
-- main

main :: IO ()
main = HL.runInputT HL.defaultSettings $ do
    h <- lift Ledger.connect
    xmes <- HL.getExternalPrint
    let player = alice -- initial interactive player
    lift $ runBotFor h bob
    lift $ runBotFor h charles -- 2nd bot
    ps <- lift $ makePlayerState h xmes player
    readLoop h xmes ps

----------------------------------------------------------------------
-- readLoop

promptPlayer :: Player -> String
promptPlayer player =
    --colourWrap (colourForPlayer player) (show player <> "> ")
    colourWrap Green (show player <> "> ")

readLoop :: Handle -> Mes -> PlayerState -> HL.InputT IO ()
readLoop h xmes ps = do
    let PlayerState{player} = ps
    lineOpt <- HL.getInputLine (promptPlayer player)
    case lineOpt of
      Nothing -> return ()
      Just line -> do
          ps' <- lift $ processLine h xmes ps line
          readLoop h xmes ps'

processLine :: Handle -> Mes -> PlayerState -> String -> IO PlayerState
processLine h xmes ps line = do
    case parseWords (words line) of
        Nothing -> do
            message $ "failed to parse: " <> line
            return ps
        Just res -> runParsed h xmes ps res


----------------------------------------------------------------------
-- parse console input line

data LQuery
    = ShowFullState --debug
    | ShowOpenState
    -- TODO: individual game state

data Parsed
    = Submit LCommand
    | Query LQuery
    | Become Player

parseWords :: [String] -> Maybe Parsed
parseWords = \case
    ["become",p] -> do
        return $ Become (Player p)
    ["show"] ->
        return $ Query ShowOpenState
    ["show","full"] ->
        return $ Query ShowFullState
    ["offer"] ->
        return $ Submit Local.OfferNewGameToAnyone
    ["offer",p] -> do
        return $ Submit $ Local.OfferGameL (Player p)
    ["accept",o] -> do
        oid <- parseOnum o
        return $ Submit $ Local.AcceptOfferL oid
    ["move",g,p,n] -> do
        gid <- parseGnum g
        pileNum <- readMaybe p
        howMany <- readMaybe n
        return $ Submit $ Local.MakeMoveL gid (Move {pileNum,howMany})
    _ ->
        Nothing

parseOnum :: String -> Maybe Onum
parseOnum s = fmap Onum (readMaybe s)

parseGnum :: String -> Maybe Gnum
parseGnum s = fmap Gnum (readMaybe s)

----------------------------------------------------------------------
-- run thhe parsed command

runParsed :: Handle -> Mes -> PlayerState -> Parsed -> IO PlayerState
runParsed h xmes ps = \case
    Submit lc -> do
        runSubmit h message ps lc
        return ps
    Query lq -> do
        let PlayerState{sv} = ps
        s <- readMVar sv
        runLocalQuery s lq
        return ps
    Become player' -> do
        message $ "becoming: " <> show player'
        let PlayerState{stream} = ps
        closeStream stream (Closed "changing player")
        makePlayerState h xmes player'

runLocalQuery :: State -> LQuery -> IO ()
runLocalQuery s = \case
    ShowFullState -> message (show s)
    ShowOpenState -> message (show (Local.getOpenState s))


----------------------------------------------------------------------
-- runSubmit (used by runParsed and robot)

runSubmit :: Handle -> Mes -> PlayerState -> LCommand -> IO ()
runSubmit h mes ps lc = do
    --mes $ "lc: " <> show lc
    let PlayerState{player,sv} = ps
    s <- readMVar sv
    case Local.externCommand player s lc of
        Nothing -> do
            mes $ "bad local command: " <> show lc
            return ()
        Just xc -> do
            Ledger.sendCommand h xc >>= \case
                Nothing -> return ()
                Just rej -> do
                    mes $ "command rejected by ledger: " <> rej


----------------------------------------------------------------------
-- Manage updates in response to XTrans from the ledger

manageUpdates :: Handle -> Player -> Mes -> MVar State -> IO (Stream XTrans)
manageUpdates h player mes sv = do
    PF{past,future} <- Ledger.getTrans player h
    modifyMVar_ sv (\s -> return $ foldl Local.applyTransPureSimple s past)
    _ <- forkIO (updateX mes sv future)
    return future

updateX :: Mes -> MVar State -> Stream XTrans -> IO ()
updateX mes sv stream = loop
  where
    loop = takeStream stream >>= \case
        Left Closed{} -> do
            mes "transaction stream is closed"
            return () -- forked thread will terminate
        Right xt ->
            do applyX mes sv xt; loop

applyX :: Mes -> MVar State -> XTrans -> IO ()
applyX mes sv xt = do
    s <- takeMVar sv
    --mes $ "xt: " <> show xt
    (lts,s') <- either fail return (Local.applyTrans s xt)
    mapM_ (\lt -> mes $ "lt: " <> show lt) lts -- TODO: improve message for "local trans"
    putMVar sv s'


----------------------------------------------------------------------
-- robot

runBotFor :: Handle -> Player -> IO ()
runBotFor h player = do
    ps <- makePlayerState h noMes player
    _tid <- forkIO (robot h noMes ps)
    return ()


robot :: Handle -> Mes -> PlayerState -> IO ()
robot h mes ps = loop
  where
    loop = do
        sleep 2
        mes "thinking..."
        let PlayerState{sv} = ps
        s <- readMVar sv
        forM_ (Local.lookForAnAction s) (runSubmit h noMes ps) -- quiet!
        loop
