-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module UI(interactiveMain) where

import Control.Concurrent
import Control.Monad.Trans.Class (lift)
import System.Console.ANSI(Color(..))
import Text.Read (readMaybe)
import qualified System.Console.Haskeline as HL

import DA.Ledger.Stream
import Domain
import Local(State,UserCommand,MatchNumber(..))
import Logging
import NimLedger(Handle,connect)
import Interact
import qualified Local

replyLog :: String -> IO ()
replyLog = colourLog Cyan plainLog

interactiveMain :: Player ->IO ()
interactiveMain player = HL.runInputT HL.defaultSettings $ do
    xlog <- HL.getExternalPrint
    let errLog = colourLog Red xlog
    h <- lift (connect errLog)
    ps <- lift $ makePlayerState h xlog player
    readLoop h xlog ps

-- readLoop

promptPlayer :: Player -> String
promptPlayer player = colourWrap Green (show player <> "> ")

readLoop :: Handle -> Logger -> PlayerState -> HL.InputT IO ()
readLoop h xlog ps = do
    let PlayerState{player} = ps
    lineOpt <- HL.getInputLine (promptPlayer player)
    case lineOpt of
      Nothing -> return ()
      Just line -> do
          ps' <- lift $ processLine h xlog ps line
          readLoop h xlog ps'

processLine :: Handle -> Logger -> PlayerState -> String -> IO PlayerState
processLine h xlog ps line = do
    case parseWords (words line) of
        Nothing -> do
            replyLog $ "failed to parse: " <> line
            return ps
        Just res -> runParsed h xlog ps res

-- parse console input line

data Query
    = ShowOpenState

data Parsed
    = Submit UserCommand
    | Query Query
    | Become Player

parseWords :: [String] -> Maybe Parsed
parseWords = \case
    ["become",p] -> do
        return $ Become (Player p)
    [] ->
        return $ Query ShowOpenState -- TODO: just show selected game (when implemented)
    ["show"] ->
        return $ Query ShowOpenState
    ["offer"] ->
        return $ Submit Local.OfferNewGameToAnyone
    "offer":ps -> do
        return $ Submit $ Local.OfferGameL (map Player ps)
    ["accept",o] -> do
        oid <- parseMatchNumber o
        return $ Submit $ Local.AcceptOfferL oid
    ["move",g,p,n] -> do
        gid <- parseMatchNumber g
        pileNum <- readMaybe p
        howMany <- readMaybe n
        return $ Submit $ Local.MakeMoveL gid (Move {pileNum,howMany})
    _ ->
        Nothing

parseMatchNumber :: String -> Maybe MatchNumber
parseMatchNumber s = fmap MatchNumber (readMaybe s)

-- run the parsed command

runParsed :: Handle -> Logger -> PlayerState -> Parsed -> IO PlayerState
runParsed h xlog ps = \case
    Submit lc -> do
        runSubmit h replyLog ps lc
        return ps
    Query lq -> do
        let PlayerState{sv} = ps
        s <- readMVar sv
        runLocalQuery s lq
        return ps
    Become player' -> do
        replyLog $ "becoming: " <> show player'
        let PlayerState{stream} = ps
        closeStream stream (Abnormal "changing player")
        makePlayerState h xlog player'

runLocalQuery :: State -> Query -> IO ()
runLocalQuery s = \case
    ShowOpenState -> replyLog (Local.prettyState s)
