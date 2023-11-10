-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Ledger.App.Nim.UI(interactiveMain) where

import Control.Concurrent
import Control.Monad.Trans.Class (lift)
import DA.Ledger.App.Nim.Domain
import DA.Ledger.App.Nim.Interact
import DA.Ledger.App.Nim.Local(State,UserCommand(..),MatchNumber(..),prettyState)
import DA.Ledger.App.Nim.Logging
import DA.Ledger.App.Nim.NimLedger(Handle,connect)
import DA.Ledger.Stream
import System.Console.ANSI(Color(..))
import System.Console.Haskeline qualified as HL
import Text.Read (readMaybe)

replyLog :: String -> IO ()
replyLog = colourLog Cyan plainLog

interactiveMain :: Player ->IO ()
interactiveMain player = HL.runInputT HL.defaultSettings $ do
    xlog <- HL.getExternalPrint
    let errLog = colourLog Red xlog
    h <- lift (connect errLog)
    ps <- lift $ makePlayerState h xlog player
    lift $ replyLog "type \"help\" to see available commands"
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
    | ShowHelp

data Parsed
    = Submit UserCommand
    | Query Query
    | Become Player

parseWords :: [String] -> Maybe Parsed
parseWords = \case
    ["become",p] -> do
        return $ Become (Player p)
    ["help"] ->
        return $ Query ShowHelp
    [] ->
        return $ Query ShowOpenState -- TODO: just show selected game (when implemented)
    ["show"] ->
        return $ Query ShowOpenState
    ["offer"] -> -- TODO: reinstate when proper management of known players is implemented
        Nothing -- return $ Submit Local.OfferNewGameToAnyone
    "offer":ps -> do
        return $ Submit $ OfferGameL (map Player ps)
    ["accept",o] -> do
        oid <- parseMatchNumber o
        return $ Submit $ AcceptOfferL oid
    ["move",g,p,n] -> do
        gid <- parseMatchNumber g
        pileNum <- readMaybe p
        howMany <- readMaybe n
        return $ Submit $ MakeMoveL gid (Move {pileNum,howMany})
    _ ->
        Nothing

parseMatchNumber :: String -> Maybe MatchNumber
parseMatchNumber s = fmap MatchNumber (readMaybe s)

helpText :: String
helpText = unlines [
    "show               : Show the local state.",
    "<RETURN>           : Alias for show.",
    "help               : Display this help text.",
    "offer [Player*]    : Offer a new game against any of a list of players.",
    "accept [Match-Id]  : Accept an offer of a new game.",
    "move [M] [P] [N]   : In match M, from pile P, take N matchsticks."
    ]

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
    ShowOpenState -> replyLog (prettyState s)
    ShowHelp -> replyLog helpText
