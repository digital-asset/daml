-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module DA.Daml.Assistant.Command
    ( Command(..)
    , BuiltinCommand(..)
    , SdkCommandInfo(..)
    , SdkCommandName(..)
    , SdkCommandArgs(..)
    , SdkCommandPath(..)
    , UserCommandArgs(..)
    , tryBuiltinCommand
    , getCommand
    ) where

import DA.Daml.Assistant.Types
import Data.List
import Data.Maybe
import Data.Foldable
import Options.Applicative.Types
import Options.Applicative.Extended
import System.Environment
import System.FilePath
import Data.Either.Extra
import Control.Exception.Safe
import System.Process

-- | Parse command line arguments without SDK command info. Returns Nothing if
-- any error occurs, meaning the command may not be parseable without SDK command
-- info, or it might just be an error.
tryBuiltinCommand :: IO (Maybe Command)
tryBuiltinCommand = do
    args <- getArgs
    pure $ getParseResult (getCommandPure [] args)

-- | Parse command line arguments with SDK command info, exiting on failure.
getCommand :: [SdkCommandInfo] -> IO Command
getCommand sdkCommands = do
    args <- getArgs
    handleParseResult (getCommandPure sdkCommands args)

getCommandPure :: [SdkCommandInfo] -> [String] -> ParserResult Command
getCommandPure sdkCommands args =
    let parserInfo = info (commandParser sdkCommands <**> helper) forwardOptions
        parserPrefs = prefs showHelpOnError
    in execParserPure parserPrefs parserInfo args

subcommand :: Text -> Text -> InfoMod Command -> Parser Command -> Mod CommandFields Command
subcommand name desc infoMod parser =
    command (unpack name) (info parser (infoMod <> progDesc (unpack desc)))

builtin :: Text -> Text -> InfoMod Command -> Parser BuiltinCommand -> Mod CommandFields Command
builtin name desc mod parser =
    subcommand name desc mod (Builtin <$> parser)

isHidden :: SdkCommandInfo -> Bool
isHidden = isNothing . sdkCommandDesc

dispatch :: SdkCommandInfo -> Mod CommandFields Command
dispatch info = subcommand
    (unwrapSdkCommandName $ sdkCommandName info)
    (fromMaybe "" $ sdkCommandDesc info)
    forwardOptions
    (Dispatch info . UserCommandArgs <$> sdkCommandArgsParser info)

sdkCommandArgsParser :: SdkCommandInfo -> Parser [String]
sdkCommandArgsParser info = fromM (go (unwrapSdkCommandArgs $ sdkCommandArgs info))
  where go args = do
          mx <- oneM $ optional $ strArgument $ completer $
              case sdkCommandForwardCompletion info of
                  Forward enriched -> nestedCompl enriched args
                  NoForward -> defaultCompleter
          case mx of
            Nothing -> return []
            Just x -> (x :) <$> go (args ++ [x])
        nestedCompl enriched args = mkCompleter $ \arg -> do
          let path = unwrapSdkPath (sdkCommandSdkPath info) </> unwrapSdkCommandPath (sdkCommandPath info)
          let createProc = proc
                  path
                  ( [ "--bash-completion-enriched" | getEnrichedCompletion enriched ]
                  <>
                  ("--bash-completion-index"
                  : show (length args + 1)
                  : concatMap (\x -> ["--bash-completion-word", x]) ("daml" : args ++ [arg])
                  ))
          stdout <- readCreateProcess createProc (repeat ' ')
          pure $ lines stdout

commandParser :: [SdkCommandInfo] -> Parser Command
commandParser cmds | (hidden, visible) <- partition isHidden cmds = asum
    [ subparser -- visible commands
        $  builtin "version" "Display Daml version information" mempty (Version <$> versionParser <**> helper)
        <> builtin "install" "Install the specified SDK version" mempty (Install <$> installParser <**> helper)
        <> builtin "uninstall" "Uninstall the specified SDK version" mempty (Uninstall <$> uninstallParser <**> helper)
        <> foldMap dispatch visible
    , subparser -- hidden commands
        $  internal
        <> builtin "exec" "Execute command with daml environment." forwardOptions
            (Exec <$> strArgument (metavar "CMD") <*> many (strArgument (metavar "ARGS")))
        <> foldMap dispatch hidden
    ]

versionParser :: Parser VersionOptions
versionParser = VersionOptions
    <$> flagYesNoAuto "all" False "Display all available versions." idm
    <*> flagYesNoAuto "snapshots" False "Display all available snapshot versions." idm
    <*> flagYesNoAuto "assistant" False "Display Daml assistant version." idm

installParser :: Parser InstallOptions
installParser = InstallOptions
    <$> optional (RawInstallTarget <$> argument str (metavar "TARGET" <> completeWith ["latest"] <> help "The SDK version to install. Use 'latest' to download and install the latest stable SDK version available. Run 'daml install' to see the full set of options."))
    <*> flagYesNoAuto "snapshots" False "Pick up snapshot versions with daml install latest." idm
    <*> (InstallAssistant <$> flagYesNoAuto' "install-assistant" "Install associated Daml assistant version. Can be set to \"yes\" (always installs), \"no\" (never installs), or \"auto\" (installs if newer). Default is \"auto\"." idm)
    <*> iflag ActivateInstall "activate" hidden "Activate installed version of daml"
    <*> iflag ForceInstall "force" (short 'f') "Overwrite existing installation"
    <*> iflag QuietInstall "quiet" (short 'q') "Don't display installation messages"
    <*> fmap SetPath (flagYesNoAuto' "set-path" "Adjust PATH automatically" idm)
    <*> fmap BashCompletions (flagYesNoAuto' "bash-completions" "Install bash completions for Daml assistant. Default is yes for linux and mac, no for windows." idm)
    <*> fmap ZshCompletions (flagYesNoAuto' "zsh-completions" "Install Zsh completions for Daml assistant. Default is yes for linux and mac, no for windows." idm)
    where
        iflag p name opts desc = fmap p (switch (long name <> help desc <> opts))

uninstallParser :: Parser SdkVersion
uninstallParser =
    argument readSdkVersion (metavar "VERSION" <> help "The SDK version to uninstall.")

readSdkVersion :: ReadM SdkVersion
readSdkVersion =
    eitherReader (mapLeft displayException . parseVersion . pack)

-- | Completer that uses the builtin bash completion.
-- We use this as a fallback for commands that do not use optparse-applicative to at least get file completions.
defaultCompleter :: Completer
defaultCompleter = mkCompleter $ \word -> do
-- The implementation here is a variant of optparse-applicative’s `bashCompleter`.
  let cmd = unwords ["compgen", "-o", "bashdefault", "-o", "default", "--", requote word]
  result <- tryIO $ readProcess "bash" ["-c", cmd] ""
  return . lines . fromRight [] $ result

-- | Strongly quote the string we pass to compgen.
--
-- We need to do this so bash doesn't expand out any ~ or other
-- chars we want to complete on, or emit an end of line error
-- when seeking the close to the quote.
--
-- This is copied from Options.Applicative.Builder.Completer which annoyingly does not expose this.
requote :: String -> String
requote s =
  let
    -- Bash doesn't appear to allow "mixed" escaping
    -- in bash completions. So we don't have to really
    -- worry about people swapping between strong and
    -- weak quotes.
    unescaped =
      case s of
        -- It's already strongly quoted, so we
        -- can use it mostly as is, but we must
        -- ensure it's closed off at the end and
        -- there's no single quotes in the
        -- middle which might confuse bash.
        ('\'': rs) -> unescapeN rs

        -- We're weakly quoted.
        ('"': rs)  -> unescapeD rs

        -- We're not quoted at all.
        -- We need to unescape some characters like
        -- spaces and quotation marks.
        elsewise   -> unescapeU elsewise
  in
    strong unescaped

  where
    strong ss = '\'' : foldr go "'" ss
      where
        -- If there's a single quote inside the
        -- command: exit from the strong quote and
        -- emit it the quote escaped, then resume.
        go '\'' t = "'\\''" ++ t
        go h t    = h : t

    -- Unescape a strongly quoted string
    -- We have two recursive functions, as we
    -- can enter and exit the strong escaping.
    unescapeN = goX
      where
        goX ('\'' : xs) = goN xs
        goX (x : xs) = x : goX xs
        goX [] = []

        goN ('\\' : '\'' : xs) = '\'' : goN xs
        goN ('\'' : xs) = goX xs
        goN (x : xs) = x : goN xs
        goN [] = []

    -- Unescape an unquoted string
    unescapeU = goX
      where
        goX [] = []
        goX ('\\' : x : xs) = x : goX xs
        goX (x : xs) = x : goX xs

    -- Unescape a weakly quoted string
    unescapeD = goX
      where
        -- Reached an escape character
        goX ('\\' : x : xs)
          -- If it's true escapable, strip the
          -- slashes, as we're going to strong
          -- escape instead.
          | x `elem` ("$`\"\\\n" :: String) = x : goX xs
          | otherwise = '\\' : x : goX xs
        -- We've ended quoted section, so we
        -- don't recurse on goX, it's done.
        goX ('"' : xs)
          = xs
        -- Not done, but not a special character
        -- just continue the fold.
        goX (x : xs)
          = x : goX xs
        goX []
          = []
