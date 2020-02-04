-- Copyright (c) 2020 The DAML Authors. All rights reserved.
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
import Options.Applicative.Extended
import System.Environment
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
    (Dispatch info . UserCommandArgs <$>
        many (strArgument (metavar "ARGS" <> completer defaultCompleter)))

commandParser :: [SdkCommandInfo] -> Parser Command
commandParser cmds | (hidden, visible) <- partition isHidden cmds = asum
    [ subparser -- visible commands
        $  builtin "version" "Display DAML version information" mempty (Version <$> versionParser <**> helper)
        <> builtin "install" "Install the specified DAML SDK version" mempty (Install <$> installParser <**> helper)
        <> builtin "uninstall" "Uninstall the specified DAML SDK version" mempty (Uninstall <$> uninstallParser <**> helper)
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
    <*> flagYesNoAuto "assistant" False "Display DAML assistant version." idm

installParser :: Parser InstallOptions
installParser = InstallOptions
    <$> optional (RawInstallTarget <$> argument str (metavar "TARGET" <> completeWith ["latest"] <> help "The SDK version to install. Use 'latest' to download and install the latest stable SDK version available. Run 'daml install' to see the full set of options."))
    <*> (InstallAssistant <$> flagYesNoAuto' "install-assistant" "Install associated DAML assistant version. Can be set to \"yes\" (always installs), \"no\" (never installs), or \"auto\" (installs if newer). Default is \"auto\"." idm)
    <*> iflag ActivateInstall "activate" hidden "Activate installed version of daml"
    <*> iflag ForceInstall "force" (short 'f') "Overwrite existing installation"
    <*> iflag QuietInstall "quiet" (short 'q') "Don't display installation messages"
    <*> fmap SetPath (flagYesNoAuto "set-path" True "Adjust PATH automatically. This option only has an effect on Windows." idm)
    <*> fmap BashCompletions (flagYesNoAuto' "bash-completions" "Install bash completions for DAML assistant. Default is yes for linux and mac, no for windows." idm)
    <*> fmap ZshCompletions (flagYesNoAuto' "zsh-completions" "Install Zsh completions for DAML assistant. Default is yes for linux and mac, no for windows." idm)
    where
        iflag p name opts desc = fmap p (switch (long name <> help desc <> opts))

uninstallParser :: Parser SdkVersion
uninstallParser =
    argument readSdkVersion (metavar "VERSION" <> help "The SDK version to uninstall.")

readSdkVersion :: ReadM SdkVersion
readSdkVersion =
    eitherReader (mapLeft displayException . parseVersion . pack)

-- | Completer that uses the builtin bash completion.
-- We use this to ensure that `daml build -o foo` will still complete to `daml build -o foobar.dar`.
defaultCompleter :: Completer
defaultCompleter = mkCompleter $ \word -> do
-- The implementation here is a variant of optparse-applicative’s `bashCompleter`.
  let cmd = unwords ["compgen", "-o", "bashdefault", "-o", "default", "--", requote word]
  result <- tryIO $ readProcess "bash" ["-c", cmd] ""
  return . lines . either (const []) id $ result

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
