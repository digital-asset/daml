-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Cli.Output
  ( writeOutput
  , writeOutputBSL
  , diagnosticsLogger
  , hDiagnosticsLogger
  ) where

import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy                           as BSL
import           Data.String                                    (IsString)
import qualified Data.Text.Encoding as T
import Development.IDE.LSP.Protocol
import Development.IDE.Types.Diagnostics
import Development.IDE.Types.Location
import Language.Haskell.LSP.Messages
import System.IO
import           Control.Exception (bracket)

-- | Write some text to the destination specified on the command line.
--
--   If this was a regular file then write it there,
--   otherwise if the command line specified '-' as the file then write
--   the text to stdout.
writeOutputWith :: (IsString a) => (Handle -> a -> IO ()) -> FilePath -> a -> IO ()
writeOutputWith write outFile output =
    bracket open close $ \handle -> do
      write handle output
      write handle "\n"
  where
    useStdOut = outFile == "-"

    open
      | useStdOut = return stdout
      | otherwise = openFile outFile WriteMode

    close handle
      | useStdOut = return ()
      | otherwise = hClose handle

writeOutput :: FilePath -> String -> IO ()
writeOutput = writeOutputWith hPutStr

writeOutputBSL :: FilePath -> BSL.ByteString -> IO ()
writeOutputBSL = writeOutputWith BSL.hPutStr

-- WARNING: Here be dragons
-- T.putStrLn is locale-dependent. This seems to cause issues with Nix’ patched glibc that
-- relies on LOCALE_ARCHIVE being set correctly. This is the case in our dev-env
-- but not when we ship the SDK. If LOCALE_ARCHIVE is not set properly the colored
-- diagnostics get eaten somewhere in glibc and we don’t even get a write syscall containing them.
printDiagnostics :: Handle -> [FileDiagnostic] -> IO ()
printDiagnostics _ [] = return ()
printDiagnostics handle xs = BS.hPutStrLn handle $ T.encodeUtf8 $ showDiagnosticsColored xs

diagnosticsLogger :: FromServerMessage -> IO ()
diagnosticsLogger = hDiagnosticsLogger stderr

hDiagnosticsLogger :: Handle -> FromServerMessage -> IO ()
hDiagnosticsLogger handle = \case
    EventFileDiagnostics fp diags -> printDiagnostics handle $ map (toNormalizedFilePath' fp,ShowDiag,) diags
    _ -> pure ()
