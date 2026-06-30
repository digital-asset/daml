-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE GADTs #-}
module DA.Daml.Compiler.Output
  ( writeOutput
  , writeOutputBSL
  , diagnosticsLogger
  , hDiagnosticsLogger
  , bufferingDiagnosticsLogger
  , flushDiagnostics
  , printDiagnostics
  , printDiagnosticsSimple
  ) where

import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy                           as BSL
import           Data.IORef
import           Data.String                                    (IsString)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Development.IDE.Core.Shake (NotificationHandler(..))
import Development.IDE.Types.Diagnostics
import Development.IDE.Types.Location
import qualified Language.LSP.Types as LSP
import System.Directory (makeAbsolute)
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
printDiagnostics handle xs = do
    xs' <- mapM makeAbsoluteDiag xs
    BS.hPutStrLn handle $ TE.encodeUtf8 $ showDiagnosticsColored xs'

makeAbsoluteDiag :: FileDiagnostic -> IO FileDiagnostic
makeAbsoluteDiag (fp, showDiag, diag) = do
    absPath <- makeAbsolute (fromNormalizedFilePath fp)
    pure (toNormalizedFilePath' absPath, showDiag, diag)

-- | Print diagnostics in a simplified format showing only file path and message.
-- Output format: "Failure in: <path>\nMessage: <message>"
printDiagnosticsSimple :: Handle -> [FileDiagnostic] -> IO ()
printDiagnosticsSimple _ [] = return ()
printDiagnosticsSimple handle xs = do
    xs' <- mapM makeAbsoluteDiag xs
    mapM_ (printSimpleDiag handle) xs'

printSimpleDiag :: Handle -> FileDiagnostic -> IO ()
printSimpleDiag handle (fp, _, LSP.Diagnostic{_message = msg}) = do
    let filePath = fromNormalizedFilePath fp
    BS.hPutStrLn handle $ TE.encodeUtf8 $ T.pack $ "Failure in: " <> filePath
    BS.hPutStrLn handle $ TE.encodeUtf8 $ "Message:\n" <> msg

diagnosticsLogger :: NotificationHandler
diagnosticsLogger = hDiagnosticsLogger stderr

hDiagnosticsLogger :: Handle -> NotificationHandler
hDiagnosticsLogger handle = NotificationHandler $ \m params -> case (m, params) of
    (LSP.STextDocumentPublishDiagnostics, LSP.PublishDiagnosticsParams (uriToFilePath' -> Just fp) _ (List diags)) ->
        printDiagnostics handle $ map (toNormalizedFilePath' fp,ShowDiag,) diags
    _ -> pure ()

bufferingDiagnosticsLogger :: IORef [FileDiagnostic] -> NotificationHandler
bufferingDiagnosticsLogger ref = NotificationHandler $ \m params -> case (m, params) of
    (LSP.STextDocumentPublishDiagnostics, LSP.PublishDiagnosticsParams (uriToFilePath' -> Just fp) _ (List diags)) ->
        modifyIORef ref (++ map (toNormalizedFilePath' fp, ShowDiag,) diags)
    _ -> pure ()

flushDiagnostics :: IORef [FileDiagnostic] -> IO ()
flushDiagnostics ref = do
    diags <- readIORef ref
    printDiagnosticsSimple stderr diags
