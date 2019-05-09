-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DAML.Assistant.Util
    ( module DAML.Assistant.Util
    , fromRightM
    , fromMaybeM
    ) where

import DAML.Assistant.Types
import DAML.Project.Util
import System.FilePath
import Control.Exception.Safe
import Control.Applicative
import Control.Monad.Extra hiding (fromMaybeM)

-- | Calculate the ascendants of a path, i.e. the successive parents of a path,
-- including the path itself, all the way to its root. For example:
--
--     ascendants "/foo/bar/baz" == ["/foo/bar/baz", "/foo/bar", "/foo", "/"]
--     ascendants "~/foo/bar/baz" == ["~/foo/bar/baz", "~/foo/bar", "~/foo", "~"]
--     ascendants "./foo/bar/baz" == ["./foo/bar/baz", "./foo/bar", "./foo", "."]
--     ascendants "../foo/bar/baz" == ["../foo/bar/baz", "../foo/bar", "../foo", ".."]
--     ascendants "foo/bar/baz"  == ["foo/bar/baz", "foo/bar", "foo", "."]
--
ascendants :: FilePath -> [FilePath]
ascendants "" = ["."]
ascendants "~" = ["~"]
ascendants ".." = [".."]
ascendants p =
    let p' = takeDirectory (dropTrailingPathSeparator p)
        ps = if p == p' then [] else ascendants p'
    in p : ps

-- | Throw an assistant error.
throwErr :: Text -> IO a
throwErr msg = throwIO (assistantError msg)

-- | Handle IOExceptions by wrapping them in an AssistantError, and
-- add context to any assistant errors that are missing context.
wrapErr :: Text -> (IO a -> IO a)
wrapErr ctx = handleIO (throwIO . wrapIOException)
            . handle   (throwIO . addErrorContext)

    where
        wrapIOException :: IOException -> AssistantError
        wrapIOException ioErr =
            AssistantError
                { errContext  = Just ctx
                , errMessage  = Nothing
                , errInternal = Just (pack (show ioErr))
                }

        addErrorContext :: AssistantError -> AssistantError
        addErrorContext err =
            err { errContext = errContext err <|> Just ctx }


-- | Throw an assistant error if the passed value is Nothing.
-- Otherwise return the underlying value.
--
--     required msg Nothing == throwErr msg
--     required msg . Just  == pure
--
required :: Text -> Maybe t -> IO t
required msg = maybe (throwErr msg) pure

-- | Same as 'required' but operates over Either values.
--
--     requiredE msg (Left e) = throwErr (msg <> "\nInternal error: " <> show e)
--     requiredE msg . Right  = pure
--
requiredE :: Exception e => Text -> Either e t -> IO t
requiredE msg = fromRightM (throwIO . assistantErrorBecause msg . pack . displayException)

-- | Catch IOExceptions and re-throw them as AssistantError with a helpful message.
requiredIO :: Text -> IO t -> IO t
requiredIO msg m = catchIO m (rethrow msg)

-- | Same as requiredIO but also catches and re-throws any synchronous exception.
requiredAny :: Text -> IO a -> IO a
requiredAny msg m = m `catches`
    [ Handler $ \ (e :: AssistantError) -> throwIO e
    , Handler $ \ (e :: SomeException) -> rethrow msg e
    ]

-- | Rethrow an exception with helpful message.
rethrow :: Exception e => Text -> e -> IO a
rethrow msg = throwIO . assistantErrorBecause msg . pack . displayException

-- | Like 'whenMaybeM' but only returns a 'Just' value if the test is false.
unlessMaybeM :: Monad m => m Bool -> m t -> m (Maybe t)
unlessMaybeM = whenMaybeM . fmap not
