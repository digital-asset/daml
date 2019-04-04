-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Sdk.Data.Either.Extra
    ( mapLeft
    , fmapLeft
    , swapLefts
    , joinLefts
    , getLeft
    ) where

mapLeft :: (a -> b) -> Either a c -> Either b c
mapLeft f = either (Left . f) Right

fmapLeft :: Functor f => (a -> b) -> f (Either a c) -> f (Either b c)
fmapLeft f = fmap (mapLeft f)

swapLefts :: Either a (Either b c) -> Either b (Either a c)
swapLefts = either (Right . Left) (fmap Right)

joinLefts :: Either a (Either a b) -> Either a b
joinLefts = either Left (either Left Right)

getLeft :: Either a b -> Maybe a
getLeft (Left l) = Just l
getLeft _right   = Nothing
