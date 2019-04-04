-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DA.Sdk.Cli.Navigator.Conf
    ( User(..)
    , navigatorConfig
    , partyToUser
    ) where

import           Data.Monoid   ((<>))
import           Data.Maybe    (catMaybes)
import qualified Data.Text     as T
import           DA.Sdk.Pretty ((<+>))
import qualified DA.Sdk.Pretty as P

data User = User
    { userName     :: T.Text
    , userParty    :: T.Text
    , userPassword :: Maybe T.Text
    , userRole     :: Maybe T.Text
    }
    deriving (Show)

bracketed :: P.Doc ann -> [P.Doc ann] -> P.Doc ann
bracketed name cs = case cs of
    []  -> P.nest 2 $ name <+> "{" <+> "}"
    _ -> P.nest 2 $ P.sep
        [ name
        , P.nest 2 $ P.sep
            [ "{"
            , P.vsep $ map (<> ",") cs
            ]
        , "}"
        ]

keyValue :: P.Doc ann -> P.Doc ann -> P.Doc ann
keyValue key value = key <+> "= \"" <> value <> "\""

partyToUser :: T.Text -> User
partyToUser party = User party party Nothing Nothing

-- test :: [User]
-- test =
--     [ User "Alice" "Alice" (Just "password") (Just "bank")
--     , User "Bob" "Bob" (Just "password") (Just "bank")
--     ]

instance P.Pretty User where
    pretty (User name party mbPassword mbRole) = bracketed (P.t name) $ catMaybes
        [ Just $ keyValue "party" (P.pretty party)
        , keyValue "password" . P.pretty <$> mbPassword
        , keyValue "role" . P.pretty <$> mbRole
        ]

navigatorConfig :: [User] -> P.Doc ann
navigatorConfig users = bracketed "users" $ fmap P.pretty users
