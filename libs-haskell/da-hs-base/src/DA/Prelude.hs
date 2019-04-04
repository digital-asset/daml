-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE TemplateHaskell #-}

-- | Custom Prelude without partial functions and a more extensive default
-- export list.
module DA.Prelude
  (
    -- * Prelude without partial functions
    module Prelude

    -- ** Total variants of partial functions

    -- TODO (SM): switch to 'located-base' for the 'xxxNote' versions
  , atMay        , atDef        , atNote
  , tailMay      , tailDef      , tailNote
  , initMay      , initDef      , initNote
  , headMay      , headDef      , headNote
  , lastMay      , lastDef      , lastNote
  , minimumMay   , minimumDef   , minimumNote
  , maximumMay   , maximumDef   , maximumNote
  , minimumByMay , minimumByDef , minimumByNote
  , maximumByMay , maximumByDef , maximumByNote
  , foldr1May    , foldr1Def    , foldr1Note
  , foldl1May'   , foldl1Def'   , foldl1Note'
  , scanl1May    , scanl1Def    , scanl1Note
  , scanr1May    , scanr1Def    , scanr1Note
  , assertNote
  , readMay      , readDef      , readNote
                 , fromJustDef  , fromJustNote

    -- * Common exports from Control.*
  , module Control.Applicative
  , module Control.Monad
  , liftIO

    -- * Common exports from Data.*
  , module Data.Char
  , module Data.Function
  , module Data.Int
  , module Data.List
  , module Data.List.Extra
  , module Data.Maybe
  , module Data.Ord
  , module Data.Proxy
  , module Data.Tuple
  , module Data.Void
  , module Data.Word

  , partitionEithers
  , singleton

  , Data
  , Typeable
  , Hashable

    -- ** Tagged values
  , Tagged(Tagged, unTagged)
  , retag
  , untag

    -- ** GHC Generics support
  , Generic

    -- ** Make default instances
  , concatSequenceA
  , makeInstances
  , makeInstancesAltJson
  , makeInstancesExcept
  , makeConstrainedInstances

  , makeUnderscoreLenses
  ) where




-- orphan instances (all of them must be brought into scope here)
-----------------------------------------------------------------

import Orphans.Lib_aeson ()
import Orphans.Lib_binary ()
import Orphans.Lib_hashable ()


-- normal imports
-----------------

import Control.Applicative
import Control.Exception
import Control.Lens (set)
import Control.Monad
import Control.Monad.IO.Class (liftIO)

import Data.Char
import Data.Data     (Data)
import Data.Typeable (Typeable)
import Data.Either   (partitionEithers)

import Data.Function
import Data.Int
import Data.Hashable (Hashable)
import Data.List hiding
  (
    -- hide partial functions
    minimumBy
  , maximumBy
  , foldl1
  , foldl1'

    -- foldl and nub are almost always the wrong choice performance-wise.
  , foldl
  , nub
  )
import Data.List.Extra (nubOrd)
import Data.Maybe


import Data.Ord
import Data.Proxy
import Data.Tagged          (Tagged(Tagged, unTagged), retag, untag)
import Data.Tuple
import Data.Void
import Data.Word

import GHC.Generics (Generic)

import Prelude hiding
  (
    -- hide partial functions
    tail
  , init
  , head
  , last
  , minimum
  , maximum
  , foldr1
  , scanl1
  , scanr1
  , read

    -- foldl is almost always the wrong choice performance-wise.
  , foldl
  )

-- Total variants of partial functions. See above for the collection.
import Safe


-- qualified imports
--------------------

import qualified "template-haskell" Language.Haskell.TH        as TH
import qualified Control.Lens.TH            as Lens.TH
import qualified Data.Aeson                 as Aeson
import qualified Data.Aeson.TH.Extended     as Aeson.TH
import qualified Data.Binary                as Binary


------------------------------------------------------------------------------
-- Support for fewer TH splices (speeds up GHCJS compilation)
------------------------------------------------------------------------------

-- | This function is mainly intended to be used to combine many TH-splices
-- into a single one, as we pay a per-splice cost of starting a node-js
-- executable and loading all libraries when compiling TH with GHCJS.
concatSequenceA :: (Applicative f) => [f [a]] -> f [a]
concatSequenceA = fmap concat . sequenceA


------------------------------------------------------------------------------
-- Support for simplified instance declarations via Template Haskell
------------------------------------------------------------------------------

-- NOTE (SM): this code is an experiment that aims to provide a single place
-- for declaring what instances our datatypes support by default.


-- | One TH line of deriving our usual instances (ToJSON, FromJSON, Generic, Eq, Show, Ord)
makeInstances :: TH.Name -> TH.Q [TH.Dec]
makeInstances = makeInstancesExcept []

makeInstancesAltJson :: String -> TH.Name -> TH.Q [TH.Dec]
makeInstancesAltJson prefix name =
  concatSequenceA
  [ makeConstrainedInstancesExcept [] [''Aeson.ToJSON, ''Aeson.FromJSON] name
  , Aeson.TH.deriveDAJSON prefix name
  ]

makeInstancesExcept :: [TH.Name] -> TH.Name -> TH.Q [TH.Dec]
makeInstancesExcept = makeConstrainedInstancesExcept []

makeConstrainedInstances :: [TH.Name] -> TH.Name -> TH.Q [TH.Dec]
makeConstrainedInstances constraints = makeConstrainedInstancesExcept constraints []

makeConstrainedInstancesExcept :: [TH.Name] -> [TH.Name] -> TH.Name -> TH.Q [TH.Dec]
makeConstrainedInstancesExcept constraints exceptions name = do
    info <- TH.reify name
    case info of
      TH.TyConI (TH.DataD    [] _name []   _mbKind _constructors _deriving) -> arg0
      TH.TyConI (TH.NewtypeD [] _name []   _mbKind _constructors _deriving) -> arg0
      TH.TyConI (TH.DataD    [] _name vars _mbKind _constructors _deriving) -> argN $ length vars
      TH.TyConI (TH.NewtypeD [] _name vars _mbKind _constructors _deriving) -> argN $ length vars

      info' -> do
        TH.reportError $ "makeInstances can't currently handle type definition like this: " ++ show info'
        return []
  where
    except :: TH.Name -> b -> [b]
    except x b = if x `elem` exceptions then [] else [ b ]

    appConT = TH.appT . TH.conT

    -- no type arguments, e.g.:
    -- instance FromJSON X
    -- instance ToJSON X
    arg0 :: TH.Q [TH.Dec]
    arg0 = do
      let genD :: TH.Name -> TH.Q [TH.Dec]
          genD t = return <$>
              TH.standaloneDerivD
                   (return [])                          -- no predicates
                   (appConT t (TH.conT name))

      let genI :: TH.Name -> TH.Q [TH.Dec]
          genI t = return <$>
              TH.instanceD
                   (return [])                          -- no predicates
                   (appConT t (TH.conT name)) -- ToJSON X
                   []                                   -- no implementation, default is good

      fmap concat $ sequence $ concat
        [ except ''Binary.Binary $ genI ''Binary.Binary
        , except ''Eq $ genD ''Eq
        , except ''Ord $ genD ''Ord
        , except ''Show $ genD ''Show
        , except ''Data $ genD ''Data
        , except ''Generic $ genD ''Generic
        , except ''Aeson.ToJSON $ Aeson.TH.deriveDAToJSON "" name
        , except ''Aeson.FromJSON $ Aeson.TH.deriveDAFromJSON "" name
        ]

    -- specific number of type arguments, e.g. argN 2:
    -- instance (FromJSON a, FromJSON b) => FromJSON (X a b)
    argN :: Int -> TH.Q [TH.Dec]
    argN nArgs = do
        fmap concat $ sequence $ concat
          [ except ''Eq             $ genD (''Eq : constraints) ''Eq
          , except ''Ord            $ genD (''Ord : constraints) ''Ord
          , except ''Show           $ genD (''Show : constraints) ''Show
          , except ''Data           $ genD (''Data : constraints) ''Data
          , except ''Generic        $ genD [] ''Generic
          , except ''Binary.Binary  $ genI (''Binary.Binary : constraints) ''Binary.Binary
          , except ''Aeson.ToJSON toJSON
          , except ''Aeson.FromJSON fromJSON
          ]
      where
        -- Use deriveDAToJSON/FromJSON when possible
        toJSON
          | null constraints = Aeson.TH.deriveDAToJSON "" name
          | otherwise        = genI (''Aeson.ToJSON : constraints) ''Aeson.ToJSON

        fromJSON = if null constraints
          then Aeson.TH.deriveDAFromJSON "" name
          else genI (''Aeson.FromJSON : constraints) ''Aeson.FromJSON

        genD :: [TH.Name] -> TH.Name -> TH.Q [TH.Dec]
        genD cs0 class_ = return <$>
            do
              let cs = nubOrd cs0
              argNames <- mapM (\n -> TH.newName ("arg" <> show n)) [1..nArgs]
              TH.standaloneDerivD
                (mapM (\(var, c) -> appConT c (TH.varT var)) [(x, y) | x <- argNames, y <- cs ])
                (appConT class_ $ foldl' TH.appT (TH.conT name) $ map TH.varT argNames)

        genI :: [TH.Name] -> TH.Name -> TH.Q [TH.Dec]
        genI cs0 t = return <$>
            do
              let cs = nubOrd cs0
              argNames <- mapM (\n -> TH.newName ("arg" <> show n)) [1..nArgs]
              TH.instanceD
                (mapM (\(var, c) -> appConT c (TH.varT var)) [(x, y) | x <- argNames, y <- cs ])
                (appConT t $ foldl' TH.appT (TH.conT name) $ map TH.varT argNames)
                [] -- no implementation, default is good

-- | Alternative name for 'pure' to make the intention of creating a singleton
-- container clearer at call sites. It can be used to create, e.g., singleton
-- lists or singleton 'Data.NonEmpty.NonEmpty' lists.
singleton :: Applicative f => a -> f a
singleton = pure

-- | Generate a lens for every field in a record. The name of the lens is the
-- name of the field prefixed by an underscore. For instance, for
--
-- > data Foo = Foo{bar :: Int, _baz :: Bool}
--
-- it will generate
--
-- > _bar :: Lens' Foo Int
-- > __baz :: Lens' Foo Bool
makeUnderscoreLenses :: TH.Name -> TH.DecsQ
makeUnderscoreLenses =
  Lens.TH.makeLensesWith (set Lens.TH.lensField noUnderscoreNoPrefixNamer Lens.TH.lensRules)
  where
    noUnderscoreNoPrefixNamer _ _ n = [Lens.TH.TopName (TH.mkName ('_':TH.nameBase n))]
