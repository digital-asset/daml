-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Proto3.WellInterned (
  module DA.Daml.LF.Proto3.WellInterned
  ) where

import           Data.Data
import           Data.Generics.Aliases
import qualified Data.Vector as Hs (all)

import           DA.Daml.LF.Proto3.DerivingData           ()

import qualified Com.Digitalasset.Daml.Lf.Archive.DamlLf2 as P

------------------------------------------------------------------------
-- Property definition
------------------------------------------------------------------------

{-
Assumptions:

- The AST the Com.Digitalasset.Daml.Lf.Archive.DamlLf2 is _at least as deeply
  nested_ than the actual protobuff spec, so therefore, the height of the
  Com.Digitalasset.Daml.Lf.Archive.DamlLf2 AST is an overestimation of the
  protobuff spec's height. This is justified, for example, by the fact that
  Com.Digitalasset.Daml.Lf.Archive.DamlLf2 encodes a sum type by two nodes: one
  newtype that captures the message's name, which contains another type suffixed
  with Sum that contains the actual sum. Therefore, if the
  Com.Digitalasset.Daml.Lf.Archive.DamlLf2 AST is flat enough, it for sure is in
  the protobuff spec.
-}

data ConcreteConstrCount = ConcreteConstrCount
    { kinds :: Int
    , types :: Int
    , exprs :: Int
    }
    deriving Show

instance Semigroup ConcreteConstrCount where
    (ConcreteConstrCount k1 t1 e1) <> (ConcreteConstrCount k2 t2 e2) =
        ConcreteConstrCount (k1 + k2) (t1 + t2) (e1 + e2)

instance Monoid ConcreteConstrCount where
    mempty = ConcreteConstrCount 0 0 0

singleKind, singleType, singleExpr :: ConcreteConstrCount
singleKind = mempty{kinds = 1}
singleType = mempty{types = 1}
singleExpr = mempty{exprs = 1}

{-
Count the number of concrete constructors for kinds, types, and expressions.
This code would be a lot simpler if we would simply return a bool or another
pass/failiure (such as Either), simply asserting at the point of a concrete
constructor that children do not contain concrete constructors. However, giving
the counts is a bit more inforamative for debugging, and this code will change
anyways if and when we allow for trees of size n
-}
countConcreteConstrs :: Data a => a -> ConcreteConstrCount
countConcreteConstrs =
  let
    countKinds (k :: P.KindSum) = case k of
      (P.KindSumInternedKind _) -> mempty
      _                         -> singleKind <> mconcat (gmapQ countConcreteConstrs k)

    countTypes (t :: P.TypeSum) = case t of
      (P.TypeSumInternedType _) -> mempty
      _                         -> singleType <> mconcat (gmapQ countConcreteConstrs t)

    countExprs (e :: P.ExprSum) = case e of
      (P.ExprSumInternedExpr _) -> mempty
      _                         -> singleExpr <> mconcat (gmapQ countConcreteConstrs e)

    genericCase x = mconcat (gmapQ countConcreteConstrs x)

  in genericCase `extQ` countKinds `extQ` countTypes `extQ` countExprs

checkWellInterned :: ConcreteConstrCount -> Bool
checkWellInterned ConcreteConstrCount{..} =
    kinds + types + exprs <= 1

wellInterned :: Data a => a -> Bool
wellInterned = checkWellInterned . countConcreteConstrs

packageWellInternedBools :: P.Package -> [Bool]
packageWellInternedBools P.Package{..} =
  [ wellInterned packageModules
  , wellInterned packageInternedStrings
  , wellInterned packageInternedDottedNames
  , wellInterned packageMetadata
  , Hs.all wellInterned packageInternedTypes
  , Hs.all wellInterned packageInternedKinds
  , Hs.all wellInterned packageInternedExprs
  , wellInterned packageImportsSum
  ]

packageWellInterned :: P.Package -> Either String ()
packageWellInterned p =
  if b then Right () else Left $ "Package not well interned, results: " ++ show bs
    where
      bs = packageWellInternedBools p
      b = and bs
