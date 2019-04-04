-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}
module DA.Daml.LF.Simplifier(
    freeVarsStep,
    simplifyModule,
    ) where

import Control.Lens hiding (para)
import Data.Functor.Foldable
import qualified Data.Set as Set
import qualified Safe
import qualified Safe.Exact as Safe
import DA.Daml.LF.Decimal (mkDecimal, Decimal)
import DA.Daml.LF.Decimal as Decimal
import Control.Monad (guard)
import Data.Fixed (Fixed(..))

import DA.Prelude
import DA.Daml.LF.Ast
import DA.Daml.LF.Ast.Optics
import DA.Daml.LF.Ast.Recursive

type VarSet = Set.Set ExprVarName

-- | Models an approximation of the error safety of an expression. 'Unsafe'
-- means the expression might throw an error. @'Safe' /n/@ means that the
-- expression won't throw an error unless you apply it to at least /n/
-- arguments.
data Safety
  = Unsafe
  | Safe Int  -- number is >= 0
  deriving (Eq, Ord)

data Info = Info
  { freeVars :: VarSet
  , safety   :: Safety
  }

-- | @'cata' freeVarsStep@ maps an 'Expr' to its free term variables.
freeVarsStep :: ExprF VarSet -> VarSet
freeVarsStep = \case
  EVarF x -> Set.singleton x
  EValF _ -> mempty
  EBuiltinF _ -> mempty
  ERecConF _ fs -> foldMap snd fs
  ERecProjF _ _ s -> s
  ERecUpdF _ _ s1 s2 -> s1 <> s2
  EVariantConF _ _ s -> s
  ETupleConF fs -> foldMap snd fs
  ETupleProjF _ s -> s
  ETupleUpdF _ s1 s2 -> s1 <> s2
  ETmAppF s1 s2 -> s1 <> s2
  ETyAppF s _ -> s
  ETmLamF (x, _) s -> x `Set.delete` s
  ETyLamF _ s -> s
  ECaseF s as -> s <> foldMap snd as
  ELetF b s -> fvBinding b s
  ENilF _ -> mempty
  EConsF _ s1 s2 -> s1 <> s2
  ENoneF _ -> mempty
  ESomeF _ s -> s
  EUpdateF u ->
    case u of
      UPureF _ s -> s
      UBindF b s -> fvBinding b s
      UCreateF _ s -> s
      UExerciseF _ _ s1 s2 s3 -> s1 <> s2 <> s3
      UFetchF _ s1 -> s1
      UGetTimeF -> mempty
      UEmbedExprF _ s -> s
      UFetchByKeyF rbk -> retrieveByKeyFKey rbk
      ULookupByKeyF rbk -> retrieveByKeyFKey rbk
  EScenarioF e ->
    case e of
      SPureF _ s -> s
      SBindF b s -> fvBinding b s
      SCommitF _ s1 s2 -> s1 <> s2
      SMustFailAtF _ s1 s2 -> s1 <> s2
      SPassF s -> s
      SGetTimeF -> mempty
      SGetPartyF s -> s
      SEmbedExprF _ s -> s
  ELocationF _ e -> e
  where
    fvBinding :: BindingF VarSet -> VarSet -> VarSet
    fvBinding (BindingF (x, _) s1) s2 = s1 <> (x `Set.delete` s2)

decrSafety :: Safety -> Safety
decrSafety = \case
  Unsafe        -> Unsafe
  Safe n
    | n > 0     -> Safe (n-1)
    | otherwise -> Unsafe

incrSafety :: Safety -> Safety
incrSafety = \case
  Unsafe -> Safe 0
  Safe n -> Safe (n+1)

-- | @'cata' safetyStep@ approximates the safety of an expression.
safetyStep :: ExprF Safety -> Safety
safetyStep = \case
  EVarF _ -> Safe 0
  EValF _ -> Unsafe
  EBuiltinF b ->
    case b of
      BEInt64 _           -> Safe 0
      BEDecimal _         -> Safe 0
      BEText _            -> Safe 0
      BETimestamp _       -> Safe 0
      BEParty _           -> Safe 0
      BEDate _            -> Safe 0
      BEEnumCon _         -> Safe 0
      BEError             -> Safe 0
      BEEqual _           -> Safe 2
      BELess _            -> Safe 2
      BELessEq _          -> Safe 2
      BEGreaterEq _       -> Safe 2
      BEGreater _         -> Safe 2
      BEToText _          -> Safe 1
      BEAddDecimal        -> Safe 1
      BESubDecimal        -> Safe 1
      BEMulDecimal        -> Safe 1
      BEDivDecimal        -> Safe 1
      BERoundDecimal      -> Safe 1
      BEAddInt64          -> Safe 1
      BESubInt64          -> Safe 1
      BEMulInt64          -> Safe 1
      BEDivInt64          -> Safe 1
      BEModInt64          -> Safe 1
      BEExpInt64          -> Safe 1
      BEInt64ToDecimal    -> Safe 1
      BEDecimalToInt64    -> Safe 0 -- crash if the decimal doesn't fit
      BEFoldl             -> Safe 2
      BEFoldr             -> Safe 2
      BEMapEmpty          -> Safe 1
      BEMapInsert         -> Safe 3
      BEMapLookup         -> Safe 2
      BEMapDelete         -> Safe 2
      BEMapToList         -> Safe 1
      BEMapSize           -> Safe 1
      BEEqualList         -> Safe 2 -- expects 3, 2-safe
      BEExplodeText       -> Safe 1
      BEImplodeText       -> Safe 1
      BESha256Text        -> Safe 1
      BEAppendText        -> Safe 2
      BETimestampToUnixMicroseconds -> Safe 1
      BEUnixMicrosecondsToTimestamp -> Safe 0 -- can fail if the int represents an out-of-bounds date
      BEDateToUnixDays -> Safe 1
      BEUnixDaysToDate -> Safe 0 -- can fail if the int represents an out-of-bounds date
      BETrace -> Unsafe -- we make it unsafe so that it never gets erased
      BEEqualContractId -> Safe 2
      BEPartyToQuotedText -> Safe 1
      BEPartyFromText -> Safe 1
  ERecConF _ fs -> minimum (Safe 0 : map snd fs)
  ERecProjF _ _ s -> s `min` Safe 0
  ERecUpdF _ _ s1 s2 -> s1 `min` s2 `min` Safe 0
  EVariantConF _ _ s -> s `min` Safe 0
  ETupleConF fs -> minimum (Safe 0 : map snd fs)
  ETupleProjF _ s -> s `min` Safe 0
  ETupleUpdF _ s1 s2 -> s1 `min` s2 `min` Safe 0
  ETmAppF s1 s2 ->
    case s2 of
      Unsafe -> Unsafe
      Safe _ -> decrSafety s1
  ETyAppF s _ -> s
  ETmLamF _ s -> incrSafety s
  ETyLamF _ s -> s
  ECaseF s1 as
    | Safe _ <- s1 -> Safe.minimumDef Unsafe (map snd as)
    | otherwise    -> Unsafe
  ELetF (BindingF _ s1) s2
    | Safe _ <- s1 -> s2
    | otherwise    -> Unsafe
  ENilF _ -> Safe 0
  EConsF _ s1 s2
    | Safe _ <- s1, Safe _ <- s2 -> Safe 0
    | otherwise                  -> Unsafe
  -- NOTE(MH): Updates and scenarios do probably not appear in positions related
  -- to the record boilerplate. If this changes, we need to revisit the next two
  -- cases.
  EUpdateF _ -> Unsafe
  EScenarioF _ -> Unsafe
  ELocationF _ s -> s
  ENoneF _ -> Safe 0
  ESomeF _ s
    | Safe _ <- s -> Safe 0
    | otherwise   -> Unsafe

infoStep :: ExprF Info -> Info
infoStep e = Info (freeVarsStep (fmap freeVars e)) (safetyStep (fmap safety e))

simplifyExpr :: Expr -> Expr
simplifyExpr = fst . cata go
  where
    go :: ExprF (Expr, Info) -> (Expr, Info)
    go = \case
      -- <...; f = e; ...>.f    ==>    e
      ETupleProjF f (ETupleCon fes, s)
        -- NOTE(MH): We're deliberately overapproximating the potential of
        -- bottoms and the set of free variables below to avoid recomputing
        -- them.
        | Safe _ <- safety s, Just e <- f `lookup` fes -> (e, s)

      -- let x = e in x    ==>    e
      ELetF (BindingF (x, _) e) (EVar x', _)
        | x == x' -> e

      -- let x = x in e    ==>    e
      ELetF (BindingF (x, _) (EVar x', _)) e
        | x == x' -> e

      -- let x = <...; f = e; ...> in x.f    ==>    e
      ELetF (BindingF (x, _) (ETupleCon fes, s)) (ETupleProj f (EVar x'), _)
        -- NOTE(MH): See NOTE above on @s@.
        | x == x', Safe _ <- safety s, Just e <- f `lookup` fes -> (e, s)

      -- let x = <f1 = e1; ...; fn = en> in T {f1 = x.f1; ...; fn = x.fn}
      -- ==>
      -- T {f1 = e1; ...; fn = en}
      ELetF (BindingF (x1, _) (ETupleCon fes1, s)) (ERecCon t fes2, _)
        | Just bs <- Safe.zipWithExactMay matchField fes1 fes2
        , and bs ->
            (ERecCon t fes1, s)
        where
          matchField (f1, _) (f2, e2)
            | f1 == f2, ETupleProj f3 (EVar x3) <- e2, f1 == f3, x1 == x3 = True
            | otherwise = False

      -- let x = e1 in e2    ==>    e2, if e1 cannot be bottom and x is not free in e2
      ELetF (BindingF (x, _) e1) e2
        | Safe _ <- safety (snd e1)
        , x `Set.notMember` freeVars (snd e2) -> e2

      -- (let x = e1 in e2).f    ==>    let x = e1 in e2.f
      -- NOTE(MH): The reason for the choice of `s1` and `s2` is as follows:
      -- - If `fv(let x = e1 in e2) ⊆ V`, then `fv(e1) ⊆ V` and
      --   `fv(e2) ⊆ V ∪ {x}`.
      -- - If `let x = e1 in e2` is k-safe, then `e1` is 0-safe and `e2` is
      --   k-safe.
      ETupleProjF f (ELet (Binding (x, t) e1) e2, Info fv sf) ->
        go $ ELetF (BindingF (x, t) (e1, s1)) (go $ ETupleProjF f (e2, s2))
        where
          s1 = Info fv (sf `min` Safe 0)
          s2 = Info (Set.insert x fv) sf

      -- constant folding of some binary expressions. particularly useful since
      -- we forbid top-level definitions which are not values, but we want to allow
      -- top level definitions which like @1 + 2 + 3@.
      --
      -- see 'DA.Daml.LF.TypeChecker.Value' for more info on the check mentioned above.
      --
      -- we give up on anything ill typed, since we haven't type checked yet here.
      e0@(ETmAppF (ETmApp (EBuiltin bltin) arg1, _) (arg2, _)) -> do
        let giveUp = (embed (fmap fst e0), infoStep (fmap snd e0))
        let expectInt64 = \case
              EBuiltin (BEInt64 i) -> Just i
              _ -> Nothing
        let expectDecimal = \case
              EBuiltin (BEDecimal i) -> Just i
              _ -> Nothing
        -- primitive values have no free variables and the take no arguments.
        let primitiveInfo = Info mempty (Safe 0)
        let binaryDecimal f checkSecondArg = maybe giveUp ((, primitiveInfo) . EBuiltin . BEDecimal) $ do
              dec1 <- expectDecimal arg1
              dec2 <- expectDecimal arg2 >>= checkSecondArg
              f dec1 dec2
        let binaryInt64 f checkSecondArg = maybe giveUp ((, primitiveInfo) . EBuiltin . BEInt64) $ do
              int1 <- expectInt64 arg1
              int2 <- expectInt64 arg2 >>= checkSecondArg
              f int1 int2
        let dontCheckSecondArg arg = pure arg
        case bltin of
          BEAddDecimal -> binaryDecimal (checkDecimal (+)) dontCheckSecondArg
          BESubDecimal -> binaryDecimal (checkDecimal (-)) dontCheckSecondArg
          BEMulDecimal -> binaryDecimal (checkDecimal (*)) dontCheckSecondArg
          BEAddInt64 -> binaryInt64 (checkInt64 (+)) dontCheckSecondArg
          BESubInt64 -> binaryInt64 (checkInt64 (-)) dontCheckSecondArg
          BEMulInt64 -> binaryInt64 (checkInt64 (*)) dontCheckSecondArg
          _ -> giveUp

      -- e    ==>    e
      e -> (embed (fmap fst e), infoStep (fmap snd e))

    fixed10ToDecimal (MkFixed x) = mkDecimal x 10

    fixed10FromDecimal = either (const Nothing) Just . legacyDecimalToDamlLfDecimal

    checkInt64 :: (Integer -> Integer -> Integer) -> Int64 -> Int64 -> Maybe Int64
    checkInt64 f (toInteger -> a) (toInteger -> b) = do
      let res = f a b
      guard (res >= toInteger (minBound :: Int64) && res <= toInteger (maxBound :: Int64))
      return (fromInteger res)

    checkDecimal :: (Decimal -> Decimal -> Decimal) -> Fixed E10 -> Fixed E10 -> Maybe (Fixed E10)
    checkDecimal f (fixed10ToDecimal -> a) (fixed10ToDecimal -> b) = fixed10FromDecimal (f a b)

{-
-- this floats out outermost lets in their own definitions.
--
-- e.g.
--
-- @
-- def foo: Integer = let x: Integer = <x-body> in let y: Integer = <y-body> in <foo-body>
-- @
--
-- becomes
--
-- @
-- def foo$x: Integer = <x-body>
-- def foo$y: Integer = <y-body> -- with x substituted with foo$x
-- def foo: Integer = <foo-body> -- with x substituted with foo$x and y substituted with foo$y
-- @
--
-- we do this because we force all top-level definitions to be values,
-- which is quite annoying in conjunction with lets, since
--
-- @
-- def bar: Int = do
--   let z = <z-body>
--   <bar-body>
-- @
--
-- is not a value even if <bar-body> is.
floatOutOutermostLets ::
     ModuleName -- ^ only used to generate names for the generated defs
  -> DefValue
  -> [DefValue]
floatOutOutermostLets modName def0@(DefValue loc (defName, _) noPartyLits _isTest body mbInfo) =
  case mbInfo of
    Nothing -> maybe [def0] (\(defs, body') -> def0{dvalBody = body'} : defs) (go mempty loc body)
    Just{} -> error ("floatOutOutermostLets: trying to process DefValue " ++ show defName ++ " with info -- please fill in the infos _after_ this function")
  where
    -- we store the current mapping to newly created values to avoid
    -- quadratic behavior if we were to replace at each step. likewise,
    -- we also store directly the Expr in the HashMap rather than
    -- ExprValName, also to avoid retraversing all of the hash map at
    -- each step.
    --
    -- however this means that we don't work if we encounter two lets
    -- with the same name. right now we just give up. TODO(FM) do not
    -- give up.
    --
    -- that's what the 'Maybe' is for -- if it's 'Nothing' it means
    -- that we've given up.
    go :: HMS.HashMap ExprVarName Expr -> Maybe SourceLoc -> Expr -> Maybe ([DefValue], Expr)
    go replaced loc0 = \case
      ELet (Binding (varName, typ) body0) rest0 -> do
        guard (not (HMS.member varName replaced))
        let body1 = substVars replaced body0
        let valName = Tagged (untag defName <> "$" <> untag varName)
        let def = DefValue loc0 (valName, typ) noPartyLits (IsTest False) body1 Nothing
        let qualifiedValName = Qualified PRSelf modName valName
        (defs, rest) <- go (HMS.insert varName (EVal qualifiedValName) replaced) loc0 rest0
        return (def : defs, rest)
      ELocation loc1 e -> go replaced (Just loc1) e
      e -> return ([], substVars replaced e)
-}

simplifyModule :: Module -> Module
simplifyModule = over moduleExpr simplifyExpr

legacyDecimalToDamlLfDecimal :: Decimal -> Either String (Fixed E10)
legacyDecimalToDamlLfDecimal scaled = do
  let scaledShown = review Decimal.stringToDecimal scaled
  let unscaled = scaled * 10^(10 :: Integer)
  let unscaledRounded = toIntegerD unscaled
  if unscaled /= fromIntegerD unscaledRounded
    then Left $
      "Decimal does not fit in precision 38, scale 10, because we'd lose decimal digits: " ++ scaledShown
    else do
      if abs unscaledRounded >= 10^(38::Integer)
        then Left ("Decimal overflows precision 38: " ++ scaledShown)
        else pure (MkFixed unscaledRounded)
