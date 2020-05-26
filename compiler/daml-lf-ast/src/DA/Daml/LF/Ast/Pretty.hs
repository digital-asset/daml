-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE FlexibleInstances  #-}
{-# LANGUAGE PatternSynonyms #-}
module DA.Daml.LF.Ast.Pretty(
    (<:>)
    ) where

import qualified Data.Ratio                 as Ratio
import           Control.Lens
import           Control.Lens.Ast   (rightSpine)
import Data.Maybe
import qualified Data.NameMap as NM
import qualified Data.Text          as T
import qualified Data.Time.Clock.POSIX      as Clock.Posix
import qualified Data.Time.Format           as Time.Format
import           Data.Foldable (toList)

import           DA.Daml.LF.Ast.Base hiding (dataCons)
import           DA.Daml.LF.Ast.TypeLevelNat
import           DA.Daml.LF.Ast.Util
import           DA.Daml.LF.Ast.Optics
import           DA.Pretty hiding (keyword_, type_)

infixr 6 <:>
(<:>) :: Doc ann -> Doc ann -> Doc ann
x <:> y = x <-> ":" <-> y

keyword_ :: String -> Doc ann
keyword_ = string

kind_ :: Doc ann -> Doc ann
kind_ = id

type_ :: Doc ann -> Doc ann
type_ = id

prettyDottedName :: [T.Text] -> Doc ann
prettyDottedName = hcat . punctuate "." . map pretty

instance Pretty PackageId where
    pPrint = pretty . unPackageId

instance Pretty ModuleName where
    pPrint = prettyDottedName . unModuleName

instance Pretty TypeSynName where
    pPrint = prettyDottedName . unTypeSynName

instance Pretty TypeConName where
    pPrint = prettyDottedName . unTypeConName

instance Pretty ChoiceName where
    pPrint = pretty . unChoiceName

instance Pretty FieldName where
    pPrint = pretty . unFieldName

instance Pretty VariantConName where
    pPrint = pretty . unVariantConName

instance Pretty TypeVarName where
    pPrint = pretty . unTypeVarName

instance Pretty ExprVarName where
    pPrint = pretty . unExprVarName

instance Pretty ExprValName where
    pPrint = pretty . unExprValName

prettyModuleRef :: (PackageRef, ModuleName) -> Doc ann
prettyModuleRef (pkgRef, modName) = docPkgRef <> pretty modName
  where
    docPkgRef = case pkgRef of
      PRSelf -> empty
      PRImport pkgId -> pretty pkgId <> ":"

instance Pretty a => Pretty (Qualified a) where
    pPrint (Qualified pkgRef modName x) =
        prettyModuleRef (pkgRef, modName) <> ":" <> pretty x

instance Pretty SourceLoc where
  pPrint (SourceLoc mbModRef slin scol elin ecol) =
    hcat
    [ maybe empty (\modRef -> prettyModuleRef modRef <> ":") mbModRef
    , int slin, ":", int scol, "-", int elin, ":", int ecol
    ]

withSourceLoc :: Maybe SourceLoc -> Doc ann -> Doc ann
withSourceLoc mbLoc doc =
  maybe doc (\loc -> "@location" <> parens (pretty loc) $$ doc) mbLoc

precHighest, precKArrow, precTApp, precTFun, precTForall :: Rational
precHighest = 1000  -- NOTE(MH): Used for type applications in 'Expr'.
precKArrow  = 0
precTApp    = 2
precTFun    = 1
precTForall = 0

prettyFunArrow, prettyForall, prettyHasType :: Doc ann
prettyFunArrow = "->"
prettyForall   = "forall"
prettyHasType  = ":"

instance Pretty Kind where
  pPrintPrec lvl prec = \case
    KStar -> "*"
    KNat -> "nat"
    KArrow k1 k2 ->
      maybeParens (prec > precKArrow) $
        pPrintPrec lvl (succ precKArrow) k1 <-> prettyFunArrow <-> pPrintPrec lvl precKArrow k2

-- FIXME(MH): Use typeConAppToType.
instance Pretty TypeConApp where
  pPrintPrec lvl prec (TypeConApp con args) =
    maybeParens (prec > precTApp && not (null args)) $
      pretty con
      <-> hsep (map (pPrintPrec lvl (succ precTApp)) args)

instance Pretty BuiltinType where
  pPrint = \case
    BTInt64          -> "Int64"
    BTDecimal        -> "Decimal"
    BTNumeric -> "Numeric"
    BTText           -> "Text"
    BTTimestamp      -> "Timestamp"
    BTParty          -> "Party"
    BTUnit -> "Unit"
    BTBool -> "Bool"
    BTList -> "List"
    BTUpdate -> "Update"
    BTScenario -> "Scenario"
    BTDate           -> "Date"
    BTContractId -> "ContractId"
    BTOptional -> "Optional"
    BTTextMap -> "TextMap"
    BTGenMap -> "GenMap"
    BTArrow -> "(->)"
    BTAny -> "Any"
    BTTypeRep -> "TypeRep"

prettyRecord :: (Pretty a) =>
  PrettyLevel -> Doc ann -> [(FieldName, a)] -> Doc ann
prettyRecord lvl sept fields =
  braces (sep (punctuate ";" (map prettyField fields)))
  where
    prettyField (name, thing) = hang (pretty name <-> sept) 2 (pPrintPrec lvl 0 thing)

prettyStruct :: (Pretty a) =>
  PrettyLevel -> Doc ann -> [(FieldName, a)] -> Doc ann
prettyStruct lvl sept fields =
  "<" <> sep (punctuate ";" (map prettyField fields)) <> ">"
  where
    prettyField (name, thing) = hang (pretty name <-> sept) 2 (pPrintPrec lvl 0 thing)

instance Pretty Type where
  pPrintPrec lvl prec = \case
    TVar v -> pretty v
    TCon c -> pretty c
    TSynApp s args ->
      maybeParens (prec > precTApp) $
      pretty s <-> hsep [pPrintPrec lvl (succ precTApp) arg | arg <- args ]
    TApp (TApp (TBuiltin BTArrow) tx) ty ->
      maybeParens (prec > precTFun)
        (pPrintPrec lvl (succ precTFun) tx <-> prettyFunArrow <-> pPrintPrec lvl precTFun ty)
    TApp tf ta ->
      maybeParens (prec > precTApp) $
        pPrintPrec lvl precTApp tf <-> pPrintPrec lvl (succ precTApp) ta
    TBuiltin b -> pretty b
    t0@TForall{} ->
      let (vs, t1) = view _TForalls t0
      in  maybeParens (prec > precTForall)
            (prettyForall <-> hsep (map (prettyAndKind lvl) vs) <> "."
             <-> pPrintPrec lvl precTForall t1)
    TStruct fields -> prettyStruct lvl prettyHasType fields
    TNat n -> integer (fromTypeLevelNat n)

precEApp, precEAbs :: Rational
precEApp = 2
precEAbs = 0

prettyLambda, prettyTyLambda, prettyLambdaDot, prettyTyLambdaDot, prettyAltArrow :: Doc ann
prettyLambda      = "\\"
prettyTyLambda    = "/\\"
prettyLambdaDot   = "."
prettyTyLambdaDot = "."
prettyAltArrow    = "->"

instance Pretty PartyLiteral where
  pPrint = quotes . pretty . unPartyLiteral

instance Pretty BuiltinExpr where
  pPrintPrec lvl prec = \case
    BEInt64 n -> pretty (toInteger n)
    BEDecimal dec -> string (show dec)
    BENumeric n -> string (show n)
    BEText t -> string (show t) -- includes the double quotes, and escapes characters
    BEParty p -> pretty p
    BEUnit -> keyword_ "unit"
    BEBool b -> keyword_ $ case b of { False -> "false"; True -> "true" }
    BEError -> "ERROR"
    BEEqualGeneric -> "EQUAL"
    BELessGeneric -> "LESS"
    BELessEqGeneric -> "LESS_EQ"
    BEGreaterGeneric -> "GREATER"
    BEGreaterEqGeneric -> "GREATER_EQ"
    BEEqual t     -> maybeParens (prec > precEApp) ("EQUAL"      <-> prettyBTyArg lvl t)
    BELess t      -> maybeParens (prec > precEApp) ("LESS"       <-> prettyBTyArg lvl t)
    BELessEq t    -> maybeParens (prec > precEApp) ("LESS_EQ"    <-> prettyBTyArg lvl t)
    BEGreater t   -> maybeParens (prec > precEApp) ("GREATER"    <-> prettyBTyArg lvl t)
    BEGreaterEq t -> maybeParens (prec > precEApp) ("GREATER_EQ" <-> prettyBTyArg lvl t)
    BEToText t    -> maybeParens (prec > precEApp) ("TO_TEXT"    <-> prettyBTyArg lvl t)
    BEAddDecimal -> "ADD_DECIMAL"
    BESubDecimal -> "SUB_DECIMAL"
    BEMulDecimal -> "MUL_DECIMAL"
    BEDivDecimal -> "DIV_DECIMAL"
    BERoundDecimal -> "ROUND_DECIMAL"
    BEAddNumeric -> "ADD_NUMERIC"
    BESubNumeric -> "SUB_NUMERIC"
    BEMulNumeric -> "MUL_NUMERIC"
    BEDivNumeric -> "DIV_NUMERIC"
    BERoundNumeric -> "ROUND_NUMERIC"
    BECastNumeric -> "CAST_NUMERIC"
    BEShiftNumeric -> "SHIFT_NUMERIC"
    BEInt64ToNumeric -> "INT64_TO_NUMERIC"
    BENumericToInt64 -> "NUMERIC_TO_INT64"
    BEEqualNumeric -> "EQUAL_NUMERIC"
    BELessEqNumeric -> "LEQ_NUMERIC"
    BELessNumeric -> "LESS_NUMERIC"
    BEGreaterEqNumeric -> "GEQ_NUMERIC"
    BEGreaterNumeric -> "GREATER_NUMERIC"
    BENumericFromText -> "FROM_TEXT_NUMERIC"
    BEToTextNumeric -> "TO_TEXT_NUMERIC"
    BEAddInt64 -> "ADD_INT64"
    BESubInt64 -> "SUB_INT64"
    BEMulInt64 -> "MUL_INT64"
    BEDivInt64 -> "DIV_INT64"
    BEModInt64 -> "MOD_INT64"
    BEExpInt64 -> "EXP_INT64"
    BEFoldl -> "FOLDL"
    BEFoldr -> "FOLDR"
    BETextMapEmpty -> "TEXTMAP_EMPTY"
    BETextMapInsert -> "TEXTMAP_INSERT"
    BETextMapLookup -> "TEXTMAP_LOOKUP"
    BETextMapDelete -> "TEXTMAP_DELETE"
    BETextMapSize -> "TEXTMAP_SIZE"
    BETextMapToList -> "TEXTMAP_TO_LIST"
    BEGenMapEmpty -> "GENMAP_EMPTY"
    BEGenMapInsert -> "GENMAP_INSERT"
    BEGenMapLookup -> "GENMAP_LOOKUP"
    BEGenMapDelete -> "GENMAP_DELETE"
    BEGenMapSize -> "GENMAP_SIZE"
    BEGenMapKeys -> "GENMAP_KEYS"
    BEGenMapValues -> "GENMAP_VALUES"
    BEEqualList -> "EQUAL_LIST"
    BEAppendText -> "APPEND_TEXT"
    BETimestamp ts -> pretty (timestampToText ts)
    BEDate date -> pretty (dateToText date)
    BEInt64ToDecimal -> "INT64_TO_DECIMAL"
    BEDecimalToInt64 -> "DECIMAL_TO_INT64"
    BETimestampToUnixMicroseconds -> "TIMESTAMP_TO_UNIX_MICROSECONDS"
    BEUnixMicrosecondsToTimestamp -> "UNIX_MICROSECONDS_TO_TIMESTAMP"
    BEDateToUnixDays -> "DATE_TO_UNIX_DAYS"
    BEUnixDaysToDate -> "UNIX_DAYS_TO_DATE"
    BEExplodeText -> "EXPLODE_TEXT"
    BEImplodeText -> "IMPLODE_TEXT"
    BESha256Text -> "SHA256_TEXT"
    BETrace -> "TRACE"
    BEEqualContractId -> "EQUAL_CONTRACT_ID"
    BEPartyFromText -> "FROM_TEXT_PARTY"
    BEInt64FromText -> "FROM_TEXT_INT64"
    BEDecimalFromText -> "FROM_TEXT_DECIMAL"
    BEPartyToQuotedText -> "PARTY_TO_QUOTED_TEXT"
    BETextToCodePoints -> "TEXT_TO_CODE_POINTS"
    BETextFromCodePoints -> "TEXT_FROM_CODE_POINTS"
    BECoerceContractId -> "COERCE_CONTRACT_ID"
    BETextToUpper -> "TEXT_TO_UPPER"
    BETextToLower -> "TEXT_TO_LOWER"
    BETextSlice -> "TEXT_SLICE"
    BETextSliceIndex -> "TEXT_SLICE_INDEX"
    BETextContainsOnly -> "TEXT_CONTAINS_ONLY"
    BETextReplicate -> "TEXT_REPLICATE"
    BETextSplitOn -> "TEXT_SPLIT_ON"
    BETextIntercalate -> "TEXT_INTERCALATE"

    where
      epochToText fmt secs =
        T.pack $
        Time.Format.formatTime Time.Format.defaultTimeLocale fmt $
        Clock.Posix.posixSecondsToUTCTime (fromRational secs)

      timestampToText micros =
        epochToText "%0Y-%m-%dT%T%QZ" (toInteger micros Ratio.% (10 ^ (6 :: Integer)))

      dateToText days = epochToText "%0Y-%m-%d" ((toInteger days * 24 * 60 * 60) Ratio.% 1)

prettyAndKind :: Pretty a => PrettyLevel -> (a, Kind) -> Doc ann
prettyAndKind lvl (v, k) = case k of
    KStar -> pPrintPrec lvl 0 v
    _ -> parens (pPrintPrec lvl 0 v <-> prettyHasType <-> kind_ (pPrintPrec lvl 0 k))

prettyAndType :: Pretty a => PrettyLevel -> (a, Type) -> Doc ann
prettyAndType lvl (x, t) = pPrintPrec lvl 0 x <-> prettyHasType <-> type_ (pPrintPrec lvl 0 t)

instance Pretty CasePattern where
  pPrintPrec _lvl _prec = \case
    CPVariant tcon con var ->
      pretty tcon <> ":" <> pretty con
      <-> pretty var
    CPEnum tcon con -> pretty tcon <> ":" <> pretty con
    CPUnit -> keyword_ "unit"
    CPBool b -> keyword_ $ case b of { False -> "false"; True -> "true" }
    CPNil -> keyword_ "nil"
    CPCons hdVar tlVar -> keyword_ "cons" <-> pretty hdVar <-> pretty tlVar
    CPDefault -> keyword_ "default"
    CPNone -> keyword_ "none"
    CPSome bodyVar -> keyword_ "some" <-> pretty bodyVar

instance Pretty CaseAlternative where
  pPrintPrec lvl _prec (CaseAlternative pat expr) =
    hang (pPrintPrec lvl 0 pat <-> prettyAltArrow) 2 (pPrintPrec lvl 0 expr)

instance Pretty Binding where
  pPrintPrec lvl _prec (Binding binder expr) =
    hang (prettyAndType lvl binder <-> "=") 2 (pPrintPrec lvl 0 expr)

prettyTyArg :: PrettyLevel -> Type -> Doc ann
prettyTyArg lvl t = type_ ("@" <> pPrintPrec lvl precHighest t)

prettyBTyArg :: PrettyLevel -> BuiltinType -> Doc ann
prettyBTyArg lvl = prettyTyArg lvl . TBuiltin

prettyTmArg :: PrettyLevel -> Expr -> Doc ann
prettyTmArg lvl = pPrintPrec lvl (succ precEApp)

tplArg :: Qualified TypeConName -> Arg
tplArg tpl = TyArg (TCon tpl)

instance Pretty Arg where
  pPrintPrec lvl _prec = \case
    TmArg e -> prettyTmArg lvl e
    TyArg t -> prettyTyArg lvl t

prettyAppDoc :: PrettyLevel -> Rational -> Doc ann -> [Arg] -> Doc ann
prettyAppDoc lvl prec d as = maybeParens (prec > precEApp) $
  sep (d : map (nest 2 . pPrintPrec lvl 0) as)

prettyAppKeyword :: PrettyLevel -> Rational -> String -> [Arg] -> Doc ann
prettyAppKeyword lvl prec kw = prettyAppDoc lvl prec (keyword_ kw)

prettyApp :: PrettyLevel -> Rational -> Expr -> [Arg] -> Doc ann
prettyApp lvl prec f = prettyAppDoc lvl prec (pPrintPrec lvl precEApp f)

instance Pretty Update where
  pPrintPrec lvl prec = \case
    UPure typ arg ->
      prettyAppKeyword lvl prec "upure" [TyArg typ, TmArg arg]
    upd@UBind{} -> maybeParens (prec > precEAbs) $
      let (binds, body) = view (rightSpine (unlocate $ _EUpdate . _UBind)) (EUpdate upd)
      in  keyword_ "ubind" <-> vcat (map (pPrintPrec lvl 0) binds)
          $$ keyword_ "in" <-> pPrintPrec lvl 0 body
    UCreate tpl arg ->
      prettyAppKeyword lvl prec "create" [tplArg tpl, TmArg arg]
    UExercise tpl choice cid Nothing arg ->
      -- NOTE(MH): Converting the choice name into a variable is a bit of a hack.
      prettyAppKeyword lvl prec "exercise"
      [tplArg tpl, TmArg (EVar (ExprVarName (unChoiceName choice))), TmArg cid, TmArg arg]
    UExercise tpl choice cid (Just actor) arg ->
      -- NOTE(MH): Converting the choice name into a variable is a bit of a hack.
      prettyAppKeyword lvl prec "exercise_with_actors"
      [tplArg tpl, TmArg (EVar (ExprVarName (unChoiceName choice))), TmArg cid, TmArg actor, TmArg arg]
    UFetch tpl cid ->
      prettyAppKeyword lvl prec "fetch" [tplArg tpl, TmArg cid]
    UGetTime ->
      keyword_ "get_time"
    UEmbedExpr typ e ->
      prettyAppKeyword lvl prec "uembed_expr" [TyArg typ, TmArg e]
    UFetchByKey RetrieveByKey{..} ->
      prettyAppKeyword lvl prec "ufetch_by_key" [tplArg retrieveByKeyTemplate, TmArg retrieveByKeyKey]
    ULookupByKey RetrieveByKey{..} ->
      prettyAppKeyword lvl prec "ulookup_by_key" [tplArg retrieveByKeyTemplate, TmArg retrieveByKeyKey]

instance Pretty Scenario where
  pPrintPrec lvl prec = \case
    SPure typ arg ->
      prettyAppKeyword lvl prec "spure" [TyArg typ, TmArg arg]
    scen@SBind{} -> maybeParens (prec > precEAbs) $
      let (binds, body) = view (rightSpine (_EScenario . _SBind)) (EScenario scen)
      in  keyword_ "sbind" <-> vcat (map (pPrintPrec lvl 0) binds)
          $$ keyword_ "in" <-> pPrintPrec lvl 0 body
    SCommit typ actor upd ->
      prettyAppKeyword lvl prec "commit" [TyArg typ, TmArg actor, TmArg upd]
    SMustFailAt typ actor upd ->
      prettyAppKeyword lvl prec "must_fail_at" [TyArg typ, TmArg actor, TmArg upd]
    SPass delta ->
      prettyAppKeyword lvl prec "pass" [TmArg delta]
    SGetTime ->
      keyword_ "get_time"
    SGetParty name ->
      prettyAppKeyword lvl prec "get_party" [TmArg name]
    SEmbedExpr typ e ->
      prettyAppKeyword lvl prec "sembed_expr" [TyArg typ, TmArg e]

instance Pretty Expr where
  pPrintPrec lvl prec = \case
    EVar x -> pretty x
    EVal z -> pretty z
    EBuiltin b -> pPrintPrec lvl prec b
    ERecCon (TypeConApp tcon targs) fields ->
      maybeParens (prec > precEApp) $
        sep $
          pretty tcon
          : map (nest 2 . prettyTyArg lvl) targs
          ++ [nest 2 (prettyRecord lvl "=" fields)]
    ERecProj (TypeConApp tcon targs) field rec ->
      prettyAppDoc lvl prec
        (pretty tcon <> "." <> pretty field)
        (map TyArg targs ++ [TmArg rec])
    ERecUpd (TypeConApp tcon targs) field record update ->
      maybeParens (prec > precEApp) $
        sep $
          pretty tcon
          : map (nest 2 . prettyTyArg lvl) targs
          ++ [nest 2 (braces updDoc)]
      where
        updDoc = sep
          [ pPrintPrec lvl 0 record
          , keyword_ "with"
          , hang (pretty field <-> "=") 2 (pPrintPrec lvl 0 update)
          ]
    EVariantCon (TypeConApp tcon targs) con arg ->
      prettyAppDoc lvl prec
        (pretty tcon <> ":" <> pretty con)
        (map TyArg targs ++ [TmArg arg])
    EEnumCon tcon con ->
      pretty tcon <> ":" <> pretty con
    EStructCon fields ->
      prettyStruct lvl "=" fields
    EStructProj field expr -> pPrintPrec lvl precHighest expr <> "." <> pretty field
    EStructUpd field struct update ->
          "<" <> updDoc <> ">"
      where
        updDoc = sep
          [ pPrintPrec lvl 0 struct
          , keyword_ "with"
          , hang (pretty field <-> "=") 2 (pPrintPrec lvl 0 update)
          ]
    e@ETmApp{} -> uncurry (prettyApp lvl prec) (e ^. _EApps)
    e@ETyApp{} -> uncurry (prettyApp lvl prec) (e ^. _EApps)
    e0@ETmLam{} -> maybeParens (prec > precEAbs) $
      let (bs, e1) = view (rightSpine (unlocate _ETmLam)) e0
      in  hang (prettyLambda <> hsep (map (parens . prettyAndType lvl) bs) <> prettyLambdaDot)
            2 (pPrintPrec lvl 0 e1)
    e0@ETyLam{} -> maybeParens (prec > precEAbs) $
      let (ts, e1) = view (rightSpine (unlocate _ETyLam)) e0
      in  hang (prettyTyLambda <> hsep (map (prettyAndKind lvl) ts) <> prettyTyLambdaDot)
            2 (pPrintPrec lvl 0 e1)
    ECase scrut alts -> maybeParens (prec > precEApp) $
      keyword_ "case" <-> pPrintPrec lvl 0 scrut <-> keyword_ "of"
      $$ nest 2 (vcat (map (pPrintPrec lvl 0) alts))
    e0@ELet{} -> maybeParens (prec > precEAbs) $
      let (binds, e1) = view (rightSpine (unlocate _ELet)) e0
      in  keyword_ "let" <-> vcat (map (pPrintPrec lvl 0) binds)
          $$ keyword_ "in" <-> pPrintPrec lvl 0 e1
    ENil elemType ->
      prettyAppKeyword lvl prec "nil" [TyArg elemType]
    ECons elemType headExpr tailExpr ->
      prettyAppKeyword lvl prec "cons" [TyArg elemType, TmArg headExpr, TmArg tailExpr]
    EUpdate upd -> pPrintPrec lvl prec upd
    EScenario scen -> pPrintPrec lvl prec scen
    ELocation loc x
        | lvl >= PrettyLevel 1 -> prettyAppDoc lvl prec ("@location" <> parens (pretty loc)) [TmArg x]
        | otherwise -> pPrintPrec lvl prec x
    ESome typ body -> prettyAppKeyword lvl prec "some" [TyArg typ, TmArg body]
    ENone typ -> prettyAppKeyword lvl prec "none" [TyArg typ]
    EToAny ty body -> prettyAppKeyword lvl prec "to_any" [TyArg ty, TmArg body]
    EFromAny ty body -> prettyAppKeyword lvl prec "from_any" [TyArg ty, TmArg body]
    ETypeRep ty -> prettyAppKeyword lvl prec "type_rep" [TyArg ty]

instance Pretty DefTypeSyn where
  pPrintPrec lvl _prec (DefTypeSyn mbLoc syn params typ) =
    withSourceLoc mbLoc $ (keyword_ "synonym" <-> lhsDoc) $$ nest 2 (pPrintPrec lvl 0 typ)
    where
      lhsDoc = pretty syn <-> hsep (map (prettyAndKind lvl) params) <-> "="

instance Pretty DefDataType where
  pPrintPrec lvl _prec (DefDataType mbLoc tcon (IsSerializable serializable) params dataCons) =
    withSourceLoc mbLoc $ case dataCons of
    DataRecord fields ->
      hang (keyword_ "record" <-> lhsDoc) 2 (prettyRecord lvl prettyHasType fields)
    DataVariant variants ->
      (keyword_ "variant" <-> lhsDoc) $$ nest 2 (vcat (map prettyVariantCon variants))
    DataEnum enums ->
      (keyword_ "enum" <-> lhsDoc) $$ nest 2 (vcat (map prettyEnumCon enums))
    where
      lhsDoc =
        serializableDoc <-> pretty tcon <-> hsep (map (prettyAndKind lvl) params) <-> "="
      serializableDoc = if serializable then "@serializable" else empty
      prettyVariantCon (name, typ) =
        "|" <-> pretty name <-> pPrintPrec prettyNormal precHighest typ
      prettyEnumCon name = "|" <-> pretty name

instance Pretty DefValue where
  pPrintPrec lvl _prec (DefValue mbLoc binder (HasNoPartyLiterals noParties) (IsTest isTest) body) =
    withSourceLoc mbLoc $
    vcat
      [ hang (keyword_ kind <-> annot <-> prettyAndType lvl binder <-> "=") 2 (pPrintPrec lvl 0 body) ]
    where
      kind = if isTest then "test" else "def"
      annot = if noParties then empty else "@partyliterals"

prettyTemplateChoice ::
  PrettyLevel -> ModuleName -> TypeConName -> TemplateChoice -> Doc ann
prettyTemplateChoice lvl modName tpl (TemplateChoice mbLoc name isConsuming actor selfBinder argBinder retType update) =
  withSourceLoc mbLoc $
    vcat
    [ hsep
      [ keyword_ "choice"
      , keyword_ (if isConsuming then "consuming" else "non-consuming")
      , pretty name
      , parens (prettyAndType lvl (selfBinder, TContractId (TCon (Qualified PRSelf modName tpl))))
      , parens (prettyAndType lvl argBinder), prettyHasType, pPrintPrec lvl 0 retType
      ]
    , nest 2 (keyword_ "by" <-> pPrintPrec lvl 0 actor)
    , nest 2 (keyword_ "to" <-> pPrintPrec lvl 0 update)
    ]

prettyTemplate ::
  PrettyLevel -> ModuleName -> Template -> Doc ann
prettyTemplate lvl modName (Template mbLoc tpl param precond signatories observers agreement choices mbKey) =
  withSourceLoc mbLoc $
    keyword_ "template" <-> pretty tpl <-> pretty param
    <-> keyword_ "where"
    $$ nest 2 (vcat ([signatoriesDoc, observersDoc, precondDoc, agreementDoc, choicesDoc] ++ mbKeyDoc))
    where
      signatoriesDoc = keyword_ "signatory" <-> pPrintPrec lvl 0 signatories
      observersDoc = keyword_ "observer" <-> pPrintPrec lvl 0 observers
      precondDoc = keyword_ "ensure" <-> pPrintPrec lvl 0 precond
      agreementDoc = hang (keyword_ "agreement") 2 (pPrintPrec lvl 0 agreement)
      choicesDoc = vcat (map (prettyTemplateChoice lvl modName tpl) (NM.toList choices))
      mbKeyDoc = toList $ do
        key <- mbKey
        return $ vcat
          [ keyword_ "key" <-> pPrintPrec lvl 0 (tplKeyType key)
          , nest 2 (keyword_ "body" <-> pPrintPrec lvl 0 (tplKeyBody key))
          , nest 2 (keyword_ "maintainers" <-> pPrintPrec lvl 0 (tplKeyMaintainers key))
          ]

prettyFeatureFlags :: FeatureFlags -> Doc ann
prettyFeatureFlags
  FeatureFlags
  { forbidPartyLiterals } =
  fcommasep $ catMaybes
    [ optionalFlag forbidPartyLiterals "+ForbidPartyLiterals"
    ]
  where
    optionalFlag flag name
      | flag = Just name
      | otherwise = Nothing

instance Pretty Module where
  pPrintPrec lvl _prec (Module modName _path flags synonyms dataTypes values templates) =
    vsep $ moduleHeader ++  map (nest 2) defns
    where
      defns = concat
        [ map (pPrintPrec lvl 0) (NM.toList synonyms)
        , map (pPrintPrec lvl 0) (NM.toList dataTypes)
        , map (pPrintPrec lvl 0) (NM.toList values)
        , map (prettyTemplate lvl modName) (NM.toList templates)
        ]
      prettyFlags = prettyFeatureFlags flags
      moduleHeader
        | isEmpty prettyFlags = [keyword_ "module" <-> pretty modName <-> keyword_ "where"]
        | otherwise = [prettyFlags, keyword_ "module" <-> pretty modName <-> keyword_ "where"]

instance Pretty PackageName where
    pPrint = pretty . unPackageName

instance Pretty PackageVersion where
    pPrint = pretty . unPackageVersion

instance Pretty PackageMetadata where
    pPrint (PackageMetadata name version) = pretty name <> "-" <> pretty version

instance Pretty Package where
  pPrintPrec lvl _prec (Package version modules metadata) =
    vcat
      [ "daml-lf" <-> pPrintPrec lvl 0 version
      , "metadata" <-> pPrintPrec lvl 0 metadata
      , vsep $ map (pPrintPrec lvl 0) (NM.toList modules)
      ]
