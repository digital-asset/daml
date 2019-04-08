{-# LANGUAGE ViewPatterns #-}

module Metadata(
    readMetadata, Metadata(..)
    ) where

import System.IO.Extra
import Data.Tuple.Extra
import Data.Char
import System.FilePath
import Data.List.Extra


data Metadata = Da_haskell_library
    {dhl_dir :: FilePath
    ,dhl_name :: String
    ,dhl_src_strip_prefix :: String
    ,dhl_srcs :: [String]
    ,dhl_deps :: [String]
    ,dhl_hazel_deps :: [String]
    } deriving Show

readMetadata :: FilePath -> IO [Metadata]
readMetadata file = do
    src <- readFile' file
    return $ map (\x -> x{dhl_dir = takeDirectory file}) $ search $ lexPython src

lexPython :: String -> [String]
lexPython (dropWhile isSpace -> ('\'':xs)) | (inner,'\'':xs) <- break (== '\'') xs = ("\"" ++ inner ++ "\"") : lexPython xs
lexPython x = case lex x of
    [("#",x)] -> lexPython $ drop 1 $ dropWhile (/= '\n') x
    [("","")] -> []
    [(x,y)] -> x : lexPython y
    [] -> []

search :: [String] -> [Metadata]
search ("da_haskell_library":xs)
    | Just (fields, rest) <- paren xs
    = f (Da_haskell_library [] [] [] [] [] []) fields : search rest
    where
        f r ("name":"=":name:xs) = f r{dhl_name = read name} xs
        f r ("src_strip_prefix":"=":name:xs) = f r{dhl_src_strip_prefix = read name} xs
        f r ("srcs":"=":"glob":"(":"[":glob:"]":")":xs) = f r{dhl_srcs = [read glob]} xs
        f r ("srcs":"=":"native":".":"glob":"(":"[":glob:"]":")":xs) = f r{dhl_srcs = [read glob]} xs
        f r ("hazel_deps":"=":(square -> Just (names, xs))) = f r{dhl_hazel_deps = map read $ filter (/= ",") names} xs
        f r ("deps":"=":(square -> Just (names, xs))) = f r{dhl_deps = delete "" $ map (last . wordsBy (`elem` "/:") . read) $ filter (/= ",") names} xs
        f r (x:xs) = f r xs
        f r [] = r
search (x:xs) = search xs
search [] = []


paren = bracketed "(" ")"
square = bracketed "[" "]"

bracketed :: String -> String -> [String] -> Maybe ([String], [String])
bracketed open close (x:xs) | x == open = f 1 xs
    where
        f _ [] = Nothing
        f 1 (x:xs) | x == close = Just ([], xs)
        f i (x:xs) = first (x:) <$> f i2 xs
            where i2 = if x == close then i-1 else if x == open then i+1 else i
bracketed _ _ _ = Nothing


{-
da_haskell_library (
    name = "daml-lf-proto",
    src_strip_prefix = "src",
    srcs = glob (["src/**/*.hs"]),
    extra_srcs = ["//daml-lf/archive:da/daml_lf_dev.proto"],
    deps = [
        "//compiler/daml-lf-ast",
        "//libs-haskell/da-hs-base",
        "//libs-haskell/da-hs-pretty",
        "//daml-lf/archive:daml_lf_haskell_proto",
        "//nix/third-party/proto3-suite:proto3-suite",
    ],
    hazel_deps = [
        "base",
        "bytestring",
        "containers",
        "cryptonite",
        "either",
        "lens",
        "memory",
        "scientific",
        "template-haskell",
        "text",
        "vector",
    ],
    visibility = ["//visibility:public"],
)
-}
