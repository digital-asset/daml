## Introduction to gRPC-Haskell

*This tutorial assumes that you already have a basic understanding of gRPC as well as Haskell.* For an intoduction to the concepts of gRPC, see the [official tutorials](http://www.grpc.io/docs/tutorials/).

This will go through a basic example of using the library, with the `arithmetic` example in the `examples/arithmetic` directory. After cloning this repository, it would be a good idea to run `stack haddock` from within the repository directory to generate the documentation so you can read more about the functions and types we're using as we go. Also remember that [typed holes](https://wiki.haskell.org/GHC/Typed_holes) can be very handy.

To build the examples, you can run

```
$ stack build --flag grpc-haskell:with-examples
```

The gRPC service we will be implementing provides two amazing functions:

1. `Add`, which adds two integers.
2. `RunningSum`, which receives a stream of integers from the client and finally returns a single integer that is the sum of all the integers it has received.

You can run the examples by running `stack exec arithmetic-server` and `stack exec arithmetic-client`.

### Library Organization

**tl;dr: you probably only need to import `Network.GRPC.HighLevel.Generated`.** Other modules are exposed for advanced users only.

This library exposes quite a few modules, but you won't need to worry about most of them. They are currently organized based on the level of abstraction they afford over using the C [gRPC Core library](http://www.grpc.io/grpc/core/) directly:

* *`Unsafe`* modules directly wrap functions in the gRPC Core library. Using them directly is like using C: you need to think about memory management, pointers, and so on. The rest of the library is built on top of these functions and users of gRPC-haskell should never need to deal with the `Unsafe` modules directly.
* *`LowLevel`* modules still require an understanding of the gRPC Core library, but guarantee memory and thread safety. Only advanced users with special requirements would use `LowLevel` modules directly.
* *`HighLevel`* modules give you an opinionated Haskell interface to gRPC that should cover most use cases while (hopefully) being easy to use. You should only need to import the `Network.GRPC.HighLevel.Generated` module to start using the library. If you need to import other modules, we probably forgot to re-export something and you should open an issue or PR.

### Getting started

To start out, we need to generate code for our protocol buffers and RPCs. The `compile-proto-file` command is provided as part of `proto3-suite`. You can either use `stack install` in the `proto3-suite` repository to install the command globally, or use `stack exec` from within the `grpc-haskell` directory.

```
$ stack exec -- compile-proto-file --proto examples/echo/echo.proto > examples/echo/echo-hs/Echo.hs
```

The `.proto` file compiler always names the generated module the same as the `.proto` file, capitalizing the first letter if it is not already. Since our proto file is `arithmetic.proto`, the generated code should be placed in `Arithmetic.hs`.

The important things to notice in this generated file are:

1. For each proto message type, an equivalent Haskell type with the same name has been generated.
2. The `arithmeticServer` function takes a a record containing handlers for each RPC endpoint and some options, and starts a server. So, you just need to call this function to get a server running.
3, The `arithmeticClient` function takes a `Client` (which is just a proof that the gRPC core has been started) and gives you a record of functions that can be used to run RPCs.

### The server

First, we need to turn on some language extensions:

```haskell
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE OverloadedStrings #-}
```

All we need to do to run a server is call the `arithmeticServer` function:

```haskell
main :: IO ()
main = arithmeticServer handlers options
```

So we just need to define `handlers` and `options`.

`options` is easy-- it's just some basic options for the server. We can just use the default options for now, which will start the server listening on `localhost:50051`:

```haskell
options :: ServiceOptions
options = defaultServiceOptions
```

`handlers` is a bit more involved. Its type is `Arithmetic ServerRequest ServerResponse`. Values of this type contain a record field for each RPC defined in your `.proto` file.

```haskell
handlers :: Arithmetic ServerRequest ServerResponse
handlers = Arithmetic { arithmeticAdd = addHandler
                      , arithmeticRunningSum = runningSumHandler
                      }
```

You can think of the handlers as being of type `ServerRequest -> ServerResponse`, though there are a few more type parameters in there. The most important one is the first parameter, which specifies whether the RPC is streaming (`ClientStreaming`, `ServerStreaming`, or `BidiStreaming`) or not (`Normal`).

The `ServerRequest` passed to your handler contains all the tools you will need to handle the request, including:

1. The metadata the client sent with the request.
2. The protocol buffer message sent with the request, which has already been parsed into a Haskell type for you.
3. If it's a streaming request, you will also be given functions for sending or receiving messages in the stream.

#### The unary RPC handler for `Add`

So, let's pattern match on the `ServerRequest` for the `addHandler` function:

```haskell
addHandler (ServerNormalRequest metadata (TwoInts x y)) = -- to be continued!
```

The body of the `addHandler` function just needs to add `x` and `y` and then bundle the answer up in a `ServerResponse`:

```haskell
addHandler (ServerNormalRequest _metadata (TwoInts x y)) = do
  let answer = OneInt (x + y)
  return (ServerNormalResponse answer
                               [("metadata_key_one", "metadata_value")]
                               StatusOk
                               "addition is easy!")
```

Since this is a non-streaming "Normal" RPC, we use the the `ServerNormalResponse` constructor. Its parameters are the response message, some (optional) metadata key-value pairs, a status code, and a string with additional details about the status, which would normally be used to explain any errors in handling the request.

#### The client streaming handler for `RunningSum`

Now let's make our `runningSumHandler`. Since this is an RPC where the server reads from a stream of numbers, we pattern match on the `ServerReaderRequest` constructor:

```haskell
runningSumHandler req@(ServerReaderRequest metadata recv) = -- to be continued!
```

Unlike the unary "Normal" request handler, we don't get a message from the client in this pattern match. Instead, we get an IO action `recv`, which we can run to wait for the client to send us another message.

There are three possibilities when we try to receive another message from the client:

1. The RPC breaks with some gRPC error, such as losing the connection with the client.
2. We receive another message from the client.
3. The client has sent its last message and is waiting for a response.

We write a simple loop that keeps track of the running sum and finally sends off a `ServerReaderResponse` when the client finishes streaming or an error occurs:

```haskell
runningSumHandler req@(ServerReaderRequest metadata recv) =
  loop 0
    where loop !i =
            do msg <- recv
               case msg of
                 Left err -> return (ServerReaderResponse
                                      Nothing
                                      []
                                      StatusUnknown
                                      (fromString (show err)))
                 Right (Just (OneInt x)) -> loop (i + x)
                 Right Nothing -> return (ServerReaderResponse
                                           (Just (OneInt i))
                                           []
                                           StatusOk
                                           "")
```

The `ServerReaderResponse` type is almost the same as `ServerNormalResponse`, except that the first argument, the message to send back to the client, is optional. Otherwise, it takes metadata (which we leave empty), a status code, and a string containing more information about the status code.

### The client

The client-side code generated for us is `arithmeticClient`, which takes a `Client` as input and gives us a record containing actions that execute RPCs. To start up the C gRPC library and get a `Client`, we use `withGRPCClient`, which takes a `ClientConfig`:



```haskell
clientConfig :: ClientConfig
clientConfig = ClientConfig { clientServerHost = "localhost"
                            , clientServerPort = 50051
                            , clientArgs = []
                            , clientSSLConfig = Nothing
                            }

main :: IO ()
main = withGRPCClient clientConfig $ \client -> do
  (Arithmetic arithmeticAdd arithmeticRunningSum) <- arithmeticClient client
  -- to be continued!
```

Now that we are on the client side, the `Arithmetic` record contains functions that make RPC requests. You can think of these functions as roughly having the type `ClientRequest -> ClientResult`. Like before, the particular constructors will vary depending on whether the RPC is streaming or not.

#### Requesting unary RPC

Here we construct a `ClientNormalRequest`, which takes as input a message, a timeout in seconds, and metadata. The result is a `ClientNormalResponse`, containing the server's response, the initial and trailing metadata for the call, and the status and status details string.

```haskell
-- Request for the Add RPC
  ClientNormalResponse (OneInt x) _meta1 _meta2 _status _details
    <- arithmeticAdd (ClientNormalRequest (TwoInts 2 2) 1 [])
  print ("2 + 2 = " ++ (show x))
```

#### Executing a client streaming RPC

Doing a streaming request is slightly trickier. As input to the streaming RPC action, we pass in another IO action that tells `grpc-haskell` what to send. It takes a `send` action as input. This is a bit convoluted, but it guarantees that you can't send streaming messages outside of the context of a streaming call!

```haskell
-- Request for the RunningSum RPC
ClientWriterResponse reply _streamMeta1 _streamMeta2 streamStatus streamDtls
  <- arithmeticRunningSum $ ClientWriterRequest 1 [] $ \send -> do
      eithers <- mapM send [OneInt 1, OneInt 2, OneInt 3]
                   :: IO [Either GRPCIOError ()]
      case sequence eithers of
        Left err -> error ("Error while streaming: " ++ show err)
        Right _ -> return ()
```

Each `send` potentially returns an error message, with the type `Either GRPCIOError ()`. We use `sequence` to run all the `send` actions, and then use `sequence` again to collapse all the `Either`s. If an error is encountered while streaming, there's nothing we can do to salvage the RPC, so a more serious program would need to do some application-specific error-handling. Since this is just a tutorial, we print an error message and exit. Otherwise, we `return ()` to finish sending.

We can now inspect the `reply` to get our answer to the RPC.

```haskell
case reply of
  Just (OneInt y) -> print ("1 + 2 + 3 = " ++ show y)
  Nothing -> putStrLn ("Client stream failed with status "
                       ++ show streamStatus
                       ++ " and details "
                       ++ show streamDtls)
```

To run the examples and see the requests, start the `arithmetic-server` process in the background, and then run the `arithmetic-client` process:

```
$ stack exec -- arithmetic-server &
$ stack exec -- arithmetic-client
```
