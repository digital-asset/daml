# nix-store-gcs-proxy - A HTTP nix store that proxies requests to Google Storage

Nix supports multiple store backends such as file, http, s3, ... but not
Google Storage.

Here we provide a http store backend for nix, that will proxy all the reads
and writes to Google Storage.

## Usage

Make sure to have the google credentials installed in `~/.config/gcloud` or
the `GOOGLE_APPLICATION_CREDENTIALS` environment variable.

Start the server in one terminal: `./nix-store-gcs-proxy --bucket-name
<name-of-your-bucket>`

Then in another terminal, use `nix copy --to http://localhost:3000?secret-key=path/to/secret.key <INSTALLABLE>`. Eg:

```sh
$ nix-store --generate-binary-cache-key cache1.example.org cache.key cache.pub
$ nix copy --to http://localhost:3000?secret-key=$PWD/cache.key nixpkgs.hello
```

## TODO

* Section that explains how to setup GCS with the LB CDN.
