<!--

This file is meant to be displayed as the description of the
digitalasset/daml-sdk image on Docker Hub. Unfortunately, updating that is a
manual process at the moment. This README is the source of truth and should
overwrite the one on Docker Hub should they differ.

-->

# Dockerized Daml SDK

> This image is not supported for production use-cases. Please contact Digital
> Asset to obtain supported production-ready artifacts.

Digital Asset's [Daml SDK](https://docs.daml.com/) in a can.

## Tags

* `${SDK_VERSION}`

> Starting with tag 1.7.0, these images are signed.

## Verifying signatures

You can configure your Docker client to only pull & run signed Docker images by
setting the `DOCKER_CONTENT_TRUST` environment variable to 1.

This, however, only checks that the image is signed. If you want to further
check the provenance of the signature, you can use `docker trust inspect
--pretty digitalasset/daml-sdk:$TAG`; you should see a signer called
`automation` with the key
`533a6e09faa512f974f217668580da1ceb6aa5b00aad34ea1240afc7d249703f` and a
repository key of
`f5dc2aee6aed2d05d7eda75db7aa2b3fac7fc67afbb880d03535d5a5295a0d3b`.

## Quick start

* Ensure Docker is [installed](https://www.docker.com/get-started)
* Check out existing demo Daml project (or use your own):
  ```
  git clone https://github.com/digital-asset/ex-bond-trading.git
  cd ex-bond-trading
  ```
* Run Daml scenarios:
  ```
  DOCKER_CONTENT_TRUST=1 docker run --rm -it -v $PWD:/data digitalasset/daml-sdk:$SDK_VERSION bash -c "cd \$(mktemp -d) && cp -r /data/* ./ && DAML_SDK_VERSION=$SDK_VERSION daml test"
  ```

> Note: This image is primarily intended for CI workflows, where the benefits
> of caching Docker images can outweigh the awkwardness of the above command.
> For local development, we strongly recommend installing the Daml SDK on the
> host development machine instead, by running `curl https://get.daml.com |
> bash`. For production use-cases, we strongly recommend using a supported
> production binary, which can be obtained by contacting Digital Asset.

## License

View [license information](https://www.apache.org/licenses/LICENSE-2.0) for the
software contained in this image.

As with all Docker images, these likely also contain other software which may
be under other licenses (such as Bash, etc from the base distribution, along
with any direct or indirect dependencies of the primary software being
contained).

As for any pre-built image usage, it is the image user's responsibility to
ensure that any use of this image complies with any relevant licenses for all
software contained within.
