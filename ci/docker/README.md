<!--

This file is meant to be displayed as the description of the
digitalasset/daml-sdk image on Docker Hub. Unfortunately, updating that is a
manual process at the moment. This README is the source of truth and should
overwrite the one on Docker Hub should they differ.

-->

# Dockerized DAML SDK

Digital Asset's [DAML SDK](https://docs.daml.com/) in a can

## Tags

* `${SDK_VERSION}` (_Alpine Linux_)

## Quick start

* Ensure Docker is [installed](https://www.docker.com/get-started)
* Check out existing demo DAML project (or use your own):
```
git clone https://github.com/digital-asset/ex-bond-trading.git
cd ex-bond-trading
```
* Run DAML scenarios:
```
docker run --rm -it -v $PWD:/data digitalasset/daml-sdk:${SDK_VERSION} bash -c "~/.daml/bin/daml test --files /data/src/main/daml/Test.daml"
```

> Note: This image is primarily intended for CI workflows, where the benefits of caching Docker images can outweigh the awkwardness of the above command. For local development, we strongly recommend installing the DAML SDK on the host development machine instead, by running `curl https://get.daml.com | bash`.

## License
View [license information](https://www.apache.org/licenses/LICENSE-2.0) for the software contained in this image.

As with all Docker images, these likely also contain other software which may be under other licenses (such as Bash, etc from the base distribution, along with any direct or indirect dependencies of the primary software being contained).

As for any pre-built image usage, it is the image user's responsibility to ensure that any use of this image complies with any relevant licenses for all software contained within.
