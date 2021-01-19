FROM adoptopenjdk:8-jdk-hotspot
ARG VERSION
# This is needed to get the DNS requests
# from Haskell binaries to succeed.
# Otherwise they fail to even resolve localhost.
RUN echo 'hosts: files dns' > /etc/nsswitch.conf
RUN addgroup --system daml && adduser --system --ingroup daml daml

# To allow user daml to issue sudo commands
RUN apt-get update || apt-get update
RUN apt-get -y install sudo
RUN echo 'daml ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

USER daml
RUN curl https://get.daml.com | sh -s $VERSION \
    && printf "auto-install: false\nupdate-check: never\n" >> /home/daml/.daml/daml-config.yaml

ENV PATH="/home/daml/.daml/bin:${PATH}"
WORKDIR /home/daml
