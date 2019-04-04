#!/usr/bin/env bash
openssl req -newkey rsa:2048 -nodes -x509 -days 36500 -keyout localhost.key -out localhost.crt
openssl x509 -in localhost.crt -text -noout
