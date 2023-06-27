#!/usr/bin/env bash

VERSION=18


check_image_available=$(docker image ls | egrep 'filemakerServer1'$VERSION'\s+latest')

if [ -z "" ]; then
    echo "Docker image 'filemakerServer$VERSION:latest' not found. Starting to build it. This may take a while ..."
    cd /tmp/
    git clone --depth 1 https://github.com/steamedeo/filemaker-docker.git
    cd filemaker-docker
    docker build -t filemakerServer$VERSION .
else
    echo "Docker image 'filemakerServer$VERSION:latest' found locally."
fi
