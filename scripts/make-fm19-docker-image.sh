#!/usr/bin/env bash

check_image_available=$(docker image ls | egrep 'filemakerServer19\s+latest')

if [ -z "" ]; then
    echo "Docker image 'filemakerServer19:latest' not found. Starting to build it. This may take a while ..."
    cd /tmp/
    git clone --depth 1 https://github.com/steamedeo/filemaker-docker.git
    cd filemaker-docker
    docker build -t filemakerServer19 .
else
    echo "Docker image 'filemakerServer19:latest' found locally."
fi
