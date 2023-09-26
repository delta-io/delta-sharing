#!/bin/bash
./gradlew server:imageBuild -Dquarkus.package.type=native -Dquarkus.native.container-build=true -Dquarkus.container-image.name=server-native