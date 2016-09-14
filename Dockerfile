FROM java:8

MAINTAINER Arvind Kandhare [arvind.kandhare@emc.com]

COPY git-repo/master-pravega/ /opt/streaming/


COPY  git-repo/controller-pravega/controller/server/build/install/server/  /opt/controller/

WORKDIR /opt/streaming/



RUN  ./gradlew jar

WORKDIR ../..

COPY ./docker_entrypoint.sh  /

ENTRYPOINT [ "./docker_entrypoint.sh" ]
