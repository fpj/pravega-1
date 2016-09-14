#!/bin/bash

cd /opt/streaming  && ./gradlew startServer 
  
cd /opt/controller && ./bin/server 
