#!/usr/bin/env bash
sbt assembly
./sync_machine.sh bricks02
