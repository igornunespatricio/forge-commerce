#!/bin/bash

# fix permissions
chown -R spark:spark /home/spark || true

# start jupyter
exec "$@"