#!/usr/bin/env bash

./gen-subs-config-load.py
sleep 1
./gen-ne-load-m1.py
sleep 1
./gen-ne-load-m2.py
sleep 1
./gen-ne-load-m3.py
sleep 1
./gen-ne-load-m4.py
sleep 1
./gen-subs-config-load-delete.py
