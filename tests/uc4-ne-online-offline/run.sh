#!/usr/bin/env bash

./gen-subs-config-onoff.py
sleep 1
./gen-ne-onoff-m1.py
sleep 1
./gen-ne-onoff-m2.py
sleep 1
./gen-ne-onoff-m3.py
sleep 1
./gen-ne-onoff-m4.py
sleep 1
./gen-subs-config-onoff-delete.py
