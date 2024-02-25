#!/usr/bin/env bash

./gen-subs-config-ram.py
sleep 1
./gen-ne-ram-m1.py
sleep 1
./gen-ne-ram-m2.py
sleep 1
./gen-ne-ram-m3.py
sleep 1
./gen-ne-ram-m4.py
sleep 1
./gen-subs-config-ram-delete.py
