## UC3 load: multi user multi NE

Goal: Subscriptions are not overwritten and both users receive notifications on
the telemetry changes of the same NB.

### Create subscriptions

User `user/01` and `user/02` are subscribed to RAM `>` 90% on NEs with tag 
`x86-64`.

Two NEs are sending telemetry: `ne/01` and `ne/02`. Both NEs are owned by the
users. Only `ne/02` has tag `x86-64`.

Produce subscriptions to `subscription-config` topic:

```bash
export PYTHONPATH=~/SixSq/code/nuvla/notifications/code/src:$PYTHONPATH
./gen-subs-config-ram.py 
```

Check:

```bash
kafka-console-consumer --bootstrap-server localhost:9092 --from-beginning \
    --property print.timestamp=true --property print.key=true \
    --property print.value=true --topic subscription-config
```

### Send telemetry and check results

Eight telemetry messages are sent to `NE_TELEM_RESOURCES_REKYED_S` topic with
the following expected results in the `NOTIFICATION_S` topic.

To check the results:
```shell
 kafka-console-consumer --bootstrap-server localhost:9092 --from-beginning \
     --property print.timestamp=true --property print.key=true \
     --property print.value=true --topic NOTIFICATION_S 
```

To run scripts below, export:
```bash
export PYTHONPATH=~/SixSq/code/nuvla/notifications/code/src:$PYTHONPATH
```

1. Input: `ne/01` and `ne/02` RAM is below 90%. Output: nothing in `NOTIFICATION_S`
   ```bash
   ./gen-ne-ram-m1.py
   ```
2. Input: `ne/01` and `ne/02` RAM moves above 90%. Output: notification on `ne/02`
   for `user/02` and `user/02` in `NOTIFICATION_S`
   ```bash
   ./gen-ne-ram-m2.py
   ```
3. Input: `ne/01` and `ne/02` RAM is above 90%. Output: nothing in `NOTIFICATION_S`
   ```bash
   ./gen-ne-ram-m3.py
   ```
4. Input: `ne/01` and `ne/02` RAM moves below 90%. Output: recover message on `ne/02`
   for `user/02` and `user/02` in `NOTIFICATION_S`
   ```bash
   ./gen-ne-ram-m4.py
   ```
### Delete subscriptions

```bash
export PYTHONPATH=~/SixSq/code/nuvla/notifications/code/src:$PYTHONPATH
./gen-subs-config-ram-delete.py 
```
