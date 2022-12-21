## UC4: NE online/offline

Goal: NE online/offline state changes are covered by the notifications.

### Create subscriptions

User `user/01` is subscribed to NE online/offline on NEs with tag `x86-64`.

Two NE is sending telemetry: `ne/01`.

Produce subscriptions to `subscription-config` topic:

```bash
export PYTHONPATH=~/SixSq/code/nuvla/notifications/code/src:$PYTHONPATH
./gen-subs-config-onoff.py 
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

1. Input: `ne/01` ONLINE and ONLINE_PREV are True. Output: nothing in `NOTIFICATION_S`
   ```bash
   ./gen-ne-onoff-m1.py
   ```
2. Input: `ne/01` ONLINE is False and ONLINE_PREV is True. Output: notification on `ne/01`
   for `user/01` with `recovery` False in `NOTIFICATION_S`
   ```bash
   ./gen-ne-onoff-m2.py
   ```
3. Input: `ne/01` ONLINE and ONLINE_PREV are False. Output: nothing in `NOTIFICATION_S`
   ```bash
   ./gen-ne-onoff-m3.py
   ```
4. Input: `ne/01` ONLINE is True and ONLINE_PREV is False. Output: notification on `ne/01`
   for `user/01` with `recovery` True in `NOTIFICATION_S`
   ```bash
   ./gen-ne-onoff-m4.py
   ```
### Delete subscriptions

```bash
export PYTHONPATH=~/SixSq/code/nuvla/notifications/code/src:$PYTHONPATH
./gen-subs-config-onoff-delete.py 
```
