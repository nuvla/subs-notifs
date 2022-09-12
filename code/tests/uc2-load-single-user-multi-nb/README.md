## UC1 load: single user multi NE

Goal: User 

### Create subscriptions

User `user/01` is subscribed to CPU load `>` 90% on NEs with tag `x86-64`.

Two NEs are sending telemetry: `ne/01` and `ne/02`. Both NEs are owned by the
user. Only `ne/02` has tag `x86-64`.

Result: Produce subscriptions to `subscription-config` topic:

```bash
export PYTHONPATH=~/SixSq/code/nuvla/notifications/code/src:$PYTHONPATH
./gen-subs-config-load.py 
```

Check:

```bash
kafka-console-consumer --bootstrap-server localhost:9092 --from-beginning \
    --property print.timestamp=true --property print.key=true \
    --property print.value=true --topic subscription-config
```

### Send telemetry and check results

Eight telemetry messages are sent to `NB_TELEM_RESOURCES_REKYED_S` topic with
the following expected results in the `notifications` topic.

To check the results:
```shell
 kafka-console-consumer --bootstrap-server localhost:9092 --from-beginning \
     --property print.timestamp=true --property print.key=true \
     --property print.value=true --topic notifications 
```

To run scripts below, export:
```bash
export PYTHONPATH=~/SixSq/code/nuvla/notifications/code/src:$PYTHONPATH
```

1. Input: `ne/01` and `ne/02` load is below 90%. Output: nothing in `notifications`
   ```bash
   ./gen-nb-load-m1.py
   ```
2. Input: `ne/01` and `ne/02` load moves above 90%. Output: notification on `ne/02`
   for `user/01` in `notifications`
   ```bash
   ./gen-nb-load-m2.py
   ```
3. Input: `ne/01` and `ne/02` load is above 90%. Output: nothing in `notifications`
   ```bash
   ./gen-nb-load-m3.py
   ```
4. Input: `ne/01` and `ne/02` load moves below 90%. Output: recover message on `ne/02`
   for `user/01` in `notifications`
   ```bash
   ./gen-nb-load-m4.py
   ```
   
### Delete subscription

```bash
export PYTHONPATH=~/SixSq/code/nuvla/notifications/code/src:$PYTHONPATH
./gen-subs-config-load-delete.py 
```
