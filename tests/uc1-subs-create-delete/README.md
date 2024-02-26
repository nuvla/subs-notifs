## UC1 crate and delete subscription

Goal: User creates and deletes subscription. The `subs-notifs` microservice adds
and then removes the subscription.

### Run the test

A random user creates a random subscription with ID 
`subscription-config/create-delete-01`.

```bash
export PYTHONPATH=~/SixSq/code/nuvla/notifications/code:$PYTHONPATH
./gen-subs-create-delete.py 
```

### Validate results

Check in Kafka. 

```bash
kafka-console-consumer --bootstrap-server localhost:9092 --from-beginning \
    --property print.timestamp=true --property print.key=true \
    --property print.value=true --topic subscription-config
    
CreateTime:1662984748983        subscription-config/create-delete-01    {"id": "subscription-config/create-delete-01", ...}
CreateTime:1662984748984        subscription-config/create-delete-01    null
```

Check logs of `subs-notifs`. It should display a list of current subscriptions
first containing the subscription ID and then without.

```shell
2022-09-12 12:12:28,989 - main - 1 - DEBUG - current keys:
2022-09-12 12:12:28,990 - main - 1 - DEBUG -    nuvlabox: ['subscription-config/create-delete-01']
2022-09-12 12:12:29,989 - main - 1 - DEBUG - current keys:
2022-09-12 12:12:29,990 - main - 1 - DEBUG -    nuvlabox: []
```
