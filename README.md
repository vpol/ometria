```

cd cmd/importer

Minimal runnable (once)
go build && ./importer -mailchimp_key <api_key> -mailchimp_list_ids <comma_separated_ids> -ometria_key <api_key>

Run as a daemon with 1h interval
go build && ./importer -mailchimp_key <api_key> -mailchimp_list_ids <comma_separated_ids> -ometria_key <api_key> -importer_mode daemon -scheduler_period 60m
```

Ideas:
- Moving average of updated number to control the sequential vs fan-out requests
- Do not accumulate updates. Pass data via channel. Still track the latest across all updates. 200byte record * 10mln - 2Gb (realistically 4-5)

 