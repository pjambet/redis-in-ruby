# Rebuilding Redis in Ruby

The hugo code for [Rebuilding Redis in Ruby](https://redis.pjam.me)

# Current status

The project's code follows an iterative approach where each chapter continues where the previous one left off.

The latest completed Chapter is [on Sorted Sets](code/chapter-10/).

# Running locally

You can run the server locally with the following command from inside the corresponding chapter folder: `ruby -r"./server" -e "BYORedis::Server.new"`. By default it runs on port 2000. It starts a Redis Protocol compliant server, which you can interact with any Redis clients, such as `redis-cli`:

``` sh
> redis-cli -p 2000
127.0.0.1:2000> SET a b
OK
127.0.0.1:2000> GET a
"b"
```
