---
title: "Chapter 4 - Adding the missing options to SET"
date: 2020-07-23T10:27:27-04:00
lastmod: 2020-07-23T10:27:27-04:00
draft: true
keywords: []
description: "In this chapter we add the missing features to the SET command we implemented in chapter 2, EX, PX, NX, XX & KEEPTTL"
comment: false
---

## What we'll cover

We implemented a simplified version of `SET` in [Chapter 2][chapter-2], in this chapter, we will complete the command, and implement all [its options][redis-doc-set]. Note that we're still not following the [Redis Protocol][redis-protocol], we will do that in a later chapter. Doing so will require some significant refactoring,

## Planning our changes

The [`SET`][redis-doc-set] commands accepts the following options:


- EX seconds -- Set the specified expire time, in seconds.
- PX milliseconds -- Set the specified expire time, in milliseconds.
- NX -- Only set the key if it does not already exist.
- XX -- Only set the key if it already exist.
- KEEPTTL -- Retain the time to live associated with the key.

As noted in the documentation, there is some overlap with some of the options above and the following commands: [SETNX][redis-doc-setnx], [SETEX][redis-doc-setex], [PSETEX][redis-doc-psetex]. As of this writing these three commands are not officially deprecated, but the documentation mentions that they might soon. Given that we can access the same features through the `NX`, `EX` & `PX` options respectively, we will not implement these three commands.

`SET a-key a-value EX 10` is equivalent to `SETEX a-key 10 a-value`, which you can demonstrate in `redis-cli` session:

```
127.0.0.1:6379> SET a-key a-value EX 10
OK
127.0.0.1:6379> TTL "a-key"
(integer) 8
127.0.0.1:6379> SETEX a-key 10 a-value
OK
127.0.0.1:6379> TTL "a-key"
(integer) 8
```

[`TTL`][redis-doc-ttl] returned 8 in both cases, because it took me about 2s to type the `TTL` command, and by that time about 8s where left, of the initial 10.

### The EX & PX options

We will first add support for the EX & PX options, both are followed by one argument, an integer for the number of the seconds and milliseconds specifying how long the key will be readable for. Once the duration has elapsed, the key is deleted, and calling `GET` would return `(nil)` as if it was never set.

When a key is set with either of these two options, or through the `SETEX` & `PSETEX` commands, but we are ignoring these for the sake of simplicity, Redis adds the key to different dictionary, `db->expires`. This dictionary is dedicated to storing keys with a TTL, the key is the same key and the value is the timestamp of the expiration, in milliseconds.

Redis uses two approaches, in two different places, to delete keys with a TTL. The first one is the lazy approach, when reading a key, it checks if it has a ttl, if it does and it is in the past, it deletes the key and does not proceed with the read.

The other one is a more proactive approach, redis periodically scans the list of keys with a ttl value and deletes the expired one. This action, performed in `serverCron` is part of the [event loop][redis-doc-event-loop]. The event loop is defined in `ae.c` and starts in the `aeMain` function, it continuously executes the `aeProcessEvents` function in a while loop, until the `stop` flag is set to `1`, which essentially never happens when the server is running under normal circumstances. The `aeStop` function is the only function doing this and it is only used in `redis-benchmark.c` & `example-ae.c`.

`aeProcessEvents` is a fairly big function, it would be hard to summarize it all here, but it first uses the `aeApiPoll` function, which is what we covered in the previous chapter. It processes the events coming from the poll, if any and then calls `processTimeEvents`.

Redis maintains a list of time events, as described in the event loop documentation page, for periodic task. When the server is initialized, one time event is created, for the function `serverCron`. This function is responsible for a lot of things. This how it is described in the [source code][redis-source-server-cron]:

> - Active expired keys collection (it is also performed in a lazy way on lookup).
> - Software watchdog.
> - Update some statistic.
> - Incremental rehashing of the DBs hash tables.
> - Triggering BGSAVE / AOF rewrite, and handling of terminated children.
> - Clients timeout of different kinds.
> - Replication reconnection.
> - Many more...

We're only interested in the first one for now, the active expiration of keys. Redis runs on a single thread, which essentially continuously runs the event loop described above. This means that each operation performed in the event loop effectively blocks the ones waiting to be processed. Redis tries to optimize for this by making all the operations as fast as possible.

`serverCron` is first added to the time events with a time set to 1ms in the future. The return value of function executed as a time event dictates if it is removed from the time event queue or if it is rescheduled in the future. `serverCron` returns a value based on the frequency set as config. By default 100ms. That means that it won't run more than 10 times per second.

This is one of the reasons the documentation provides the time complexity for each commands. Most commands are O(1), and commands like [`KEYS`][redis-doc-keys], with an O(n) complexity are not recommended in a production environment.

If Redis were to scan all the keys in the expires dictionary on every iteration of the event loop it would be an O(n) operation. Put differently, as you add keys with a TTL, you would slow down the process of active expiration. To prevent this, Redis only scans the expires up to certain amount. The `activeExpireCycle` contains a lot of optimizations that we will not explore for now. [EXPLAIN WHY].

I think that it's worth stopping for a second to recognize the benefits of having two types of expiration. The lazy approach gets the job done as it guarantees that no expired keys will ever be returned, but if a key is set with a ttl and never read again it would still unnecessarily sit in memory. The incremental active approach solves this problem, while still being optimized for speed and does not pause the server to clean all the keys.

### The NX, XX & KEEPTTL options

These options don't require all the digging we had to do for the previous two. We will add the necessary validations for the options, that is, checking that there is no arguments after either of these and based on the flag add the required check before inserting the value in the `@data_store` hash.

The `KEEPTTL` options is also simpler. If it is present, we will not remove the matching entry in the `@expires` hash, if it is not present, we will.

Most of the complexity resides in the validations of the command, to make sure that it has a valid format.

### Adding validation

Before adding these options, validating the `SET` command did not require a lot of work. In its simple form, it requires a key and value. If both are present, the command is valid, if one is missing, it is a "wrong number of arguments" error.

This rule still applies but we need to add more to support the different combinations of possible options. Let's look at the rules we need to support:

- You can only specify the PX or the EX option, not both. Note that the `redis-cli` has a user friendly interface that hints at this constraints by displaying the following `key value [EX seconds|PX milliseconds] [NX|XX] [KEEPTTL]` when you start typing `SET`. The `|` character between `EX seconds` & `PX milliseconds` expresses the or condition.
- Following the hints from redis-cli, we can only specify `NX` or `XX`, not both.
- The redis-cli hint does not make this obvious, but you can only specify `KEEPTTL` if neither `EX` or `PX` or present. The following command `SET 1 2 EX 1 KEEPTTL` returns `(error) ERR syntax error`

It's also worth mentioning that the order is not important, both commands are equivalent:

```
SET a-key a-value NX EX 10
```

```
SET a-key a-value EX 10 NX
```

## Let's write some code!

We are making the following changes to the server:

- Add our own, simplified event loop, including support for time events
- Accepts options for the expiration related options, EX & PX
- Accepts options for the presence or absence of a key, NX & XX
- Delete expired keys on read
- Setting a key without KEEPTTL removes any previously set TTL

``` ruby
require 'socket'
require 'timeout'
require 'logger'

require_relative './get_command'
require_relative './set_command'

class BasicServer

  COMMANDS = [
    'GET',
    'SET',
  ]

  MAX_EXPIRE_LOOKUPS_PER_CYCLE = 20

  def initialize
    @logger = Logger.new(STDOUT)
    @logger.level = ENV['DEBUG'] ? Logger::DEBUG : Logger::INFO

    @clients = []
    @data_store = {}
    @expires = {}

    @server = TCPServer.new 2000
    @time_events = []
    @logger.debug "Server started at: #{ Time.now }"
    add_time_event do
      server_cron
    end

    start_event_loop
  end

  private

  def add_time_event(&block)
    @time_events << block
  end

  def start_event_loop
    loop do
      # Selecting blocks, so if there's no client, we don't have to call it, which would
      # block, we can just keep looping
      result = IO.select(@clients + [@server], [], [], 1)
      sockets = result ? result[0] : []
      process_poll_events(sockets)
      process_time_events
    end
  end

  def process_poll_events(sockets)
    sockets.each do |socket|
      begin
        if socket.is_a?(TCPServer)
          @clients << @server.accept
        elsif socket.is_a?(TCPSocket)
          client_command_with_args = socket.read_nonblock(1024, exception: false)
          if client_command_with_args.nil?
            @clients.delete(socket)
          elsif client_command_with_args == :wait_readable
            # There's nothing to read from the client, we don't have to do anything
            next
          elsif client_command_with_args.strip.empty?
            @logger.debug "Empty request received from #{ client }"
          else
            response = handle_client_command(client_command_with_args.strip)
            @logger.debug "Response: #{ response }"
            socket.puts response
          end
        else
          raise "Unknown socket type: #{ socket }"
        end
      rescue Errno::ECONNRESET
        @clients.delete(socket)
      end
    end
  end

  def process_time_events
    @time_events.each { |time_event| time_event.call }
  end

  def handle_client_command(client_command_with_args)
    command_parts = client_command_with_args.split
    command = command_parts[0]
    args = command_parts[1..-1]
    if COMMANDS.include?(command)
      if command == 'GET'
        get_command = GetCommand.new(@data_store, @expires, args)
        get_command.call
      elsif command == 'SET'
        set_command = SetCommand.new(@data_store, @expires, args)
        set_command.call
      end
    else
      formatted_args = args.map { |arg| "`#{ arg }`," }.join(' ')
      "(error) ERR unknown command `#{ command }`, with args beginning with: #{ formatted_args }"
    end
  end

  def server_cron
    start_timestamp = Time.now
    keys_fetched = 0

    @expires.each do |key, value|
      if @expires[key] < Time.now.to_f * 1000
        @logger.debug "Evicting #{ key }"
        @expires.delete(key)
        @data_store.delete(key)
      end

      keys_fetched += 1
      if keys_fetched >= MAX_EXPIRE_LOOKUPS_PER_CYCLE
        break
      end
    end

    end_timestamp = Time.now
    @logger.debug do
      sprintf(
        "It took %.7f ms to process %i keys", (end_timestamp - start_timestamp), keys_fetched)
    end
  end
end
```
_listing 4.1: server.rb_

``` ruby
class SetCommand

  ValidationError = Class.new(StandardError)

  CommandOption = Struct.new(:kind)
  CommandOptionWithValue = Struct.new(:kind, :validator)

  IDENTITY = ->(value) { value }

  OPTIONS = {
    'EX' => CommandOptionWithValue.new(
      'expire',
      ->(value) { validate_integer(value) * 1000 },
    ),
    'PX' => CommandOptionWithValue.new(
      'expire',
      ->(value) { validate_integer(value) },
    ),
    'KEEPTTL' => CommandOption.new('expire'),
    'NX' => CommandOption.new('presence'),
    'XX' => CommandOption.new('presence'),
  }

  ERRORS = {
    'expire' => '(error) ERR value is not an integer or out of range',
  }

  def self.validate_integer(str)
    Integer(str)
  rescue ArgumentError, TypeError
    raise ValidationError, '(error) ERR value is not an integer or out of range'
  end

  def initialize(data_store, expires, args)
    @data_store = data_store
    @expires = expires
    @args = args

    @options = {}
  end

  def call
    key, value = @args.shift(2)
    if key.nil? || value.nil?
      return "(error) ERR wrong number of arguments for 'SET' command"
    end

    parse_result = parse_options

    if !parse_result.nil?
      return parse_result
    end

    existing_key = @data_store[key]

    if @options['presence'] == 'NX' && !existing_key.nil?
      '(nil)'
    elsif @options['presence'] == 'XX' && existing_key.nil?
      '(nil)'
    else

      @data_store[key] = value
      expire_option = @options['expire']

      # The implied third branch is if expire_option == 'KEEPTTL', in which case we don't have
      # to do anything
      if expire_option.is_a? Integer
        @expires[key] = (Time.now.to_f * 1000).to_i + expire_option
      elsif expire_option.nil?
        @expires.delete(key)
      end

      'OK'
    end

  rescue ValidationError => e
    e.message
  end

  private

  def parse_options
    while @args.any?
      option = @args.shift
      option_detail = OPTIONS[option]

      if option_detail
        option_values = parse_option_arguments(option, option_detail)
        existing_option = @options[option_detail.kind]

        if existing_option
          return '(error) ERR syntax error'
        else
          @options[option_detail.kind] = option_values
        end
      else
        return '(error) ERR syntax error'
      end
    end
  end

  def parse_option_arguments(option, option_detail)

    case option_detail
    when CommandOptionWithValue
      option_value = @args.shift
      option_detail.validator.call(option_value)
    when CommandOption
      option
    else
      raise "Unknown command option type: #{ option_detail }"
    end
  end
end
```
_listing 4.2: set_command.rb_

``` ruby
class GetCommand

  def initialize(data_store, expires, args)
    @logger = Logger.new(STDOUT)
    @data_store = data_store
    @expires = expires
    @args = args
  end

  def call
    if @args.length != 1
      "(error) ERR wrong number of arguments for 'GET' command"
    else
      check_if_expired
      @data_store.fetch(@args[0], '(nil)')
    end
  end

  private

  def check_if_expired
    expires_entry = @expires[@args[0]]
    if expires_entry && expires_entry < Time.now.to_f * 1000
      logger.debug "evicting #{ @args[0] }"
      @expires.delete(@args[0])
      @data_store.delete(@args[0])
    end
  end
end
```
_listing 4.3: get_command.rb_

The `server.rb` started getting pretty big so we extracted the logic for `GET` & `SET` to different files, and gave them their own classes.

---

Note: See `db.c`, functions:

1. `server.c` defines all the commands in `redisCommand`:   https://github.com/antirez/redis/blob/6.0/src/server.c#L201-L203
2. `t_string.c` defines the handler in `setCommand`: https://github.com/antirez/redis/blob/6.0/src/t_string.c#L97-L147
3  `t_string.c` defines a more specific handlers after options are parsed: https://github.com/antirez/redis/blob/6.0/src/t_string.c#L71-L79 & https://github.com/antirez/redis/blob/6.0/src/t_string.c#L89
4. `db.c` defines the `setExpire` function: https://github.com/antirez/redis/blob/6.0/src/db.c#L1190-L1206

Keys are deleted on reads

1. `server.c` defines the handler for `GET`: https://github.com/antirez/redis/blob/6.0/src/server.c#L187-L189
2. `t_string.c` defines the handler for `getCommand`: https://github.com/antirez/redis/blob/6.0/src/t_string.c#L179-L181 & the generic one: https://github.com/antirez/redis/blob/6.0/src/t_string.c#L164-L177
3. `db.c` defines `lookupKeyReadOrReply`: https://github.com/antirez/redis/blob/6.0/src/db.c#L163-L167
4. `db.c` defines `lookupKeyRead`  https://github.com/antirez/redis/blob/6.0/src/db.c#L143-L147 as well as `lookupKeyReadWithFlags`: https://github.com/antirez/redis/blob/6.0/src/db.c#L149-L157
5. `db.c` defines `expireIfNeeded`: https://github.com/antirez/redis/blob/6.0/src/db.c#L1285-L1326

In `expire.c`: `activeExpireCycleTryExpire`: https://github.com/antirez/redis/blob/6.0/src/expire.c#L35-L74

In `serverCron` and `databasesCron`, called from `arCreateTimeEvent`

On init, `aeCreateTimeEvent` is called, 1ms in the future. On each ae loop, we look at time events, `processTimeEvents`.

It loops through, starting at `timeEventHead`
If timeEvent func returns `AE_NOMORE`, `-1`, event is removed on next iteration, otherwise, it's bumped by `restval` ms. `serverCron` returns `1000/server.hz`.

in `evict.c`, in `freeMemoryIfNeeded`, keys might get deleted.

### And a few more tests

We changed a lot of code and added more features, this calls for more tests. Here is

---

- INCR / INCRBY
- DECR / DECRBY
- MGET
- MSET

Reminder:
[Redis Protocol](https://redis.io/topics/protocol#resp-bulk-strings)


[chapter-2]:/post/chapter-2-respond-to-get-and-set/
[redis-protocol]:https://redis.io/topics/protocol
[redis-doc-set]:https://redis.io/commands/set
[redis-doc-setnx]:https://redis.io/commands/setnx
[redis-doc-setex]:https://redis.io/commands/setex
[redis-doc-psetex]:https://redis.io/commands/psetex
[redis-doc-ttl]:http://redis.io/commands/ttl
[redis-doc-event-loop]:https://redis.io/topics/internals-rediseventlib
[redis-docs-keys]:https://redis.io/commands/keys
