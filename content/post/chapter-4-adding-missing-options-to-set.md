---
title: "Chapter 4 - Adding the missing options to the SET command"
date: 2020-07-23T10:27:27-04:00
lastmod: 2020-07-23T10:27:27-04:00
draft: false
keywords: []
summary: "In this chapter we add the missing features to the SET command we implemented in chapter 2, EX, PX, NX, XX & KEEPTTL"
comment: false
---

## What we'll cover

We implemented a simplified version of the `SET` command in [Chapter 2][chapter-2], in this chapter we will complete the command by implementing all [its options][redis-doc-set]. Note that we're still not following the [Redis Protocol][redis-protocol], we will address that in the next chapter.

## Planning our changes

The [`SET`][redis-doc-set] commands accepts the following options:


- EX seconds -- Set the specified expire time, in seconds.
- PX milliseconds -- Set the specified expire time, in milliseconds.
- NX -- Only set the key if it does not already exist.
- XX -- Only set the key if it already exists.
- KEEPTTL -- Retain the Time To Live (TTL) associated with the key

As noted in the documentation, there is some overlap with some of the options above and the following commands: [SETNX][redis-doc-setnx], [SETEX][redis-doc-setex], [PSETEX][redis-doc-psetex]. As of this writing these three commands are not officially deprecated, but the documentation mentions that it might happen soon. Given that we can access the same features through the `NX`, `EX` & `PX` options respectively, we will not implement these three commands.

`SET a-key a-value EX 10` is equivalent to `SETEX a-key 10 a-value`, which we can demonstrate in a `redis-cli` session:

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

[`TTL`][redis-doc-ttl] returned 8 in both cases, because it took me about 2s to type the `TTL` command, and by that time about 8s were left, of the initial 10.

### The EX & PX options

Both EX & PX options are followed by one argument, an integer for the number of the seconds and milliseconds specifying how long the key will be readable for. Once the duration has elapsed, the key is deleted, and calling `GET` would return `(nil)` as if it was never set or explicitly deleted by the `DEL` command.

When a key is set with either of these two options, or through the `SETEX` & `PSETEX` commands, but we are ignoring these for the sake of simplicity, Redis adds the key to different dictionary, internally called [`db->expires`][redis-source-db-expires] in the C code. This dictionary is dedicated to storing keys with a TTL, the key is the same key and the value is the timestamp of the expiration, in milliseconds.

Redis uses two approaches to delete keys with a TTL. The first one is a lazy approach, when reading a key, it checks if it exists in the `expires` dictionary, if it does and the value is lower than the current timestamp in milliseconds, it deletes the key and does not proceed with the read.

{{% admonition info "\"Lazy\" \& \"Eager\"" %}}

The terms lazy is often used in programming, it describes an operation that is put on the back-burner and delayed until it absolutely has to be performed.

In the context of Redis, it makes sense to describe the eviction strategy described above as lazy since Redis might still store keys that are effectively expired and will only guarantee their deletion until they are accessed past their expiration timestamp.

The opposite approach is "eager", where an operation is performed as soon as possible, whether or not it could be postponed.

{{% /admonition %}}


The other one is a more proactive approach, Redis periodically scans a subset of the list of keys with a TTL value and deletes the expired one. This action, performed in the [`serverCron`][redis-source-server-cron] function is part of the [event loop][redis-doc-event-loop]. The event loop is defined in `ae.c` and starts in the `aeMain` function, it continuously executes the `aeProcessEvents` function in a while loop, until the `stop` flag is set to `1`, which essentially never happens when the server is running under normal circumstances. The `aeStop` function is the only function doing this and it is only used in `redis-benchmark.c` & `example-ae.c`.

`aeProcessEvents` is a fairly big function, it would be hard to summarize it all here, but it first uses the `aeApiPoll` function, which is what we covered in the previous chapter. It processes the events from the poll result, if any and then calls `processTimeEvents`.

Redis maintains a list of time events, as described in the event loop documentation page, for periodic task. When the server is initialized, one time event is created, for the `serverCron` function. This function is responsible for a lot of things, this is how it is described in the [source code][redis-source-server-cron-comments]:

> This is our timer interrupt, called server.hz times per second.\
> Here is where we do a number of things that need to be done asynchronously.\
> For instance:
> - Active expired keys collection (it is also performed in a lazy way on lookup).
> - Software watchdog.
> - Update some statistic.
> - Incremental rehashing of the DBs hash tables.
> - Triggering BGSAVE / AOF rewrite, and handling of terminated children.
> - Clients timeout of different kinds.
> - Replication reconnection.
> - Many more...

We're only interested in the first item in this list for now, the active expiration of keys. Redis runs the event loop on a single thread. This means that each operation performed in the event loop effectively blocks the ones waiting to be processed. Redis tries to optimize for this by making all the operations performed in the event loop "fast".

This is one of the reasons why the documentation provides the time complexity for each commands. Most commands are O(1), and commands like [`KEYS`][redis-doc-keys], with an O(n) complexity are not recommended in a production environment, it would prevent the server from processing any incoming commands while iterating through all the keys.

If Redis were to scan all the keys in the expires dictionary on every iteration of the event loop it would be an O(n) operation, where n is the number of keys with a TTL value. Put differently, as you add keys with a TTL, you would slow down the process of active expiration. To prevent this, Redis only scans the expires up to certain amount. The `activeExpireCycle` contains a lot of optimizations that we will not explore for now for the sake of simplicity.

One of these optimizations takes care of maintaining statistics about the server, among those Redis keeps track of an estimate of the number of keys that are expired but not yet deleted. Using this it will attempt to expire more keys to try to keep this number under control and prevent it from growing too fast if keys start expiring faster than the normal rate at which they get deleted.

`serverCron` is first added to the time events with a time set to 1ms in the future. The return value of functions executed as time events dictates if it is removed from the time event queue or if it is rescheduled in the future. `serverCron` returns a value based on the frequency set as config. By default 100ms. That means that it won't run more than 10 times per second.

I think that it's worth stopping for a second to recognize the benefits of having two eviction strategies for expired keys. The lazy approach gets the job done as it guarantees that no expired keys will ever be returned, but if a key is set with a TTL and is never read again it would unnecessarily sit in memory, using space. The incremental active approach solves this problem, while still being optimized for speed and does not pause the server to clean all the keys.

{{% admonition info "Big O Notation" %}}

The [Big O Notation][big-o-notation] is used to describe the time complexity of operations. In other words, it describes how much slower, or not, an operation would be, as the size of the elements it operates on increases.

The way that I like to think about it is to transcribe the O notation to a function with a single parameter n, that returns the value inside the parentheses after O. So O(n) — which is the complexity of the [`KEYS`][redis-doc-keys] command — would become `def fn(n); n; end;` if written in Ruby, or `let fn = (n) => n` in javascript. O(1) — which is the complexity of the [`SET`][redis-doc-set] command — would be `def fn(n); 1; end;`, O(logn)  — which is the complexity of the [`ZADD`][redis-doc-zadd] command — would become `def fn(n); Math.log(n); end;` and O(n^2)  — as far as I know, no Redis command has such complexity — would become `def fn(n); n.pow(2); end;`.

We can play with these functions to illustrate the complexity of the different commands. `SET` has a time complexity of O(1), commonly referred to as constant time. Regardless of the number of keys stored in Redis, the operations required to fulfill a `SET` command are the same, so whether we are operating on an empty Redis server or one with millions of keys, it'll take a similar amount of time. With the function defined above we can see that, if n is the number of keys, `fn(n)` will always return `1`, regardless of n.

On the other hand `KEYS` has a complexity of O(n), where n is the number of keys stored in Redis.

It's important to note that n is always context dependent and should therefore always be specified, which Redis does on each command page. In comparison, [`DEL`][redis-doc-del] is also documented with having a time complexity of O(n), but, and this is the important part, where n is _the number of keys given to the command_. Calling `DEL a-key` has therefore a time complexity of O(1), and runs in constant time.

`KEYS` iterates through all the items in Redis and return all the keys. With the function defined above, we can see that `fn(1)` will return `1`, `fn(10)`, `10`, and so on. What this tells us is that the time required to execute `KEYS` will grow proportionally to the value of n.

Lastly, it's important to note that this does not necessarily mean that `KEYS` ran on a server with 100 items will be exactly 100 times slower than running against a server with one key. There are some operations that will have to be performed regardless, such as parsing the command and dispatching to the `keysCommand` function. These are in the category of "fixed cost", they always have to be performed. If it takes 1ms to run those and then 0.1ms per key — these are only illustrative numbers —, it would take Redis 1.1ms to run `KEYS` for one key and 10.1ms with 100 keys. It's not exactly 100 times more, but it is in the order of 100 times more.

{{% /admonition %}}

### The NX, XX & KEEPTTL options

These options are easier to implement compared to the previous two given that they are not followed by a value. Additionally, their behavior does not require implementing more components to the server, beyond a few conditions in the method that takes care of storing the key and the value specified by the `SET` command.

Most of the complexity resides in the validations of the command, to make sure that it has a valid format.

### Adding validation

Prior to adding these options, validating the `SET` command did not require a lot of work. In its simple form, it requires a key and value. If both are present, the command is valid, if one is missing, it is a "wrong number of arguments" error.

This rule still applies but we need to add a few more to support the different combinations of possible options. These are the rules we now need to support:

- You can only specify one of the PX or EX options, not both. Note that the `redis-cli` has a user friendly interface that hints at this constraint by displaying the following `key value [EX seconds|PX milliseconds] [NX|XX] [KEEPTTL]` when you start typing `SET`. The `|` character between `EX seconds` & `PX milliseconds` expresses the `OR` condition.
- Following the hints from redis-cli, we can only specify `NX` or `XX`, not both.
- The redis-cli hint does not make this obvious, but you can only specify `KEEPTTL` if neither `EX` or `PX` or present. The following command `SET 1 2 EX 1 KEEPTTL` is invalid and returns `(error) ERR syntax error`

It's also worth mentioning that the order of options does not matter, both commands are equivalent:

```
SET a-key a-value NX EX 10
```

```
SET a-key a-value EX 10 NX
```

But the following would be invalid, `EX` must be followed by an integer:

```
SET a-key a-value EX NX 10
```

## Let's write some code!

We are making the following changes to the server:

- Add our own, simplified event loop, including support for time events
- Accepts options for the expiration related options, `EX` & `PX`
- Accepts options for the presence or absence of a key, `NX` & `XX`
- Delete expired keys on read
- Setting a key without `KEEPTTL` removes any previously set TTL
- Implement the `TTL` & `PTTL` commands as they are useful to use alongside keys with a TTL

I'm giving you the complete code first and we'll look at the interesting parts one by one afterwards:

``` ruby
require 'socket'
require 'timeout'
require 'logger'
LOG_LEVEL = ENV['DEBUG'] ? Logger::DEBUG : Logger::INFO

require_relative './expire_helper'
require_relative './get_command'
require_relative './set_command'
require_relative './ttl_command'
require_relative './pttl_command'

class RedisServer

  COMMANDS = {
    'GET' => GetCommand,
    'SET' => SetCommand,
    'TTL' => TtlCommand,
    'PTTL' => PttlCommand,
  }

  MAX_EXPIRE_LOOKUPS_PER_CYCLE = 20
  DEFAULT_FREQUENCY = 10 # How many times server_cron runs per second

  TimeEvent = Struct.new(:process_at, :block)

  def initialize
    @logger = Logger.new(STDOUT)
    @logger.level = LOG_LEVEL

    @clients = []
    @data_store = {}
    @expires = {}

    @server = TCPServer.new 2000
    @time_events = []
    @logger.debug "Server started at: #{ Time.now }"
    add_time_event(Time.now.to_f.truncate + 1) do
      server_cron
    end

    start_event_loop
  end

  private

  def add_time_event(process_at, &block)
    @time_events << TimeEvent.new(process_at, block)
  end

  def nearest_time_event
    now = (Time.now.to_f * 1000).truncate
    nearest = nil
    @time_events.each do |time_event|
      if nearest.nil?
        nearest = time_event
      elsif time_event.process_at < nearest.process_at
        nearest = time_event
      else
        next
      end
    end

    nearest
  end

  def select_timeout
    if @time_events.any?
      nearest = nearest_time_event
      now = (Time.now.to_f * 1000).truncate
      if nearest.process_at < now
        0
      else
        (nearest.process_at - now) / 1000.0
      end
    else
      0
    end
  end

  def start_event_loop
    loop do
      timeout = select_timeout
      @logger.debug "select with a timeout of #{ timeout }"
      result = IO.select(@clients + [@server], [], [], timeout)
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
            commands = client_command_with_args.strip.split("\n")
            commands.each do |command|
              response = handle_client_command(command.strip)
              @logger.debug "Response: #{ response }"
              socket.puts response
            end
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
    @time_events.delete_if do |time_event|
      next if time_event.process_at > Time.now.to_f * 1000

      return_value = time_event.block.call

      if return_value.nil?
        true
      else
        time_event.process_at = (Time.now.to_f * 1000).truncate + return_value
        @logger.debug "Rescheduling time event #{ Time.at(time_event.process_at / 1000.0).to_f }"
        false
      end
    end
  end

  def handle_client_command(client_command_with_args)
    @logger.debug "Received command: #{ client_command_with_args }"
    command_parts = client_command_with_args.split
    command_str = command_parts[0]
    args = command_parts[1..-1]

    command_class = COMMANDS[command_str]

    if command_class
      command = command_class.new(@data_store, @expires, args)
      command.call
    else
      formatted_args = args.map { |arg| "`#{ arg }`," }.join(' ')
      "(error) ERR unknown command `#{ command_str }`, with args beginning with: #{ formatted_args }"
    end
  end

  def server_cron
    start_timestamp = Time.now
    keys_fetched = 0

    @expires.each do |key, _|
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
        "Processed %i keys in %.3f ms", keys_fetched, (end_timestamp - start_timestamp) * 1000)
    end

    1000 / DEFAULT_FREQUENCY
  end
end
```
_listing 4.1: server.rb_

``` ruby
class SetCommand

  ValidationError = Class.new(StandardError)

  CommandOption = Struct.new(:kind)
  CommandOptionWithValue = Struct.new(:kind, :validator)

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
    @logger = Logger.new(STDOUT)
    @logger.level = LOG_LEVEL
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
    @logger.level = LOG_LEVEL
    @data_store = data_store
    @expires = expires
    @args = args
  end

  def call
    if @args.length != 1
      "(error) ERR wrong number of arguments for 'GET' command"
    else
      key = @args[0]
      ExpireHelper.check_if_expired(@data_store, @expires, key)
      @data_store.fetch(key, '(nil)')
    end
  end
end
```
_listing 4.3: get_command.rb_

``` ruby
class PttlCommand

  def initialize(data_store, expires, args)
    @logger = Logger.new(STDOUT)
    @logger.level = LOG_LEVEL
    @data_store = data_store
    @expires = expires
    @args = args
  end

  def call
    if @args.length != 1
      "(error) ERR wrong number of arguments for 'PTTL' command"
    else
      key = @args[0]
      ExpireHelper.check_if_expired(@data_store, @expires, key)
      key_exists = @data_store.include? key
      if key_exists
        ttl = @expires[key]
        if ttl
          (ttl - (Time.now.to_f * 1000)).round
        else
          -1
        end
      else
        -2
      end
    end
  end
end
```
_listing 4.4: pttl_command.rb_

``` ruby
class TtlCommand

  def initialize(data_store, expires, args)
    @data_store = data_store
    @expires = expires
    @args = args
  end

  def call
    if @args.length != 1
      "(error) ERR wrong number of arguments for 'TTL' command"
    else
      pttl_command = PttlCommand.new(@data_store, @expires, @args)
      result = pttl_command.call.to_i
      if result > 0
        (result / 1000.0).round
      else
        result
      end
    end
  end
end
```
_listing 4.5: ttl_command.rb_

``` ruby
module ExpireHelper

  def self.check_if_expired(data_store, expires, key)
    expires_entry = expires[key]
    if expires_entry && expires_entry < Time.now.to_f * 1000
      logger.debug "evicting #{ key }"
      expires.delete(key)
      data_store.delete(key)
    end
  end

  def self.logger
    @logger ||= Logger.new(STDOUT).tap do |l|
      l.level = LOG_LEVEL
    end
  end
end
```
_listing 4.6: expire_helper.rb_


### The changes

#### Splitting it the logic in multiple files

The `server.rb` file started getting pretty big so we extracted the logic for `GET` & `SET` to different files, and gave them their own classes.

#### Time events

In order to implement the eviction logic for keys having an expiration, we refactored how we call the `IO.select` method. Our implementation is loosely based on the one built in Redis, [ae][redis-doc-ae]. The `RedisServer` — renamed from `BasicServer` in the previous chapters — starts the event loop in its constructor. The event loop is a never ending loop that calls `select`, processes all the incoming events and then process time events, if any need to be processed.

We introduced the `TimeEvent` class, defined as follows:

``` ruby
TimeEvent = Struct.new(:process_at, :block)
```

The `process_at` field is an `Integer` that represents the timestamp, in milliseconds, for when the event should be processed. The `block` field is the actual code that will be run. For now, there's only one type of events, `server_cron`. It is first added to the `@time_events` list with a `process_at` value set to 1ms in the future.

Time events can be either one-off, they'll run only once, or repeating, they will be rescheduled at some point in the future after being processed. This behavior is driven by the return value of the `block` field. If the block returns `nil`, the time event is removed from the `@time_events` list, if it returns an integer `return_value`, the event is rescheduled for `return_value` milliseconds in the future, by changing the value of `process_at`. By default the `server_cron` method is configured with a frequency of 10 Hertz (hz), which means it will run up to 10 times per second, or put differently, every 100ms. This is why the return value of `server_cron` is `1000 / DEFAULT_FREQUENCY`  — 1000 is the number of milliseconds, if frequency was 20, it would return 50, as in, it should run every 50ms.

This behavior makes sure that we don't run the `server_cron` method too often, it effectively gives a higher priority to handling client commands, and new clients connecting.

#### Select timeout

When we introduced `IO.select` in [Chapter 3][chapter-3-select], we used it without the timeout argument. This wasn't a problem then because the server had nothing else to do. It would either need to accept a new client, or reply to a client command, and both would be handled through `select`.

The server needs to do something else beside waiting on `select` now, run the time events when they need to be processed. In order to do so, Redis uses a timeout with its abstraction over select and other multiplexing libraries, `aeApiPoll`. Redis can, under some conditions, use no timeout, which we're going to ignore for now, for the sake of simplicity. When using a timeout, Redis makes sure that waiting on the timeout will not delay any future time events that should be processed instead of waiting on `select`. In order to achieve this, Redis looks at all the time events and finds the nearest one, and sets a timeout equivalent to the time between now and that event. This guarantees that even if there's no activity between now and when the next time event should be processed, redis will stop waiting on `aeApiPoll` and process the time events.

We're replicating this logic in the `select_timeout` method. It starts by delegating the task of finding the nearest time event through the `nearest_time_event`, which iterates through all the time events in the `@time_events` array and find the one with the smallest value for `process_at`.

In concrete terms, in `RedisServer`, `server_cron` runs every 100ms, so when we call `IO.select`, the next time event will be at most 100ms in the future. The timeout given to `select` will be a value between 0 and 100ms.

#### Parsing options

Probably one of the most complicated changes introduced in this chapter, at least for me as I was implementing it. The logic is in the `SetCommand` class. We first define all the possible options in the `OPTIONS` constant. Each option is a key/value pair where the key is the option as expected in the command string and the value is an instance of `CommandOption` or `CommandOptionWithValue`. After extracting and validating the first three elements of the string, respectively, the `SET` string, followed by the key and the value, we split the rest on spaces and process them from left to right, with the `shift` method. For every option we find, we look up the `OPTIONS` hash to retrieve the matching `CommandOption` or `CommandOptionWithValue` instance. If `nil` is returned, it means that the given option is invalid, this is a syntax error. Note that once again, for the sake of simplicity, we did not implement case insensitive commands the way Redis does.

If the an option is found, but we had already found one of the same kind, `presence` or `expire`, this is also a syntax error. This check allows us to consider the following commands as invalid:

```
SET key value EX 1 PX 2
SET key value EX 1 KEEPTTL
SET key value NX XX
```

Finally, we attempt to parse the option argument, if necessary, only `EX` and `PX` have an argument, the others one do not, this is why we use two different classes here. `parse_option_arguments` will return the option itself if we found an option that should not be followed by an argument, that is either `NX`, `XX` or `KEEPTTL`. If we found one of the other two options, `option_detail` will be an instance of `CommandOptionWithValue`, we use `shift` once again to obtain the next element in the command string and feed it to the validator block.

The validator blocks are very similar for the options, they both validate that the string is a valid integer, but the `EX` validator multiplies the final result by 1000 to convert the value from seconds to milliseconds.

The values are then stored in the `@options` hash, with either the `presence` or `expire` key, based on the `kind` value. This allows us to read from the `@options` hash in the call method to apply the logic required to finalize the implementation of these options.

If `@options['presence']` is set to `NX` and there is already a value at the same key, we return `nil` right away. Similarly if it is set to `XX` and there is no key, we also return nil.

Finally, we always set the value for the key in the `@data_store` hash, but the behavior regarding the secondary hash, `@expires`, is different depending on the value of `@options['expire']`. If it is set to an integer, we use this integer and add it to the current time, in milliseconds, in the `@expires` hash. If the value is nil, it means that `KEEPTTL` was not passed, so we remove any value that may have previously been set by a previous `SET` command with the same key and value for either `PX` or `EX`.

**Why not use a regular expression?**

Good question! The short answer is that after spending some time trying to use a regular expression, it did not feel easier, as a reference this where I got, just before I gave up:

``` ruby
/^SET \d+ \d+ (?:EX (?<ex>\d+)|PX (?<px>\d+)|KEEPTTL)?(:? ?(?<nx-or-xx>NX|XX))?(?: (?:EX (?<ex>\d+)|PX (?<px>\d+)))?$/
```

This regexp works for some cases but incorrectly considers the following as valid, Redis cannot process a `SET` command with `KEEPTTL` and `EX 1`:

```
SET 1 2 KEEPTTL XX EX 1
```

It _might_ be possible to use a regular expression here, given that the grammar of the `SET` command does not allow that many permutations but even if it is, I don't think it'll be simpler than the solution we ended up with.

#### Lazy evictions

The `server_cron` time event takes care of cleaning up expired key every 100ms, but we also want to implement the "lazy eviction", the same way Redis does. That is, if `server_cron` hasn't had the chance to evict an expired key yet, and the server receives a `GET` command for the same key, we want to return nil and evict the key instead of returning it.

This logic is implemented in the `ExpireHelper` module , in the `check_if_expired` method. This method checks if there is an entry in the `@expires` hash, and if there is it compares its value, a timestamp in milliseconds with the current time. If the value in `@expires` is smaller, the key is expired and it deletes it. This will cause the `GetCommand`, `TtlCommand` & `PttlCommand` classes to return `(nil)` even if `server_cron` hasn't had a chance to delete the expired keys.

#### New commands: TTL & PTTL

We added two new commands, `TTL` & `PTTL`. Both return the ttl of the given key as an integer, if it exists, the difference is that `TTL` returns the value in seconds, whereas `PTTL` returns it in milliseconds.

Given the similarity of these two commands, we only implemented the logic in the `PttlCommand` class, and reused from the `TtlCommand` class where we transform the value in milliseconds to a value in seconds before returning it.

#### Logger

As the complexity of the codebase grew, it became useful to add logging statements. Such statements could be simple calls to `puts`, `print` or `p`, but it is useful to be able to conditionally turn them on and off based on their severity. Most of the logs we added are only useful when debugging an error and are otherwise really noisy. All these statements are logged with `@logger.debug`, and the severity of the logger is set based on the `DEBUG` environment variable. This allows us to enable all the debug logs by adding the `DEBUG=t` statement before running the server:

``` bash
DEBUG=true ruby -r"./server" -e "RedisServer.new"
```

### And a few more tests

We changed a lot of code and added more features, this calls for more tests.

We added a special instruction, `sleep <duration>` to allow us to easily write tests for the `SET` command with any of the expire based options. For instance, to test that `SET key value PX 100` actually works as expected, we want to wait at least 100ms, and assert that `GET key` returns `(nil)` instead of `value`.

We also added a new way to specify assertion, with the syntax `[ 'PTTL key', '2000+/-20' ]`. This is useful for the `PTTL` command because it would be impossible to know exactly how long it'll take the computer running the tests to execute the `PTTL` command after running the `SET` command. We can however estimate a reasonable range. In this case, we are assuming that the machine running the test will take less than 20ms to run `PTTL` by leveraging the minitest assertion `assert_in_delta`.

I also added the option to set the `DEBUG` environment variable, which you can use when running all the tests or an individual test:

``` bash
// All tests:
DEBUG=t ruby test.rb // Any values will work, even "false", as long as it's not nil
// Or a specific test
DEBUG=t ruby test.rb --name "RedisServer::SET#test_0005_handles the PX option with a valid argument"
```

There is now a `begin/rescue` for `Interrupt` in the forked process. This is to prevent an annoying stacktrace from being logged when we kill the process with `Process.kill('INT', child)` after sending all the commands to the server.

``` ruby
require 'minitest/autorun'
require 'timeout'
require 'stringio'
require './server'

describe 'RedisServer' do

  # ...

  def with_server

    child = Process.fork do
      unless !!ENV['DEBUG']
        # We're effectively silencing the server with these two lines
        # stderr would have logged something when it receives SIGINT, with a complete stacktrace
        $stderr = StringIO.new
        # stdout would have logged the "Server started ..." & "New client connected ..." lines
        $stdout = StringIO.new
      end

      begin
        RedisServer.new
      rescue Interrupt => e
        # Expected code path given we call kill with 'INT' below
      end
    end

    yield

  ensure
    if child
      Process.kill('INT', child)
      Process.wait(child)
    end
  end

  def assert_command_results(command_result_pairs)
    with_server do
      command_result_pairs.each do |command, expected_result|
        if command.start_with?('sleep')
          sleep command.split[1].to_f
          next
        end
        begin
          socket = connect_to_server
          socket.puts command
          response = socket.gets
          # Matches "2000+\-10", aka 2000 plus or minus 10
          regexp_match = expected_result.match /(\d+)\+\/-(\d+)/
          if regexp_match
            # The result is a range
            assert_in_delta regexp_match[1].to_i, response.to_i, regexp_match[2].to_i
          else
            assert_equal expected_result + "\n", response
          end
        ensure
          socket.close if socket
        end
      end
    end
  end


  # ...

  describe 'TTL' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'TTL', '(error) ERR wrong number of arguments for \'TTL\' command' ],
      ]
    end

    it 'returns the TTL for a key with a TTL' do
      assert_command_results [
        [ 'SET key value EX 2', 'OK'],
        [ 'TTL key', '2' ],
        [ 'sleep 0.5' ],
        [ 'TTL key', '1' ],
      ]
    end

    it 'returns -1 for a key without a TTL' do
      assert_command_results [
        [ 'SET key value', 'OK' ],
        [ 'TTL key', '-1' ],
      ]
    end

    it 'returns -2 if the key does not exist' do
      assert_command_results [
        [ 'TTL key', '-2' ],
      ]
    end
  end

  describe 'PTTL' do
    it 'handles unexpected number of arguments' do
      assert_command_results [
        [ 'PTTL', '(error) ERR wrong number of arguments for \'PTTL\' command' ],
      ]
    end

    it 'returns the TTL in ms for a key with a TTL' do
      assert_command_results [
        [ 'SET key value EX 2', 'OK'],
        [ 'PTTL key', '2000+/-20' ], # Initial 2000ms +/- 20ms
        [ 'sleep 0.5' ],
        [ 'PTTL key', '1500+/-20' ], # Initial 2000ms, minus ~500ms of sleep, +/- 20ms
      ]
    end

    it 'returns -1 for a key without a TTL' do
      assert_command_results [
        [ 'SET key value', 'OK' ],
        [ 'PTTL key', '-1' ],
      ]
    end

    it 'returns -2 if the key does not exist' do
      assert_command_results [
        [ 'PTTL key', '-2' ],
      ]
    end
  end

  # ...

  describe 'SET' do

    # ...

    it 'handles the EX option with a valid argument' do
      assert_command_results [
        [ 'SET 1 3 EX 1', 'OK' ],
        [ 'GET 1', '3' ],
        [ 'sleep 1' ],
        [ 'GET 1', '(nil)' ],
      ]
    end

    it 'rejects the EX option with an invalid argument' do
      assert_command_results [
        [ 'SET 1 3 EX foo', '(error) ERR value is not an integer or out of range']
      ]
    end

    it 'handles the PX option with a valid argument' do
      assert_command_results [
        [ 'SET 1 3 PX 100', 'OK' ],
        [ 'GET 1', '3' ],
        [ 'sleep 0.1' ],
        [ 'GET 1', '(nil)' ],
      ]
    end

    it 'rejects the PX option with an invalid argument' do
      assert_command_results [
        [ 'SET 1 3 PX foo', '(error) ERR value is not an integer or out of range']
      ]
    end

    it 'handles the NX option' do
      assert_command_results [
        [ 'SET 1 2 NX', 'OK' ],
        [ 'SET 1 2 NX', '(nil)' ],
      ]
    end

    it 'handles the XX option' do
      assert_command_results [
        [ 'SET 1 2 XX', '(nil)'],
        [ 'SET 1 2', 'OK'],
        [ 'SET 1 2 XX', 'OK'],
      ]
    end

    it 'removes ttl without KEEPTTL' do
      assert_command_results [
        [ 'SET 1 3 PX 100', 'OK' ],
        [ 'SET 1 2', 'OK' ],
        [ 'sleep 0.1' ],
        [ 'GET 1', '2' ],
      ]
    end

    it 'handles the KEEPTTL option' do
      assert_command_results [
        [ 'SET 1 3 PX 100', 'OK' ],
        [ 'SET 1 2 KEEPTTL', 'OK' ],
        [ 'sleep 0.1' ],
        [ 'GET 1', '(nil)' ],
      ]
    end

    it 'accepts multiple options' do
      assert_command_results [
        [ 'SET 1 3 NX EX 1', 'OK' ],
        [ 'GET 1', '3' ],
        [ 'SET 1 3 XX KEEPTTL', 'OK' ],
      ]
    end

    it 'rejects with more than one expire related option' do
      assert_command_results [
        [ 'SET 1 3 PX 1 EX 2', '(error) ERR syntax error'],
        [ 'SET 1 3 PX 1 KEEPTTL', '(error) ERR syntax error'],
        [ 'SET 1 3 KEEPTTL EX 2', '(error) ERR syntax error'],
      ]
    end

    it 'rejects with both XX & NX' do
      assert_command_results [
        [ 'SET 1 3 NX XX', '(error) ERR syntax error'],
      ]
    end
  end

  # ...
end
```

## Conclusion

The `SET` commands implemented by `RedisServer` now behaves the same way it does with Redis. Well, almost. Let's take a look at what happens if we were to use `redis-cli` against our own server. Let's start by running our server with

``` bash
ruby -r"./server" -e "RedisServer.new"
```

and in another shell open `redis-cli` on port 2000:

``` bash
redis-cli -p 2000
```

And type the following:

```
SET key value EX 200
```

And boom! It crashes!

```
Error: Protocol error, got "(" as reply type byte
```

This is because `RedisServer` does not implement the [Redis Protocol, RESP][redis-protocol]. This is what the next chapter is all about. At the end of chapter 5 we will be able to use `redis-cli` against our own server. Exciting!

## Code

As usual, the code [is available on GitHub](https://github.com/pjambet/redis-in-ruby/tree/master/code/chapter-4).

## Appendix A: Links to the Redis source code

If you're interested in digging into the Redis source code but would like some pointers as to where to start, you've come to the right place. The Redis source code is really well architected and overall relatively easy to navigate, so you are more than welcome to start the adventure on your own. That being said, it did take me a while to find the locations of functions I was interested in, such as: "where does redis handle the eviction of expired keys", and a few others.

Before jumping in the code, you might want to read this article that explains some of the main data structures used by Redis: http://blog.wjin.org/posts/redis-internal-data-structure-dictionary.html.

In no particular orders, the following is a list of links to the Redis source code on GitHub, for features related to the implementation of keys with expiration:

### Handling of the SET command:

- `server.c` defines all the commands in `redisCommand`:   https://github.com/antirez/redis/blob/6.0/src/server.c#L201-L203
- `t_string.c` defines the handler in `setCommand`: https://github.com/antirez/redis/blob/6.0/src/t_string.c#L97-L147
-  `t_string.c` defines a more specific handlers after options are parsed: https://github.com/antirez/redis/blob/6.0/src/t_string.c#L71-L79 & https://github.com/antirez/redis/blob/6.0/src/t_string.c#L89
- `db.c` defines the `setExpire` function: https://github.com/antirez/redis/blob/6.0/src/db.c#L1190-L1206

### Key deletion in `serverCron`

- `server.c` defines the handler for `GET`: https://github.com/antirez/redis/blob/6.0/src/server.c#L187-L189
- `t_string.c` defines the handler for `getCommand`: https://github.com/antirez/redis/blob/6.0/src/t_string.c#L179-L181 & the generic one: https://github.com/antirez/redis/blob/6.0/src/t_string.c#L164-L177
- `db.c` defines `lookupKeyReadOrReply`: https://github.com/antirez/redis/blob/6.0/src/db.c#L163-L167
- `db.c` defines `lookupKeyRead`  https://github.com/antirez/redis/blob/6.0/src/db.c#L143-L147 as well as `lookupKeyReadWithFlags`: https://github.com/antirez/redis/blob/6.0/src/db.c#L149-L157
- `db.c` defines `expireIfNeeded`: https://github.com/antirez/redis/blob/6.0/src/db.c#L1285-L1326
- `expire.c` defines `activeExpireCycleTryExpire` which implements the deletion of expired keys: https://github.com/antirez/redis/blob/6.0/src/expire.c#L35-L74
- `expire.c` defines `activeExpireCycle` which implement the sampling of keys and the logic to make sure that there are not too many expired keys in the `expires` dict:https://github.com/redis/redis/blob/6.0/src/expire.c#L123



## Appendix B: Playing with RedisServer using `nc`

If you want to manually interact with the server, an easy way is to use `nc`, the same way we used in [Chapter 1][chapter-1]. `nc` has no awareness of the Redis command syntax, so it will not stop you from making typos:

``` bash
❯ nc localhost 2000
GET 1
(nil)
SET 1 2
OK
GET 1
2
SET 1 2 EX 5
OK
GET 1
2
GET 1
2
GET 1
(nil)
SET 1 2 XX
(nil)
SET 1 2 NX
OK
SET 1 2 XX
OK
DEL 1
(error) ERR unknown command `DEL`, with args beginning with: `1`,
SET 1 2 PX 100
OK
SET 1 2 XX
(nil)
```



[chapter-1]:/post/chapter-1-basic-server/
[chapter-2]:/post/chapter-2-respond-to-get-and-set/
[chapter-3-select]:/post/chapter-3-multiple-clients/#lets-use-select
[redis-protocol]:https://redis.io/topics/protocol
[redis-doc-set]:https://redis.io/commands/set
[redis-doc-del]:https://redis.io/commands/del
[redis-doc-setnx]:https://redis.io/commands/setnx
[redis-doc-setex]:https://redis.io/commands/setex
[redis-doc-psetex]:https://redis.io/commands/psetex
[redis-doc-zadd]:http://redis.io/commands/zadd
[redis-doc-ttl]:http://redis.io/commands/ttl
[redis-doc-pttl]:http://redis.io/commands/pttl
[redis-doc-event-loop]:https://redis.io/topics/internals-rediseventlib
[redis-doc-keys]:https://redis.io/commands/keys
[redis-source-server-cron]:https://github.com/antirez/redis/blob/6.0.0/src/server.c#1849
[redis-source-server-cron-comments]:https://github.com/antirez/redis/blob/6.0.0/src/server.c#L1826-L1838
[big-o-notation]:https://en.wikipedia.org/wiki/Big_O_notation
[redis-doc-ae]:https://redis.io/topics/internals-rediseventlib
[wikipedia-syntax]:https://en.wikipedia.org/wiki/Syntax_(programming_languages)
[redis-source-db-expires]:https://github.com/redis/redis/blob/6.0.0/src/server.h#L645
