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

`SET a-key a-value EX 10` is equivalent to `SETEX a-key 10 a-value`, which you can demonstrate in a `redis-cli` session:

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

Redis maintains a list of time events, as described in the event loop documentation page, for periodic task. When the server is initialized, one time event is created, for the function `serverCron`. This function is responsible for a lot of things. This is how it is described in the [source code][redis-source-server-cron]:

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

We're only interested in the first item in this list for now, the active expiration of keys. Redis runs on a single thread, which continuously runs the event loop described above. This means that each operation performed in the event loop effectively blocks the ones waiting to be processed. Redis tries to optimize for this by making all the operations as fast as possible.

`serverCron` is first added to the time events with a time set to 1ms in the future. The return value of function executed as a time event dictates if it is removed from the time event queue or if it is rescheduled in the future. `serverCron` returns a value based on the frequency set as config. By default 100ms. That means that it won't run more than 10 times per second.

This is one of the reasons the documentation provides the time complexity for each commands. Most commands are O(1), and commands like [`KEYS`][redis-doc-keys], with an O(n) complexity are not recommended in a production environment.

If Redis were to scan all the keys in the expires dictionary on every iteration of the event loop it would be an O(n) operation. Put differently, as you add keys with a TTL, you would slow down the process of active expiration. To prevent this, Redis only scans the expires up to certain amount. The `activeExpireCycle` contains a lot of optimizations that we will not explore for now. [EXPLAIN WHY].

I think that it's worth stopping for a second to recognize the benefits of having two types of expiration. The lazy approach gets the job done as it guarantees that no expired keys will ever be returned, but if a key is set with a ttl and never read again it would still unnecessarily sit in memory. The incremental active approach solves this problem, while still being optimized for speed and does not pause the server to clean all the keys.

{{% admonition info "Big O Notation" %}}

The [Big O Notation][big-o-notation] is used to describe the time complexity of operations. In other words, it describes how much slower, or not, the operation would be, as the size of the elements it operates on increases.

The way that I like to think about it is to transcribe the O notation to a function with a single parameter n, that returns the value inside the parentheses after O. So O(n) — which is the complexity of the [`KEYS`][redis-doc-keys] command — would become `def fn(n); n; end;` if written in Ruby, or `let fn = (n) => n` in javascript. O(1) — which is the complexity of the [`SET`][redis-doc-set] command — would be `def fn(n); 1; end;` and O(logn)  — which is the complexity of the [`ZADD`][redis-doc-zadd] command — would become `def fn(n); Math.log(n); end;` and O(n^2)  — as far as I know, no Redis commands have such complexity — would become `def fn(n); n.pow(2); end;`.

We can play with these functions to illustrate the complexity of the different commands. `SET` is O(1), commonly referred to as constant time. Regardless of the number of keys currently stored in Redis, the operations required to fulfill a `SET` command are the same, so whether we are operating on an empty Redis server or one with millions of keys, it'll take a similar amount of time.

On the other hand `KEYS` has a complexity of O(n), where n is the number of keys stored in Redis — it's important to note that n is always context dependent and should be specified, which Redis does on each command page. `KEYS` is written in a way that will iterate through all the items in Redis and return all the keys. Running it on a server with a single key will first require to run operations independently of the number of keys stored, such as parsing the command, delegating it to the function handling the keys command, and only then look at all the keys, in this example, it'll be a

[FINISH THIS]

{{% /admonition %}}

### The NX, XX & KEEPTTL options

These options are easier to implement compared to the previous two given that they are not followed by a value and that their behavior does not require implementing more components to the server, beyond adding a few conditions in the method that takes care of storing the key and the value specified by the `SET` command.

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

I'm giving you the complete code first and we'll look at the interesting parts one by one afterwards:

``` ruby
require 'socket'
require 'timeout'
require 'logger'
LOG_LEVEL = ENV['DEBUG'] ? Logger::DEBUG : Logger::INFO

require_relative './get_command'
require_relative './set_command'

class RedisServer

  COMMANDS = [
    'GET',
    'SET',
  ]

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
      check_if_expired
      @data_store.fetch(@args[0], '(nil)')
    end
  end

  private

  def check_if_expired
    expires_entry = @expires[@args[0]]
    if expires_entry && expires_entry < Time.now.to_f * 1000
      @logger.debug "evicting #{ @args[0] }"
      @expires.delete(@args[0])
      @data_store.delete(@args[0])
    end
  end
end
```
_listing 4.3: get_command.rb_

### The changes

#### Splitting it the logic in multiple files

The `server.rb` started getting pretty big so we extracted the logic for `GET` & `SET` to different files, and gave them their own classes.

#### Time events

In order to implement the eviction logic for keys having an expiration, we refactored how we call the `IO.select` method. Our implementation is loosely based on the one built in Redis, [ae][redis-doc-ae]. The `RedisServer` — renamed from `BasicServer` in the previous chapters — starts the event loop in its constructor. The event loop is a never ending loop that calls `select`, processes all the incoming events and then process time events, if any need to be processed.

We introduced the `TimeEvent` class, defined as follows:

``` ruby
TimeEvent = Struct.new(:process_at, :block)
```

The `process_at` field is an `Integer` that represents the timestamp, in milliseconds for when the event should be processed. The `block` field is the actual code that will be run. For now, there's only one type of events, `server_cron`. It is first added to the `@time_events` with a `process_at` value set to 1ms in the future.

Time events can be either one-off, they'll run only once, or repeating, they will be rescheduled at some point in the future after being processed. This behavior is driven by the return value of the `block` field. If the block returns `nil`, the time event is removed from the `@time_events` list, if it returns an integer `return_value`, the event is rescheduled for `return_value` milliseconds in the future, by changing the value of `process_at`. By default the `server_cron` method is configured with a frequency of 10 Hertz (hz), which means it will run up to 10 times per second, or put differently, every 100ms. This is why the return value of `server_cron` is `1000 / DEFAULT_FREQUENCY`  — 1000 is the number of milliseconds, if frequency was 20, it would return 50, as in, it should run every 50ms.

This behavior makes sure that we don't run the `server_cron` method too often, it effectively gives a higher priority to handling client commands, and new clients connecting.

#### Select timeout

When we introduced `IO.select` in [Chapter 3][chapter-3-select], we used it without the timeout argument. This wasn't a problem then because the server had nothing else to do. It would either need to accept a new client, or reply to a client command, and both would be handled through `select`.

The server needs to do something else beside waiting on `select` now, run the time events when they need to be processed. In order to do so, Redis uses a timeout with its abstraction over select and other multiplexing libraries, `aeApiPoll`. Redis can, under some conditions, use no timeout, which we're going to ignore for now, for the sake of simplicity. When using a timeout, Redis makes sure that waiting on the timeout will delay any future events that should be processed instead of waiting on `select`. In order to achieve this, Redis looks at all the time events and finds the nearest one, and sets a timeout equivalent to the time between now and that event. This guarantees that even if there's no activity between now and when the next time event should be processed, redis will stop waiting on `aeApiPoll` and process the time event.

We're replicating this logic in the `select_timeout` method. It starts by delegating the task of finding the nearest time event through the `nearest_time_event`, which iterates through all the time events in the `@time_events` array and find the one with the smallest value for `process_at`.

#### Parsing options

Probably one of the most complicated changes introduced in this chapter, at least for me as I was implementing it. The logic is in the `SetCommand` class. We first define all the possible options in the `OPTIONS` constant. Each option a key/value pair where the key is the option as expected in the command string and the value is an instance of `CommandOption` or `CommandOptionWithValue`. These two structs allow us to declare what the possible options are and to write the code that parses the input string in a fairly generic manner, in `parse_options`. After extracting and validating the first three elements of the string, respectively, the `SET` string, followed by the key and the value, we split the rest on spaces and process them from left to right, with the `shift` method. For every option we find, we look up the `OPTIONS` hash to retrieve the matching `CommandOption` or `CommandOption` with value class. If nil is returned, it means that the given option is invalid, this is a syntax error. Note that once again, for the sake of simplicity, we did not implement case insensitive command the way Redis does.

If the an option is found, but we had already found one of the same kind, `presence` or `expire`, this is also a syntax error. This check allows us to consider the following commands as invalid:

```
SET key value EX 1 PX 2
SET key value EX 1 KEEPTTL
SET key value NX XX
```

Finally, we attempt to parse the option argument, if necessary, only `EX` and `PX` have an argument, the others one do not, this is why we use two different classes here. `parse_option_arguments` will return the option itself if we found an option that should not be followed by an argument, that is either `NX`, `XX` or `KEEPTTL`. If we found one of the other two options, `option_detail` will be an instance of `CommandOptionWithValue`, we use `shift` once again to obtain the next element in the command string and feed it to the validator block.

The validator blocks are very similar for the options, they both validate that the string is a valid integer, but the `EX` validator multiplies the final result by 1000 to convert the value from seconds to milliseconds.

The values are then stored in the `@options` hash, with either the `presence` or `expire` key, based on the `kind` value. This allows us to read from the `@options` hash in the call method to apply the logic require to finalize the implementation of these options.

If `@options['presence']` is set to `NX` and there is already a value at the same key, we return `nil` right away. Similarly if it is set to `XX` and there is no key, we also return nil.

Finally, we always set the value for the key, but the behavior regarding the secondary hash, `@expires` is different depending on the value of `@options['expire']`. If it is set to an integer, we use this integer and as it to the current time, in milliseconds, in the `@expires` hash. If the value is nil, it means that `KEEPTTL` was not passed, so we remove any value that may have previously been set by a previous `SET` command with the same key and value for either `PX` or `EX`.

Note that the process of parsing more complex expressions is more complicated than what we're doing here, and can involve multiple steps, such as [lexing before parsing][wikipedia-syntax]. Given the simplicity of the `SET` command we can get away with our simpler approach here.

**Why not use a regular expression?**

Good question! The answer is that after spending some time trying to implement this as a regular expression, it did not feel easier, as a reference this where I got, just before I gave up:

``` ruby
/^SET \d+ \d+ (?:EX (?<ex>\d+)|PX (?<px>\d+)|KEEPTTL)?(:? ?(?<nx-or-xx>NX|XX))?(?: (?:EX (?<ex>\d+)|PX (?<px>\d+)))?$/
```

This regexp works for some cases but incorrectly considers the following as valid:

```
SET 1 2 KEEPTTL XX EX 1
```

It _might_ be possible to use a regular expression here, given that the grammar of the `SET` command does not allow that many permutations but even if it is, I don't think it'll be simpler than the solution we ended up with.

#### Lazy evictions

The `server_cron` time event takes care of cleaning up expired key every 100ms, but we also want to implement the "lazy eviction", the same way Redis does. That is, if `server_cron` hasn't had the chance to evict an expired key yet, and the server receives a `GET` command for the same key, we want to return nil and evict the key instead of returning it.

This logic is implemented in the `GetCommand` class [FINISH ME]

#### Logger

[FINISH ME]

### And a few more tests

We changed a lot of code and added more features, this calls for more tests. Here is the new tests.

We added a special instruction, `sleep <duration>` to allow us to easily write tests for the `SET` command with any of the expire based options. For instance, to test that `SET key value PX 100` actually works as expected, we want to wait at least 100ms, and assert that `GET key` returns `(nil)` instead of `value`.

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
          assert_equal expected_result + "\n", response
        ensure
          socket.close if socket
        end
      end
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

``` bash
SET key value EX 200
```

And boom! It crashes!

``` bash
Error: Protocol error, got "(" as reply type byte
```

This is because `RedisServer` does not implement the [Redis Protocol][redis-protocol]. This is what the next chapter. At the end of chapter 5 we will be able to use `redis-cli` against our own server. Exciting!

## Appendix: Links to the Redis source code

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



[chapter-2]:/post/chapter-2-respond-to-get-and-set/
[chapter-3-select]:/post/chapter-3-multiple-clients/#lets-use-select
[redis-protocol]:https://redis.io/topics/protocol
[redis-doc-set]:https://redis.io/commands/set
[redis-doc-setnx]:https://redis.io/commands/setnx
[redis-doc-setex]:https://redis.io/commands/setex
[redis-doc-psetex]:https://redis.io/commands/psetex
[redis-doc-ttl]:http://redis.io/commands/ttl
[redis-doc-event-loop]:https://redis.io/topics/internals-rediseventlib
[redis-doc-keys]:https://redis.io/commands/keys
[redis-source-server-cron]:https://github.com/antirez/redis/blob/6.0/src/server.c#L1826-L1843[
[big-o-notation]:https://en.wikipedia.org/wiki/Big_O_notation
[redis-doc-ae]:https://redis.io/topics/internals-rediseventlib
[wikipedia-syntax]:https://en.wikipedia.org/wiki/Syntax_(programming_languages)
