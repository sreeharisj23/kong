local semaphore = require "ngx.semaphore"


local ngx = ngx
local kong = kong
local math = math
local type = type
local pcall = pcall
local table = table
local string = string
local select = select
local unpack = unpack
local assert = assert
local setmetatable = setmetatable


local LOG_WAIT = 60
local BUCKET_SIZE = 1000
local QUEUE_SIZE = 100000


local RECURRING = {
  second = 1,
  minute = 60,
  hour = 3600,
  day = 86400,
  week = 604800,
  month = 2629743.833,
  year = 31556926,
}


local function get_pending(self)
  local head = self.head
  local tail = self.tail
  if head < tail then
    head = head + QUEUE_SIZE
  end
  return head - tail
end


local function job_thread(self, index)
  while not ngx.worker.exiting() do
    local ok, err = self.work:wait(1)
    if ok then
      if self.head ~= self.tail then
        local tail = self.tail == QUEUE_SIZE and 1 or self.tail + 1
        local job = self.jobs[tail]
        self.tail = tail
        self.jobs[tail] = nil
        self.running = self.running + 1
        self.time[tail][2] = ngx.now() * 1000
        ok, err = job()
        self.time[tail][3] = ngx.now() * 1000
        self.running = self.running - 1
        if self.counter < QUEUE_SIZE then
          self.counter = self.counter + 1
        end
        if not ok then
          self.errored = self.errored + 1
          kong.log.err("async thread #", index, " job error: ", err)
        end
      end

    elseif err ~= "timeout" then
      kong.log.err("async thread #", index, " wait error: ", err)
    end
  end

  return true
end


local function log_thread(self)
  local waited = 0

  local debug  = QUEUE_SIZE / 1000
  local info   = QUEUE_SIZE / 100
  local notice = QUEUE_SIZE / 10
  local warn   = QUEUE_SIZE

  while not ngx.worker.exiting() do
    ngx.sleep(1)
    waited = waited + 1
    if waited == LOG_WAIT then
      waited = 0
      local pending = get_pending(self)

      local msg = string.format("async jobs: %u running, %u pending, %u errored, %u refused",
                                self.running, pending, self.errored, self.refused)
      if pending <= debug then
        kong.log.debug(msg)
      elseif pending <= info then
        kong.log.info(msg)
      elseif pending <= notice then
        kong.log.notice(msg)
      elseif pending < warn then
        kong.log.warn(msg)
      else
        kong.log.err(msg)
      end
    end
  end

  return true
end


local function init_worker_timer(premature, self)
  if premature then
    return true
  end

  local t = kong.table.new(101, 0)

  for i = 1, 100 do
    t[i] = ngx.thread.spawn(job_thread, self, i)
  end

  t[101] = ngx.thread.spawn(log_thread, self)

  local ok, err = ngx.thread.wait(t[1],  t[2],  t[3],  t[4],  t[5],  t[6],  t[7],  t[8],  t[9],  t[10],
                                  t[11], t[12], t[13], t[14], t[15], t[16], t[17], t[18], t[19], t[20],
                                  t[21], t[22], t[23], t[24], t[25], t[26], t[27], t[28], t[29], t[30],
                                  t[31], t[32], t[33], t[34], t[35], t[36], t[37], t[38], t[39], t[40],
                                  t[41], t[42], t[43], t[44], t[45], t[46], t[47], t[48], t[49], t[50],
                                  t[51], t[52], t[53], t[54], t[55], t[56], t[57], t[58], t[59], t[60],
                                  t[61], t[62], t[63], t[64], t[65], t[66], t[67], t[68], t[69], t[70],
                                  t[71], t[72], t[73], t[74], t[75], t[76], t[77], t[78], t[79], t[80],
                                  t[81], t[82], t[83], t[84], t[85], t[86], t[87], t[88], t[89], t[90],
                                  t[91], t[92], t[93], t[94], t[95], t[96], t[97], t[98], t[99], t[100],
                                  t[101])

  if not ok then
    kong.log.err("async thread error: ", err)
  end

  for i = 101, 1, -1 do
    ngx.thread.kill(t[i])
  end

  return init_worker_timer(ngx.worker.exiting(), self)
end


local function every_timer(premature, self, delay)
  if premature then
    return true
  end

  local bucket = self.buckets[delay]
  for i = 1, bucket.head do
    local ok, err = bucket.jobs[i](self)
    if not ok then
      kong.log.err(err)
    end
  end

  return true
end


local function create_job(func, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, ...)
  local argc = select("#", ...)
  local args = argc > 0 and { ... }

  if not args then
    return function()
      return pcall(func, ngx.worker.exiting(), a1, a2, a3, a4, a5, a6, a7, a8, a9, a10)
    end
  end

  return function()
    local pok, res, err = pcall(func, ngx.worker.exiting(), a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, unpack(args, 1, argc))
    if not pok then
      return nil, res
    end

    if not err then
      return true
    end

    return nil, err
  end
end


local function queue_job(self, recurring, func, ...)
  local pending = get_pending(self)
  if pending == QUEUE_SIZE then
    self.refused = self.refused + 1
    return nil, "async queue is full"
  end

  self.head = self.head == QUEUE_SIZE and 1 or self.head + 1
  self.jobs[self.head] = recurring and func or create_job(func, ...)
  self.time[self.head][1] = ngx.now() * 1000
  self.work:post()

  return true
end


local function create_recurring_job(job)
  local running = false

  local recurring_job = function()
    running = true
    local ok, err = job()
    running = false
    return ok, err
  end

  return function(self)
    if running then
      return nil, "recurring job is already running"
    end

    return queue_job(self, true, recurring_job)
  end
end


local function get_stats(self, from, to)
  local data, count = self:data(from, to)

  local max_latency
  local min_latency
  local mean_latency
  local median_latency
  local p95_latency
  local p99_latency
  local max_runtime
  local min_runtime
  local mean_runtime
  local median_runtime
  local p95_runtime
  local p99_runtime

  if count == 1 then
    local queued  = data[1][1]
    local started = data[1][2]
    local ended   = data[1][3]

    local latency = started - queued
    local runtime = ended - started

    max_latency    = latency
    min_latency    = latency
    mean_latency   = latency
    median_latency = latency
    p95_latency    = latency
    p99_latency    = latency
    max_runtime    = runtime
    min_runtime    = runtime
    mean_runtime   = runtime
    median_runtime = runtime
    p95_runtime    = runtime
    p99_runtime    = runtime

  elseif count > 1 then
    local tot_latency = 0
    local tot_runtime = 0

    local latencies = kong.table.new(count, 0)
    local runtimes  = kong.table.new(count, 0)

    for i = 1, count do
      local queued  = data[i][1]
      local started = data[i][2]
      local ended   = data[i][3]

      local latency = started - queued
      tot_latency   = latency + tot_latency
      latencies[i]  = latency
      max_latency   = math.max(latency, max_latency or latency)
      min_latency   = math.min(latency, min_latency or latency)

      local runtime = ended - started
      tot_runtime   = runtime + tot_runtime
      runtimes[i]   = runtime
      max_runtime   = math.max(runtime, max_runtime or runtime)
      min_runtime   = math.min(runtime, min_runtime or runtime)
    end

    mean_latency = math.floor(tot_latency / count + 0.5)
    mean_runtime = math.floor(tot_runtime / count + 0.5)

    table.sort(latencies)
    table.sort(runtimes)

    local median_index = count / 2
    local p95_index = 0.95 * count
    local p99_index = 0.99 * count

    if median_index == math.floor(median_index) then
      median_latency = math.floor((latencies[median_index] + latencies[median_index + 1]) / 2 + 0.5)
      median_runtime = math.floor(( runtimes[median_index] +  runtimes[median_index + 1]) / 2 + 0.5)
    else
      median_index = math.floor(median_index + 0.5)
      median_latency = latencies[median_index]
      median_runtime = runtimes[median_index]
    end

    if p95_index == math.floor(p95_index) then
      p95_latency = math.floor((latencies[p95_index] + latencies[p95_index + 1]) / 2 + 0.5)
      p95_runtime = math.floor(( runtimes[p95_index] +  runtimes[p95_index + 1]) / 2 + 0.5)

    else
      p95_index   = math.floor(p95_index + 0.5)
      p95_latency = latencies[p95_index]
      p95_runtime = runtimes[p95_index]
    end

    if p99_index == math.floor(p99_index) then
      p99_latency = math.floor((latencies[p99_index] + latencies[p99_index + 1]) / 2 + 0.5)
      p99_runtime = math.floor(( runtimes[p99_index] +  runtimes[p99_index + 1]) / 2 + 0.5)

    else
      p99_index   = math.floor(p99_index + 0.5)
      p99_latency = latencies[p99_index]
      p99_runtime = runtimes[p99_index]
    end
  end

  return {
    latency = {
      mean   = mean_latency   or 0,
      median = median_latency or 0,
      p95    = p95_latency    or 0,
      p99    = p99_latency    or 0,
      max    = max_latency    or 0,
      min    = min_latency    or 0,
    },
    runtime = {
      mean   = mean_runtime   or 0,
      median = median_runtime or 0,
      p95    = p95_runtime    or 0,
      p99    = p99_runtime    or 0,
      max    = max_runtime    or 0,
      min    = min_runtime    or 0,
    },
  }
end


local async = {}


async.__index = async


---
-- Creates a new instance of `kong.async`
--
-- @treturn table an instance of `kong.async`
function async.new()
  local time = kong.table.new(QUEUE_SIZE, 0)
  for i = 1, QUEUE_SIZE do
    time[i] = kong.table.new(3, 0)
  end

  return setmetatable({
    jobs = kong.table.new(QUEUE_SIZE, 0),
    time = time,
    work = semaphore.new(),
    buckets = {},
    running = 0,
    errored = 0,
    refused = 0,
    counter = 0,
    head = 0,
    tail = 0,
  }, async)
end


---
-- Initializes `kong.async` timers
--
-- @treturn boolean|nil `true` on success, `nil` on error
-- @treturn string|nil  `nil` on success, error message `string` on error
function async:init_worker()
  local ok, err = ngx.timer.at(0, init_worker_timer, self)
  if not ok then
    return nil, err
  end

  return true
end


---
-- Run a function asynchronously
--
-- @tparam  function   a function to run asynchronously
-- @tparam  ...[opt]   function arguments
-- @treturn true|nil   `true` on success, `nil` on error
-- @treturn string|nil `nil` on success, error message `string` on error
function async:run(func, ...)
  return queue_job(self, false, func, ...)
end

---
-- Run a function repeatedly but non-overlapping
--
-- @tparam  number|string function execution interval (a non-zero positive number
--                        or `"second"`, `"minute"`, `"hour"`, `"month" or `"year"`)
-- @tparam  function      a function to run asynchronously
-- @tparam  ...[opt]      function arguments
-- @treturn true|nil      `true` on success, `nil` on error
-- @treturn string|nil    `nil` on success, error message `string` on error
function async:every(delay, func, ...)
  delay = RECURRING[delay] or delay

  assert(type(delay) == "number" and delay > 0, "invalid delay, must be number greater than zero or " ..
                                                "'second', 'minute', 'hour', 'month' or 'year'")

  local bucket = self.buckets[delay]
  if bucket then
    if bucket.head == BUCKET_SIZE then
      self.refused = self.refused + 1
      return nil, "async bucket (" .. delay .. ") is full"
    end

  else
    local ok, err = ngx.timer.every(delay, every_timer, self, delay)
    if not ok then
      return nil, err
    end

    bucket = {
      jobs = kong.table.new(BUCKET_SIZE, 0),
      head = 0,
      delay = delay,
    }

    self.buckets[delay] = bucket
  end

  bucket.head = bucket.head + 1
  bucket.jobs[bucket.head] = create_recurring_job(create_job(func, ...))

  return true
end


---
-- Kong async raw metrics data
--
-- @tparam  from[opt]  data start time (from unix epoch)
-- @tparam  to[opt]    data end time (from unix epoch)
-- @treturn table      a table containing the metrics
-- @treturn number     number of metrics returned
function async:data(from, to)
  local time = self.time
  local counter = self.counter
  if not from and not to then
    return time, counter
  end

  from = from and from * 1000 or 0
  to   = to   and to   * 1000 or math.huge

  local filtered = kong.table.new(counter, 0)
  local count = 0
  for i = 1, self.counter do
    if time[i][1] >= from and time[i][3] <= to then
      count = count + 1
      filtered[count] = time[i]
    end
  end

  return filtered, count
end


---
-- Return statistics
--
-- @tparam  opts[opt] data start time (from unix epoch)
-- @treturn table     a table containing calculated statistics
function async:stats(opts)
  local stats
  local pending = get_pending(self)
  if not opts then
    stats = get_stats(self)
    stats.pending  = pending
    stats.running  = self.running
    stats.errored  = self.errored
    stats.refused  = self.refused

  else
    local now = ngx.now()

    local all    = opts.all    and get_stats(self)
    local minute = opts.minute and get_stats(self, now - 60)
    local hour   = opts.hour   and get_stats(self, now - 3600)

    stats = {
      pending  = pending,
      running  = self.running,
      errored  = self.errored,
      refused  = self.refused,
      latency  = {
        all    = all.latency,
        hour   = hour.latency,
        minute = minute.latency,
      },
      runtime  = {
        all    = all.runtime,
        hour   = hour.runtime,
        minute = minute.runtime,
      },
    }
  end

  return stats
end


return async
