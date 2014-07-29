{EventEmitter} = require 'events'
redis = require 'redis'
{Set} = require 'simplesets'
weak = require 'weak'

class NotificationMonitor extends EventEmitter

  constructor: (notificationCenter)->
    @notificationCenter = notificationCenter
    @channels = new Set()
    @ref = weak(this, (obj)-> obj._cleanup())

  addChannel: (channel)->
    if not @channels.has(channel)
      @channels.add(channel)
      @notificationCenter._addChannelMonitor(channel, @ref)

  removeChannel: (channel)->
    @channels.remove(channel)
    @notificationCenter._removeChannelMonitor(channel, @ref)

  handleChannelNotification: (channel, key) ->
    @emit("notification", channel, key) 

  close: ->
    for channel in @channels.array()
      @notificationCenter._removeChannelMonitor(channel, @ref)

  _cleanup: ->
    @close()

class WaitingNotificationMonitor extends NotificationMonitor

  constructor: (notificationCenter)->
    super(notificationCenter)
    @waitingChannels = {}
    @waitingTokens = {}
    @on 'notification', (channel, key) -> 
      @emit("#{channel}/#{key}", channel, key) 

  waitForNotification: (channel, key, timeout, cb) ->

    obj = {}

    waiting = @waitingChannels[channel] ||= new Set()

    obj.cleanup = =>
      clearTimeout(obj.token)
      waiting.remove(obj.token)
      delete @waitingTokens[obj.token]
      if waiting.size() == 0
        @removeChannel(channel)
      @removeListener("#{channel}/#{key}", obj.handler) # will this throw error

    obj.handler = (channel, key) -> 
      obj.cleanup()
      cb(null, [channel, key])

    obj.timeout = -> 
      obj.cleanup()
      cb(new Error("timeout"), null)

    @once("#{channel}/#{key}", obj.handler)
    obj.token = setTimeout(obj.timeout, timeout)
    @waitingTokens[obj.token] = obj 
    
    if waiting.size() == 0
      waiting.add(channel)
      @addChannel(channel)

    return obj.token

    cancelWaitForNotification: (token)->
      obj = @waitingTokens[token]  
      if obj?
        obj.cleanup()


class DistributedNotificationCenter extends EventEmitter

  constructor: ({redisFactory})->
    @infoExpireAfter = 10
    @waitMonitor = new WaitingNotificationMonitor(this)
  
    @channels = {}
    @pub = redisFactory()
    @sub = redisFactory()
    @sub.on 'message', (channel, message) => 
      monitors = @channels[channel]
      if monitors
        for monitor in monitors.array()
          if monitor.handleChannelNotification?
            monitor.handleChannelNotification(channel, message)
          else
            console.log("monitor cant handle notification, removing")
            monitors.remove(monitor)

  _addChannelMonitor: (channel, monitor)->
    monitors = @channels[channel] ||= new Set()
    monitors.add(monitor)
    if monitors.size() == 1
      @sub.subscribe(channel)

  _removeChannelMonitor: (channel, monitor)->
    monitors = @channels[channel]
    if not monitors
      return
    monitors.remove(monitor)
    if monitors.size() == 0
      delete @channels[channel]
      @sub.unsubscribe(channel)

  _infoKey: (channel, key)->
    "notifications/#{channel}/#{key}/info"

  postNotification: (channel, key, info)->
    multi = @pub.multi()
    infoKey = @_infoKey(channel, key) 
    multi.hmset(infoKey, info)
    multi.expire(infoKey, @infoExpireAfter) # keep event for an hour
    multi.publish(channel, key)
    multi.exec((res, err)->
      # if we get an error should at least print it out
    )

  checkForNotification: (channel, key, cb)->
    infoKey = @_infoKey(channel, key) 
    @pub.hgetall(infoKey, cb)

  waitForNotification: (channel, key, timeout, cb)->
    @waitMonitor.waitForNotification(channel, key, timeout, cb)

  cancelWaitForNotification: (token)->
    @waitMonitor.cancelWaitForNotification(token)

  newMonitor: ->
    obj = new NotificationMonitor(this)
    return obj 

test = -> 

  set = new Set()
  ff = "ff"
  set.add(ff)
  set.add("x")
  set.add("f")
  console.log(set.array(), set.has(ff), set.has("x"), set.has("no"))

  redisFactory = ->
    redis.createClient()

  dsc = new DistributedNotificationCenter(redisFactory: redisFactory)

  monitor = dsc.newMonitor()
  monitor.addChannel("test")

  monitor.on('notification', (channel, message)->
    console.log("got notification", channel, message)
    )

  dsc.waitForNotification('test2', 'foo', 3000, (err, res)->
    console.log("waitForNotification callback", res, err)
  )
  dsc.waitForNotification('test2', 'foo', 3000, (err, res)->
    console.log("waitForNotification callback2", res, err)
  )

  dsc.checkForNotification('test', 'foo', (err, res)->
    console.log("checkForNotification callback", res, err)
  )

  dsc.postNotification('test', 'foo2', {bar: "baz", x: 1})
  
  setTimeout(->

      dsc.postNotification('test2', 'foo', {bar: "baz", x: 1})
      dsc.postNotification('test2', 'foo', {bar: "baz", x: 1})
      dsc.postNotification('test2', 'foo', {bar: "baz", x: 1})

      dsc.waitForNotification('test2', 'foo', 3000, (err, res)->
        console.log("waitForNotification callback", res, err)
      )

      dsc.checkForNotification('test', 'foo', (err, res)->
        console.log("checkForNotification callback", res, err); 
      )

      dsc.postNotification('test', 'foo3', {bar: "baz", x: 1})
      
      setTimeout(->
          monitor.removeChannel('test')
          dsc.postNotification('test', 'foo4', {bar: "baz", x: 1})
          dsc.postNotification('test1', 'foo1', {bar: "baz", x: 1})
          monitor.addChannel('test22')

          setTimeout(->
              monitor = null 
              setTimeout(->
                  dsc.postNotification('test22', 'foo1', {bar: "baz", x: 1})
                  if global.gc?
                    global.gc()
                ,1000)

            ,1000)

        , 1000)

    , 1000)


main = ()->
    test()

if require.main == module
    main()


module.exports = DistributedNotificationCenter