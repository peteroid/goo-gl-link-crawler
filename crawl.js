var redis = require('redis')
var fetch = require('node-fetch')
var Promise = require('bluebird')

var client = redis.createClient()
var counts = {
  total: 0,
  success: 0
}

const tryInterval = 500
var shouldBeWaiting = false

const ALL_CHARS = '1234567890qwertyuiopasdfghjklzxcvbnmPOIUYTREWQASFGHJKLZXCVBNM'
String.prototype.randomize = function (n) {
  var chars = this.split('')
  return Array.apply(null, Array(n)).map(_ => {
    return chars[(Math.random() * chars.length) | 0]
  }).join('')
}

client.on('connect', (err, data) => {
  console.log(err || 'connected')
  setInterval(tryRandomLink, tryInterval)
})

function tryRandomLink () {
  if (shouldBeWaiting) return
  var nextToken = ALL_CHARS.randomize(6)
  var nextKey = toRedisKey(nextToken)
  client.exists(nextKey, (err, exists) => {
    if (!exists) {
      tryGetLinkAndSave(nextToken)
    } else {
      client.type(nextKey, (err, valueType) => {
        if (err) {
          console.log(err)
        } else {
          if (valueType == 'string') {
            console.log('%s already checked. skip.', nextToken)
            // console.log('recorded as %s. retry now', value)
          } else if (valueType == 'hash') {
            console.log('ready crawled. skip.')
          }
        }
      })
    }
  })
}

function tryGetLinkAndSave (token) {
  counts.total++
  getLinkJson(token).then(json => {
    if (json['error_message']) {
      recordFailToken(token, json['error_message'])
    } else {
      saveValidJsonToRedis(token, json)
    }
  }, console.log)
}

function getLinkJson (token) {
  return new Promise((resolve, reject) => {
    fetch("https://goo.gl/api/analytics", {
      method: 'POST',
      headers: {
        "Content-Type": "application/x-www-form-urlencoded"
      },
      body: 'security_token&url=http%3A%2F%2Fgoo.gl%2F' + (token || 'BKGdcm')
    }).then(res => {
        return res.json()
      }).then(resolve).catch(reject)
  })
}

function recordFailToken (token, errMsg) {
  if (!client.connected) return
  if (errMsg == 'Quota exceeded') {
    var sec = 30 + parseInt(Math.random() * 45)
    console.log('no quota. wait for %ss', sec)
    waitForSeconds(sec)
  }
  var key = toRedisKey(token)
  client.set(key, errMsg, function (err) {
    if (err) {
      console.log(err)
    } else {
      console.log("Fail on %s : %s", token, errMsg)
    }
  })
}

function saveValidJsonToRedis (token, json) {
  if (!client.connected) return
  var allTimeClicks = json.details['all time'].clicks
  var shortUrlClicks = parseInt(allTimeClicks['short_url'])
  var longUrlClicks = parseInt(allTimeClicks['long_url'])
  var key = toRedisKey(token)
  client.hmset(key, {
    raw: JSON.stringify(json),
    shortUrlClicks: shortUrlClicks,
    longUrlClicks: longUrlClicks,
    clicks: shortUrlClicks < longUrlClicks ? longUrlClicks : shortUrlClicks
  }, (err, data) => {
    counts.success++
    var rate = parseInt(counts.success / counts.total * 1000) / 100
    console.log(err || key + ' digged! rate: ' + rate + '%')
    console.log(json['long_url'])
  })
}

function waitForSeconds (sec) {
  shouldBeWaiting = true
  setTimeout(function () {
    shouldBeWaiting = false
  }, sec * 1000)
}

function toRedisKey (token) {
  return '_goo_gl_' + token
}