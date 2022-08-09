const express = require('express')
const pg = require('pg')

const app = express()
// configs come from standard PostgreSQL env vars
// https://www.postgresql.org/docs/9.6/static/libpq-envars.html
const pool = new pg.Pool({
  host: "work-samples-db.cx4wctygygyq.us-east-1.rds.amazonaws.com",
  user: "readonly",
  database: "work_samples",
  password: "w2UIO@#bg532!",
  port: 5432
})
//for rate Limiting
var userList = [];
const timeDifference = 2 * 60 * 60 * 1000;  // 2 hour
const requestLimit = 2; //1 limit
var rateLimiting = function (req, res, next) {
  const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
  var currentTime = new Date();
  // new Date(currentTime.getTime()+timeDifference)
  if (!userList[ip]) {
    var obj = {
      requestLimit: requestLimit - 1,
      expireTime: new Date(currentTime.getTime() + timeDifference)
    }
    userList[ip] = obj;
    next()
  }
  else {
    if (currentTime > userList[ip].expireTime) {
      var obj = {
        requestLimit: requestLimit - 1,
        expireTime: new Date(currentTime.getTime() + timeDifference)
      }
      userList[ip] = obj;
      next()
    }
    else{
      if(userList[ip].requestLimit > 0){
        var obj = {
          requestLimit: userList[ip].requestLimit - 1,
          expireTime: userList[ip].expireTime
        }
        userList[ip] = obj;
        next()
      }
      else{
        res.status(429).send(`You have exceeded the ${requestLimit} requests in ${timeDifference/60/60/1000} hrs limit!`);
      }
    }
  }


};
app.use(rateLimiting)



const queryHandler = (req, res, next) => {
  pool.query(req.sqlQuery).then((r) => {
    return res.json(r.rows || [])
  }).catch(next)
}


app.get('/', (req, res) => {
  res.send('Welcome to EQ Works 😎')
})

app.get('/events/hourly', (req, res, next) => {
  req.sqlQuery = `
    SELECT date, hour, events
    FROM public.hourly_events
    ORDER BY date, hour
    LIMIT 168;
  `
  return next()
}, queryHandler)

app.get('/events/daily', (req, res, next) => {
  req.sqlQuery = `
    SELECT date, SUM(events) AS events
    FROM public.hourly_events
    GROUP BY date
    ORDER BY date
    LIMIT 7;
  `
  return next()
}, queryHandler)

app.get('/stats/hourly', (req, res, next) => {
  req.sqlQuery = `
    SELECT date, hour, impressions, clicks, revenue
    FROM public.hourly_stats
    ORDER BY date, hour
    LIMIT 168;
  `
  return next()
}, queryHandler)

app.get('/stats/daily', (req, res, next) => {
  req.sqlQuery = `
    SELECT date,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(revenue) AS revenue
    FROM public.hourly_stats
    GROUP BY date
    ORDER BY date
    LIMIT 7;
  `
  return next()
}, queryHandler)

app.get('/poi', (req, res, next) => {
  req.sqlQuery = `
    SELECT *
    FROM public.poi;
  `
  return next()
}, queryHandler)

app.listen(process.env.PORT || 5555, (err) => {
  if (err) {
    console.error(err)
    process.exit(1)
  } else {
    console.log(`Running on ${process.env.PORT || 5555}`)
  }
})

// last resorts
process.on('uncaughtException', (err) => {
  console.log(`Caught exception: ${err}`)
  process.exit(1)
})
process.on('unhandledRejection', (reason, p) => {
  console.log('Unhandled Rejection at: Promise', p, 'reason:', reason)
  process.exit(1)
})
