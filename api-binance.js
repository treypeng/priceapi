
// -------------
// binance-api.js
// -------------
//
// Helper module to grab data from Binance.
//
// Binance returns an array of arrays:
/*
[
  [
    1499040000000,      // Open time
    "0.01634790",       // Open
    "0.80000000",       // High
    "0.01575800",       // Low
    "0.01577100",       // Close
    "148976.11427815",  // Volume
    1499644799999,      // Close time
    "2434.19055334",    // Quote asset volume
    308,                // Number of trades
    "1756.87402397",    // Taker buy base asset volume
    "28.46694368",      // Taker buy quote asset volume
    "17928899.62484339" // Ignore.
  ]
]
*/


const F_TIME=0, F_OPEN=1, F_CLOSE=4, F_HIGH=2, F_LOW=3, F_VOL=5;

//start=1514764800000&sort=1
const URL = 'https://api.binance.com/api/v1'
const VERB = 'klines';

const API_DELAY = 2000;  // Binance is very generous and can go much faster but let's play nice
const PAGE_SIZE = 1000;

const DEFAULT_SYM = 'ADABTC';
const DEFAULT_INTERVAL = '1d';

// require('./CANDLE_SPAN_LUT.js');

let CANDLE_SPAN_MS = [];
CANDLE_SPAN_MS['8h'] = 8 * 60 * 60 * 1000;  // minutes * seconds * miliseconds
CANDLE_SPAN_MS['4h'] = 4 * 60 * 60 * 1000;  // minutes * seconds * miliseconds
CANDLE_SPAN_MS['6h'] = 6 * 60 * 60 * 1000;  // minutes * seconds * miliseconds
CANDLE_SPAN_MS['1d'] = 24 * 60 * 60 * 1000;  // minutes * seconds * miliseconds
CANDLE_SPAN_MS['1h'] = 60 * 60 * 1000;  // minutes * seconds * miliseconds
CANDLE_SPAN_MS['15m'] = 15 * 60 * 1000;
CANDLE_SPAN_MS['30m'] = 30 * 60 * 1000;
CANDLE_SPAN_MS['5m'] = 5 * 60 * 1000;
CANDLE_SPAN_MS['1m'] = 1 * 60 * 1000;

// let SYMBOL_CLEANER = [];
// SYMBOL_CLEANER['tETHUSD'] = 'ethusd';
// SYMBOL_CLEANER['tBTCUSD'] = 'btcusd';
// SYMBOL_CLEANER['tBCHBTC'] = 'bchbtc';

const LOGGING = true;

const fetch = require('node-fetch');

// start_time | end_time; Date() objects

// Get historical candle data from Bitmex in the given time range
exports.range = async function(start_time_ms, end_time_ms, symbol, interval, data_callback)
{
  if (!symbol || !interval) throw Error("symbol and interval required");

  // At this juncture, end_time_ms should be aligned to the requested interval
  // Since binance end dates are inclusive, simply chip 1 ms of the figure
  end_time_ms--;

  let start_time = new Date(start_time_ms);
  let end_time = new Date(end_time_ms);


  // This function's symbol parameter should be of the normalised form 'ethusd'
  // instead of finex specific 'tETHUSD'

  // SYMBOL_CLEANER table is used to LUT between the two. (At the end of
  // data acquisition the API is obviously in finex format 'tETHUSD'
  // // so the table fixes that too
  //
  // for (let s in SYMBOL_CLEANER)
  // {
  //   if (symbol == SYMBOL_CLEANER[s])
  //   {
  //     symbol = s; break;
  //   }
  // }

  // console.log((new Date(end_time)).toISOString());
  // Bitfinex returns open (yet to close) candles by default if you request endtime = Date.now()
  // this is bad if we're building a historical database as the candle will most likely
  // change (o)hlc before closing and won't be overwritten by my code here at a later date.
  // so snap the end time to the nearest closed candle
  // God this shit is a messy pain in the arse.

  //FIXME: removed this for binance, needed?
  // end_time = quantise_end_time(end_time, interval);

  // console.log((new Date(end_time)).toISOString());

  let timespan_ms = CANDLE_SPAN_MS[interval];

  // We're getting [interval] candles, find out how many there are between the two dates
  let candles = Math.round(Math.abs(start_time.getTime() - end_time.getTime()) / timespan_ms);


  let remainder = candles % PAGE_SIZE;
  let num_pages = (candles - remainder) / PAGE_SIZE;
  if (remainder) num_pages++;

  if (candles <= PAGE_SIZE) num_pages = 1;

  let bin_size = interval;
  let urls = [];



  let miliseconds_per_page = PAGE_SIZE * timespan_ms;


  // console.log(`************\n => PAGES: ${num_pages}, ETA: ${((num_pages * (2500 + API_DELAY))/(1000*60))<<0} minutes\n************`);

  // Now generate a list of urls to call in sequence
  for (let p=0; p<num_pages; p++)
  {
    let start_offset = start_time.getTime() + (p * miliseconds_per_page);

    // - timespan_ms: minus one candle. The time spans past to Finex are *inclusive*
    let end_offset = start_offset + miliseconds_per_page - timespan_ms ;

    // this is the last page, make sure we get the last calculated candle
    if (p == num_pages-1) {
      end_offset += timespan_ms;
    }

    if (end_offset > end_time.getTime())
    end_offset = end_time.getTime();

    // console.log(((new Date(start_offset)).toISOString()) + " -> " + ((new Date(end_offset)).toISOString()));

    // https://api.binance.com/api/v1/klines?symbol=FUNBTC&interval=1d&startTime=1506531600000

    // let url =`${URL}/${VERB}/trade:${bin_size}:${symbol}/hist?start=${start_offset}&end=${end_offset}&sort=1&limit=${PAGE_SIZE}`;
    let url =`${URL}/${VERB}?symbol=${symbol}&interval=${bin_size}&startTime=${start_offset}&endTime=${end_offset}&limit=${PAGE_SIZE}`;
    // conso
    // console.log(url)
    // console.log((new Date(end_offset)).toISOString());
    // let url = `${URL}/${VERB}?binSize=${bin_size}&partial=false&symbol=${BITMEX_SYMBOL}&start=${start}&count=${BITMEX_PAGE_SIZE}&reverse=false&startTime=${start_time_iso}&endTime=${end_time_iso}`;
    urls.push(url);
  }

  let data_params = {
    exchange: 'binance',
    sym: symbol,
    bin_size: bin_size
  };

  // Make the requests sequentially
  let res = await process_requests(urls, data_params, data_callback);

  // Process the data and flatten the 'pages' also add UNIX timestamp for use later

  return data_callback ? [] : flatten_and_add_unix_timestamp('binance', symbol, bin_size, res);
}

function quantise_end_time(t, interval)
{
  // snaps minutes e.g. 15:41:21.119 ->  15:30:00.000
  let m_granularity = CANDLE_SPAN_MS[interval] / (60 * 1000); // get minutes e.g. 15, 60

  let date1 = new Date(t.getTime());

  let mins = date1.getUTCMinutes();
  let mins_snapped = mins - (mins % m_granularity);
  date1.setUTCMinutes(mins_snapped);
  date1.setUTCSeconds(0);
  date1.setUTCMilliseconds(0);

  // Now move the end time back a whole candle span because
  // end_time in slightly counterintuitive Bitfinex-speak means
  // the final OPEN time of the candle range you're requesting
  // which will mean an unclosed candle.
  return new Date(date1.getTime() - CANDLE_SPAN_MS[interval]);
}


async function process_requests(urls, cb_opts, cb)
{
  let data = [];

  // for (const item of urls)
  for (let t=0; t<urls.length; t++)
  {

    log(`Requesting => ${urls[t]}`);

    let candle_page = await get_data(urls[t])

    // log(`Page ${t}`);

    // console.log(candle_page);

    if (cb)
    {

      cb(flatten_and_add_unix_timestamp(
          cb_opts.exchange, cb_opts.sym, cb_opts.bin_size, [candle_page]
        ));

    } else {
      data.push(candle_page);
    }

    if (t < (urls.length-1))
    {
      log(`Waiting ${API_DELAY} ms...`);
      await delay();
    }

  }

  return data;
}

// Make the actual call here
const get_data = async url => {

    const response = await fetch(url);
    const data = await response.json();
    return data;

// ?   return [];

};

// Helper function, let's not overload mex and get our ass timeed-out
function delay()
{
  return new Promise(resolve => setTimeout(resolve, API_DELAY));
}

function log(text)
{
  if (LOGGING) console.log(text);
}

// Takes array-of-arrays (Mex pages) and turns into single array
function flatten_and_add_unix_timestamp(exch, sym, bin_size, pages)
{
  let candles = [];

  if (!pages) return [];

  for (let p of pages)
  {
    // for each candle in this page
    if (!Array.isArray(p)) continue;

    for (let c of p)
    {
        candles.push({
          exchange: exch,
          symbol: sym,
          interval: bin_size,
          timestamp: c[F_TIME],
          open: Number(c[F_OPEN]),
          high: Number(c[F_HIGH]),
          low: Number(c[F_LOW]),
          close: Number(c[F_CLOSE]),
          volume: Number(c[F_VOL])
        });

      // /candles.push(c);
    }
  }

 return candles;

}
