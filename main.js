
const Bitfinex = require('./api-bitfinex');
const Binance = require('./api-binance');

const HOUR_1 = (1000 * 60 * 60 * 1); // milliseconds

const TIME_NOW = Date.now();
const ONE_WEEK_AGO = TIME_NOW - (HOUR_1 * 24 * 7);

(async() => {


let symbol = 'BTCUSD';
let timeframe = '1d';

console.log(`Pulling '${symbol}:${timeframe}' data from FINEX...`);

// Runs repeatedly until all data is retrieved
await Bitfinex.range(ONE_WEEK_AGO, TIME_NOW, symbol, timeframe, ( candles ) =>
{

  for (let candle of candles)
      console.log( candle );

});



symbol = 'ETHUSDT'; // tether market on binance

console.log(`Pulling '${symbol}:${timeframe}' data from BINANCE...`);

// Runs repeatedly until all data is retrieved
await Binance.range(ONE_WEEK_AGO, TIME_NOW, symbol, timeframe, ( candles ) =>
{

  for (let candle of candles)
      console.log( candle );

});



})();
