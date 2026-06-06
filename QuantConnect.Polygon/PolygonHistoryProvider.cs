/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Lean.Engine.HistoricalData;
using QuantConnect.Logging;
using QuantConnect.Util;
using QuantConnect.Data.Consolidators;
using QuantConnect.Configuration;
using System.Collections.Concurrent;

namespace QuantConnect.Lean.DataSource.Polygon
{
    public partial class PolygonDataProvider : MappedSynchronizingHistoryProvider
    {
        private const string MassiveHistoryAdjustSource = "massive";

        private static readonly string HistoryAdjustSource = Config.Get("polygon-history-adjust-source");

        private readonly ConcurrentDictionary<string, IReadOnlyList<DividendResult>> _dividendReferenceCache = new();

        private int _dataPointCount;

        /// <summary>
        /// Indicates whether a error for an invalid start time has been fired, where the start time is greater than or equal to the end time in UTC.
        /// </summary>
        private volatile bool _invalidStartTimeErrorFired;

        /// <summary>
        /// Indicates whether an error has been fired due to invalid conditions if the TickType is <seealso cref="TickType.Quote"/> and the <seealso cref="Resolution"/> is greater than one second.
        /// </summary>
        private volatile bool _invalidTickTypeAndResolutionErrorFired;

        /// <summary>
        /// Gets the total number of data points emitted by this history provider
        /// </summary>
        public override int DataPointCount => _dataPointCount;

        /// <summary>
        /// Initializes this history provider to work for the specified job
        /// </summary>
        /// <param name="parameters">The initialization parameters</param>
        public override void Initialize(HistoryProviderInitializeParameters parameters)
        {
        }

        /// <summary>
        /// Gets the history for the requested security
        /// </summary>
        /// <param name="request">The historical data request</param>
        /// <returns>An enumerable of BaseData points</returns>
        public override IEnumerable<BaseData>? GetHistory(HistoryRequest request)
        {
            if (request.Symbol.IsCanonical() ||
                !IsSupported(request.Symbol.SecurityType, request.DataType, request.TickType, request.Resolution))
            {
                // It is Logged in IsSupported(...)
                return null;
            }

            if (request.TickType == TickType.OpenInterest)
            {
                if (!_unsupportedTickTypeMessagedLogged)
                {
                    _unsupportedTickTypeMessagedLogged = true;
                    Log.Trace($"PolygonDataProvider.GetHistory(): Unsupported tick type: {TickType.OpenInterest}");
                }
                return null;
            }

            // Quote data can only be fetched from Polygon from their Quote Tick endpoint,
            // which would be too slow for anything above second resolution or long time spans.
            if (request.TickType == TickType.Quote && request.Resolution > Resolution.Second)
            {
                if (!_invalidTickTypeAndResolutionErrorFired)
                {
                    _invalidTickTypeAndResolutionErrorFired = true;
                    Log.Error("PolygonDataProvider.GetHistory(): Quote data above second resolution is not supported.");
                }
                return null;
            }

            if (request.EndTimeUtc < request.StartTimeUtc)
            {
                if (!_invalidStartTimeErrorFired)
                {
                    _invalidStartTimeErrorFired = true;
                    Log.Error($"{nameof(PolygonDataProvider)}.{nameof(GetHistory)}:InvalidDateRange. The history request start date must precede the end date, no history returned");
                }
                return null;
            }


            // Use the trade aggregates API for resolutions above tick for fastest results
            if (request.TickType == TickType.Trade && request.Resolution > Resolution.Tick)
            {
                var data = GetAggregates(request);

                if (data == null)
                {
                    return null;
                }

                return data;
            }

            return GetHistoryThroughDataConsolidator(request);
        }

        private IEnumerable<BaseData>? GetHistoryThroughDataConsolidator(HistoryRequest request)
        {
            IDataConsolidator consolidator;
            IEnumerable<BaseData> history;

            if (request.TickType == TickType.Trade)
            {
                consolidator = request.Resolution != Resolution.Tick
                    ? new TickConsolidator(request.Resolution.ToTimeSpan())
                    : FilteredIdentityDataConsolidator.ForTickType(request.TickType);
                history = GetTrades(request);
            }
            else
            {
                consolidator = request.Resolution != Resolution.Tick
                    ? new TickQuoteBarConsolidator(request.Resolution.ToTimeSpan())
                    : FilteredIdentityDataConsolidator.ForTickType(request.TickType);
                history = GetQuotes(request);
            }

            BaseData? consolidatedData = null;
            DataConsolidatedHandler onDataConsolidated = (s, e) =>
            {
                consolidatedData = (BaseData)e;
            };
            consolidator.DataConsolidated += onDataConsolidated;

            foreach (var data in history)
            {
                consolidator.Update(data);
                if (consolidatedData != null)
                {
                    Interlocked.Increment(ref _dataPointCount);
                    yield return consolidatedData;
                    consolidatedData = null;
                }
            }

            consolidator.DataConsolidated -= onDataConsolidated;
            consolidator.DisposeSafely();
        }

        /// <summary>
        /// Gets the trade bars for the specified history request
        /// </summary>
        private IEnumerable<TradeBar> GetAggregates(HistoryRequest request)
        {
            var ticker = _symbolMapper.GetBrokerageSymbol(request.Symbol, true);
            var resolutionTimeSpan = request.Resolution.ToTimeSpan();
            // Aggregates API gets timestamps in milliseconds
            var start = Time.DateTimeToUnixTimeStampMilliseconds(request.StartTimeUtc.RoundDown(resolutionTimeSpan));
            var end = Time.DateTimeToUnixTimeStampMilliseconds(request.EndTimeUtc.RoundDown(resolutionTimeSpan));
            var historyTimespan = GetHistoryTimespan(request.Resolution);

            var massiveDividendAdjusted = IsMassiveDividendAdjusted(request.DataNormalizationMode);
            var splitAdjusted = massiveDividendAdjusted || request.DataNormalizationMode != DataNormalizationMode.Raw;

            var resource = $"v2/aggs/ticker/{ticker}/range/1/{historyTimespan}/{start}/{end}";
            var parameters = new Dictionary<string, string>
            {
                ["adjusted"] = splitAdjusted.ToString()
            };

            var dividendReference = massiveDividendAdjusted ? GetDividendReference(ticker) : null;

            foreach (var bar in RestApiClient.DownloadAndParseData<AggregatesResponse>(resource, parameters)
                                             .SelectMany(response => response.Results))
            {
                var utcTime = Time.UnixMillisecondTimeStampToDateTime(bar.Timestamp);
                var time = GetTickTime(request.Symbol, utcTime);
                var dividendFactor = dividendReference != null
                    ? GetDividendAdjustmentFactor(dividendReference, time.Date)
                    : 1m;
                Interlocked.Increment(ref _dataPointCount);
                yield return new TradeBar(time, request.Symbol,
                    bar.Open * dividendFactor, bar.High * dividendFactor, bar.Low * dividendFactor, bar.Close * dividendFactor,
                    bar.Volume, resolutionTimeSpan);
            }
        }

        private static bool IsMassiveDividendAdjusted(DataNormalizationMode normalizationMode)
        {
            return normalizationMode != DataNormalizationMode.Raw &&
                HistoryAdjustSource.Equals(MassiveHistoryAdjustSource, StringComparison.InvariantCultureIgnoreCase);
        }

        private IReadOnlyList<DividendResult> GetDividendReference(string ticker)
        {
            return _dividendReferenceCache.GetOrAdd(ticker, FetchDividendReference);
        }

        private IReadOnlyList<DividendResult> FetchDividendReference(string ticker)
        {
            var parameters = new Dictionary<string, string> { ["ticker"] = ticker };
            return RestApiClient.DownloadAndParseData<DividendsResponse>("stocks/v1/dividends", parameters)
                .SelectMany(response => response.Results)
                .Where(dividend => dividend.HistoricalAdjustmentFactor.HasValue)
                .OrderBy(dividend => dividend.ExDividendDate)
                .ToList();
        }

        private static decimal GetDividendAdjustmentFactor(IReadOnlyList<DividendResult> dividendReference, DateTime date)
        {
            var nextDividend = dividendReference.FirstOrDefault(dividend => dividend.ExDividendDate.Date > date);
            return nextDividend?.HistoricalAdjustmentFactor ?? 1m;
        }

        /// <summary>
        /// Gets the trade ticks that will potentially be aggregated for the specified history request
        /// </summary>
        private IEnumerable<Tick> GetTrades(HistoryRequest request)
        {
            return GetTicks<TradesResponse, Trade>(request,
                (time, symbol, responseTick) => new Tick(time, request.Symbol, string.Empty, GetExchangeCode(responseTick.ExchangeID),
                    responseTick.Volume, responseTick.Price));
        }

        /// <summary>
        /// Gets the quote ticks that will potentially be aggregated for the specified history request
        /// </summary>
        private IEnumerable<Tick> GetQuotes(HistoryRequest request)
        {
            Tick makeTick<T>(DateTime time, Symbol symbol, T responseTick) where T : Quote =>
                new Tick(time, request.Symbol, string.Empty, GetExchangeCode(responseTick.ExchangeID),
                    responseTick.BidSize, responseTick.BidPrice, responseTick.AskSize, responseTick.AskPrice);

            if (request.Symbol.SecurityType == SecurityType.Option)
            {
                return GetTicks<OptionQuotesResponse, OptionQuote>(request, makeTick);
            }

            return GetTicks<QuotesResponse, Quote>(request, makeTick);
        }

        private IEnumerable<Tick> GetTicks<TResponse, TTick>(HistoryRequest request, Func<DateTime, Symbol, TTick, Tick> tickFactory)
            where TResponse : BaseResultsResponse<TTick>
            where TTick : ResponseTick
        {
            var resolutionTimeSpan = request.Resolution.ToTimeSpan();
            // Trades API gets timestamps in nanoseconds
            var start = Time.DateTimeToUnixTimeStampNanoseconds(request.StartTimeUtc.RoundDown(resolutionTimeSpan));
            var end = Time.DateTimeToUnixTimeStampNanoseconds(request.EndTimeUtc.RoundDown(resolutionTimeSpan));
            var ticker = _symbolMapper.GetBrokerageSymbol(request.Symbol);
            var tickTypeStr = request.TickType == TickType.Trade ? "trades" : "quotes";

            var resource = $"v3/{tickTypeStr}/{ticker}";
            var parameters = new Dictionary<string, string>
            {
                ["timestamp.gte"] = start.ToString(),
                ["timestamp.lt"] = end.ToString(),
                ["order"] = "asc"
            };

            foreach (var tick in RestApiClient.DownloadAndParseData<TResponse>(resource, parameters)
                                             .SelectMany(response => response.Results))
            {
                var utcTime = Time.UnixNanosecondTimeStampToDateTime(tick.Timestamp);
                var time = GetTickTime(request.Symbol, utcTime);
                yield return tickFactory(time, request.Symbol, tick);
            }
        }

        /// <summary>
        /// Converts the given resolution into the corresponding timespan for the Polygon.io API
        /// </summary>
        private static string GetHistoryTimespan(Resolution resolution)
        {
            switch (resolution)
            {
                case Resolution.Daily:
                    return "day";

                case Resolution.Hour:
                    return "hour";

                case Resolution.Minute:
                    return "minute";

                case Resolution.Second:
                    return "second";

                default:
                    throw new Exception($"Unsupported resolution: {resolution}.");
            }
        }
    }
}
