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

using Newtonsoft.Json;

namespace QuantConnect.Lean.DataSource.Polygon
{
    /// <summary>
    /// Models a Massive/Polygon-shaped corporate-actions dividends REST API response
    /// </summary>
    public class DividendsResponse : BaseResultsResponse<DividendResult>
    {
    }

    /// <summary>
    /// Models a single dividend record exposing the cumulative historical adjustment factor
    /// </summary>
    public class DividendResult
    {
        /// <summary>
        /// The ex-dividend date on which the security begins trading without the dividend
        /// </summary>
        [JsonProperty("ex_dividend_date")]
        public DateTime ExDividendDate { get; set; }

        /// <summary>
        /// The cumulative historical adjustment factor normalized to today's basis.
        /// Null when not provided by the data source.
        /// </summary>
        [JsonProperty("historical_adjustment_factor")]
        public decimal? HistoricalAdjustmentFactor { get; set; }
    }
}
