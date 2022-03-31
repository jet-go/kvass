/*
 * Tencent is pleased to support the open source community by making TKEStack available.
 *
 * Copyright (C) 2012-2019 Tencent. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package coordinator

// MetricsInfo contains statistic of metrics
type MetricsInfo struct {
	// MetricsTotal show total metrics in last scrape
	MetricsTotal uint64 `json:"metricsTotal"`
	//
	SamplesTotal uint64 `json:"samplesTotal"`
	// LastSamples show the last samples of all metrics
	LastSamples map[string]uint64 `json:"lastSamples"`
}
