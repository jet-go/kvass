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

package shard

import (
	"fmt"
	"net/url"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"tkestack.io/kvass/pkg/api"
	"tkestack.io/kvass/pkg/prom"
	"tkestack.io/kvass/pkg/scrape"
	"tkestack.io/kvass/pkg/target"
)

// Shard is a prometheus shard
type Shard struct {
	// ID is the unique ID for differentiate each shard
	ID       string
	Replicas []*Replica
}

type Replica struct {
	// ID is the unique ID for differentiate different replica of shard
	ID string
	// APIGet is a function to do stand api request to target
	// exposed this field for user to writ unit testing easily
	APIGet func(url string, ret interface{}) error
	// APIPost is a function to do stand api request to target
	// exposed this field for user to writ unit testing easily
	APIPost func(url string, req interface{}, ret interface{}) (err error)
	// scraping is the cached ScrapeStatus fetched from sidecar last time
	// TODO: undo caps
	scraping map[uint64]*target.ScrapeStatus
	url      string
	log      logrus.FieldLogger
	// Ready indicate this shard is ready
	ready bool
}

// NewShard create a Shard
func NewShard(id string, reps []*Replica) *Shard {
	return &Shard{
		ID:       id,
		Replicas: reps,
	}
}

// NewReplica create a Repica with empty scraping cache
func NewReplica(id string, url string, ready bool, log logrus.FieldLogger) *Replica {
	return &Replica{
		ID:      id,
		APIGet:  api.Get,
		APIPost: api.Post,
		url:     url,
		log:     log,
		ready:   ready,
	}
}

type scrapeStatSeries map[string]*scrape.StatisticsSeriesResult
type scrapeStatSeriesResp struct {
	data   scrapeStatSeries
	status bool
}

//Samples return the sample statistics of last scrape
func (r *Replica) Samples(param url.Values) (*scrapeStatSeries, error) {
	ret := scrapeStatSeries{}
	u := r.url + "/api/v1/shard/samples/"
	if len(param) != 0 {
		u += "?" + param.Encode()
	}

	err := r.APIGet(u, &ret)
	if err != nil {
		return nil, fmt.Errorf("get samples info from %s failed : %s", r.ID, err.Error())
	}

	return &ret, nil
}

//Samples return the sample statistics of last scrape
func (s *Shard) Samples(jobName string, withMetricsDetail bool) (scrapeStatSeries, error) {
	result := make([]*scrapeStatSeriesResp, len(s.Replicas))
	param := url.Values{}
	if jobName != "" {
		param["job"] = []string{jobName}
	}
	if withMetricsDetail {
		param["with_metrics_detail"] = []string{"true"}
	}

	// TODO: use generic/common for below pattern
	g := errgroup.Group{}
	for i, rep := range s.Replicas {
		rep := rep
		i := i
		g.Go(func() error {
			res, err := rep.Samples(param)
			if err != nil {
				return err
			}
			result[i] = &scrapeStatSeriesResp{*res, true}

			return nil
		})
	}
	err := g.Wait()
	// return the data from any succesful shard replica
	for _, res := range result {
		if res.status {
			return res.data, nil
		}
	}

	return scrapeStatSeries{}, err
}

type runTimeInfoResult struct {
	data   *RuntimeInfo
	status bool
}

// RuntimeInfo return the runtime status of this shard
func (s *Shard) RuntimeInfo() (*RuntimeInfo, error) {
	g := errgroup.Group{}
	result := make([]*runTimeInfoResult, len(s.Replicas))

	for i, rep := range s.Replicas {
		rep := rep
		i := i
		g.Go(func() error {
			res := &RuntimeInfo{}
			err := rep.APIGet(rep.url+"/api/v1/shard/runtimeinfo/", &res)
			if err != nil {
				return fmt.Errorf("get runtime info from %s failed : %s", rep.ID, err.Error())
			}
			result[i] = &runTimeInfoResult{res, true}
			return nil
		})
	}
	err := g.Wait()

	// return the data from any succesful shard replica
	for _, res := range result {
		if res.status {
			return res.data, nil
		}
	}

	return &RuntimeInfo{}, err
}

// RuntimeInfo return the runtime status of this shard
func (r *Replica) RuntimeInfo() (*RuntimeInfo, error) {
	res := &RuntimeInfo{}
	err := r.APIGet(r.url+"/api/v1/shard/runtimeinfo/", &res)
	if err != nil {
		return res, fmt.Errorf("get runtime info from %s failed : %s", r.ID, err.Error())
	}

	return res, nil
}

type scrapeStatus map[uint64]*target.ScrapeStatus
type scrapeStatusResult struct {
	data   scrapeStatus
	status bool
}

// TargetStatus return the target runtime status that Group scraping
// cached result will be send if something wrong
func (s *Shard) TargetStatus() (scrapeStatus, error) {
	g := errgroup.Group{}
	result := make([]*scrapeStatusResult, len(s.Replicas))

	for i, rep := range s.Replicas {
		rep := rep
		i := i
		g.Go(func() error {
			res := map[uint64]*target.ScrapeStatus{}
			err := rep.APIGet(rep.url+"/api/v1/shard/targets/status/", &res)
			if err != nil {
				return errors.Wrapf(err, "get targets status info from %s failed, url = %s", rep.ID, rep.url)
			}

			//must copy
			m := scrapeStatus{}
			for k, v := range res {
				newV := *v
				m[k] = &newV
			}
			rep.scraping = m
			result[i] = &scrapeStatusResult{res, true}

			return nil
		})
	}
	err := g.Wait()

	// return the data from any succesful shard replica
	for _, res := range result {
		if res.status {
			return res.data, nil
		}
	}

	return scrapeStatus{}, err
}

// UpdateConfig try update replica config by API
func (r *Replica) UpdateConfig(req *UpdateConfigRequest) error {
	return r.APIPost(r.url+"/api/v1/status/config", req, nil)
}

// UpdateConfig try update shard config by API
func (s *Shard) UpdateConfig(req *UpdateConfigRequest) error {
	g := errgroup.Group{}
	for _, rep := range s.Replicas {
		rep := rep
		g.Go(func() error { return rep.UpdateConfig(req) })
	}
	return g.Wait()
}

// UpdateExtraConfig try update shard extra config by API
func (s *Shard) UpdateExtraConfig(req *prom.ExtraConfig) error {
	g := errgroup.Group{}
	for _, rep := range s.Replicas {
		rep := rep
		g.Go(func() error {
			return rep.APIPost(rep.url+"/api/v1/status/extra_config", req, nil)
		})
	}
	return g.Wait()
}

// UpdateTarget try apply targets to sidecar
// request will be skipped if nothing changed according to r.scraping
func (s *Shard) UpdateTarget(request *UpdateTargetsRequest) error {
	newTargets := map[uint64]*target.Target{}
	for _, ts := range request.Targets {
		for _, t := range ts {
			newTargets[t.Hash] = t
		}
	}

	g := errgroup.Group{}
	for _, rep := range s.Replicas {
		rep := rep
		g.Go(func() error {
			if rep.needUpdate(newTargets) {
				if len(newTargets) != 0 || len(rep.scraping) != 0 {
					rep.log.Infof("%s need update targets", rep.ID)
				}
				return rep.APIPost(rep.url+"/api/v1/shard/targets/", &request, nil)
			}
			return nil
		})
	}
	return g.Wait()
}

func (r *Replica) needUpdate(targets map[uint64]*target.Target) bool {
	if len(targets) != len(r.scraping) || len(targets) == 0 {
		return true
	}

	for k, v := range targets {
		if r.scraping[k] == nil || r.scraping[k].TargetState != v.TargetState {
			return true
		}
	}
	return false
}

// Ready returns true if any of the shard replica is ready
func (s *Shard) Ready() (ready bool) {
	for _, rep := range s.Replicas {
		ready = ready || rep.ready
	}
	return
}
