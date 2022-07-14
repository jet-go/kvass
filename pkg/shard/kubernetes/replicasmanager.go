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

package kubernetes

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/apps/v1"
	v12 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"tkestack.io/kvass/pkg/shard"
)

// ReplicasManager select Statefusets to get shard manager
type ReplicasManager struct {
	port             int
	deletePVC        bool
	stsSelector      string
	cli              kubernetes.Interface
	lg               logrus.FieldLogger
	listStatefulSets func(ctx context.Context, opts v12.ListOptions) (*v1.StatefulSetList, error)
	stsUpdatedTime   map[string]*time.Time
}

// NewReplicasManager create a ReplicasManager
func NewReplicasManager(
	cli kubernetes.Interface,
	stsNamespace string,
	stsSelector string,
	port int,
	deletePVC bool,
	lg logrus.FieldLogger,
) *ReplicasManager {
	return &ReplicasManager{
		cli:              cli,
		port:             port,
		deletePVC:        deletePVC,
		lg:               lg,
		stsSelector:      stsSelector,
		listStatefulSets: cli.AppsV1().StatefulSets(stsNamespace).List,
		stsUpdatedTime:   map[string]*time.Time{},
	}
}

// Replicas return all shards manager
// TODO: rename method
func (g *ReplicasManager) Replicas() (shard.Manager, error) {
	sts, err := g.listStatefulSets(context.TODO(), v12.ListOptions{
		LabelSelector: g.stsSelector,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "get statefulset")
	}

	reps := make([]*v1.StatefulSet, 0)
	for _, s := range sts.Items {
		if s.Status.Replicas != s.Status.UpdatedReplicas {
			g.lg.Warnf("Statefulset %s UpdatedReplicas != Replicas, skipped", s.Name)
			g.stsUpdatedTime[s.Name] = nil
			continue
		}

		if s.Status.ReadyReplicas != s.Status.Replicas && g.stsUpdatedTime[s.Name] == nil {
			t := time.Now()
			g.lg.Warnf("Statefulset %s is not ready, try wait 2m", s.Name)
			g.stsUpdatedTime[s.Name] = &t
		}

		t := g.stsUpdatedTime[s.Name]
		if s.Status.ReadyReplicas != s.Status.Replicas && time.Now().Sub(*t) < time.Minute*2 {
			g.lg.Warnf("Statefulset %s is not ready, still waiting", s.Name)
			continue
		}

		tempS := s
		reps = append(reps, &tempS)
	}
	g.lg.Infof("total sts found count=%d", len(reps))
	return newShardManager(g.cli, reps, g.port, g.deletePVC, g.lg.WithField("sts", g.stsSelector)), nil
}
