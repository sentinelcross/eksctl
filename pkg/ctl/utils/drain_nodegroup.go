package utils

import (
	"fmt"
	"os"

	"github.com/kris-nova/logger"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	api "github.com/weaveworks/eksctl/pkg/apis/eksctl.io/v1alpha4"
	"github.com/weaveworks/eksctl/pkg/ctl/cmdutils"
	"github.com/weaveworks/eksctl/pkg/eks"

	"github.com/weaveworks/eksctl/pkg/drain"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
)

func drainNodeGroupCmd(g *cmdutils.Grouping) *cobra.Command {
	p := &api.ProviderConfig{}
	cfg := api.NewClusterConfig()
	ng := cfg.NewNodeGroup()

	cmd := &cobra.Command{
		Use:   "drain-nodegroup",
		Short: "Cordon and drain a nodegroup",
		Run: func(_ *cobra.Command, args []string) {
			if err := doDrainNodeGroup(p, cfg, ng, cmdutils.GetNameArg(args)); err != nil {
				logger.Critical("%s\n", err.Error())
				os.Exit(1)
			}
		},
	}

	group := g.New(cmd)

	group.InFlagSet("General", func(fs *pflag.FlagSet) {
		// TODO: flags to control pod deletion timeout and overal timeout, set filters e.g. to bail when local storage is found
		fs.StringVar(&cfg.Metadata.Name, "cluster", "", "EKS cluster name (required)")
		cmdutils.AddRegionFlag(fs, p)
		fs.StringVarP(&ng.Name, "name", "n", "", "Name of the nodegroup to delete (required)")
		// TODO: --undo
		// TODO: --dry-run
	})

	group.AddTo(cmd)

	return cmd
}

func doDrainNodeGroup(p *api.ProviderConfig, cfg *api.ClusterConfig, ng *api.NodeGroup, nameArg string) error {
	ctl := eks.New(p, cfg)

	if err := api.Register(); err != nil {
		return err
	}

	if err := ctl.CheckAuth(); err != nil {
		return err
	}

	if cfg.Metadata.Name == "" {
		return errors.New("--cluster must be set")
	}

	if ng.Name != "" && nameArg != "" {
		return cmdutils.ErrNameFlagAndArg(ng.Name, nameArg)
	}

	if nameArg != "" {
		ng.Name = nameArg
	}

	if ng.Name == "" {
		return fmt.Errorf("--name must be set")
	}

	if err := ctl.GetCredentials(cfg); err != nil {
		return errors.Wrapf(err, "getting credentials for cluster %q", cfg.Metadata.Name)
	}

	clientSet, err := ctl.NewStandardClientSet(cfg)
	if err != nil {
		return err
	}

	evictionGroupVersion, err := drain.CheckEvictionSupport(clientSet)
	if evictionGroupVersion == "" || err != nil {
		return errors.Wrapf(err, "pod evictions are not supported")
	}

	drainer := &drain.Helper{
		Client: clientSet,

		// IgnoreAllDaemonSets: true,
		IgnoreDaemonSets: []metav1.ObjectMeta{
			{
				Namespace: "kube-system",
				Name:      "aws-node",
			},
			{
				Namespace: "kube-system",
				Name:      "kube-proxy",
			},
			{
				Name: "node-exporter",
			},
			{
				Name: "prom-node-exporter",
			},
			{
				Name: "weave-scope",
			},
			{
				Name: "weave-scope-agent",
			},
			{
				Name: "weave-net",
			},
		},
	}

	// TODO: timeout

	drainedNodes := sets.NewString()
	// loop until all nodes are drained to handle accidential scale-up
	// or any other changes in the ASG
	for {
		nodes, err := clientSet.CoreV1().Nodes().List(ng.ListOptions())
		if err != nil {
			return err
		}

		newPendingNodes := sets.NewString()

		for _, node := range nodes.Items {
			if drainedNodes.Has(node.Name) {
				continue
			}
			newPendingNodes.Insert(node.Name)
			c := drain.NewCordonHelper(&node)
			if c.UpdateIfRequired(true) {
				err, patchErr := c.PatchOrReplace(clientSet)
				if patchErr != nil {
					logger.Warning(patchErr.Error())
				}
				if err != nil {
					logger.Critical(err.Error())
				}
				logger.Info("maked %q unschedulable", node.Name)
			} else {
				logger.Debug("node %q is already unschedulable", node.Name)
			}
		}

		if drainedNodes.HasAll(newPendingNodes.List()...) {
			break
		}
		logger.Debug("already drained: %v", drainedNodes.List())
		logger.Debug("will drain: %v", newPendingNodes.List())

		for _, node := range nodes.Items {
			pending, err := evictPods(drainer, &node, evictionGroupVersion)
			if err != nil {
				return err
			}
			logger.Debug("%d pods to be evicted from %s", pending, node.Name)
			if pending == 0 {
				drainedNodes.Insert(node.Name)
			}
		}
	}

	logger.Success("drained nodes: %v", drainedNodes.List())

	return nil
}

func evictPods(drainer *drain.Helper, node *corev1.Node, evictionGroupVersion string) (int, error) {
	list, errs := drainer.GetPodsForDeletion(node.Name)
	if len(errs) > 0 {
		return 0, fmt.Errorf("errs: %v", errs) // TODO: improve formatting
	}
	if w := list.Warnings(); w != "" {
		logger.Warning(w)
	}
	pods := list.Pods()
	pending := len(pods)
	for _, pod := range pods {
		if err := drainer.EvictPod(pod, evictionGroupVersion); err != nil {
			return pending, err
		}
	}
	return pending, nil
}
