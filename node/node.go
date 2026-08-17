package node

import (
	"fmt"

	"github.com/csdfsdffese/ljfxznode/api/panel"
	"github.com/csdfsdffese/ljfxznode/conf"
	vCore "github.com/csdfsdffese/ljfxznode/core"
	log "github.com/sirupsen/logrus"
)

type Node struct {
	controllers []*Controller
}

func New() *Node {
	return &Node{}
}

func (n *Node) Start(nodes []conf.NodeConfig, core vCore.Core) error {
	n.controllers = make([]*Controller, len(nodes))
	for i := range nodes {
		p, err := panel.New(&nodes[i].ApiConfig)
		if err != nil {
			return err
		}
		// Register controller service
		n.controllers[i] = NewController(core, p, &nodes[i].Options)
		err = n.controllers[i].Start()
		if err != nil {
			return fmt.Errorf("start node controller [%s-%s-%d] error: %s",
				nodes[i].ApiConfig.APIHost,
				nodes[i].ApiConfig.NodeType,
				nodes[i].ApiConfig.NodeID,
				err)
		}
	}
	return nil
}

func (n *Node) Close() {
	for _, c := range n.controllers {
		err := c.Close()
		if err != nil {
			// Never panic during shutdown: a failed DelNode (e.g. inbound
			// already removed) would otherwise crash the reload/exit path.
			log.WithFields(log.Fields{
				"tag": c.tag,
				"err": err,
			}).Error("Close node controller failed")
		}
	}
	n.controllers = nil
}
