package cmd

import (
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/csdfsdffese/ljfxznode/conf"
	vCore "github.com/csdfsdffese/ljfxznode/core"
	"github.com/csdfsdffese/ljfxznode/limiter"
	"github.com/csdfsdffese/ljfxznode/node"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	config string
	watch  bool
)

var serverCommand = cobra.Command{
	Use:   "server",
	Short: "Run ljfxznode server",
	Run:   serverHandle,
	Args:  cobra.NoArgs,
}

func init() {
	serverCommand.PersistentFlags().
		StringVarP(&config, "config", "c",
			"/etc/ljfxznode/config.json", "config file path")
	serverCommand.PersistentFlags().
		BoolVarP(&watch, "watch", "w",
			true, "watch file path change")
	command.AddCommand(&serverCommand)
}

func applyLogConfig(c *conf.Conf) {
	switch c.LogConfig.Level {
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "info":
		log.SetLevel(log.InfoLevel)
	case "warn":
		log.SetLevel(log.WarnLevel)
	case "error":
		log.SetLevel(log.ErrorLevel)
	}
	if c.LogConfig.Output != "" {
		f, err := os.OpenFile(c.LogConfig.Output, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			log.WithField("err", err).Error("Open log file failed, using stdout instead")
			// OpenFile 失败时 f 为 nil，继续 SetOutput 会把日志写到 nil writer 造成静默丢失
			return
		}
		log.SetOutput(f)
	}
}

func serverHandle(_ *cobra.Command, _ []string) {
	showVersion()
	c := conf.New()
	err := c.LoadFromPath(config)
	if err != nil {
		log.WithField("err", err).Error("Load config file failed")
		return
	}
	applyLogConfig(c)
	limiter.Init()
	log.Info("Start ljfxznode...")
	// self-heal reload channel: file watcher and hung tasks both signal here
	reloadCh := make(chan struct{}, 1)
	vc, err := vCore.NewCore(c.CoresConfig)
	if err != nil {
		log.WithField("err", err).Error("new core failed")
		return
	}
	err = vc.Start()
	if err != nil {
		log.WithField("err", err).Error("Start core failed")
		return
	}
	defer vc.Close()
	log.Info("Core ", vc.Type(), " started")
	nodes := node.New()
	err = nodes.Start(c.NodeConfig, vc)
	if err != nil {
		log.WithField("err", err).Error("Run nodes failed")
		return
	}
	log.Info("Nodes started")
	xdns := os.Getenv("XRAY_DNS_PATH")
	if watch {
		err = c.Watch(config, xdns, func() {
			select {
			case reloadCh <- struct{}{}:
			default: // drop if a reload is already queued
			}
		})
		if err != nil {
			log.WithField("err", err).Error("start watch failed")
			return
		}
	}
	// clear memory
	runtime.GC()
	// wait exit signal / reload signal
	osSignals := make(chan os.Signal, 1)
	signal.Notify(osSignals, syscall.SIGINT, syscall.SIGTERM)
	for {
		select {
		case <-osSignals:
			log.Info("Received exit signal, shutting down...")
			// 先停节点控制器再 return，走 defer vc.Close() 优雅关闭 core；
			// 不要用 os.Exit(0)，它会跳过所有 defer 导致连接未清理
			nodes.Close()
			return
		case <-reloadCh:
			log.Info("Received reload signal, reloading config...")
			if err := reload(config, &vc, &nodes, &c); err != nil {
				log.WithField("err", err).Panic("Reload failed")
			}
			log.Info("Reload success")
		}
	}
}

// reload closes the old core and nodes, reloads the config and starts fresh.
// It runs on the single event loop goroutine so reloads are naturally serialized.
func reload(config string, vc *vCore.Core, nodes **node.Node, c **conf.Conf) error {
	(*nodes).Close()
	if err := (*vc).Close(); err != nil {
		return err
	}
	newConf := conf.New()
	if err := newConf.LoadFromPath(config); err != nil {
		return err
	}
	applyLogConfig(newConf)
	newVc, err := vCore.NewCore(newConf.CoresConfig)
	if err != nil {
		return err
	}
	if err := newVc.Start(); err != nil {
		return err
	}
	log.Info("Core ", newVc.Type(), " restarted")
	newNodes := node.New()
	if err := newNodes.Start(newConf.NodeConfig, newVc); err != nil {
		return err
	}
	log.Info("Nodes restarted")
	*vc = newVc
	*nodes = newNodes
	*c = newConf
	runtime.GC()
	return nil
}
