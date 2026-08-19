package conf

import (
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
)

// Watch signals reload() on config file changes. The actual reload is
// executed by the caller's single event loop, so concurrent reloads are
// naturally serialized.
func (p *Conf) Watch(filePath, xDnsPath string, reload func()) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("new watcher error: %s", err)
	}
	go func() {
		var pre time.Time
		defer watcher.Close()
		for {
			select {
			case e := <-watcher.Events:
				if e.Has(fsnotify.Chmod) {
					continue
				}
				if pre.Add(10 * time.Second).After(time.Now()) {
					continue
				}
				pre = time.Now()
				go func() {
					// reload 内部（如解析面板下发配置）panic 时不得拖垮整个节点进程
					defer func() {
						if r := recover(); r != nil {
							log.Printf("Reload panicked, recovered: %v", r)
						}
					}()
					time.Sleep(5 * time.Second)
					switch filepath.Base(strings.TrimSuffix(e.Name, "~")) {
					case filepath.Base(xDnsPath):
						log.Println("DNS file changed, reloading...")
					default:
						log.Println("config file changed, reloading...")
					}
					reload()
				}()
			case err := <-watcher.Errors:
				if err != nil {
					log.Printf("File watcher error: %s", err)
				}
			}
		}
	}()
	err = watcher.Add(filePath)
	if err != nil {
		return fmt.Errorf("watch file error: %s", err)
	}
	if xDnsPath != "" {
		err = watcher.Add(xDnsPath)
		if err != nil {
			return fmt.Errorf("watch dns file error: %s", err)
		}
	}
	return nil
}
