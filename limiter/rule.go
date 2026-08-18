package limiter

import (
	"fmt"
	"regexp"

	"github.com/csdfsdffese/ljfxznode/api/panel"
)

func (l *Limiter) CheckDomainRule(destination string) (reject bool) {
	// have rule
	l.ruleLock.RLock()
	for i := range l.DomainRules {
		if l.DomainRules[i].MatchString(destination) {
			reject = true
			break
		}
	}
	l.ruleLock.RUnlock()
	return
}

func (l *Limiter) CheckProtocolRule(protocol string) (reject bool) {
	l.ruleLock.RLock()
	for i := range l.ProtocolRules {
		if l.ProtocolRules[i] == protocol {
			reject = true
			break
		}
	}
	l.ruleLock.RUnlock()
	return
}

// UpdateRule compiles the panel-provided domain regexps. regexp.Compile (not
// MustCompile) is used so that a single malformed regexp from the panel logs
// an error instead of crashing the whole process.
func (l *Limiter) UpdateRule(rule *panel.Rules) error {
	compiled := make([]*regexp.Regexp, len(rule.Regexp))
	for i := range rule.Regexp {
		re, err := regexp.Compile(rule.Regexp[i])
		if err != nil {
			return fmt.Errorf("compile domain rule %q error: %w", rule.Regexp[i], err)
		}
		compiled[i] = re
	}
	// 整表替换规则集（与并发连接的 CheckDomainRule/CheckProtocolRule 读互斥）
	l.ruleLock.Lock()
	l.DomainRules = compiled
	l.ProtocolRules = rule.Protocol
	l.ruleLock.Unlock()
	return nil
}
