package limiter

import (
	"fmt"
	"regexp"

	"github.com/csdfsdffese/ljfxznode/api/panel"
)

func (l *Limiter) CheckDomainRule(destination string) (reject bool) {
	// have rule
	for i := range l.DomainRules {
		if l.DomainRules[i].MatchString(destination) {
			reject = true
			break
		}
	}
	return
}

func (l *Limiter) CheckProtocolRule(protocol string) (reject bool) {
	for i := range l.ProtocolRules {
		if l.ProtocolRules[i] == protocol {
			reject = true
			break
		}
	}
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
	l.DomainRules = compiled
	l.ProtocolRules = rule.Protocol
	return nil
}
