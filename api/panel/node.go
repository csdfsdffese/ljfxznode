package panel

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"encoding/json"
)

// Security type
const (
	None    = 0
	Tls     = 1
	Reality = 2
)

type NodeInfo struct {
	Id           int
	Type         string
	Security     int
	PushInterval time.Duration
	PullInterval time.Duration
	RawDNS       RawDNS
	Rules        Rules

	// origin
	VAllss      *VAllssNode
	Shadowsocks *ShadowsocksNode
	Trojan      *TrojanNode
	Common      *CommonNode
}

type CommonNode struct {
	Host       string      `json:"host"`
	ServerPort int         `json:"server_port"`
	ServerName string      `json:"server_name"`
	Routes     []Route     `json:"routes"`
	BaseConfig *BaseConfig `json:"base_config"`
}

type Route struct {
	Id          int         `json:"id"`
	Match       interface{} `json:"match"`
	Action      string      `json:"action"`
	ActionValue string      `json:"action_value"`
}
type BaseConfig struct {
	PushInterval any `json:"push_interval"`
	PullInterval any `json:"pull_interval"`
}

// VAllssNode is vmess and vless node info
type VAllssNode struct {
	CommonNode
	Tls                 int             `json:"tls"`
	TlsSettings         TlsSettings     `json:"tls_settings"`
	TlsSettingsBack     *TlsSettings    `json:"tlsSettings"`
	Network             string          `json:"network"`
	NetworkSettings     json.RawMessage `json:"network_settings"`
	NetworkSettingsBack json.RawMessage `json:"networkSettings"`
	Encryption          string          `json:"encryption"`
	EncryptionSettings  EncSettings     `json:"encryption_settings"`
	Decryption          string          `json:"decryption"`
	ServerName          string          `json:"server_name"`

	// vless only
	Flow string `json:"flow"`
}

type TlsSettings struct {
	ServerName  string   `json:"server_name"`
	ServerNames []string `json:"server_names"`
	Dest        string   `json:"dest"`
	ServerPort  string   `json:"server_port"`
	ShortId     string   `json:"short_id"`
	ShortIds    []string `json:"short_ids"`
	PrivateKey  string   `json:"private_key"`
	Mldsa65Seed string   `json:"mldsa65Seed"`
	Xver        uint64   `json:"xver,string"`
}

// EffectiveServerNames prefers the multi-SNI list, falling back to the
// single server_name so panels that only send one value keep working.
func (t *TlsSettings) EffectiveServerNames() []string {
	if len(t.ServerNames) > 0 {
		return t.ServerNames
	}
	if t.ServerName != "" {
		return []string{t.ServerName}
	}
	return nil
}

// EffectiveShortIds prefers the multi short-id list, falling back to the
// single short_id.
func (t *TlsSettings) EffectiveShortIds() []string {
	if len(t.ShortIds) > 0 {
		return t.ShortIds
	}
	if t.ShortId != "" {
		return []string{t.ShortId}
	}
	return nil
}

type EncSettings struct {
	Mode          string `json:"mode"`
	Ticket        string `json:"ticket"`
	ServerPadding string `json:"server_padding"`
	PrivateKey    string `json:"private_key"`
}

type ShadowsocksNode struct {
	CommonNode
	Cipher    string `json:"cipher"`
	ServerKey string `json:"server_key"`
}

type TrojanNode struct {
	CommonNode
	Network             string          `json:"network"`
	NetworkSettings     json.RawMessage `json:"network_settings"`
	NetworkSettingsBack json.RawMessage `json:"networkSettings"`
	Tls                 int             `json:"tls"`
	TlsSettings         TlsSettings     `json:"tls_settings"`
	TlsSettingsBack     *TlsSettings    `json:"tlsSettings"`
}

type RawDNS struct {
	DNSMap  map[string]map[string]interface{}
	DNSJson []byte
}

type Rules struct {
	Regexp   []string
	Protocol []string
}

func (c *Client) GetNodeInfo() (node *NodeInfo, err error) {
	const path = "/api/v1/server/UniProxy/config"
	r, err := c.client.
		R().
		SetHeader("If-None-Match", c.nodeEtag).
		ForceContentType("application/json").
		Get(path)

	if r == nil {
		return nil, fmt.Errorf("received nil response for %s: %v", path, err)
	}
	if r.StatusCode() == 304 {
		return nil, nil
	}
	if err = c.checkResponse(r, path, err); err != nil {
		return nil, err
	}
	hash := sha256.Sum256(r.Body())
	newBodyHash := hex.EncodeToString(hash[:])
	if c.responseBodyHash == newBodyHash {
		return nil, nil
	}
	c.responseBodyHash = newBodyHash
	c.nodeEtag = r.Header().Get("ETag")

	if r.RawBody() != nil {
		defer r.RawBody().Close()
	}
	node = &NodeInfo{
		Id:   c.NodeId,
		Type: c.NodeType,
		RawDNS: RawDNS{
			DNSMap:  make(map[string]map[string]interface{}),
			DNSJson: []byte(""),
		},
	}
	// parse protocol params
	var cm *CommonNode
	switch c.NodeType {
	case "vmess", "vless":
		rsp := &VAllssNode{}
		err = json.Unmarshal(r.Body(), rsp)
		if err != nil {
			return nil, fmt.Errorf("decode v2ray params error: %s", err)
		}
		if len(rsp.NetworkSettingsBack) > 0 {
			rsp.NetworkSettings = rsp.NetworkSettingsBack
			rsp.NetworkSettingsBack = nil
		}
		if rsp.TlsSettingsBack != nil {
			rsp.TlsSettings = *rsp.TlsSettingsBack
			rsp.TlsSettingsBack = nil
		}
		cm = &rsp.CommonNode
		node.VAllss = rsp
		node.Security = node.VAllss.Tls
	case "shadowsocks":
		rsp := &ShadowsocksNode{}
		err = json.Unmarshal(r.Body(), rsp)
		if err != nil {
			return nil, fmt.Errorf("decode shadowsocks params error: %s", err)
		}
		cm = &rsp.CommonNode
		node.Shadowsocks = rsp
		node.Security = None
	case "trojan":
		rsp := &TrojanNode{}
		err = json.Unmarshal(r.Body(), rsp)
		if err != nil {
			return nil, fmt.Errorf("decode trojan params error: %s", err)
		}
		if len(rsp.NetworkSettingsBack) > 0 {
			rsp.NetworkSettings = rsp.NetworkSettingsBack
			rsp.NetworkSettingsBack = nil
		}
		if rsp.TlsSettingsBack != nil {
			rsp.TlsSettings = *rsp.TlsSettingsBack
			rsp.TlsSettingsBack = nil
		}
		cm = &rsp.CommonNode
		node.Trojan = rsp
		node.Security = rsp.Tls
	}

	if cm == nil {
		return nil, fmt.Errorf("decode params error: no valid common node parsed for type %s", c.NodeType)
	}

	// parse rules and dns
	for i := range cm.Routes {
		var matchs []string
		switch m := cm.Routes[i].Match.(type) {
		case string:
			matchs = strings.Split(m, ",")
		case []string:
			matchs = m
		case []interface{}:
			for _, v := range m {
				if s, ok := v.(string); ok {
					matchs = append(matchs, s)
				}
			}
		default:
			// nil or unsupported match type: ignore this rule instead of panicking
			continue
		}
		switch cm.Routes[i].Action {
		case "block":
			for _, v := range matchs {
				if strings.HasPrefix(v, "protocol:") {
					// protocol
					node.Rules.Protocol = append(node.Rules.Protocol, strings.TrimPrefix(v, "protocol:"))
				} else {
					// domain
					node.Rules.Regexp = append(node.Rules.Regexp, strings.TrimPrefix(v, "regexp:"))
				}
			}
		case "dns":
			if len(matchs) == 0 {
				continue
			}
			var domains []string
			domains = append(domains, matchs...)
			if matchs[0] != "main" {
				node.RawDNS.DNSMap[strconv.Itoa(i)] = map[string]interface{}{
					"address": cm.Routes[i].ActionValue,
					"domains": domains,
				}
			} else {
				dns := []byte(strings.Join(matchs[1:], ""))
				node.RawDNS.DNSJson = dns
			}
		}
	}

	// set interval
	node.PushInterval = intervalToTime(commonBaseConfig(cm).PushInterval)
	node.PullInterval = intervalToTime(commonBaseConfig(cm).PullInterval)

	node.Common = cm
	// clear
	cm.Routes = nil
	cm.BaseConfig = nil

	return node, nil
}

// commonBaseConfig returns the BaseConfig of a node's CommonNode, guarding
// against a nil BaseConfig when the panel omits the base_config block.
func commonBaseConfig(cm *CommonNode) *BaseConfig {
	if cm == nil || cm.BaseConfig == nil {
		return &BaseConfig{}
	}
	return cm.BaseConfig
}

func intervalToTime(i interface{}) time.Duration {
	if i == nil {
		return 60 * time.Second
	}
	switch reflect.TypeOf(i).Kind() {
	case reflect.Int:
		if v := i.(int); v > 0 {
			return time.Duration(v) * time.Second
		}
	case reflect.Int64:
		if v := i.(int64); v > 0 {
			return time.Duration(v) * time.Second
		}
	case reflect.String:
		s, _ := strconv.Atoi(i.(string))
		if s > 0 {
			return time.Duration(s) * time.Second
		}
	case reflect.Float64:
		if v := i.(float64); v > 0 {
			return time.Duration(v) * time.Second
		}
	}
	// invalid / zero / negative input: fall back to the default 60s so a
	// zero interval can never turn the monitor task into a busy loop
	return 60 * time.Second
}
