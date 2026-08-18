package panel

import (
	"fmt"

	"github.com/go-resty/resty/v2"
)

func (c *Client) assembleURL(path string) string {
	// path.Join 是文件路径语义，会把 "http://host/api" 清洗成 "http:/host/api"，
	// 破坏 URL（仅用于错误日志，直接拼接即可）。
	return c.APIHost + path
}
func (c *Client) checkResponse(res *resty.Response, path string, err error) error {
	if err != nil {
		return fmt.Errorf("request %s failed: %s", c.assembleURL(path), err)
	}
	if res.StatusCode() >= 400 {
		body := res.Body()
		return fmt.Errorf("request %s failed: %s", c.assembleURL(path), string(body))
	}
	return nil
}
