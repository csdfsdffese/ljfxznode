package panel

import (
	"fmt"
	"github.com/go-resty/resty/v2"
	path2 "path"
)

func (c *Client) assembleURL(path string) string {
	return path2.Join(c.APIHost + path)
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
