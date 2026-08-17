package panel

// NodeStatus is the load status payload reported to the panel via the
// UniProxy /status endpoint (V1). mem/swap/disk are in bytes.
type NodeStatus struct {
	CPU  float64        `json:"cpu"`
	Mem  *SystemSegment `json:"mem"`
	Swap *SystemSegment `json:"swap"`
	Disk *SystemSegment `json:"disk"`
}

// SystemSegment reports total/used usage of a system resource.
type SystemSegment struct {
	Total uint64 `json:"total"`
	Used  uint64 `json:"used"`
}

// ReportNodeStatus reports the node load status to the panel.
func (c *Client) ReportNodeStatus(status *NodeStatus) error {
	const path = "/api/v1/server/UniProxy/status"
	r, err := c.client.R().
		SetBody(status).
		ForceContentType("application/json").
		Post(path)
	err = c.checkResponse(r, path, err)
	if err != nil {
		return err
	}
	return nil
}
