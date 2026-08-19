package rate

import (
	"time"

	"github.com/juju/ratelimit"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
)

// Reader 是限速读取器：读取上行（客户端→目标）数据时按实际字节数消耗
// 令牌桶令牌，实现上传方向的速度限制。与 Writer 配套形成双向限速。
type Reader struct {
	reader  buf.TimeoutReader
	limiter *ratelimit.Bucket
}

// NewRateLimitReader 包装上行读取器为限速读取，返回实现 buf.TimeoutReader
// 的包装（底层非 TimeoutReader 时自动以 TimeoutWrapperReader 补足超时能力，
// 调用方可安全断言 buf.TimeoutReader）。
func NewRateLimitReader(reader buf.Reader, limiter *ratelimit.Bucket) buf.TimeoutReader {
	tr, ok := reader.(buf.TimeoutReader)
	if !ok {
		tr = &buf.TimeoutWrapperReader{Reader: reader}
	}
	return &Reader{
		reader:  tr,
		limiter: limiter,
	}
}

func (r *Reader) Close() error {
	return common.Close(r.reader)
}

func (r *Reader) ReadMultiBuffer() (buf.MultiBuffer, error) {
	mb, err := r.reader.ReadMultiBuffer()
	if err != nil {
		return nil, err
	}
	// 先取数据、后按实际字节数等待令牌：令牌消耗与数据量严格一致。
	// 若先 Wait 后读，读取长度未知会导致限速失真。
	if !mb.IsEmpty() {
		r.limiter.Wait(int64(mb.Len()))
	}
	return mb, nil
}

func (r *Reader) ReadMultiBufferTimeout(timeout time.Duration) (buf.MultiBuffer, error) {
	mb, err := r.reader.ReadMultiBufferTimeout(timeout)
	if err != nil {
		return nil, err
	}
	if !mb.IsEmpty() {
		r.limiter.Wait(int64(mb.Len()))
	}
	return mb, nil
}
