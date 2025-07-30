package gnpb

import "errors"

func (x *GetPayloadRequest) Validate() error {

	if x.GetId() < 1 {
		return errors.New("请求ID不能为空")
	}
	return nil
}
