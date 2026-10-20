package contract

import "errors"

// ErrUnsent is returned only when a trusted adapter knows no request was sent.
var ErrUnsent = errors.New("external request not sent")
