package quota

import "errors"

var ErrQueryConcurrencyExceeded = errors.New("tenant query concurrency exceeded")
