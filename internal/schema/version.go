package schema

import "github.com/dogmatiq/runkit/internal/schema/internal/version"

// TargetVersion is the schema version that this runkit version creates and
// requires.
var TargetVersion = version.Version{2, 0, 0}
