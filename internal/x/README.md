The `x` directory contains any internal packages that are shared
across multiple other internal packages within this module, but are not
specifically related to Dogma or the engine's core functionality.

Additionally, packages that are extensions to standard or third-party packages
are named with their own `x` prefix to avoid collisions and the need to use
import aliases.
