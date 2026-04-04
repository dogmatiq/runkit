-include .makefiles/Makefile
-include .makefiles/pkg/protobuf/v2/Makefile
-include .makefiles/pkg/protobuf/v2/with-primo.mk
-include .makefiles/pkg/go/v1/Makefile
-include .makefiles/pkg/vscode/v1/Makefile

GENERATED_FILES += docs/adr/README.md

docs/adr/README.md: .adr-dir $(filter-out docs/adr/README.md,$(wildcard docs/adr/*.md))
	adr generate toc -i docs/adr/README.intro.md > "$@"

.makefiles/%:
	@curl -sfL https://makefiles.dev/v1 | bash /dev/stdin "$@"
