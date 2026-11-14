# Each test package opens many short-lived database connections. On macOS, too
# many packages running at once use up the local ports, because each closed
# connection holds its port in TIME_WAIT for about 30 seconds. Running one package
# at a time is a stopgap until the tests open fewer connections. See https://github.com/dogmatiq/runkit/issues/126.
GO_TEST_ARGS = -failfast -p 1

GO_TEST_COVERAGE_ARGS = -coverpkg=./...

-include .makefiles/Makefile
-include .makefiles/pkg/changelog/v2/Makefile
-include .makefiles/pkg/go/v1/Makefile
-include .makefiles/pkg/apm/v1/Makefile

.makefiles/%:
	@curl -sfL https://makefiles.dev/v1 | bash /dev/stdin "$@"
