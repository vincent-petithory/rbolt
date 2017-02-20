all: test bench

test:
	@go test -v -cover .
.PHONY: test

bench:
	@go test -v -run NoneThanks -bench=. -test.benchtime=10s -test.benchmem
.PHONY: bench
