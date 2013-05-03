where-am-i = $(CURDIR)/$(word $(words $(MAKEFILE_LIST)),$(MAKEFILE_LIST))
THIS_MAKEFILE := $(call where-am-i)
MAKEFILE_DIR=$(dir $(THIS_MAKEFILE))

NEW_GOPATH = $(MAKEFILE_DIR)
ifdef GOPATH
  NEW_GOPATH+=":"$(GOPATH)
endif

export GOPATH := $(NEW_GOPATH)

kafka:
	go install kafka
	go test kafka -test.v

tools: force
	go install consumer
	go install offsets
	go install publisher

format:
	gofmt -w -tabwidth=2 -tabs=false src

clean:

full: format clean kafka tools

.PHONY: force 
