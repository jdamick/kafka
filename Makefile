include $(GOROOT)/src/Make.inc

TARG=kafka
GOFILES=\
	src/kafka.go\
	src/message.go\
	src/converts.go\
	src/consumer.go\
	src/publisher.go\

include $(GOROOT)/src/Make.pkg

tools: force
	make -C tools/consumer clean all
	make -C tools/publisher clean all

format:
	gofmt -w src/kafka.go

.PHONY: force 
