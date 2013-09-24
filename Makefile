kafka:
	go build -race
	go install
	go test -test.v

fix:
	go fix kafka
	go vet kafka

tools: force kafka
	cd consumer ; go build 
	cd offsets ; go build 
	cd publisher ; go build 

format:
	gofmt -w -tabwidth=2 -tabs=false kafka

clean:

full: format clean kafka tools

.PHONY: force 
