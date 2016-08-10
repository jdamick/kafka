kafka:
	go build -race
	go install
	go test -test.v

fix:
	go fix kafka
	go vet kafka

tools: force kafka
	cd tools/consumer ; go install
	cd tools/offsets ; go install
	cd tools/publisher ; go install

format:
	gofmt -w .

clean:

full: format clean kafka tools

.PHONY: force
