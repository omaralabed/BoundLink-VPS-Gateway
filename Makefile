.PHONY: build test clean

build:
	go build -o boundlink-vps .

test:
	go test ./...

clean:
	rm -f boundlink-vps
