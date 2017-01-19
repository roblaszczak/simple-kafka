FROM golang:1.7

WORKDIR /go/src/github.com/roblaszczak/simple-kafka

RUN curl https://glide.sh/get | sh

RUN go get github.com/alecthomas/gometalinter
RUN gometalinter --install