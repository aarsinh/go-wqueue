FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -o /taskqueue .

FROM gcr.io/distroless/static-debian12
COPY --from=build /taskqueue /taskqueue
EXPOSE 8080
ENTRYPOINT ["/taskqueue"]
