## GRPC server
- Our service is defined once in `.proto` and then we can write the code for server and client in any language. [Ref](https://grpc.io/docs/languages/go/basics/)
- 4 types of RPCs: simple, server-side streaming, client-side streaming, bidirectional streaming
  - Depends on whether we use `stream` keyboard with response/request or both.
  - In bidirectional streaming, both streams are independent and can be read/written to at any time. However, they are ordered.
- `_grpc.pb.go` contains 2 things:
  - `InvoicerClient`: interface for clients to call, with methods defined in the service in `.proto`
  - `InvoicerServer`: interface for servers to implement, with methods defined in the service in `.proto`
- Command to create `_grpc.pb.go` and `.pb.go`: `make generate_grpc_code`
- Now, use postman to invoke the service after running `go run cmd/main.go`
  ![alt text](images/postman.png)
  - Instead of using postman, you can use `grpcurl` to invoke the service
  - Another option is to create your own client [Ref](https://grpc.io/docs/languages/go/quickstart/)
    - For this, we create a `gRPC channel` to communicate with the server using `grpc.NewClient()`