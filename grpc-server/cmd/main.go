package main

import (
	"context"
	"fmt"
	"grpc/invoicer"
	"net"

	"google.golang.org/grpc"
)

type myInvoicerServer struct {
	invoicer.UnimplementedInvoicerServer
}

func (s *myInvoicerServer) Create(context.Context, *invoicer.CreateRequest) (*invoicer.CreateResponse, error) {
	return &invoicer.CreateResponse{
		Pdf:  []byte("pdf"),
		Docx: []byte("docx"),
	}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Printf("cannot create listener: %s\n", err.Error())
	}
	serviceRegistrar := grpc.NewServer()
	service := &myInvoicerServer{}
	// Register invoice service with empty grpc server
	invoicer.RegisterInvoicerServer(serviceRegistrar, service)
	err = serviceRegistrar.Serve(lis)
	if err != nil {
		fmt.Printf("cannot start server: %s\n", err.Error())
	}
}
