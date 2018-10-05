// Code generated by protoc-gen-go. DO NOT EDIT.
// source: execute.proto

package pipeline

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

import (
	context "golang.org/x/net/context"
	grpc "google.golang.org/grpc"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type PipelineExecuteRequest struct {
	PipelineDescription  *PipelineDescription `protobuf:"bytes,1,opt,name=pipelineDescription,proto3" json:"pipelineDescription,omitempty"`
	Inputs               []*Value             `protobuf:"bytes,2,rep,name=inputs,proto3" json:"inputs,omitempty"`
	XXX_NoUnkeyedLiteral struct{}             `json:"-"`
	XXX_unrecognized     []byte               `json:"-"`
	XXX_sizecache        int32                `json:"-"`
}

func (m *PipelineExecuteRequest) Reset()         { *m = PipelineExecuteRequest{} }
func (m *PipelineExecuteRequest) String() string { return proto.CompactTextString(m) }
func (*PipelineExecuteRequest) ProtoMessage()    {}
func (*PipelineExecuteRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_execute_4748e52c7921ce40, []int{0}
}
func (m *PipelineExecuteRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_PipelineExecuteRequest.Unmarshal(m, b)
}
func (m *PipelineExecuteRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_PipelineExecuteRequest.Marshal(b, m, deterministic)
}
func (dst *PipelineExecuteRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PipelineExecuteRequest.Merge(dst, src)
}
func (m *PipelineExecuteRequest) XXX_Size() int {
	return xxx_messageInfo_PipelineExecuteRequest.Size(m)
}
func (m *PipelineExecuteRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_PipelineExecuteRequest.DiscardUnknown(m)
}

var xxx_messageInfo_PipelineExecuteRequest proto.InternalMessageInfo

func (m *PipelineExecuteRequest) GetPipelineDescription() *PipelineDescription {
	if m != nil {
		return m.PipelineDescription
	}
	return nil
}

func (m *PipelineExecuteRequest) GetInputs() []*Value {
	if m != nil {
		return m.Inputs
	}
	return nil
}

type PipelineExecuteResponse struct {
	ResultURI            string   `protobuf:"bytes,1,opt,name=resultURI,proto3" json:"resultURI,omitempty"`
	Error                string   `protobuf:"bytes,2,opt,name=error,proto3" json:"error,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *PipelineExecuteResponse) Reset()         { *m = PipelineExecuteResponse{} }
func (m *PipelineExecuteResponse) String() string { return proto.CompactTextString(m) }
func (*PipelineExecuteResponse) ProtoMessage()    {}
func (*PipelineExecuteResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_execute_4748e52c7921ce40, []int{1}
}
func (m *PipelineExecuteResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_PipelineExecuteResponse.Unmarshal(m, b)
}
func (m *PipelineExecuteResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_PipelineExecuteResponse.Marshal(b, m, deterministic)
}
func (dst *PipelineExecuteResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PipelineExecuteResponse.Merge(dst, src)
}
func (m *PipelineExecuteResponse) XXX_Size() int {
	return xxx_messageInfo_PipelineExecuteResponse.Size(m)
}
func (m *PipelineExecuteResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_PipelineExecuteResponse.DiscardUnknown(m)
}

var xxx_messageInfo_PipelineExecuteResponse proto.InternalMessageInfo

func (m *PipelineExecuteResponse) GetResultURI() string {
	if m != nil {
		return m.ResultURI
	}
	return ""
}

func (m *PipelineExecuteResponse) GetError() string {
	if m != nil {
		return m.Error
	}
	return ""
}

func init() {
	proto.RegisterType((*PipelineExecuteRequest)(nil), "PipelineExecuteRequest")
	proto.RegisterType((*PipelineExecuteResponse)(nil), "PipelineExecuteResponse")
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// ExecutorClient is the client API for Executor service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type ExecutorClient interface {
	ExecutePipeline(ctx context.Context, in *PipelineExecuteRequest, opts ...grpc.CallOption) (*PipelineExecuteResponse, error)
}

type executorClient struct {
	cc *grpc.ClientConn
}

func NewExecutorClient(cc *grpc.ClientConn) ExecutorClient {
	return &executorClient{cc}
}

func (c *executorClient) ExecutePipeline(ctx context.Context, in *PipelineExecuteRequest, opts ...grpc.CallOption) (*PipelineExecuteResponse, error) {
	out := new(PipelineExecuteResponse)
	err := c.cc.Invoke(ctx, "/Executor/ExecutePipeline", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ExecutorServer is the server API for Executor service.
type ExecutorServer interface {
	ExecutePipeline(context.Context, *PipelineExecuteRequest) (*PipelineExecuteResponse, error)
}

func RegisterExecutorServer(s *grpc.Server, srv ExecutorServer) {
	s.RegisterService(&_Executor_serviceDesc, srv)
}

func _Executor_ExecutePipeline_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PipelineExecuteRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ExecutorServer).ExecutePipeline(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/Executor/ExecutePipeline",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExecutorServer).ExecutePipeline(ctx, req.(*PipelineExecuteRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _Executor_serviceDesc = grpc.ServiceDesc{
	ServiceName: "Executor",
	HandlerType: (*ExecutorServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "ExecutePipeline",
			Handler:    _Executor_ExecutePipeline_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "execute.proto",
}

func init() { proto.RegisterFile("execute.proto", fileDescriptor_execute_4748e52c7921ce40) }

var fileDescriptor_execute_4748e52c7921ce40 = []byte{
	// 213 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0xe2, 0x4d, 0xad, 0x48, 0x4d,
	0x2e, 0x2d, 0x49, 0xd5, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x97, 0xe2, 0x2b, 0xc8, 0x2c, 0x48, 0xcd,
	0xc9, 0xcc, 0x83, 0xf1, 0xb9, 0xcb, 0x12, 0x73, 0x4a, 0xa1, 0x1c, 0xa5, 0x06, 0x46, 0x2e, 0xb1,
	0x00, 0xa8, 0xbc, 0x2b, 0x44, 0x5b, 0x50, 0x6a, 0x61, 0x69, 0x6a, 0x71, 0x89, 0x90, 0x1b, 0x97,
	0x30, 0x4c, 0xa7, 0x4b, 0x6a, 0x71, 0x72, 0x51, 0x66, 0x41, 0x49, 0x66, 0x7e, 0x9e, 0x04, 0xa3,
	0x02, 0xa3, 0x06, 0xb7, 0x91, 0x88, 0x5e, 0x00, 0xa6, 0x5c, 0x10, 0x36, 0x0d, 0x42, 0x72, 0x5c,
	0x6c, 0x99, 0x79, 0x05, 0xa5, 0x25, 0xc5, 0x12, 0x4c, 0x0a, 0xcc, 0x1a, 0xdc, 0x46, 0x6c, 0x7a,
	0x61, 0x20, 0x07, 0x04, 0x41, 0x45, 0x95, 0x7c, 0xb9, 0xc4, 0x31, 0x5c, 0x50, 0x5c, 0x90, 0x9f,
	0x57, 0x9c, 0x2a, 0x24, 0xc3, 0xc5, 0x59, 0x94, 0x5a, 0x5c, 0x9a, 0x53, 0x12, 0x1a, 0xe4, 0x09,
	0xb6, 0x98, 0x33, 0x08, 0x21, 0x20, 0x24, 0xc2, 0xc5, 0x9a, 0x5a, 0x54, 0x94, 0x5f, 0x24, 0xc1,
	0x04, 0x96, 0x81, 0x70, 0x8c, 0x82, 0xb8, 0x38, 0x20, 0xc6, 0xe4, 0x17, 0x09, 0xb9, 0x71, 0xf1,
	0x43, 0x8d, 0x84, 0xd9, 0x20, 0x24, 0xae, 0x87, 0xdd, 0xbb, 0x52, 0x12, 0x7a, 0x38, 0x5c, 0xa1,
	0xc4, 0x90, 0xc4, 0x06, 0x0e, 0x2c, 0x63, 0x40, 0x00, 0x00, 0x00, 0xff, 0xff, 0x79, 0x7c, 0x13,
	0x0f, 0x5a, 0x01, 0x00, 0x00,
}
