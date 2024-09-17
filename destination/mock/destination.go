// Code generated by MockGen. DO NOT EDIT.
// Source: destination.go
//
// Generated by this command:
//
//	mockgen -typed -source=destination.go -destination=mock/destination.go -package=mock -mock_names=publisher=MockPublisher . publisher
//

// Package mock is a generated GoMock package.
package mock

import (
	context "context"
	reflect "reflect"

	opencdc "github.com/conduitio/conduit-commons/opencdc"
	gomock "go.uber.org/mock/gomock"
)

// MockPublisher is a mock of publisher interface.
type MockPublisher struct {
	ctrl     *gomock.Controller
	recorder *MockPublisherMockRecorder
}

// MockPublisherMockRecorder is the mock recorder for MockPublisher.
type MockPublisherMockRecorder struct {
	mock *MockPublisher
}

// NewMockPublisher creates a new mock instance.
func NewMockPublisher(ctrl *gomock.Controller) *MockPublisher {
	mock := &MockPublisher{ctrl: ctrl}
	mock.recorder = &MockPublisherMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockPublisher) EXPECT() *MockPublisherMockRecorder {
	return m.recorder
}

// Publish mocks base method.
func (m *MockPublisher) Publish(arg0 context.Context, arg1 opencdc.Record) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Publish", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// Publish indicates an expected call of Publish.
func (mr *MockPublisherMockRecorder) Publish(arg0, arg1 any) *MockPublisherPublishCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Publish", reflect.TypeOf((*MockPublisher)(nil).Publish), arg0, arg1)
	return &MockPublisherPublishCall{Call: call}
}

// MockPublisherPublishCall wrap *gomock.Call
type MockPublisherPublishCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockPublisherPublishCall) Return(arg0 error) *MockPublisherPublishCall {
	c.Call = c.Call.Return(arg0)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockPublisherPublishCall) Do(f func(context.Context, opencdc.Record) error) *MockPublisherPublishCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockPublisherPublishCall) DoAndReturn(f func(context.Context, opencdc.Record) error) *MockPublisherPublishCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// Stop mocks base method.
func (m *MockPublisher) Stop() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Stop")
	ret0, _ := ret[0].(error)
	return ret0
}

// Stop indicates an expected call of Stop.
func (mr *MockPublisherMockRecorder) Stop() *MockPublisherStopCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockPublisher)(nil).Stop))
	return &MockPublisherStopCall{Call: call}
}

// MockPublisherStopCall wrap *gomock.Call
type MockPublisherStopCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockPublisherStopCall) Return(arg0 error) *MockPublisherStopCall {
	c.Call = c.Call.Return(arg0)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockPublisherStopCall) Do(f func() error) *MockPublisherStopCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockPublisherStopCall) DoAndReturn(f func() error) *MockPublisherStopCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}
