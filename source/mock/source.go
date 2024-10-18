// Code generated by MockGen. DO NOT EDIT.
// Source: source.go
//
// Generated by this command:
//
//	mockgen -typed -source=source.go -destination=mock/source.go -package=mock -mock_names=subscriber=MockSubscriber . subscriber
//

// Package mock is a generated GoMock package.
package mock

import (
	context "context"
	reflect "reflect"

	opencdc "github.com/conduitio/conduit-commons/opencdc"
	gomock "go.uber.org/mock/gomock"
)

// MockSubscriber is a mock of subscriber interface.
type MockSubscriber struct {
	ctrl     *gomock.Controller
	recorder *MockSubscriberMockRecorder
	isgomock struct{}
}

// MockSubscriberMockRecorder is the mock recorder for MockSubscriber.
type MockSubscriberMockRecorder struct {
	mock *MockSubscriber
}

// NewMockSubscriber creates a new mock instance.
func NewMockSubscriber(ctrl *gomock.Controller) *MockSubscriber {
	mock := &MockSubscriber{ctrl: ctrl}
	mock.recorder = &MockSubscriberMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockSubscriber) EXPECT() *MockSubscriberMockRecorder {
	return m.recorder
}

// Ack mocks base method.
func (m *MockSubscriber) Ack(arg0 context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Ack", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Ack indicates an expected call of Ack.
func (mr *MockSubscriberMockRecorder) Ack(arg0 any) *MockSubscriberAckCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Ack", reflect.TypeOf((*MockSubscriber)(nil).Ack), arg0)
	return &MockSubscriberAckCall{Call: call}
}

// MockSubscriberAckCall wrap *gomock.Call
type MockSubscriberAckCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockSubscriberAckCall) Return(arg0 error) *MockSubscriberAckCall {
	c.Call = c.Call.Return(arg0)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockSubscriberAckCall) Do(f func(context.Context) error) *MockSubscriberAckCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockSubscriberAckCall) DoAndReturn(f func(context.Context) error) *MockSubscriberAckCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// Next mocks base method.
func (m *MockSubscriber) Next(ctx context.Context) (opencdc.Record, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Next", ctx)
	ret0, _ := ret[0].(opencdc.Record)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Next indicates an expected call of Next.
func (mr *MockSubscriberMockRecorder) Next(ctx any) *MockSubscriberNextCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Next", reflect.TypeOf((*MockSubscriber)(nil).Next), ctx)
	return &MockSubscriberNextCall{Call: call}
}

// MockSubscriberNextCall wrap *gomock.Call
type MockSubscriberNextCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockSubscriberNextCall) Return(arg0 opencdc.Record, arg1 error) *MockSubscriberNextCall {
	c.Call = c.Call.Return(arg0, arg1)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockSubscriberNextCall) Do(f func(context.Context) (opencdc.Record, error)) *MockSubscriberNextCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockSubscriberNextCall) DoAndReturn(f func(context.Context) (opencdc.Record, error)) *MockSubscriberNextCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// Stop mocks base method.
func (m *MockSubscriber) Stop() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Stop")
	ret0, _ := ret[0].(error)
	return ret0
}

// Stop indicates an expected call of Stop.
func (mr *MockSubscriberMockRecorder) Stop() *MockSubscriberStopCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockSubscriber)(nil).Stop))
	return &MockSubscriberStopCall{Call: call}
}

// MockSubscriberStopCall wrap *gomock.Call
type MockSubscriberStopCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockSubscriberStopCall) Return(arg0 error) *MockSubscriberStopCall {
	c.Call = c.Call.Return(arg0)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockSubscriberStopCall) Do(f func() error) *MockSubscriberStopCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockSubscriberStopCall) DoAndReturn(f func() error) *MockSubscriberStopCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}
