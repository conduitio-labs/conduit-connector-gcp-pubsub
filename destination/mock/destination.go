// Code generated by MockGen. DO NOT EDIT.
// Source: destination/destination.go

// Package mock is a generated GoMock package.
package mock

import (
	context "context"
	reflect "reflect"

	conduit_connector_sdk "github.com/conduitio/conduit-connector-sdk"
	gomock "github.com/golang/mock/gomock"
)

// MockPublisher is a mock of Publisher interface.
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
func (m *MockPublisher) Publish(arg0 context.Context, arg1 conduit_connector_sdk.Record, arg2 conduit_connector_sdk.AckFunc) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Publish", arg0, arg1, arg2)
}

// Publish indicates an expected call of Publish.
func (mr *MockPublisherMockRecorder) Publish(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Publish", reflect.TypeOf((*MockPublisher)(nil).Publish), arg0, arg1, arg2)
}

// Stop mocks base method.
func (m *MockPublisher) Stop() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Stop")
	ret0, _ := ret[0].(error)
	return ret0
}

// Stop indicates an expected call of Stop.
func (mr *MockPublisherMockRecorder) Stop() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockPublisher)(nil).Stop))
}
