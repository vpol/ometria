package simple

import (
	"context"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vpol/ometria/api"
	"github.com/vpol/ometria/internal/state"
	"testing"
)

type fakeStorage struct {
	obj state.State
}

func (m *fakeStorage) Get(id string) (state.State, error) {
	return m.obj, nil
}

func (m *fakeStorage) Update(id string, state state.State) error {
	m.obj = state
	return nil
}

type ometriaMock struct {
	mock.Mock
}

func (m *ometriaMock) Put(ctx context.Context, data []api.Ometria) error {
	args := m.Called(ctx, data)
	return args.Error(0)
}

type sourceMock struct {
	mock.Mock
}

func (m *sourceMock) Get(ctx context.Context, id string, currObject state.State) (data []api.Ometria, newObject state.State, err error) {
	args := m.Called(ctx, id, currObject)
	return args.Get(0).([]api.Ometria), args.Get(1).(state.State), args.Error(2)
}

func TestSimple_runJob(t *testing.T) {

	s := new(fakeStorage)

	o := new(ometriaMock)

	o.On("Put", mock.Anything, mock.AnythingOfType("[]api.Ometria")).Return(nil)

	tp := new(sourceMock)

	tp.On("Get", mock.Anything, mock.Anything, mock.Anything).Return([]api.Ometria{}, state.State{LastUpdate: 12345}, nil)

	r := Simple{
		ctx:     context.TODO(),
		cancel:  nil,
		storage: s,
		sink:    o,
		source:  tp,
	}

	require.Nil(t, r.runJob(context.Background(), "someid"))
	require.EqualValues(t, 12345, s.obj.LastUpdate)

}
