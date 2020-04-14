package state

type Storage interface {
	Get(id string) (State, error)
	Update(id string, data State) error
}

type State struct {
	LastUpdate int64 `json:"last_update"`
}