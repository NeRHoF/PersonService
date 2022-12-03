package model

import (
	"context"
)

type Person struct {
	ID        int64  `json:"id"`
	Email     string `json:"email"`
	Phone     string `json:"phone"`
	FirstName string `json:"firstname"`
	LastName  string `json:"lastname"`
}

type PersonLogic interface {
	Get(ctx context.Context) ([]Person, error)
	GetByID(ctx context.Context, id int64) (Person, error)
	Add(context.Context, Person) (int64, error)
	Update(context.Context, Person) error
	Delete(ctx context.Context, id int64) error
}

type PersonRepository interface {
	Get(ctx context.Context) ([]Person, error)
	GetByID(ctx context.Context, id int64) (Person, error)
	Add(ctx context.Context, per Person) (int64, error)
	Update(ctx context.Context, per Person) error
	Delete(ctx context.Context, id int64) error
}
