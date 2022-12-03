package logic

import (
	"PersonService/internal/app/model"
	"context"
	"errors"
	"time"

	"github.com/sirupsen/logrus"
)

type personLogic struct {
	personRepo     model.PersonRepository
	contextTimeout time.Duration
}

func NewPersonLogic(p model.PersonRepository, timeout time.Duration) model.PersonLogic {
	return &personLogic{
		personRepo:     p,
		contextTimeout: timeout,
	}
}

func (p *personLogic) Get(c context.Context) (res []model.Person, err error) {

	ctx, cancel := context.WithTimeout(c, p.contextTimeout)
	defer cancel()

	res, err = p.personRepo.Get(ctx)

	if err != nil {
		return nil, err
	}
	return res, nil
}

func (p *personLogic) GetByID(c context.Context, id int64) (res model.Person, err error) {

	ctx, cancel := context.WithTimeout(c, p.contextTimeout)
	defer cancel()

	res, err = p.personRepo.GetByID(ctx, id)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (p *personLogic) Update(c context.Context, person model.Person) (err error) {

	ctx, cancel := context.WithTimeout(c, p.contextTimeout)
	defer cancel()
	return p.personRepo.Update(ctx, person)
}

func (p *personLogic) Add(c context.Context, person model.Person) (id int64, err error) {

	ctx, cancel := context.WithTimeout(c, p.contextTimeout)
	defer cancel()
	return p.personRepo.Add(ctx, person)
}

func (p *personLogic) Delete(c context.Context, id int64) (err error) {
	ctx, cancel := context.WithTimeout(c, p.contextTimeout)
	defer cancel()
	existPerson, err := p.personRepo.GetByID(ctx, id)
	if err != nil {
		return err
	}
	if existPerson.ID == 0 {
		logrus.Error("person does not exist")
		return errors.New("person does not exist")
	}
	return p.personRepo.Delete(ctx, id)
}
