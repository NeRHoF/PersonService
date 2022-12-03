package http

import (
	"PersonService/internal/app/model"
	"context"
	"net/http"
	"strconv"

	"github.com/labstack/echo"
	"github.com/sirupsen/logrus"
	"gopkg.in/go-playground/validator.v9"
)

type PersonHandler struct {
	PersonLogic model.PersonLogic
}

type ResponseError struct {
	Message string `json:"message"`
}

func NewPersonHandler(rg *echo.Group, pl model.PersonLogic) {
	handler := &PersonHandler{
		PersonLogic: pl,
	}
	rg.GET("/persons/", handler.Get)
	rg.GET("/persons/getById/", handler.GetByID)
	rg.POST("/persons/", handler.Add)
	rg.PUT("/persons/", handler.Update)
	rg.DELETE("/persons/", handler.Delete)
}

func (ph *PersonHandler) Get(c echo.Context) error {
	ctx := c.Request().Context()

	res, err := ph.PersonLogic.Get(ctx)
	if err != nil {
		logrus.Error(err)
		return c.JSON(http.StatusInternalServerError, ResponseError{Message: err.Error()})
	}

	return c.JSON(http.StatusOK, res)
}

func (ph *PersonHandler) GetByID(c echo.Context) error {
	idP, err := strconv.Atoi(c.QueryParam("id"))
	if err != nil {
		logrus.Error(err)
		return c.JSON(http.StatusNotFound, err)
	}

	id := int64(idP)
	ctx := c.Request().Context()

	res, err := ph.PersonLogic.GetByID(ctx, id)
	if err != nil {
		logrus.Error(err)
		return c.JSON(http.StatusInternalServerError, ResponseError{Message: err.Error()})
	}
	return c.JSON(http.StatusOK, res)
}

func isRequestValid(per *model.Person) (bool, error) {
	validate := validator.New()
	err := validate.Struct(per)
	if err != nil {
		logrus.Error(err)
		return false, err
	}
	return true, nil
}

func (ph *PersonHandler) Add(c echo.Context) error {
	var per model.Person
	err := c.Bind(&per)
	if err != nil {
		logrus.Error(err)
		return c.JSON(http.StatusUnprocessableEntity, err.Error())
	}

	if ok, err := isRequestValid(&per); !ok {
		logrus.Error(err)
		return c.JSON(http.StatusBadRequest, err.Error())
	}

	ctx := c.Request().Context()
	if ctx == nil {
		ctx = context.Background()
	}

	_, err = ph.PersonLogic.Add(ctx, per)
	if err != nil {
		logrus.Error(err)
		return c.JSON(http.StatusInternalServerError, ResponseError{Message: err.Error()})
	}
	return c.JSON(http.StatusCreated, per)
}

func (ph *PersonHandler) Update(c echo.Context) error {
	var per model.Person
	err := c.Bind(&per)
	if err != nil {
		logrus.Error(err)
		return c.JSON(http.StatusUnprocessableEntity, err.Error())
	}

	if ok, err := isRequestValid(&per); !ok {
		logrus.Error(err)
		return c.JSON(http.StatusBadRequest, err.Error())
	}

	ctx := c.Request().Context()
	if ctx == nil {
		ctx = context.Background()
	}

	err = ph.PersonLogic.Update(ctx, per)

	if err != nil {
		logrus.Error(err)
		return c.JSON(http.StatusInternalServerError, ResponseError{Message: err.Error()})
	}
	return c.JSON(http.StatusOK, per)
}

func (ph *PersonHandler) Delete(c echo.Context) error {
	idP, err := strconv.Atoi(c.QueryParam("id"))
	if err != nil {
		logrus.Error(err)
		return c.JSON(http.StatusNotFound, err)
	}
	id := int64(idP)
	ctx := c.Request().Context()
	if ctx == nil {
		ctx = context.Background()
	}
	err = ph.PersonLogic.Delete(ctx, id)
	if err != nil {
		logrus.Error(err)
		return c.JSON(http.StatusInternalServerError, ResponseError{Message: err.Error()})
	}

	return c.NoContent(http.StatusNoContent)
}
