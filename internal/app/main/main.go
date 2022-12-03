package main

import (
	_personHandlers "PersonService/internal/handlers/http"
	_personLogic "PersonService/internal/logic"
	_personRepository "PersonService/internal/sqlLite"
	"database/sql"
	"time"

	"github.com/labstack/echo"
	_ "github.com/mattn/go-sqlite3"
	"github.com/sirupsen/logrus"
)

const (
	timeout = 20
	adress  = "localhost:9080"
)

func main() {
	connection, err := sql.Open("sqlite3", "../../../DataBase/Person.db")
	defer connection.Close()
	if err != nil {
		logrus.Error(err)
	}
	e := echo.New()

	perRepo := _personRepository.NewSqlLiteSQLPersonRepository(connection)
	timeoutCtx := time.Duration(timeout) * time.Second
	logic := _personLogic.NewPersonLogic(perRepo, timeoutCtx)
	_personHandlers.NewPersonHandler(e.Group("/person"), logic)
	logrus.Fatal(e.Start(adress))
}
