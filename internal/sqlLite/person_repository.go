package sqlLite

import (
	"PersonService/internal/app/model"
	"context"
	"database/sql"
	"errors"

	"github.com/sirupsen/logrus"
)

type SqliteSQLPersonRepository struct {
	connection *sql.DB
}

func NewSqlLiteSQLPersonRepository(connection *sql.DB) model.PersonRepository {
	return &SqliteSQLPersonRepository{connection}
}

func ErrorHandler(rows *sql.Rows) {
	errRow := rows.Close()
	if errRow != nil {
		logrus.Error(errRow)
	}
}

func (p *SqliteSQLPersonRepository) Get(context context.Context) (res []model.Person, err error) {
	query := `SELECT *
			  FROM Person 
			  ORDER BY id`
	rows, err := p.connection.QueryContext(context, query)

	if err != nil {
		logrus.Error(err)
		return nil, err
	}

	defer ErrorHandler(rows)

	res = make([]model.Person, 0)
	for rows.Next() {
		temp := model.Person{}
		err = rows.Scan(
			&temp.ID,
			&temp.Email,
			&temp.Phone,
			&temp.FirstName,
			&temp.LastName,
		)
		if err != nil {
			logrus.Error(err)
			return nil, err
		}
		res = append(res, temp)
	}

	return res, nil
}

func (p *SqliteSQLPersonRepository) GetByID(context context.Context, id int64) (res model.Person, err error) {
	query := `SELECT *
			  FROM Person 
			  WHERE id = ?`
	rows, err := p.connection.QueryContext(context, query, id)

	if err != nil {
		logrus.Error(err)
		return res, err
	}

	defer ErrorHandler(rows)

	if rows.Next() {
		err = rows.Scan(
			&res.ID,
			&res.Email,
			&res.Phone,
			&res.FirstName,
			&res.LastName,
		)
	} else {
		logrus.Error(errors.New("person not found"))
		return res, errors.New("person not found")
	}
	return res, nil
}

func (p *SqliteSQLPersonRepository) Add(context context.Context, person model.Person) (id int64, err error) {
	query := `INSERT INTO Person (email, phone, firstname, lastname) 
			  VALUES (?, ?, ?, ?)`

	res, err := p.connection.ExecContext(context, query, person.Email, person.Phone, person.FirstName, person.LastName)

	if err != nil {
		logrus.Error(err)
		return 0, err
	}
	err = rowsAffectedCheck(res)
	if err != nil {
		return 0, err
	}

	lastID, err := res.LastInsertId()

	if err != nil {
		logrus.Error(err)
		return 0, err
	}
	id = int64(lastID)

	return id, nil
}

func (p *SqliteSQLPersonRepository) Update(context context.Context, person model.Person) (err error) {

	query := `UPDATE Person SET email=?, phone=?, firstname=?, lastname=? WHERE id = ?`

	res, err := p.connection.ExecContext(context, query, person.Email, person.Phone, person.FirstName, person.LastName, person.ID)

	if err != nil {
		logrus.Error(err)
		return err
	}
	err = rowsAffectedCheck(res)
	if err != nil {
		return err
	}
	return nil
}
func (p *SqliteSQLPersonRepository) Delete(context context.Context, id int64) (err error) {

	query := `DELETE FROM person WHERE id = ?`
	res, err := p.connection.ExecContext(context, query, id)
	if err != nil {
		logrus.Error(err)
		return err
	}
	err = rowsAffectedCheck(res)
	if err != nil {
		return err
	}
	return nil

}

func rowsAffectedCheck(res sql.Result) (err error) {
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		logrus.Error(err)
		return err
	}
	if rowsAffected != 1 {
		logrus.Error(errors.New("more than one row affected"))
		return err
	}
	return nil
}
