package mysql

import (
	"github.com/semanggilab/webcore-go/app/config"
	"github.com/semanggilab/webcore-go/app/loader"
	sql "github.com/webcore-go/lib-sql"
	"gorm.io/driver/mysql"
)

type MysqlLoader struct {
	name string
}

func (a *MysqlLoader) SetName(name string) {
	a.name = name
}

func (a *MysqlLoader) Name() string {
	return a.name
}

func (l *MysqlLoader) Init(args ...any) (loader.Library, error) {
	config := args[1].(config.DatabaseConfig)
	dsn := sql.BuildDSN(config)

	db := &sql.SQLDatabase{}
	db.SetDialect(mysql.Open(dsn))
	err := db.Install(args...)
	if err != nil {
		return nil, err
	}

	db.Connect()

	// l.DB = db
	return db, nil
}
