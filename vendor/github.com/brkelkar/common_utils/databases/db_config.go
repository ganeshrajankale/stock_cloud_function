package databases

import (
	"fmt"

	"github.com/brkelkar/common_utils/logger"
	"gorm.io/gorm"
)

//DB used as cursor for database connection
var DB map[string]*gorm.DB

// DBConfig represents db configuration
type DBConfig struct {
	Host     string
	Port     int
	User     string
	DBName   string
	Password string
}

// BuildDBMsSQLConfig Create required config format
func BuildDBMsSQLConfig(host string, port int, user string, dbName string, password string) *DBConfig {
	dbConfig := DBConfig{
		Host:     host,
		Port:     port,
		User:     user,
		DBName:   dbName,
		Password: password,
	}
	logger.Debug(
		fmt.Sprintf("Connecting to Host_name: %s, at port %v, user_name %s, database name  %s",
			dbConfig.Host, dbConfig.Port, dbConfig.User, dbConfig.DBName))
	return &dbConfig
}

// DbMsSQLURL Create database connetion url
func DbMsSQLURL(dbConfig *DBConfig) string {
	return fmt.Sprintf(
		"sqlserver://%s:%s@%s:%d?database=%s",
		dbConfig.User,
		dbConfig.Password,
		dbConfig.Host,
		dbConfig.Port,
		dbConfig.DBName,
	)
}

// DbMySQLURL Create database connetion url
func DbMySQLURL(dbConfig *DBConfig) string {
	return fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?charset=utf8&parseTime=True&loc=Local",
		dbConfig.User,
		dbConfig.Password,
		dbConfig.Host,
		dbConfig.Port,
		dbConfig.DBName,
	)
}
