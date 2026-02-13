package store_opengauss

import (
	"fmt"
)

type DBConfig struct {
	UserName string `yaml:"user_name"`
	Password string `yaml:"password"`
	Ip       string `yaml:"ip"`
	Port     int    `yaml:"port"`
	Db       string `yaml:"db"`
	Schema   string `yaml:"Schema"`
}

func (c *DBConfig) getDBNS() string {
	return fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%d sslmode=disable TimeZone=Asia/Shanghai", c.Ip, c.UserName, c.Password, c.Db, c.Port)
}
