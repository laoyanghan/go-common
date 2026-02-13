package store_opengauss

import (
	"context"
	"fmt"
	"testing"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func (c *DBConfig) getDBNS() string {
	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%d sslmode=disable TimeZone=Asia/Shanghai",
		c.Ip, c.UserName, c.Password, c.Db, c.Port)
	if c.Schema != "" {
		dsn += fmt.Sprintf(" search_path=%s", c.Schema)
	}
	return dsn
}

type TestUser struct {
	ID   int `gorm:"primaryKey"`
	Name string
	Age  int
}

func getTestDB(t *testing.T) *gorm.DB {
	cfg := &DBConfig{
		UserName: "dsms",
		Password: "idcisp@DSMS0321",
		Ip:       "10.4.124.13",
		Port:     15400,
		Db:       "test_db",
		Schema:   "", // 指定schema
	}
	dsn := cfg.getDBNS()
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to connect database: %v", err)
	}
	return db.WithContext(context.Background())
}

func TestOpenGaussCURD(t *testing.T) {
	db := getTestDB(t)
	if db == nil {
		t.Fatal("db is nil")
	}
	// 创建表
	err := db.Migrator().DropTable(&TestUser{})
	if err != nil {
		t.Fatalf("failed to drop table: %v", err)
	}
	err = db.AutoMigrate(&TestUser{})
	if err != nil {
		t.Fatalf("failed to migrate: %v", err)
	}
	// Create
	u := TestUser{Name: "Alice", Age: 20}
	if err := db.Create(&u).Error; err != nil {
		t.Fatalf("create failed: %v", err)
	}
	// Read
	var u2 TestUser
	if err := db.First(&u2, u.ID).Error; err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if u2.Name != u.Name || u2.Age != u.Age {
		t.Fatalf("read data mismatch: got %+v, want %+v", u2, u)
	}
	// Update
	if err := db.Model(&u2).Update("Age", 21).Error; err != nil {
		t.Fatalf("update failed: %v", err)
	}
	var u3 TestUser
	if err := db.First(&u3, u.ID).Error; err != nil {
		t.Fatalf("read after update failed: %v", err)
	}
	if u3.Age != 21 {
		t.Fatalf("update not applied: got %d, want 21", u3.Age)
	}
	// Delete
	if err := db.Delete(&TestUser{}, u.ID).Error; err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	var count int64
	db.Model(&TestUser{}).Where("id = ?", u.ID).Count(&count)
	if count != 0 {
		t.Fatalf("delete not applied, count=%d", count)
	}
}
