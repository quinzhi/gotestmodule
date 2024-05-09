package gotestmodule

import (
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

var (
	dbTable        = "key_value"
	dataTimeFormat = "2006-01-02 15:04:05"
)

type cacheSqlite struct {
	db *sql.DB
}

// NewCache 创建 sqlite 数据库连接、初始化创建缓存数据表 key_value
func NewCache(storageFile string) (*cacheSqlite, error) {
	// 打开/创建 sqlite 数据库文件
	db, err := sql.Open("sqlite", storageFile)
	if err != nil {
		return nil, err
	}
	// 创建数据表 key_value
	if _, err := db.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ('id' INTEGER PRIMARY KEY AUTOINCREMENT,'key' VARCHAR(128) UNIQUE, 'value' TEXT, 'create_time' TIMESTAMP DEFAULT(DATETIME('now', 'localtime')), 'update_time' TIMESTAMP DEFAULT(DATETIME('now', 'localtime')));", dbTable)); err != nil {
		return nil, err
	}
	return &cacheSqlite{db: db}, err
}

// 返回关键词和对应数据、错误信息
func (c *cacheSqlite) Get(key string) (string, error) {
	row := c.db.QueryRow(fmt.Sprintf("SELECT value FROM %s WHERE `key`=?", dbTable), key)
	var res string
	if err := row.Scan(&res); err != nil {
		return "", err
	}
	return res, nil
}

// 删除关键词和对应数据 返回受影响数据行数、错误信息
func (c *cacheSqlite) Del(key string) (int64, error) {
	result, err := c.db.Exec(fmt.Sprintf("DELETE FROM %s WHERE `key`=?", dbTable), key)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// 设置关键词和对应数据 返回受影响数据行数、错误信息
func (c *cacheSqlite) Set(key, value string) (int64, error) {
	// sqlite 无存在则更新的语句 这里的插入、更新操作分开写
	if c.HasKey(key) {
		result, err := c.db.Exec(fmt.Sprintf("UPDATE %s SET `value`=?, `update_time`=? where `key`=?", dbTable), value, time.Now().Local().Format(dataTimeFormat), key)
		if err != nil {
			return 0, err
		}
		return result.RowsAffected()
	}
	result, err := c.db.Exec(fmt.Sprintf("INSERT INTO %s (`key`, `value`) VALUES (?, ?)", dbTable), key, value)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// 查询数据库是否有对应关键词数据
func (c *cacheSqlite) HasKey(key string) bool {
	row := c.db.QueryRow(fmt.Sprintf("SELECT count(*) FROM %s WHERE `key`=?", dbTable), key)
	var count int
	_ = row.Scan(&count)
	return count > 0
}

// 关闭数据连接
func (c *cacheSqlite) Close() error {
	return c.db.Close()
}