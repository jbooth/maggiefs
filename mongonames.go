package maggiefs

import (
  "labix.org/v2/mgo"
  "labix.org/v2/mgo/bson"
)

// implements nameservice
type MongoNames struct {
  mongo mgo.Session
  colPrefix string // collection prefix to identify collections belonging to this FS
}

func NewMongoNames(connString string, colPrefix string) (names NameService, err error) {
  return nil,nil
}
