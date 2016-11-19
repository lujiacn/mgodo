package mgofun

import (
	"reflect"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

//MgoFun wrap all common functions
type MgoFun struct {
	model      interface{}
	session    *mgo.Session
	collection *mgo.Collection
	Query      bson.M
	Sort       string
	Skip       int
	Limit      int
}

//NewMgoFun initiate with input model and mgo session
func NewMgoFun(s *mgo.Session, dbName string, model interface{}) *MgoFun {
	mgoFun := &MgoFun{model: model, session: s}
	collection := collection(s, dbName, model)
	mgoFun.collection = collection
	return mgoFun
}

// Collection conduct mgo.Collection
func collection(s *mgo.Session, dbName string, m interface{}) *mgo.Collection {
	var c string
	switch m.(type) {
	case string:
		c = m.(string)
	default:
		typ := reflect.TypeOf(m)
		if typ.Kind() == reflect.Ptr {
			typ = typ.Elem()
		}
		c = typ.Name()
	}
	return s.DB(dbName).C(c)
}

//General Save method
func (m *MgoFun) Save() error {
	id := reflect.ValueOf(m.model).Elem().FieldByName("Id")
	x := reflect.ValueOf(m.model).Elem().FieldByName("UpdatedAt")
	x.Set(reflect.ValueOf(time.Now()))
	_, err := m.collection.Upsert(bson.M{"_id": id.Interface()}, bson.M{"$set": m.model})
	if err != nil {
		return err
	}
	return nil
}

// Remove is softe delete
func (m *MgoFun) Remove() error {
	id := reflect.ValueOf(m.model).Elem().FieldByName("Id")
	x := reflect.ValueOf(m.model).Elem().FieldByName("IsRemoved")
	x.Set(reflect.ValueOf(true))
	y := reflect.ValueOf(m.model).Elem().FieldByName("RemovedAt")
	y.Set(reflect.ValueOf(time.Now()))
	_, err := m.collection.Upsert(bson.M{"_id": id.Interface()}, bson.M{"$set": m.model})
	if err != nil {
		return err
	}
	return nil
}

//findQ conduct mgo.Query
func (m *MgoFun) findQ() *mgo.Query {
	var query *mgo.Query
	//do not query removed value
	if m.Query != nil {
		m.Query["is_removed"] = bson.M{"$ne": true}
	} else {
		m.Query = bson.M{"is_removed": bson.M{"$ne": true}}
	}

	query = m.collection.Find(m.Query)
	//sort
	if m.Sort != "" {
		query = query.Sort(m.Sort)
	} else {
		query = query.Sort("-created_at", "-updated_at")
	}
	//skip
	if m.Skip != 0 {
		query = query.Skip(m.Skip)
	}
	//limit
	if m.Limit != 0 {
		query = query.Limit(m.Limit)
	}
	return query
}

func (m *MgoFun) findByIdQ() *mgo.Query {
	var query *mgo.Query
	id := reflect.ValueOf(m.model).Elem().FieldByName("Id").Interface()
	query = m.collection.Find(bson.M{"_id": id})
	return query
}

//Count
func (m *MgoFun) Count() int64 {
	query := m.findQ()
	count, _ := query.Count()
	return int64(count)
}

//---------retrieve functions
// FindAll except removed, i is interface address
func (m *MgoFun) FindAll(i interface{}) {
	query := m.findQ()
	query.All(i)
}

//Get will retrieve by _id
func (m *MgoFun) Get() {
	query := m.findByIdQ()
	query.One(m.model)
}

//GetByQ get first one based on query, model will be updated
func (m *MgoFun) GetByQ() {
	query := m.findQ()
	query.One(m.model)
}

//Select query and select columns
func (m *MgoFun) FindWithSelect(cols []string, i interface{}) {
	sCols := bson.M{}
	for _, v := range cols {
		sCols[v] = 1
	}
	query := m.findQ().Select(sCols)
	query.All(i)
}
