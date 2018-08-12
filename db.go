package mgodo

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/revel/revel"
)

// learn from leanote, put all db actions in one file
var (
	Session *mgo.Session
	DBName  string
	Dial    string
)

//Init setup mgo connection
func Init() {
	//init mgoDB
	Connect()

	//New bind type
	objID := bson.NewObjectId()
	revel.TypeBinders[reflect.TypeOf(objID)] = ObjectIDBinder
}

//MgoDBConnect do mgo connection
func Connect() {
	var err error
	var found bool

	Dial = revel.Config.StringDefault("mongodb.dial", "localhost")
	if MgoDBName, found = revel.Config.String("mongodb.name"); !found {
		urls := strings.Split(Dial, "/")
		MgoDBName = urls[len(urls)-1]
	}

	Session, err = mgo.Dial(Dial)

	if err != nil {
		panic("Cannot connect to database")
	}

	if Session == nil {
		Session, err = mgo.Dial(Dial)
		if err != nil {
			panic("Cannot connect to database")
		}
	}
}

func NewMgoSession() *mgo.Session {
	s := MgoSession.Clone()
	return s
}

//MgoControllerInit should be put in controller init function
func ControllerInit() {
	revel.InterceptMethod((*Controller).Begin, revel.BEFORE)
	revel.InterceptMethod((*Controller).End, revel.FINALLY)
}

//MgoController including the mgo session
type Controller struct {
	*revel.Controller
	Session *mgo.Session
}

//Begin do mgo connection
func (c *Controller) Begin() revel.Result {
	if Session == nil {
		DBConnect()
	}

	c.Session = Session.Clone()
	return nil
}

//End close mgo session
func (c *Controller) End() revel.Result {
	if c.Session != nil {
		c.Session.Close()
	}
	return nil
}

// ObjectIDBinder do binding
var ObjectIDBinder = revel.Binder{
	// Make a ObjectId from a request containing it in string format.
	Bind: revel.ValueBinder(func(val string, typ reflect.Type) reflect.Value {
		if len(val) == 0 {
			return reflect.Zero(typ)

		}
		if bson.IsObjectIdHex(val) {
			objID := bson.ObjectIdHex(val)
			return reflect.ValueOf(objID)

		}

		revel.ERROR.Print("ObjectIDBinder.Bind - invalid ObjectId!")
		return reflect.Zero(typ)

	}),
	// Turns ObjectId back to hexString for reverse routing
	Unbind: func(output map[string]string, name string, val interface{}) {
		var hexStr string
		hexStr = fmt.Sprintf("%s", val.(bson.ObjectId).Hex())
		// not sure if this is too carefull but i wouldn't want invalid ObjectIds in my App
		if bson.IsObjectIdHex(hexStr) {
			output[name] = hexStr

		} else {
			revel.ERROR.Print("ObjectIDBinder.Unbind - invalid ObjectId!")
			output[name] = ""

		}

	},
}
