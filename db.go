package mgodo

import (
	"fmt"
	"reflect"
	"strings"
	"time"

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

//App Init setup mgo connection
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
	if DBName, found = revel.Config.String("mongodb.name"); !found {
		urls := strings.Split(Dial, "/")
		DBName = urls[len(urls)-1]
	}
	if Session == nil {
		Session, err = mgo.Dial(Dial)
		if err != nil {
			revel.AppLog.Errorf("Could not connect to Mongo DB. Error: %s", err)
			for i := 0; i <= 3; i++ {
				revel.AppLog.Infof("Retry connect to database ...")
				time.Sleep(3 * time.Second)
				Session, err = mgo.Dial(Dial)
				if err == nil {
					break
				} else {
					revel.AppLog.Errorf("Could not connect to Mongo DB. Error: %s", err)
				}
			}
		}
	}
}

func NewMgoSession() *mgo.Session {
	s := Session.Clone()
	return s
}

//MgoControllerInit should be put in controller init function
func MgoControllerInit() {
	revel.InterceptMethod((*MgoController).Begin, revel.BEFORE)
	revel.InterceptMethod((*MgoController).End, revel.FINALLY)
}

//MgoController including the mgo session
type MgoController struct {
	*revel.Controller
	MgoSession *mgo.Session
}

//Begin do mgo connection
func (c *MgoController) Begin() revel.Result {
	if Session == nil {
		Connect()
	}

	c.MgoSession = Session.Clone()
	return nil
}

//End close mgo session
func (c *MgoController) End() revel.Result {
	if c.MgoSession != nil {
		c.MgoSession.Close()
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

		revel.AppLog.Errorf("ObjectIDBinder.Bind - invalid ObjectId!")
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
			revel.AppLog.Errorf("ObjectIDBinder.Bind - invalid ObjectId!")
			output[name] = ""

		}

	},
}
