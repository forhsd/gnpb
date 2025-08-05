package gnpb

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/bytedance/sonic"
	"github.com/cespare/xxhash/v2"
)

/*begin *RunStatus*/
func (s *RunStatus) Scan(value any) error {

	if value == nil {
		return nil
	}

	b, ok := value.(int64)
	if !ok {
		return fmt.Errorf("value is not []byte, value: %v", value)
	}

	*s = RunStatus(b)
	return nil
}

func (s *RunStatus) Value() (driver.Value, error) {

	if s == nil {
		return nil, nil
	}

	return int(*s), nil
}

/*end *RunStatus*/

/*begin *RelationElement*/

func (*RelationElement) GormDataType() string {
	return "jsonb"
}

func (x *RelationElement) Value() (driver.Value, error) {

	// Convert the map to JSON
	jsonData, err := sonic.Marshal(x)
	if err != nil {
		return nil, err
	}

	return string(jsonData), nil
}

func (x *RelationElement) Scan(value any) error {

	if value == nil {
		return nil
	}

	// Check the type of the value
	var bytes []byte
	switch v := value.(type) {
	case []byte:
		bytes = v
	case string:
		bytes = []byte(v)
	default:
		return errors.New("type assertion to []byte or string failed")
	}

	if err := sonic.Unmarshal(bytes, x); err != nil {
		return err
	}

	return nil
}

/*end *RelationElement*/

/*begin *DBType*/
func (s *DBType) Scan(value any) error {

	if value == nil {
		return nil
	}

	b, ok := value.(int64)
	if !ok {
		return fmt.Errorf("value is not int64, value: %v", value)
	}

	*s = DBType(b)
	return nil
}

func (s *DBType) Value() (driver.Value, error) {

	if s == nil {
		return nil, nil
	}

	return int(*s), nil
}

/*end *DBType*/

func (s *UnionIdentifier) UnionHash() (string, error) {
	var errs []error
	if s.GetHost() == "" {
		errs = append(errs, errors.New("host不能为空"))
	}
	if s.GetPort() < 1 {
		errs = append(errs, errors.New("port不能为空"))
	}
	if s.GetDbType() < 1 {
		errs = append(errs, errors.New("dbType不能为空"))
	}
	if s.GetDbName() == "" {
		errs = append(errs, errors.New("dbName不能为空"))
	}
	if s.GetSchema() == "" {
		errs = append(errs, errors.New("schema不能为空"))
	}
	if s.GetTable() == "" {
		errs = append(errs, errors.New("table不能为空"))
	}
	return HashString(
		s.GetHost(),
		s.GetPort(),
		s.GetDbType(),
		s.GetDbName(),
		s.GetSchema(),
		s.GetTable(),
	), errors.Join(errs...)
}

func Hash(item ...any) uint64 {

	var arr []string
	for _, val := range item {
		arr = append(arr, fmt.Sprintf("%[1]T %+[1]v", val))
	}

	key := strings.Join(arr, ".")
	return xxhash.Sum64([]byte(key))
}

func HashString(item ...any) string {
	return strconv.FormatUint(Hash(item...), 10)
}
