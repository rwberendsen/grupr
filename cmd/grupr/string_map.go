package main

import (
	"encoding/csv"
	"fmt"
	"strings"

	"github.com/rwberendsen/grupr/internal/util"
)

type stringMap map[string]util.StringWasQuoted

func (m stringMap) String() string {
	return fmt.Sprintf("%v", map[string]string(m))
}

func (m stringMap) Set(value string) error {
	outerReader := csv.NewReader(strings.NewReader(value))
	outerReader.Comma = ','
	outerRecord, err := outerReader.Read()
	if err != nil {
		return err
	}

	for _, outerField := range outerRecord {
		innerReader := csv.NewReader(strings.NewReader(outerField))
		innerReader.Comma = ':'
		innerRecord, err := innerReader.Read()
		if err != nil {
			return err
		}
		if len(innerRecord) != 2 {
			return fmt.Errorf("need two fields: key and value")
		}
		k := innerRecord[0]
		if _, ok := m[k]; ok {
			return fmt.Errorf("'%s': duplicate map key", k)
		}
		v := util.StringWasQuoted{S: innerRecord[1],}
		if outerField[innerReader.FieldPos(1) - 1] == '"' {
			v.WasQuoted = true
		}
		m[k] = v
	}
	return nil
}
