/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * @project stringman
 * @author jin.freestyle@gmail.com
 */

package stringman

import (
	"bytes"
	"container/list"
	"database/sql"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"time"
)

var queryNormalizer QueryNormalizer

type QueryNormalizer interface {
	normalize(stmt *QueryStatement) error
	resolveHolding(query string) string
}

type StringMan struct {
	preference         StringmanPreference
	statementMap       map[string]QueryStatement
	fieldNameConverter FieldNameConvertStrategy
}

func (s StringMan) String() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("path=[%s]", s.preference.queryFilePath))
	buffer.WriteString(fmt.Sprintf(",fileSet=[%s]", s.preference.Fileset))
	buffer.WriteString(",keys=[")
	for k, _ := range s.statementMap {
		buffer.WriteString(",")
		buffer.WriteString(k)
	}
	buffer.WriteString("]")

	return buffer.String()
}

func (man *StringMan) registStatement(queryStatement QueryStatement) error {
	if man.preference.Debug {
		man.preference.DebugLogger.Printf("registStatement stmt : %s", queryStatement)
	}
	queryStatement, err := man.buildStatement(queryStatement)
	if err != nil {
		return err
	}

	if man.preference.Debug {
		man.preference.DebugLogger.Printf("registStatement stmt (after build) : %s", queryStatement)
	}
	id := strings.ToUpper(queryStatement.Id)
	if _, exists := man.statementMap[id]; exists {
		return fmt.Errorf("duplicated user statement id : [%s]", id)
	}

	man.statementMap[id] = queryStatement
	if man.preference.Debug {
		man.preference.DebugLogger.Printf("map regist : %s", id)
	}

	return nil
}

func (man *StringMan) buildStatement(queryStatement QueryStatement) (QueryStatement, error) {
	if queryNormalizer == nil {
		queryNormalizer = newNormalizer()
		if queryNormalizer == nil {
			return queryStatement, fmt.Errorf("not found normalizer")
		}
	}

	err := queryNormalizer.normalize(&queryStatement)
	if err != nil {
		return queryStatement, err
	}

	return queryStatement, nil
}

func (man *StringMan) find(id string) (QueryStatement, error) {
	stmt, ok := man.statementMap[strings.ToUpper(id)]
	if !ok {
		return stmt, fmt.Errorf("not found text statement for id : %s", id)
	}

	return stmt, nil
}

type BuildParam map[string]interface{}

func (man *StringMan) Build(param BuildParam) (string, error) {
	pc, _, _, _ := runtime.Caller(1)
	funcName := findFunctionName(pc)
	return man.BuildWithStmt(funcName, param)
}

func (man *StringMan) BuildWithStmt(stmtIdOrUserQuery string, param BuildParam) (string, error) {
	stmt, err := man.find(stmtIdOrUserQuery)
	if err != nil {
		return "", err
	}

	if param == nil || len(param) == 0 {
		if len(stmt.columnMention) != 0 {
			return stmt.Query, fmt.Errorf("need parameter for completing text")
		}
		return stmt.Query, nil
	}

	return completeText(stmt, param)
}

func (man *StringMan) Close() error {
	return nil
}

func findFunctionName(pc uintptr) string {
	var funcName = runtime.FuncForPC(pc).Name()
	var found = strings.LastIndexByte(funcName, '.')
	if found < 0 {
		return funcName
	}
	return funcName[found+1:]
}

func completeText(stmt QueryStatement, param BuildParam) (string, error) {
	queue := list.New()

	for _, c := range stmt.columnMention {
		v, ok := param[c.name]
		if !ok {
			return stmt.Query, fmt.Errorf("not found param %s", c.name)
		}
		queue.PushBack(v)
	}

	var buffer bytes.Buffer

	holdedQuery := []byte(stmt.HoldedQuery)
	for _, b := range holdedQuery {
		if b != holdByte {
			buffer.WriteByte(b)
			continue
		}
		e := queue.Front()
		str, err := asString(e.Value)
		if err != nil {
			return "", err
		}
		buffer.WriteString(str)
		queue.Remove(e)
	}

	return buffer.String(), nil
}

const sqlyyyyMMddHHmmss = "2006-01-02 15:04:05"

func asString(v interface{}) (string, error) {
	switch s := v.(type) {
	case string:
		return fmt.Sprintf("'%s'", s), nil
	case []byte:
		return fmt.Sprintf("'%s'", string(s)), nil
	case time.Time:
		return fmt.Sprintf("'%s'", s.Format(sqlyyyyMMddHHmmss)), nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", s), nil
	case float32, float64:
		return fmt.Sprintf("%f", s), nil
	case sql.NullString:
		if !s.Valid {
			return "null", nil
		} else {
			return fmt.Sprintf("'%s'", s.String), nil
		}
	case sql.NullInt64:
		if !s.Valid {
			return "null", nil
		} else {
			return fmt.Sprintf("'%d'", s.Int64), nil
		}
	case sql.NullBool:
		if !s.Valid {
			return "null", nil
		} else {
			if s.Bool {
				return "'true'", nil
			}
			return "'false'", nil
		}
	case sql.NullFloat64:
		if !s.Valid {
			return "null", nil
		} else {
			return fmt.Sprintf("'%f'", s.Float64), nil
		}
	case bool:
		if s {
			return "true", nil
		} else {
			return "false", nil
		}
	default:
		var r = reflect.TypeOf(s)
		return "", fmt.Errorf("unsupported type %v", r)
	}
}
