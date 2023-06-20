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
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"path/filepath"
	"runtime"
	"strings"
)

type Logger interface {
	Printf(string, ...interface{})
}

type defaultLogger struct{}

func (defaultLogger) Printf(format string, a ...interface{}) {
	// do nothing...
	log.Printf(format, a...)
}

func NewStringmanPreference(filepath string) StringmanPreference {
	pref := StringmanPreference{}
	pref.queryFilePath = filepath
	pref.Fileset = "string*.xml"
	pref.fieldNameConvert = fieldNameConvertToCamel
	pref.Debug = false
	pref.DebugLogger = defaultLogger{}

	return pref
}

type StringmanPreference struct {
	queryFilePath    string
	Fileset          string
	fieldNameConvert fieldNameConvertMethod
	Debug            bool
	DebugLogger      Logger
}

func NewStringman(pref StringmanPreference) (*StringMan, error) {
	manager := &StringMan{}
	manager.preference = pref
	manager.statementMap = make(map[string]QueryStatement)

	manager.fieldNameConverter = newFieldNameConverter(pref.fieldNameConvert)

	err := loadXmlFile(manager, pref.queryFilePath, pref.Fileset)
	if err != nil {
		return nil, fmt.Errorf("fail to load xml file : %s [path=%s,fileset=%s]", err.Error(), pref.queryFilePath, pref.Fileset)
	}

	runtime.SetFinalizer(manager, closeStringman)

	return manager, nil
}

func newFieldNameConverter(fieldNameConvert fieldNameConvertMethod) FieldNameConvertStrategy {
	switch fieldNameConvert {
	case fieldNameConvertToUnderstore:
		return UnderstoreConvertStrategy{}
	}

	return CamelConvertStrategy{}
}

func closeStringman(manager *StringMan) {
	manager.Close()
}

func loadXmlFile(manager *StringMan, filePath string, fileSet string) error {
	var buffer bytes.Buffer
	buffer.WriteString(filePath)
	buffer.WriteRune(filepath.Separator)
	buffer.WriteString(fileSet)
	matches, err := filepath.Glob(buffer.String())
	if err != nil {
		return fmt.Errorf("fail to search xml file : %s [glob=%s]", err.Error(), buffer.String())
	}

	if manager.preference.Debug {
		manager.preference.DebugLogger.Printf("matches len=%d", len(matches))
	}

	for _, file := range matches {
		if !strings.HasSuffix(file, "xml") {
			continue
		}

		data, err := ioutil.ReadFile(file)
		if err != nil {
			return fmt.Errorf("fail to read file[%s] : %s", file, err.Error())
		}

		err = loadWithSax(manager, data)
		if err != nil {
			return err
		}
	}

	return nil
}

var (
	currentStmt    QueryStatement
	currentEleType declareElementType
	currentId      string
	stmtList       []QueryStatement
)

func loadWithSax(manager *StringMan, data []byte) error {
	stmtList = make([]QueryStatement, 0)
	buf := bytes.NewBuffer(data)
	dec := xml.NewDecoder(buf)

	for {
		t, tokenErr := dec.Token()
		if tokenErr != nil {
			if tokenErr == io.EOF {
				break
			}
			return tokenErr
		}

		switch t := t.(type) {
		case xml.StartElement:
			currentId = getAttr(t.Attr, attrId)
			currentEleType = buildElementType(t.Name.Local)
			if currentEleType.IsText() {
				currentStmt = newQueryStatement()
				traverseIf(dec)
			}
		case xml.CharData:
			if len(currentId) == 0 {
				break
			}
			currentStmt.Query = currentStmt.Query + string(t)
		case xml.EndElement:
			if currentEleType.IsText() {
				currentStmt.Query = strings.Trim(currentStmt.Query, cutset)
				currentId = ""
			}
		}
	}

	for _, v := range stmtList {
		err := manager.registStatement(v)
		if err != nil {
			return err
		}
	}

	return nil
}

func traverseIf(dec *xml.Decoder) {
	//var innerElement declareElementType
	//var innerSql = ""
	//var innerKey = ""
	//var innerExist = "true"

	for {
		t, tokenErr := dec.Token()
		if tokenErr != nil {
			if tokenErr == io.EOF {
				break
			}
			panic(tokenErr)
		}

		switch t := t.(type) {
		case xml.StartElement:
			//innerKey = getAttr(t.Attr, attrKey)
			//innerExist = getAttr(t.Attr, attrExist)
		case xml.CharData:
			currentStmt.Query = currentStmt.Query + string(t)
		case xml.EndElement:
			if currentEleType.IsText() {
				currentStmt.Query = strings.Trim(currentStmt.Query, cutset)
				stmtList = append(stmtList, currentStmt)
				return
			}
			currentId = ""
		}
	}
}

func getAttr(attr []xml.Attr, name string) string {
	for _, v := range attr {
		if v.Name.Local == name {
			return v.Value
		}
	}
	return ""
}

func newQueryStatement() QueryStatement {
	stmt := QueryStatement{}
	stmt.Id = currentId
	stmt.columnMention = make([]ColumnBind, 0)
	return stmt
}

const (
	attrId    = "id"
	attrKey   = "key"
	attrExist = "exist"
	cutset    = "\r\t\n "
)
