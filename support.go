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
	"fmt"
	"strings"
	"unicode"
)

const (
	delimStartCharacter = '{'
	delimStartString    = "{"
	delimStopString     = "}"
)

const (
	fieldNameConvertToUnderstore = iota
	fieldNameConvertToCamel
)

type fieldNameConvertMethod uint8

type FieldNameConvertStrategy interface {
	convertFieldName(name string) string
}

type UnderstoreConvertStrategy struct {
}

func (u UnderstoreConvertStrategy) convertFieldName(name string) string {
	// TODO
	return name
}

type CamelConvertStrategy struct {
}

func (u CamelConvertStrategy) convertFieldName(name string) string {
	var buffer bytes.Buffer
	needUpper := true
	for _, c := range name {
		if needUpper {
			buffer.WriteRune(unicode.ToUpper(c))
			needUpper = false
			continue
		}

		if c == '_' {
			needUpper = true
			continue
		}

		buffer.WriteRune(c)
	}

	return buffer.String()
}

func newNormalizer() QueryNormalizer {
	normalizer := &UserQueryNormalizer{}
	normalizer.strategy = &SimplePlaceholderStrategy{}
	return normalizer
}

type UserQueryNormalizer struct {
	strategy VariablePlaceholderStrategy
}

//var holdByte byte = '`'
var holdByte byte = 0x0

func (n *UserQueryNormalizer) normalize(stmt *QueryStatement) error {
	stmt.Query = strings.Trim(stmt.Query, " \r\n\t")
	stmt.columnMention = make([]ColumnBind, 0)
	if len(stmt.Query) < 3 {
		return fmt.Errorf("invalid query : %s", stmt.Query)
	}

	var hold bytes.Buffer

	queryLen := len(stmt.Query)
	for i := 0; i < queryLen; i++ {
		ch := stmt.Query[i]
		if ch != delimStartCharacter {
			hold.WriteByte(ch)
			continue
		}

		if i >= queryLen-2 {
			return fmt.Errorf("incompleted variable closer : %s", stmt.Query)
		}
		stopIndex := strings.Index(stmt.Query[i+1:], delimStopString)
		if stopIndex < 1 {
			return fmt.Errorf("incompleted variable closer : %s", stmt.Query)
		}

		v := stmt.Query[i+1 : i+1+stopIndex]
		if strings.Index(v, delimStartString) >= 0 {
			return fmt.Errorf("invalid variable declare format : %s", stmt.Query)
		}

		stmt.columnMention = append(stmt.columnMention, NewColumnBind(v, hold.Len()+1))

		i = i + stopIndex + 1
		hold.WriteByte(holdByte)
	}

	stmt.HoldedQuery = hold.String()
	stmt.Query = n.resolveHolding(stmt.HoldedQuery)
	return nil
}

func (n *UserQueryNormalizer) resolveHolding(query string) string {
	var buffer bytes.Buffer

	stgy := n.strategy.clone()
	queryLen := len(query)
	for i := 0; i < queryLen; i++ {
		ch := query[i]
		if ch != holdByte {
			buffer.WriteByte(ch)
			continue
		}

		buffer.WriteString(stgy.getNextMark())
	}

	return buffer.String()
}

type VariablePlaceholderStrategy interface {
	getNextMark() string
	clone() VariablePlaceholderStrategy
}

type SimplePlaceholderStrategy struct {
}

func (m *SimplePlaceholderStrategy) getNextMark() string {
	return "?"
}

func (m *SimplePlaceholderStrategy) clone() VariablePlaceholderStrategy {
	return &SimplePlaceholderStrategy{}
}
