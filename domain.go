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
	"fmt"
	"strings"
)

const (
	eleTypeUnknown = iota
	eleTypeText
	eleTypeIf
)

type declareElementType uint8

func (d declareElementType) String() string {
	switch d {
	case eleTypeText:
		return "TEXT"
	case eleTypeIf:
		return "IF"
	}
	return "UNKNOWN"
}

func (d declareElementType) IsText() bool {
	if d == eleTypeText {
		return true
	}
	return false
}

func buildElementType(stmt string) declareElementType {
	switch strings.ToLower(stmt) {
	case "text":
		return eleTypeText
	case "if":
		return eleTypeIf
	}
	return eleTypeUnknown
}

type QueryStatement struct {
	Id            string `xml:"id,attr"`
	Query         string `xml:",cdata"`
	columnMention []ColumnBind
	HoldedQuery   string
}

func (q QueryStatement) String() string {
	return fmt.Sprintf("id=[%s], queryLen=%d, columnLen=%d", q.Id, len(q.Query), len(q.columnMention))
}

type ColumnBind struct {
	name     string
	holdPos  int
	bindType columnBindType
}

func (c ColumnBind) String() string {
	return fmt.Sprintf("name=%s,holdPos=%d,bindType=%s", c.name, c.holdPos, c.bindType)
}

const (
	columnBindTypeNormal = iota
	columnBindTypeArray
)

type columnBindType uint8

func (c columnBindType) String() string {
	switch c {
	case columnBindTypeNormal:
		return "NORMAL"
	case columnBindTypeArray:
		return "ARRAY"
	}
	return "UNKNOWN"
}

func NewColumnBind(name string, pos int) ColumnBind {
	b := ColumnBind{}
	b.name = name
	b.holdPos = pos
	b.bindType = columnBindTypeNormal
	return b
}

func NewColumnBindArray(name string, pos int) ColumnBind {
	b := ColumnBind{}
	b.name = name
	b.holdPos = pos
	b.bindType = columnBindTypeArray
	return b
}

func (c ColumnBind) Name() string {
	return c.name
}
