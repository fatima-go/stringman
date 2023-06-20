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
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	xmlFilePrefix = "string."
)

var xmlFile string
var stringManager *StringMan

var xmlSample = []byte(`
<?xml version="1.0" encoding="UTF-8" ?>
<query>
    <text id="DropCityTable">
        drop table if exists city
    </text>
    <text id="CreateCityTable">
create table city (
    id  bigint NOT NULL AUTO_INCREMENT,
    name varchar(64) default null,
    age  int  default 0,
    is_man  bool default true,
    percentage float default 0.0,
    create_time datetime default CURRENT_TIMESTAMP,
    update_time datetime,
    primary key (id)
)
    </text>
	<text id="DropAlbumTable">
        drop table if exists album
    </text>
	<text id="SelectAlbumCount">
		SELECT COUNT(*) FROM album
	</text>
    <text id="CreateAlbumTable">
	create table album (
    	id  int,
    	score int,
    	primary key (id)
	)
    </text>
	<text id="InsertAlbum">
		INSERT INTO album  ( id, score ) VALUES ({Id},{Score})
	</text>
	<text id="UpsertAlbum">
		INSERT INTO album  ( id, score
        )
        VALUES
        (
            {Id},
            {Score}
        )
        ON DUPLICATE KEY
        UPDATE
            score = score + VALUES(score)
	</text>
	<text id="UpdateAlbum">
		UPDATE album SET score={Score} WHERE id={Id}
	</text>
    <text id="InsertCity">
        INSERT INTO CITY(NAME,AGE,IS_MAN,PERCENTAGE,CREATE_TIME,UPDATE_TIME) VALUES({Name},{Age},{IsMan},{Percentage},{CreateTime},{UpdateTime})
    </text>
    <text id="UpdateCityWithName">
        UPDATE CITY SET AGE={Age} WHERE NAME={Name}
    </text>
    <text id="SelectCityWithName">
        SELECT * FROM CITY WHERE NAME like {Name}
    </text>
	<text id="SelectCityWithInClause">
<![CDATA[
        SELECT * FROM CITY WHERE Age > {Age} AND Age < {Age} AND NAME IN ({Names})
]]>
    </text>
    <text id="CountCity">
        SELECT Count(*) FROM CITY
    </text>
<text id="SelectSampleMembers">
        SELECT member_no, join_chnl_type, member_type, member_svc_mang_no
        FROM flo_svc.tb_member
        WHERE member_type={MemberType}
        LIMIT 10
    </text>
</query>
`)

func TestMain(m *testing.M) {
	var err error
	xmlFile, err = prepareXmlFile()
	if err != nil {
		fmt.Printf("fail to prepare sample xml file : %s", err.Error())
		return
	}

	code := m.Run()
	os.Remove(xmlFile)
	os.Exit(code)
}

func prepareXmlFile() (string, error) {
	tempDir := os.TempDir()
	clearPreviousXmlFiles(tempDir, "*.xml")

	file, _ := ioutil.TempFile(tempDir, xmlFilePrefix)
	xmlFile := file.Name() + ".xml"
	os.Rename(file.Name(), xmlFile)

	err := ioutil.WriteFile(xmlFile, []byte(xmlSample), 0644)
	if err != nil {
		return xmlFile, err
	}

	return xmlFile, nil
}

func clearPreviousXmlFiles(path string, fileset string) {
	var buffer bytes.Buffer
	buffer.WriteString(path)
	buffer.WriteRune(filepath.Separator)
	buffer.WriteString(fileset)
	matches, err := filepath.Glob(buffer.String())
	if err != nil {
		return
	}

	for _, v := range matches {
		os.Remove(v)
	}
}

func TestBuildStringman(t *testing.T) {
	path := filepath.Dir(xmlFile)
	pref := NewStringmanPreference(path)
	pref.Fileset = xmlFilePrefix + "*.xml"
	pref.Debug = true

	man, err := NewStringman(pref)
	if err != nil {
		t.Errorf("fail to create stringman : %s\n", err.Error())
		return
	}

	stringManager = man
}

func TestBasic(t *testing.T) {
	err := updateAlbum()
	if err != nil {
		t.Errorf("fail updateAlbum : %s\n", err.Error())
		return
	}
}

// UPDATE album SET score={Score} WHERE id={Id}
func updateAlbum() error {
	expect := "UPDATE album SET score='Hello' WHERE id=1234"

	p := BuildParam{}
	p["Score"] = "Hello"
	p["Id"] = 1234
	built, err := stringManager.Build(p)
	if err != nil {
		return err
	}

	if built != expect {
		return fmt.Errorf("want=[%s], result=[%s]", expect, built)
	}

	return nil
}

// INSERT INTO CITY(NAME,AGE,IS_MAN,PERCENTAGE,CREATE_TIME,UPDATE_TIME) VALUES({Name},{Age},{IsMan},{Percentage},{CreateTime},{UpdateTime})
func TestVariableParams(t *testing.T) {
	p := make(map[string]interface{})
	p["Name"] = "Hello"
	p["Age"] = 1234
	p["IsMan"] = false
	p["Percentage"] = 16.72
	p["CreateTime"] = time.Now()
	p["UpdateTime"] = time.Now()
	built, err := stringManager.BuildWithStmt("insertCity", p)
	if err != nil {
		t.Errorf("error : %s", err.Error())
		return
	}

	log.Printf("built=[%s]\n", built)
}

func TestInvalidBuildParam(t *testing.T) {
	p := make(map[string]interface{})
	p["Unknown"] = 32
	p["Names"] = "name test will use array.. (TODO)"
	_, err := stringManager.BuildWithStmt("selectCityWithInClause", p)
	if err == nil {
		t.Errorf("want err but nil")
		return
	}

	if !strings.HasPrefix(err.Error(), "not found param") {
		t.Errorf("error is not wanted : %s", err.Error())
		return
	}
}

func TestMultipleBind(t *testing.T) {
	expect := "SELECT * FROM CITY WHERE Age > 32 AND Age < 32 AND NAME IN ('hello')"

	p := make(map[string]interface{})
	p["Age"] = 32
	p["Names"] = "hello"
	built, err := stringManager.BuildWithStmt("selectCityWithInClause", p)
	if err != nil {
		t.Errorf("fail to build : %s", err.Error())
		return
	}

	if !strings.HasPrefix(built, expect) {
		t.Errorf("want=[%s], result=[%s]", expect, built)
		return
	}
}

func TestSample(t *testing.T) {
	//expect := "SELECT * FROM CITY WHERE Age > 32 AND Age < 32 AND NAME IN ('hello')"

	selectSampleMembers()
}

func selectSampleMembers() error {
	p := make(map[string]interface{})
	p["MemberType"] = "TID"
	built, err := stringManager.Build(p)
	if err != nil {
		return err
	}

	log.Printf("built : %s\n", built)
	//if built != expect {
	//	return fmt.Errorf("want=[%s], result=[%s]", expect, built)
	//}

	return nil
}
