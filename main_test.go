package main

import "testing"

func TestSetKeyValueInMap(t *testing.T) {
	v := Handlers["SET"]([]Value{
		{bulk: "key"},
		{bulk: "value"},
	})
	if v.typ != "string" || v.str != "OK" {
		t.Errorf("didn't recieve expected response: typ=string && str=OK")
	}
}

func TestSetKeyValueAndReturnOldValue(t *testing.T) {
	v := Handlers["SET"]([]Value{
		{bulk: "key"},
		{bulk: "value"},
	})
	if v.typ != "string" || v.str != "OK" {
		t.Errorf("didn't recieve expected response: typ=string && str=OK")
	}
	v = Handlers["SET"]([]Value{
		{bulk: "key"},
		{bulk: "value2"},
		{bulk: "GET"},
	})
	if v.typ != "bulk" || v.bulk != "value" {
		t.Errorf("result type is wrong or last value wasn't returned.")
	}
}

func TestSetKeyValueIfDoesntExist(t *testing.T) {
	v := Handlers["SET"]([]Value{
		{bulk: "key"},
		{bulk: "value"},
		{bulk: "NX"},
	})
	if v.typ != "string" || v.str != "OK" {
		t.Errorf("didn't recieve expected response: typ=string && str=OK")
	}
	v = Handlers["SET"]([]Value{
		{bulk: "key"},
		{bulk: "value2"},
		{bulk: "NX"},
	})
	if v.typ != "null" {
		t.Errorf("key was set when NX option was present and key already existed")
	}
}

func TestSetKeyValueIfExists(t *testing.T) {
	v := Handlers["SET"]([]Value{
		{bulk: "key"},
		{bulk: "value"},
		{bulk: "XX"},
	})
	if v.typ != "null" {
		t.Errorf("key was set with XX option when it didn't exist already")
	}
	v = Handlers["SET"]([]Value{
		{bulk: "key"},
		{bulk: "value"},
	})

	v = Handlers["SET"]([]Value{
		{bulk: "key"},
		{bulk: "value2"},
		{bulk: "XX"},
	})
	if v.typ != "string" && v.str != "OK" {
		t.Errorf("key was set when NX option was present and key already existed")
	}
}
