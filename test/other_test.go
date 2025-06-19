package main

import (
	"encoding/json"
	"fmt"
	"github.com/robertkrimen/otto"
	"io/ioutil"
	"os"
	"regexp"
	"testing"
)

func TestOttoVm(t *testing.T) {
	//ttest1(t)
	//ttest1(t)
	ttest3(t)
}

func ttest3(t *testing.T) {
	str1 := "ccccuser"
	str2 := "user_copy"
	val := "^user$"
	reg, err := regexp.Compile(val)
	if err != nil {
		fmt.Fprintln(os.Stdout, err)
		return
	}
	fmt.Fprintln(os.Stdout, reg.MatchString(str1))
	fmt.Fprintln(os.Stdout, reg.MatchString(str2))
}
func ttest1(t *testing.T) {

	script, _ := ioutil.ReadFile("d:/ex.js")
	vm := otto.New()
	if err := vm.Set("module", make(map[string]interface{})); err != nil {
		fmt.Fprintln(os.Stdout, "create last insert id")
		return
	}
	if _, err := vm.Run(script); err != nil {
		fmt.Fprintln(os.Stdout, "create last insert id")
	}

	var arg map[string]interface{}
	docstr := "{\"_id\":{\"$oid\":\"6167c9572ef20b33da5bc5a2\"},\"uuid\":{\"$numberLong\":\"100000294053453827\"},\"agent\":\"莆田正通知识产权代理有限公司\",\"csggqh\":\"1657\",\"csggrq\":\"2019年07月27日\",\"fw\":[\"0304,研磨剂\",\"0304,磨光制剂\",\"0304,金刚砂纸\",\"0304,金刚砂布\",\"0304,金刚砂\",\"0304,砂纸\",\"0304,研磨材料\",\"0304,研磨纸（砂纸）\",\"0304,研磨砂\",\"0304,砂纸卷\"],\"fw_code\":[\"0304\",\"\",\"0304\",\"0304\",\"0304\",\"0304\",\"0304\",\"0304\",\"0304\",\"0304\"],\"gjfl\":\"3\",\"gjzcrq\":\"\",\"hqzdrq\":\"\",\"markLogoWord\":\"N\",\"name\":\"NSTRDN ZHI CHEN\",\"pic\":\"http://wcwj.sbj.cnipa.gov.cn:8080/images/TID/201901/197/EEEDCBC79CEDC6FBAD1141BE72E2D026/03/ORI.JPG\",\"sblx\":\"一般\",\"sbzt\":\"\",\"sfgysb\":\"否\",\"sqrdz_en\":\"\",\"sqrdz_zh\":\"湖北省武汉市汉江区人智里7号6楼1号\",\"sqrmc_en\":\"\",\"sqrmc_zh\":\"潘幼良\",\"sqrq\":\"2019年01月11日\",\"updated_at\":1641097882,\"yxqrq\":\"\",\"zcggqh\":\"1669\",\"zcggrq\":\"2019年10月28日\",\"zch\":\"35895197\",\"zyqqx\":\"2019年10月28日\\r\\n          至 2029年10月27日\",\"lsq\":\"0304;\",\"sbxs\":\"\",\"sbzttb\":\"LIVE/REGISTRATION/IssuedandActive\\r,注册\",\"source_id\":0,\"tm_proccess\":[{\"rn_sn\":\"35895197\",\"name1\":\"商标注册申请\",\"name2\":\"注册证发文\",\"conclusion\":\"结束\",\"date\":\"2019年12月09日\"},{\"rn_sn\":\"35895197\",\"name1\":\"商标注册申请\",\"name2\":\"受理通知书发文\",\"conclusion\":\"结束\",\"date\":\"2019年01月27日\"},{\"rn_sn\":\"35895197\",\"name1\":\"商标注册申请\",\"name2\":\"申请收文\",\"conclusion\":\"结束\",\"date\":\"2019年01月11日\"}]}"
	err := json.Unmarshal([]byte(docstr), &arg)
	if err != nil {
		fmt.Println(err)
		return
	}
	val, err := vm.Call("module.exports", nil, arg)
	if err != nil {
		vm.SetDebuggerHandler(func(vm *otto.Otto) {
		})
		fmt.Println(err)
		return
	}
	data, err := val.Export()
	fmt.Fprintln(os.Stdout, data, err)
}
func ttest2(t *testing.T) {

	vm := otto.New()
	val, err := vm.Run(`
var v = "LIVE/REGISTRATION/IssuedandActive\r,注册";
var arr = [];
v = v.replace(/\s+/ig, "").trim();
arr1 = v.split(",");
if ( arr1.length === 2) {
	arr.push(arr1[1]);
	arr = arr.concat(arr1[0].split('/'));
}
console.log(arr);
`)

	fmt.Fprintln(os.Stdout, val, err)
}
