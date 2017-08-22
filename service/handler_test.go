package service

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestUnitReplaceURL(t *testing.T) {
	Convey("test successful conversion of https url", t, func() {
		url := convertURL("https://census/edition4")
		So(url, ShouldNotBeNil)
		So(url, ShouldEqual, "s3://census/edition4")
	})

	Convey("test s3 url does not get converted", t, func() {
		url := convertURL("s3://census/edition5")
		So(url, ShouldNotBeNil)
		So(url, ShouldEqual, "s3://census/edition5")
	})

	Convey("test successful conversion of http url", t, func() {
		url := convertURL("http://census/edition4")
		So(url, ShouldNotBeNil)
		So(url, ShouldEqual, "s3://census/edition4")
	})

	Convey("test successful conversion of HTTP url", t, func() {
		url := convertURL("HTTP://census/edition4")
		So(url, ShouldNotBeNil)
		So(url, ShouldEqual, "s3://census/edition4")
	})
}
