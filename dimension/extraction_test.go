package dimension

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

var CSVLine = []string{"20", "", "", "time", "Year", "2016/17", "UK", "Division", "Premier-League"}
var CSVLine2 = []string{"20", "", "", "time", "Year", "2015/16", "UK", "Division", "Championship"}

var badCSVLine = []string{"20", "", "", "time", "Year", "2016/17", "UK", "Division", "Premier-League", "test-failure"}

var dimensionsData = make(map[string]string)

var extract = &Extract{
	Dimensions:   dimensionsData,
	Line:         CSVLine,
	ImportAPIURL: "http://test-url.com",
	InstanceID:   "123",
}

func TestUnitExtract(t *testing.T) {
	Convey("test successful extraction of dimensions", t, func() {

		Convey("where all dimensions are unique", func() {
			dimensions, err := extract.Extract()
			So(err, ShouldBeNil)
			So(dimensions["123_time_Year_2016/17"], ShouldResemble, Request{Dimension: "123_time_Year_2016/17", DimensionValue: "2016/17", ImportAPIURL: extract.ImportAPIURL, InstanceID: extract.InstanceID})
			So(dimensions["123_UK_Division_Premier-League"], ShouldResemble, Request{Dimension: "123_UK_Division_Premier-League", DimensionValue: "Premier-League", ImportAPIURL: extract.ImportAPIURL, InstanceID: extract.InstanceID})
		})

		Convey("where some dimensions are unique", func() {
			extract.Line = CSVLine2
			extract.Dimensions["123_time_Year_2015/16"] = "2015/16"
			dimensions, err := extract.Extract()
			So(err, ShouldBeNil)
			So(dimensions["123_UK_Division_Championship"], ShouldResemble, Request{Dimension: "123_UK_Division_Championship", DimensionValue: "Championship", ImportAPIURL: extract.ImportAPIURL, InstanceID: extract.InstanceID})
		})
	})

	Convey("test unsuccessful extraction of dimensions", t, func() {
		extract.Line = badCSVLine
		_, err := extract.Extract()
		So(err, ShouldNotBeNil)
		So(err, ShouldResemble, &InvalidNumberOfColumns{badCSVLine})
	})
}
