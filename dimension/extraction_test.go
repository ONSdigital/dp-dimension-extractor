package dimension_test

import (
	"testing"

	"github.com/ONSdigital/dp-api-clients-go/dataset"
	"github.com/ONSdigital/dp-dimension-extractor/dimension"
	. "github.com/smartystreets/goconvey/convey"
)

var headerRow = []string{"Observation", "Data Marking", "Time Codelist", "Time", "League Codelist", "League"}
var CSVLine = []string{"20", "", "Year", "2016/17", "PL01", "Premier-League"}
var CSVLine2 = []string{"20", "", "Year", "2015/16", "", "Championship"}

var badCSVLine = []string{"20", "", "Year", "2016/17", "", ""}
var badCSVLine2 = []string{"20", "", "Year", "2016/17", "PL01", "Premier-League", "test-failure"}

var dimensionsData = make(map[string]string)

var extract = &dimension.Extract{
	DimensionColumnOffset: 2,
	HeaderRow:             headerRow,
	Line:                  CSVLine,
	InstanceID:            "123",
	CodelistMap:           makeCodelists(),
}

func makeCodelists() map[string]string {
	codelist := make(map[string]string)
	codelist["time"] = "1234-435435-5675"
	codelist["league"] = "dgdfg-435435-5675"
	return codelist
}

func TestUnitExtract(t *testing.T) {

	Convey("test creation of extract object/stuct", t, func() {
		newExtract := &dimension.Extract{
			DimensionColumnOffset: 2,
			HeaderRow:             headerRow,
			InstanceID:            "123",
			Line:                  CSVLine,
			CodelistMap:           makeCodelists(),
		}
		So(newExtract, ShouldResemble, extract)
	})

	Convey("test successful extraction of dimensions", t, func() {
		Convey("where all dimensions are unique", func() {
			dimensions, err := extract.Extract()
			So(err, ShouldBeNil)
			So(dimensions["Time_Year"], ShouldResemble, dataset.OptionPost{Name: "time", Option: "Year", Code: "Year", Label: "2016/17", CodeList: "1234-435435-5675"})
			So(dimensions["League_PL01"], ShouldResemble, dataset.OptionPost{Name: "league", Option: "PL01", Code: "PL01", Label: "Premier-League", CodeList: "dgdfg-435435-5675"})
		})
	})

	Convey("test unsuccessful extraction of dimensions", t, func() {
		Convey("missing dimension values", func() {
			extract.Line = badCSVLine
			_, err := extract.Extract()
			So(err, ShouldNotBeNil)
			So(err, ShouldResemble, &dimension.MissingDimensionValues{Line: badCSVLine})
		})

		Convey("invalid number of columns", func() {
			extract.Line = badCSVLine2
			_, err := extract.Extract()
			So(err, ShouldNotBeNil)
			So(err, ShouldResemble, &dimension.InvalidNumberOfColumns{Line: badCSVLine2})
		})
	})
}
