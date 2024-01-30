package like

import (
	"compress/bzip2"
	"encoding/xml"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/coyove/bbolt"
	"github.com/tidwall/gjson"
)

var db DB

func init() {
	db.Store, _ = bbolt.Open("fts.db", 0644, &bbolt.Options{
		FreelistType: bbolt.FreelistMapType,
		NoSync:       true,
	})
	db.Namespace = "test"
	db.MaxChars = 50000
}

func TestLocalJson(t *testing.T) {
	buf, _ := os.ReadFile("/home/coyove/feed_1706076921.json")
	names := strings.Split(*(*string)(unsafe.Pointer(&buf)), "\n")
	size := 0
	for i, line := range names {
		s := gjson.Parse(line).Get("content").Str
		if s == "" {
			continue
		}
		size += len(s)
		db.Index(Document{
			Content: s,
			Score:   uint32(time.Now().Unix()),
		}.SetIntID(uint64(i)))
		fmt.Println(i, len(names), size)
		if i >= 200000 {
			break
		}
	}

	// Index(db, "test", Document{Score: 100}.SetIntID(999), "zzzzzz\U00010FFA", 100)
	// fmt.Println(fts.Search(db, "test", "zzz", nil, 100))
	// fmt.Println(fts.Search(db, "test", "\U00010FFA", nil, 100))
	// fts.Index(db, "test", fts.Document{Score: 100}.SetIntID(999), "\U00010FFBworld", 100)
	// fmt.Println(fts.Search(db, "test", "zzz", nil, 100))
	// fmt.Println(fts.Search(db, "test", "\U00010FFA", nil, 100))
	// fmt.Println(fts.Search(db, "test", "\U00010FFB", nil, 100))
	// return

	res, _ := db.Search("âge hello do", nil, 100, &SearchMetrics{})
	for _, r := range res {
		fmt.Println(r, r.Highlight("<<<", ">>>"))
	}

	fmt.Println(db.Count())

}

func TestFillWikiContent(t *testing.T) {
	in, _ := os.Open("zhwiki-20231201-pages-articles-multistream1.xml-p1p187712.bz2")
	rd := bzip2.NewReader(in)
	z := xml.NewDecoder(rd)
	size := 0
	var names []string
	for p := false; ; {
		t, err := z.Token()
		if err != nil {
			break
		}

		switch t := t.(type) {
		case xml.StartElement:
			if t.Name.Local == "page" {
				p = true
				continue
			}
		case xml.EndElement:
			if t.Name.Local == "page" {
				p = false
			}
		}
		if !p {
			continue
		}

		switch t := t.(type) {
		case xml.StartElement:
			tt, _ := z.Token()
			data, _ := tt.(xml.CharData)
			switch t.Name.Local {
			case "text":
				data := *(*string)(unsafe.Pointer(&data))
				start := time.Now()
				db.Index(Document{
					Score:   uint32(time.Now().Unix()),
					Content: data,
				}.SetStringID(names[len(names)-1]))
				// }.SetIntID(uint64(len(names)-1)), data, 50000)
				size += len(data)
				fmt.Println(len(names), size, time.Since(start))
			case "title":
				names = append(names, string(data))
			}
		}

		if len(names) >= 1000 {
			break
		}
	}
}

func TestSearch(t *testing.T) {
	var search string
	// search = "康德"
	// search = "b站 曹操"
	// search = "b站"
	search = "玩 😌"
	search = "中華職棒 玩"
	// search = "箔"
	// search = " \" my world\""
	// search = "hijk ijkl"

	db.Index(Document{Content: "this is my world"}.SetIntID(0x100000))
	db.Index(Document{Content: "world is my home"}.SetIntID(0x100001))
	db.Index(Document{Content: "abcdefghijklmnopqrstuvwxyz"}.SetIntID(0x100002))

	fmt.Println("=======")
	start := time.Now()

	for cursor := []byte(nil); ; {
		m := &SearchMetrics{}
		docs, next := db.Search(search, cursor, 5, m)
		for _, i := range docs {
			fmt.Println(i, i.Highlight(" <<<", ">>> "))
		}
		fmt.Println("=======", m.Seek, m.Scan, m.Miss)
		if len(next) == 0 {
			break
		}
		cursor = next
	}

	fmt.Println(time.Since(start))
}
