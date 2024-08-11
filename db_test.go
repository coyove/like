package like

import (
	"compress/bzip2"
	"encoding/csv"
	"encoding/xml"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
	"unsafe"
)

var db DB

func init() {
	db.OpenDefault("fts.db")
	db.Namespace = "test"
	shortDocString = true
}

func saveContent(id string, data []byte) {
	tx, _ := db.Store.Begin(true)
	bk, _ := tx.CreateBucketIfNotExists([]byte("data"))
	bk.Put([]byte(id), data)
	tx.Commit()
}

func loadContent(id string) (data string) {
	tx, _ := db.Store.Begin(false)
	bk := tx.Bucket([]byte("data"))
	data = string(bk.Get([]byte(id)))
	tx.Rollback()
	return data
}

func TestDataset(t *testing.T) {
	in, _ := os.Open("dataset/full_dataset.csv")
	defer in.Close()
	rd := csv.NewReader(in)
	start := time.Now()
	docs := []IndexDocument{}

	for i := 0; ; i++ {
		line, err := rd.Read()
		if err != nil {
			break
		}
		if i == 0 {
			continue
		}

		title := line[1]
		ingr := line[2]
		steps := line[3]
		docs = append(docs, IndexDocument{
			Score:   uint32(i),
			Content: title + " " + ingr + " " + steps,
		}.SetStringID(title+" "+ingr+" "+steps))

		if len(docs) == 1000 {
			err := db.BatchIndex(docs)
			if err != nil {
				panic(err)
			}
			docs = docs[:0]
			fmt.Println(i, time.Now())
		}
	}
	fmt.Println("indexed in", time.Since(start))

	// Index(db, "test", Document{Score: 100}.SetIntID(999), "zzzzzz\U00010FFA", 100)
	// fmt.Println(fts.Search(db, "test", "zzz", nil, 100))
	// fmt.Println(fts.Search(db, "test", "\U00010FFA", nil, 100))
	// fts.Index(db, "test", fts.Document{Score: 100}.SetIntID(999), "\U00010FFBworld", 100)
	// fmt.Println(fts.Search(db, "test", "zzz", nil, 100))
	// fmt.Println(fts.Search(db, "test", "\U00010FFA", nil, 100))
	// fmt.Println(fts.Search(db, "test", "\U00010FFB", nil, 100))
	// return

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
				id := names[len(names)-1]
				start := time.Now()
				db.Index(IndexDocument{
					Score:   uint32(time.Now().Unix()),
					Content: *(*string)(unsafe.Pointer(&data)),
				}.SetStringID(id))
				saveContent(id, data)
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

func createTemp() *DB {
	rand.Seed(time.Now().Unix())

	path := filepath.Join(os.TempDir(), "test.db")
	os.Remove(path)

	var db DB
	db.OpenDefault(path)
	db.Namespace = "test"
	return &db
}

func TestMaxDocs(t *testing.T) {
	db := createTemp()
	defer db.Store.Close()

	db.maxDocsTest = 10

	for i := 0; i < 100; i++ {
		db.Index(IndexDocument{Content: strconv.Itoa(i), Score: uint32(i)}.SetIntID(uint64(i)))
	}

	res, _ := db.Search("", nil, 20, nil)
	if len(res) != 10 {
		t.Fatal(len(res))
	}
	for i, doc := range res {
		if doc.IntID() != uint64(99-i) {
			t.Fatal(res)
		}
	}

	m := map[int]int{}
	low := 10000
	for i := 90; i < 100; i++ {
		if rand.Intn(2) == 1 {
			db.Index(IndexDocument{Content: strconv.Itoa(i), Score: uint32(i)}.SetIntID(uint64(i)))
			m[i] = i
		} else {
			s := i - rand.Intn(50)
			db.Index(IndexDocument{Rescore: true, Score: uint32(s)}.SetIntID(uint64(i)))
			m[i] = s
			if s < low {
				low = s
			}
		}
	}

	total, _, _ := db.Count()
	if total != 10 {
		t.Fatal(total)
	}

	m[100] = 100
	db.Index(IndexDocument{Content: "100", Score: 100}.SetIntID(100))
	for k, v := range m {
		if v == low {
			delete(m, k)
		}
	}

	res, _ = db.Search("", nil, 20, nil)
	if len(res) != 10 {
		t.Fatal(len(res))
	}
	for _, doc := range res {
		if doc.Score != uint32(m[int(doc.IntID())]) {
			t.Fatal(res)
		}
	}
	fmt.Println(res)
}

func TestChar0(t *testing.T) {
	db := createTemp()
	defer db.Store.Close()

	db.Index(IndexDocument{Content: "a b c d e f", Score: 1}.SetIntID(1))
	db.Index(IndexDocument{Content: "d e f g h i", Score: 2}.SetIntID(2))

	res, _ := db.Search("", nil, 10, nil)
	if len(res) != 2 || res[0].IntID() != 2 || res[1].IntID() != 1 {
		t.Fatal(res)
	}

	db.Index(IndexDocument{Content: "a b c d e g", Score: 3}.SetIntID(1))
	res, _ = db.Search("", nil, 10, nil)
	if len(res) != 2 || res[0].IntID() != 1 || res[1].IntID() != 2 {
		t.Fatal(res)
	}

	db.Index(IndexDocument{Rescore: true, Score: 4}.SetIntID(2))
	res, _ = db.Search("", nil, 10, nil)
	if len(res) != 2 || res[0].IntID() != 2 || res[0].Score != 4 {
		t.Fatal(res)
	}

	res, _ = db.Search("g -i", nil, 10, nil)
	if len(res) != 1 || res[0].IntID() != 1 {
		t.Fatal(res)
	}

	res, _ = db.Search("-i", nil, 10, nil)
	if len(res) != 1 || res[0].IntID() != 1 {
		t.Fatal(res)
	}

	db.Delete(IndexDocument{}.SetIntID(2))
	res, _ = db.Search("", nil, 10, nil)
	if len(res) != 1 || res[0].IntID() != 1 {
		t.Fatal(res)
	}
}

func TestAuto(t *testing.T) {
	db := createTemp()
	defer db.Store.Close()

	var m *Metrics
	var N = 10
	var res []Document
	var start, next []byte
	search := func(q string) {
		m = &Metrics{}
		res, next = db.Search(q, start, N, m)
	}

	db.Index(IndexDocument{Content: "abcdefg", Score: 1}.SetIntID(1))
	search("cdef")
	if len(res) == 0 || res[0].IntID() != 1 || res[0].Score != 1 {
		t.Fatal(res)
	}

	db.Index(IndexDocument{Content: "abcefg"}.SetIntID(1))
	search("cdef")
	if len(res) != 0 {
		t.Fatal(res)
	}

	db.Index(IndexDocument{Score: 100, Rescore: true}.SetIntID(1))
	search("cef")
	if len(res) == 0 || res[0].IntID() != 1 || res[0].Score != 100 {
		t.Fatal(res)
	}

	db.Index(IndexDocument{Content: "efg abc"}.SetIntID(2))
	search("efg abc")
	if len(res) != 2 {
		t.Fatal(res)
	}

	db.Index(IndexDocument{Score: 200, Rescore: true}.SetIntID(2))
	search("\"efg abc\"")
	if len(res) != 1 || res[0].IntID() != 2 || res[0].Score != 200 {
		t.Fatal(res)
	}

	hl := &Highlighter{Left: "<", Right: ">", Gap: 10}
	db.Index(IndexDocument{Content: "can't"}.SetIntID(3))
	search("can't")
	if hl := res[0].Highlight(hl); hl != "<can't>" {
		t.Fatal(hl)
	}
	search("can t")
	if hl := res[0].Highlight(hl); hl != "<can't>" {
		t.Fatal(hl)
	}

	for i, start, end := 0, 0, 20; i < end; i++ {
		text := ""
		for j := start; j < end; j++ {
			if j != i {
				text += fmt.Sprintf(" %d", j)
			}
		}
		fmt.Println(i, "=>", text)
		db.Index(IndexDocument{Content: text, Score: uint32(i + 1)}.SetIntID(uint64(i)))
	}

	N = 2
	search("5 -\"5 6 7 8\"")
	if len(res) != 2 || res[0].IntID() != 8 || res[1].IntID() != 7 {
		t.Fatal(res)
	}
	start = next
	search("5 -\"5 6 7 8\"")
	if len(res) != 1 || res[0].IntID() != 6 || len(next) != 0 {
		t.Fatal(res)
	}
	start = nil
	search("0 -19")
	if len(res) != 1 || res[0].IntID() != 19 {
		t.Fatal(res)
	}

	db.Index(IndexDocument{Content: "\u0E40\u0E14\u0E30\u0E17\u0E31\u0E01\u0E44\u0E1B\u0E1B"}.SetIntID(100))
	db.Index(IndexDocument{Content: "\u0E40\u0E14\u0E30\u0E17\u0E01\u0E44\u0E1B\u0E1B\u0E31"}.SetIntID(101))
	search("\u0E17\u0E31\u0E01")
	if len(res) != 1 || res[0].IntID() != 100 {
		t.Fatal(res)
	}

	db.Index(IndexDocument{Content: "عايزه بنت اسألها على حاجه ضروري"}.SetIntID(200))
	search("بنت")
	if len(res) != 1 || res[0].IntID() != 200 {
		t.Fatal(res)
	}
	search("å")
	fmt.Println(m)

	for i := 0; i < 120; i++ {
		r := [...]int{2, 3, 5, 7}[i%4]
		d := IndexDocument{Score: uint32(i), Content: "or"}
		d.Content += " " + strconv.Itoa(i) + " "
		for j := 0; j < 50; j++ {
			if j%r == 0 {
				d.Content += " " + strconv.Itoa(j)
			}
		}
		// d = d.SetIntID(uint64(i))
		d = d.SetStringID(strconv.Itoa(r) + " " + d.Content)
		// 0 2 4 6 8 ... 98
		// 0 3 6 9 ... 99
		// 0 5 10 15 ... 95
		// 0 7 14 21 ... 94
		// ...
		db.Index(d)
	}

	// var docs []Document
	//
	//	for start, docs = nil, docs[:0]; ; {
	//		N = rand.Intn(3) + 3
	//		search("or 10|20|30")
	//		docs = append(docs, res...)
	//		if len(next) == 0 {
	//			break
	//		}
	//		start = next
	//	}
	//
	//	if len(docs) != 90 {
	//		for _, doc := range docs {
	//			fmt.Println(doc)
	//		}
	//		t.Fatal(len(docs))
	//	}
	//
	//	for start, docs = nil, docs[:0]; ; {
	//		N = rand.Intn(3) + 3
	//		search("or 25|49 -102")
	//		docs = append(docs, res...)
	//		if len(next) == 0 {
	//			break
	//		}
	//		start = next
	//	}
	//
	//	if len(docs) != 60-1+2 {
	//		for _, doc := range docs {
	//			fmt.Println(doc)
	//		}
	//		t.Fatal(len(docs))
	//	}
}

func TestSearch(t *testing.T) {
	var search string
	// search = "康德"
	// search = "b站 曹操"
	// search = "b站"
	// search = "玩 😌"
	search = "中華職棒 一"
	// search = "箔"
	// search = " world view"
	search = "milk"
	// search = "hijklm"

	dummy := func(id string, content string) {
		db.Index(IndexDocument{
			Content: content,
		}.SetStringID(id))
		saveContent(id, []byte(content))
	}
	dummy("100000", "this is my world")
	dummy("100001", "world is my home")
	dummy("100002", "abcdefghijklmnopqrstuvwxyz")
	dummy("100003", "不像我都不能跑步")

	db.Index(IndexDocument{Score: uint32(time.Now().Unix()), Rescore: true}.SetStringID("100002"))

	fmt.Println("=======")

	var tot, hl time.Duration
	for cursor := []byte(nil); ; {
		m := &Metrics{}
		start := time.Now()
		docs, next := db.Search(search, cursor, 5, m)
		tot += time.Since(start)

		var x []string
		for _, i := range docs {
			x = append(x, loadContent(string(i.ID)))
		}

		start = time.Now()
		for i := range x {
			if x[i] == "" {
				x[i] = string(docs[i].ID)
			}
			hl := &Highlighter{Left: " <<<", Right: ">>> ", Expand: 20}
			x[i] = docs[i].Highlight(hl)
		}
		hl += time.Since(start)

		for i, line := range x {
			fmt.Println(docs[i], "==", line)
		}

		fmt.Println("=======", m)
		if len(next) == 0 {
			break
		}
		cursor = next
		break
	}
	fmt.Println(tot, hl)

}
