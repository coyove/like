package like

import (
	"compress/bzip2"
	"encoding/csv"
	"encoding/xml"
	"fmt"
	"os"
	"testing"
	"time"
	"unsafe"

	"github.com/coyove/bbolt"
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

func TestSearch(t *testing.T) {
	var search string
	// search = "康德"
	// search = "b站 曹操"
	// search = "b站"
	// search = "玩 😌"
	search = "中華職棒 一"
	// search = "箔"
	search = " world view"
	search = "egg \"soup cans\" bread"

	dummy := func(id string, content string) {
		db.Index(IndexDocument{
			Content: content,
		}.SetStringID(id))
		saveContent(id, []byte(content))
	}
	dummy("100000", "this is my world")
	dummy("100001", "world is my home")
	dummy("100002", "abcdefghijklmnopqrstuvwxyz")

	fmt.Println("=======")

	var tot, hl time.Duration
	for cursor := []byte(nil); ; {
		m := &SearchMetrics{}
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
			x[i] = m.Highlight(x[i], " <<<", ">>> ")
		}
		hl += time.Since(start)

		for i, line := range x {
			fmt.Println(docs[i], "==", line)
		}

		fmt.Println("=======", m.Seek, m.SwitchHead, m.FastSwitchHead, m.Scan, m.Miss)
		if len(next) == 0 {
			break
		}
		cursor = next
	}
	fmt.Println(tot, hl)

}
