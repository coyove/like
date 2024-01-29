package main

import (
	"compress/bzip2"
	"encoding/xml"
	"fmt"
	"os"
	"strings"
	"time"
	"unsafe"
	"zzz/fts"

	"github.com/coyove/bbolt"
)

func main() {
	// os.Remove("fts.db")
	db, _ := bbolt.Open("fts.db", 0644, &bbolt.Options{
		FreelistType: bbolt.FreelistMapType,
		NoSync:       true,
	})
	names := []string{}

	if true {
		// buf, _ := os.ReadFile("/home/coyove/feed_1706076921.json")
		// names = strings.Split(*(*string)(unsafe.Pointer(&buf)), "\n")
		// size := 0
		// for i, line := range names {
		// 	s := gjson.Parse(line).Get("content").Str
		// 	if s == "" {
		// 		continue
		// 	}
		// 	size += len(s)
		// 	fts.Index(db, "test", fts.Document{
		// 		Content: s,
		// 		Score:   uint32(time.Now().Unix()),
		// 	}.SetIntID(uint64(i)), 10000)
		// 	fmt.Println(i, len(names), size)
		// 	// if i%2 == 0 {
		// 	// 	fts.Delete(db, "test", fts.Document{}.SetIntID(uint64(i)))
		// 	// }
		// 	if i >= 200000 {
		// 		break
		// 	}
		// }

		// fts.Index(db, "test", fts.Document{Score: 100}.SetIntID(999), "zzzzzz\U00010FFA", 100)
		// fmt.Println(fts.Search(db, "test", "zzz", nil, 100))
		// fmt.Println(fts.Search(db, "test", "\U00010FFA", nil, 100))
		// fts.Index(db, "test", fts.Document{Score: 100}.SetIntID(999), "\U00010FFBworld", 100)
		// fmt.Println(fts.Search(db, "test", "zzz", nil, 100))
		// fmt.Println(fts.Search(db, "test", "\U00010FFA", nil, 100))
		// fmt.Println(fts.Search(db, "test", "\U00010FFB", nil, 100))
		// return

		res, _, _ := fts.Search(db, "test", "âge hello do", nil, 100)
		for _, r := range res {
			fmt.Println(r, r.Highlight("hl"))
		}

		fmt.Println(fts.Count(db, "test"))
		return
	}

	if false {
		in, _ := os.Open("zhwiki-20231201-pages-articles-multistream1.xml-p1p187712.bz2")
		rd := bzip2.NewReader(in)
		z := xml.NewDecoder(rd)
		size := 0
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
					fts.Index(db, "test", fts.Document{
						Score:   uint32(time.Now().Unix()),
						Content: data,
					}.SetStringID(names[len(names)-1]), 50000)
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

	var search string
	// search = "康德"
	// search = "b站 曹操"
	// search = "b站"
	search = "b站 玩"
	// search = "中華職棒"
	// search = "箔"
	files, _ := os.ReadDir("data")

	if false {
		fts.Index(db, "test", fts.Document{Content: " Коммунизм乐队和民防乐队还有列托夫是什么关系？ New", Score: 11}.SetIntID(0x100000000), 10000)
		res, _, _ := fts.Search(db, "test", "мун", nil, 1000)
		for _, r := range res {
			fmt.Println(r, r.Highlight("hl"))
		}
		return
	}

	if false {
		for _, f := range files {
			buf, err := os.ReadFile("data/" + f.Name())
			if err != nil {
				panic(err)
			}
			if len(buf) == 0 {
				panic(f.Name())
			}
			data := string(buf)
			if strings.Contains(data, search) {
				fmt.Println(f.Name())
			}

			names = append(names, f.Name())
			fts.Index(db, "test", fts.Document{
				Content: strings.Replace(data, "\r", "", -1),
				Score:   uint32(time.Now().Unix()),
			}.SetStringID(f.Name()), 50000)
		}
	}

	// fts.Delete(db, "test", fts.Document{}.SetStringID("这周来重庆玩3天，有推荐吗"))

	fmt.Println("=======")
	start := time.Now()

	for cursor := []byte(nil); ; {
		idx, next, _ := fts.Search(db, "test", search, cursor, 5)
		// db.View(func(tx *bbolt.Tx) error {
		// 	bk := tx.Bucket([]byte("ids"))
		for _, i := range idx {
			fmt.Println(i, i.Highlight("hl"))
		}
		// 	return nil
		// })
		fmt.Println("=======")
		if len(next) == 0 {
			break
		}
		cursor = next
	}

	fmt.Println(time.Since(start))
}
