module github.com/coyove/like

go 1.20

require (
	github.com/cespare/xxhash v1.1.0
	github.com/coyove/bbolt v1.3.9-0.20240227033235-c2dac416ece3
	github.com/pierrec/lz4/v4 v4.1.21
	golang.org/x/text v0.14.0
)

require golang.org/x/sys v0.5.0 // indirect

// replace github.com/coyove/bbolt v1.3.9-0.20240125143137-5711267b6f67 => ../bbolt
