package array16

func AddSat(a, b uint16) uint16 {
	if a < 65535-b {
		return a + b
	}
	return 65535
}

func SubSat(a, b uint16) uint16 {
	if a >= b {
		return a - b
	}
	return 0
}
