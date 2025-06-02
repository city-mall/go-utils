package utils

func Paginate(len, skip, size int) (int, int) {
	if skip == 0 && size == 0 {
		return 0, len // to send all data when nothing is specified
	}
	if skip > len {
		skip = len
	}

	end := skip + size
	if end > len {
		end = len
	}
	return skip, end
}
