package hash

type (
	Hash struct {
		record Record
	}

	Record map[string]map[string][]byte
)

func New() *Hash {
	return &Hash{make(Record)}
}

func (h *Hash) HSet(key string, field string, value []byte) int {
	if !h.exist(key) {
		h.record[key] = make(map[string][]byte)
	}

	h.record[key][field] = value
	return 0
}

func (h *Hash) HSetNx(key string, field string, value []byte) int {
	if !h.exist(key) {
		h.record[key] = make(map[string][]byte)
	}

	if _, exist := h.record[key][field]; !exist {
		h.record[key][field] = value
	}
	return 0
}

func (h *Hash) HGet(key string, field string) ([]byte, int) {
	if !h.exist(key) {
		return []byte{}, 1
	}

	if _, exist := h.record[key][field]; !exist {
		return []byte{}, 1
	}
	return h.record[key][field], 0
}

func (h *Hash) HGetAll(key string) ([][]byte, int) {
	if !h.exist(key) {
		return [][]byte{}, 1
	}

	res := [][]byte{}
	for k, v := range h.record[key] {
		res = append(res, []byte(k), v)
	}
	return res, 0
}

func (h *Hash) HDel (key, field string) int {
	if !h.exist(key) {
		return 1
	}

	if _, exist := h.record[key][field]; !exist {
		return 1
	}
	delete(h.record[key], field)
	return 0
}
func (h *Hash) HKeyExists(key string) bool {
	return h.exist(key)
}

func (h *Hash) HExists(key, field string) int {
	if !h.exist(key) {
		return 1
	}

	if _, exist := h.record[key][field]; !exist {
		return 1
	}
	return 0
}

func (h *Hash) HLen(key string) int {
	if !h.exist(key) {
		return 0
	}

	return len(h.record[key])
}

func (h *Hash) HKeys(key string) ([]string, int) {
	if !h.exist(key) {
		return []string{}, 1
	}

	res := []string{}
	for k := range h.record[key] {
		res = append(res, k)
	}
	return res, 0
}

func (h *Hash) HVals(key string) ([][]byte, int) {
	if !h.exist(key) {
		return [][]byte{}, 1
	}

	res := [][]byte{}
	for _, v := range h.record[key] {
		res = append(res, v)
	}
	return res, 0
}

func (h *Hash) HClear(key string) int {
	if !h.exist(key) {
		return 1
	}
	delete(h.record, key)
	return 0
}

func (h *Hash) exist(key string) bool {
	_, exist := h.record[key]
	return exist
}
