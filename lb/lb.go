package lb

import "hash/crc32"

type Hash func(data []byte) uint32

type Data struct {
	Key   string
	Value string
}

type Balancer struct {
	hash     Hash
	channels []chan *Data
	size     int
}

func New(size int, fn Hash, bufferSize int) *Balancer {

	if fn == nil {
		fn = crc32.ChecksumIEEE
	}

	b := &Balancer{
		size:     size,
		hash:     fn,
		channels: make([]chan *Data, size),
	}
	for i := 0; i < size; i++ {
		b.channels[i] = make(chan *Data, bufferSize)
	}
	return b
}

func (b *Balancer) Handle(data *Data) {
	hash := int(b.hash([]byte(data.Key)))
	b.channels[hash%b.size] <- data
}

func (b *Balancer) Hash(data []byte) uint32 {
	return b.hash(data)
}

func (b *Balancer) GetChannels() []chan *Data {
	return b.channels
}
