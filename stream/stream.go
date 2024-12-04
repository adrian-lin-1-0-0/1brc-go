package stream

type Stream struct {
	count int
	mean  float64
	max   float64
	min   float64
}

func New() *Stream {
	return &Stream{}
}

func (s *Stream) Add(value float64) {
	s.count++
	s.mean = s.mean + (value-s.mean)/float64(s.count)
	if s.count == 1 {
		s.min = value
		s.max = value
	} else {
		if value < s.min {
			s.min = value
		}
		if value > s.max {
			s.max = value
		}
	}
}

func (s *Stream) Count() int {
	return s.count
}

func (s *Stream) Mean() float64 {
	return s.mean
}

func (s *Stream) Max() float64 {
	return s.max
}

func (s *Stream) Min() float64 {
	return s.min
}
