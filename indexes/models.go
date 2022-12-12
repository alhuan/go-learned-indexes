package indexes

import "unsafe"
import "math"

type Model interface {
	Train(keyValues []KeyValue, offset int64, compression_factor float64)
	Predict(key uint64) float64
	Size() int64
}

// Linear Regression

type LinearRegression struct {
	slope		float64
	intercept 	float64
}

func (lr LinearRegression) Train(keyValues []KeyValue, offset int64, compression_factor float64) {
	n := len(keyValues)

	if (n == 0){
		lr.slope = 0
		lr.intercept = 0
		return
	}
	if (n == 1){
		lr.slope = 0
		lr.intercept = compression_factor * float64(offset)
		return
	}

	var mean_x, mean_y, c, m2 float64

	mean_x = 0.0
	mean_y = 0.0
	c = 0.0
	m2 = 0.0

	for i, kv := range keyValues {
		y := float64(offset) + float64(i)

		dx := float64(kv.Key) - mean_x
		mean_x += dx / float64(i + 1)
		mean_y += (y - mean_y) / float64(i + 1)
		c += dx * (y - mean_y)

		dx2 := float64(kv.Key) - mean_x
		m2 += dx * dx2
	}

	covar := c / float64(n - 1)
	variance := m2 / float64(n - 1)

	if variance == 0 {
		lr.slope = 0
		lr.intercept = mean_y
	} else {
		lr.slope = covar / variance * compression_factor
		lr.intercept = mean_y * compression_factor - lr.slope * mean_x
	}
	return
}

func (lr LinearRegression) Size() int64 {
	return int64(unsafe.Sizeof(lr))
}

func (lr LinearRegression) Predict(key uint64) float64 {
	return lr.slope * float64(key) + lr.intercept;
}

// Linear Spline

type LinearSpline struct {
	slope		float64
	intercept	float64
}

func (ls LinearSpline) Train(keyValues []KeyValue, offset int64, compression_factor float64) {
	n := len(keyValues)

	if (n == 0){
		ls.slope = 0
		ls.intercept = 0
		return
	}
	if (n == 1){
		ls.slope = 0
		ls.intercept = compression_factor * float64(offset)
		return
	}

	var numerator float64
	var denominator float64

	numerator = float64(n)
	denominator = float64(keyValues[n - 1].Key - keyValues[0].Key)

	if denominator == 0 {
		ls.slope = 0
	} else {
		ls.slope = numerator / denominator * compression_factor
	}

	ls.intercept = float64(offset) * compression_factor - ls.slope * float64(keyValues[0].Key)

	return
}

func (ls LinearSpline) Size() int64 {
	return int64(unsafe.Sizeof(ls))
}

func (ls LinearSpline) Predict(key uint64) float64 {
	return ls.slope * float64(key) + ls.intercept;
}

// CubicSpline

type CubicSpline struct {
	a	float64 // cubic coeff
	b 	float64 // quadratic coeff 
	c 	float64 // linear coeff
	d 	float64 // intercept
}

func (cs CubicSpline) Train(keyValues []KeyValue, offset int64, compression_factor float64) {
	n := len(keyValues)

	if (n == 0){
		cs.a = 0
		cs.b = 0
		cs.c = 1
		cs.d = 0
		return
	}
	if (n == 1){
		cs.a = 0
		cs.b = 0
		cs.c = 0
		cs.d = compression_factor * float64(offset)
		return
	}

	var xmin, ymin, xmax, ymax float64
	var x1, y1, x2, y2 float64
	var sxn, syn float64 

	xmin = float64(keyValues[0].Key)
	ymin = compression_factor * float64(offset)
	xmax = float64(keyValues[n - 1].Key)
	ymax = compression_factor * float64(offset + int64(n) - 1)

	x1 = 0
	y1 = 0
	x2 = 1
	y2 = 1

	sxn = 0
	syn = 0

	for i, kv := range keyValues {
		var x, y float64
		x = float64(kv.Key)
		y = (float64(offset) + float64(i)) * compression_factor

		sxn = (x - xmin) / (xmax - xmin)
		if sxn > 0 {
			syn = (y - ymin) / (ymax - ymin)
			break
		}
	}

	var m1, m2 float64
	var sxp, syp float64

	m1 = (syn - y1) / (sxn - x1)
	sxp = 0
	syp = 0

	for i, kv := range keyValues {
		var x, y float64
		x = float64(kv.Key)
		y = (float64(offset) + float64(i)) * compression_factor

		sxp = (x - xmin) / (xmax - xmin)
		if sxp > 0 {
			syp = (y - ymin) / (ymax - ymin)
			break
		}
	}

	m2 = (y2 - syp) / (x2 - sxp)

	if sq := m1 * m1 + m2 * m2; sq > 9 {
		var tau float64
		tau = 3 / math.Sqrt(sq)
		m1 *= tau
		m2 *= tau
	}

	var cube float64
	cube = math.Pow(xmax - xmin, 3)

	cs.a = (m1 + m2 - 2) / cube
	cs.b = -(xmax * (2 * m1 * m2 - 3) + xmin * (m1 + 2 * m2 - 3)) / cube
	cs.c = (m1 * xmax * xmax + m2 * xmin * xmin + xmax * xmin + (2 * m1 + 2 * m2 - 6)) / cube
	cs.d = -xmin * (m1 * xmax * xmax + xmax * xmin * (m2 - 3) + xmin * xmin) / cube

	cs.a *= ymax - ymin
	cs.b *= ymax - ymin
	cs.c *= ymax - ymin
	cs.d *= ymax - ymin
	cs.d += ymin

	return
}

func (cs CubicSpline) Size() int64 {
	return int64(unsafe.Sizeof(cs))
}

func (cs CubicSpline) Predict(key uint64) float64 {
	var tmp, v1, v2, v3 float64
	tmp = float64(key)
	v1 = cs.a * tmp + cs.b
	v2 = v1 * tmp + cs.c
	v3 = v2 * tmp + cs.d
	return v3
}