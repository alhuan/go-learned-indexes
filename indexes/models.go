package indexes

import "unsafe"

// Linear Regression

type LinearRegression struct {
	slope		float64
	intercept 	float64
}

func NewLinearRegression(keyValues []KeyValue, offset int64, compression_factor float64) LinearRegression {
	n := len(keyValues)
	lr := LinearRegression{}

	if (n == 0){
		lr.slope = 0
		lr.intercept = 0
		return lr
	}
	if (n == 1){
		lr.slope = 0
		lr.intercept = compression_factor * float64(offset)
		return lr
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
	return lr
}

func (lr LinearRegression) Size() int64 {
	return int64(unsafe.Sizeof(lr))
}

func (lr LinearRegression) Predict(key uint64) float64 {
	return lr.slope * float64(key) + lr.intercept;
}